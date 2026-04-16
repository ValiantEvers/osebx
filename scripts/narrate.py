#!/usr/bin/env python3
"""
OSEBX Market Brief — Narration Layer
=====================================
Reads brief.json (produced by analyze.py) and produces insights.json:
the human-readable briefing the frontend renders.

This script does NO analysis. Its only job is to pass the pre-computed
brief to an LLM with a tight prompt, validate the response against a
strict Pydantic schema, and write the result. If the LLM returns
malformed output, the script exits non-zero and the workflow keeps
the previous insights.json — the dashboard never shows broken prose.

Intended to run weekly via GitHub Actions immediately after analyze.py.
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import anthropic  # pip install anthropic
from pydantic import BaseModel, Field, ValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MODEL = "claude-sonnet-4-6"  # cheap, fast, more than good enough for narration
MAX_TOKENS = 1200
API_KEY_ENV = "ANTHROPIC_API_KEY"

# ---------------------------------------------------------------------------
# Pydantic schema — strict, matches what the frontend expects
# ---------------------------------------------------------------------------

class NarratedItem(BaseModel):
    """A drivers/risks/opportunities item with narration added.

    The `ticker` and `evidence` fields must be copied verbatim from
    brief.json — the LLM cannot invent or alter numbers. Only `claim`
    is its own prose.
    """
    ticker: str
    claim: str = Field(max_length=120)  # hard ≤15 words
    evidence: str = Field(max_length=80)


class WatchlistNarration(BaseModel):
    ticker: str
    claim: str = Field(max_length=120)
    missed_on: str


class Insights(BaseModel):
    as_of: str
    regime: str
    headline: str = Field(max_length=90)  # ≤12 words
    market_take: str = Field(max_length=450)  # 2–3 sentences
    drivers: list[NarratedItem]
    risks: list[NarratedItem]
    opportunities: list[NarratedItem]
    watchlist: list[WatchlistNarration]

    # Pass-through fields — frontend renders these directly
    breadth: float
    benchmarks: dict
    thresholds_used: dict


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are the narration layer for the evers.no/osebx weekly brief.

You receive a pre-computed analytical brief (brief.json) and write human-
readable prose around it. You do NOT analyse. You do NOT add or remove
items. You do NOT change any number. Your job is to write the headline,
market_take paragraph, and one claim sentence per item.

STYLE
- Tone: senior sell-side analyst writing an internal note. Dry, confident,
  numbers-led.
- Vocabulary to use: clustered, compressed, divergent, thin breadth,
  leadership, extended, narrow, rotated into/out of, stretched, crowded.
- Vocabulary to AVOID: significant, dynamic, vibrant, robust, navigate,
  landscape, unprecedented, notable, it is important to note, headwinds,
  tailwinds, in light of.
- No forecasts. No mention of Norges Bank, Fed, elections, or news events.
- Language: English.
- Tickers stay in $TICKER form exactly as given.

HARD RULES
1. Copy every `ticker` from the brief verbatim into the output.
2. Every `evidence` string must reference ONE metric that exists in the
   brief's `metrics` dict for that item, formatted as
   "<metricName> <value>" (e.g. "sharpeRatio 4.13").
3. Each `claim` is ≤15 words. Each `headline` is ≤12 words.
4. `market_take` is 2–3 sentences. Lead with the benchmark spread
   (OBX minus OSEBX) if non-trivial. Name the regime. If breadth is
   outside 50%±20%, mention it explicitly.
5. Never claim a recommendation. "Leadership," "stretched," "cheap relative
   to its own history" are fine. "Buy," "sell," "avoid" are not.
6. For watchlist items: the claim must mention the `missed_on` gate
   (e.g., "near-buy held back by extended valuation").
7. If the brief has thin_data: true, add "data coverage thin this week"
   to market_take.

OUTPUT
Return ONLY valid JSON matching the schema below. No preamble. No
markdown fences. No explanation.

{
  "as_of": "<copy from brief>",
  "regime": "<copy from brief>",
  "headline": "<≤12 words>",
  "market_take": "<2–3 sentences>",
  "drivers": [ { "ticker": "...", "claim": "...", "evidence": "..." }, ... ],
  "risks":   [ { "ticker": "...", "claim": "...", "evidence": "..." }, ... ],
  "opportunities": [ { "ticker": "...", "claim": "...", "evidence": "..." }, ... ],
  "watchlist": [ { "ticker": "...", "claim": "...", "missed_on": "..." }, ... ],
  "breadth": <copy from brief>,
  "benchmarks": <copy from brief>,
  "thresholds_used": <copy from brief>
}
"""


def build_user_message(brief: dict) -> str:
    """The user message is just the brief itself, serialised."""
    return (
        "Here is this week's brief. Produce insights.json per the rules.\n\n"
        f"```json\n{json.dumps(brief, indent=2)}\n```"
    )


# ---------------------------------------------------------------------------
# LLM call with validation
# ---------------------------------------------------------------------------

def call_claude(brief: dict) -> dict:
    """Single API call with prompt caching on the system prompt."""
    api_key = os.environ.get(API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"Missing {API_KEY_ENV}")

    client = anthropic.Anthropic(api_key=api_key)

    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},  # cache across weekly runs
            }
        ],
        messages=[
            {"role": "user", "content": build_user_message(brief)}
        ],
    )

    text = "".join(
        block.text for block in response.content if block.type == "text"
    ).strip()

    # Strip markdown fences if the model added them despite the instruction
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    log.info(f"Input tokens: {response.usage.input_tokens}  "
             f"Output tokens: {response.usage.output_tokens}  "
             f"Cache read: {getattr(response.usage, 'cache_read_input_tokens', 0)}")

    return json.loads(text)


def validate_insights(raw: dict, brief: dict) -> Insights:
    """Schema validation + cross-check that LLM didn't swap tickers.

    The two fatal failure modes are malformed JSON (caught by Pydantic)
    and ticker substitution (the LLM picks a different $TICKER than the
    brief had). We check tickers explicitly.
    """
    insights = Insights.model_validate(raw)

    def tickers(items):
        return {i["ticker"] if isinstance(i, dict) else i.ticker for i in items}

    brief_drivers = {d["ticker"] for d in brief["drivers"]}
    brief_risks = {r["ticker"] for r in brief["risks"]}
    brief_opps = {o["ticker"] for o in brief["opportunities"]}
    brief_watch = {w["ticker"] for w in brief["watchlist"]}

    if tickers(insights.drivers) != brief_drivers:
        raise ValueError(
            f"Driver tickers changed: brief={brief_drivers} "
            f"vs insights={tickers(insights.drivers)}"
        )
    if tickers(insights.risks) != brief_risks:
        raise ValueError(
            f"Risk tickers changed: brief={brief_risks} "
            f"vs insights={tickers(insights.risks)}"
        )
    if tickers(insights.opportunities) != brief_opps:
        raise ValueError(
            f"Opportunity tickers changed: brief={brief_opps} "
            f"vs insights={tickers(insights.opportunities)}"
        )
    if tickers(insights.watchlist) != brief_watch:
        raise ValueError(
            f"Watchlist tickers changed: brief={brief_watch} "
            f"vs insights={tickers(insights.watchlist)}"
        )

    return insights


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(in_path: str = "brief.json", out_path: str = "insights.json") -> int:
    log.info("=" * 60)
    log.info("OSEBX Market Brief — Narration Layer")
    log.info("=" * 60)

    brief = json.loads(Path(in_path).read_text())
    log.info(f"Loaded brief: regime={brief['regime']} "
             f"drivers={len(brief['drivers'])} risks={len(brief['risks'])} "
             f"opps={len(brief['opportunities'])} watch={len(brief['watchlist'])}")

    try:
        raw = call_claude(brief)
    except Exception as e:
        log.error(f"LLM call failed: {e}")
        return 1

    try:
        insights = validate_insights(raw, brief)
    except (ValidationError, ValueError) as e:
        log.error(f"Validation failed — keeping previous insights.json")
        log.error(f"Details: {e}")
        # Write raw response for post-mortem
        Path("insights_rejected.json").write_text(json.dumps(raw, indent=2))
        return 2

    Path(out_path).write_text(insights.model_dump_json(indent=2))
    log.info(f"Wrote {out_path}")
    log.info(f"Headline: {insights.headline}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
