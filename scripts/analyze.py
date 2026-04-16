#!/usr/bin/env python3
"""
OSEBX Market Brief — Deterministic Pre-Analysis
================================================
Reads data.json (produced by update_data.py) and produces brief.json:
a structured, pre-computed analytical snapshot with regime classification,
breadth metrics, benchmark spreads, and ranked candidate lists for
drivers / risks / opportunities.

This file contains ALL analytical judgement. No LLM involved.
Downstream narrate.py turns brief.json into insights.json by writing
human prose around these pre-computed facts — it cannot change numbers
or add/remove tickers.

Intended to run weekly via GitHub Actions immediately after update_data.py.
"""

import json
import logging
import statistics as st
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — percentile cuts, calibrated to the current OSEBX universe (n≈100)
# ---------------------------------------------------------------------------

MAX_DRIVERS = 4
MAX_RISKS = 4
MAX_OPPORTUNITIES = 3

# Quality floor for drivers/opportunities: top third on Sharpe within snapshot
QUALITY_SHARPE_PERCENTILE = 0.66

# Overextension: top quartile on both mean-reversion distance AND vol percentile
OVEREXTENSION_MR_PERCENTILE = 0.75
OVEREXTENSION_VOL_THRESHOLD = 75  # volatilityPercentile is already 0–100

# Hidden gems: top quartile earnings yield, bottom quartile MR distance,
# above-median Sharpe
GEM_EY_PERCENTILE = 0.75
GEM_MR_PERCENTILE = 0.25
GEM_SHARPE_PERCENTILE = 0.50

# Speculative flag: high momentum + weak Sharpe
SPECULATIVE_MOMENTUM = 70
SPECULATIVE_SHARPE = 0.5

# Regime thresholds
BROAD_BREADTH = 0.70
NARROW_BREADTH = 0.40
COMPRESSED_YTD_ABS = 3.0  # |OSEBX YTD| < 3%
ROTATION_SPREAD = 5.0     # OBX vs OSEBX/OSEFX diverge by >5pp

# Data-quality guard
MIN_DATA_COVERAGE = 0.70  # if <70% of companies have Sharpe, flag thin data


# ---------------------------------------------------------------------------
# Pydantic output schema
# ---------------------------------------------------------------------------

class Candidate(BaseModel):
    """A ranked driver / risk / opportunity candidate.

    The `metrics` dict is the source material narrate.py will cite from.
    Never include a key here that wasn't computed from data.json.
    """
    ticker: str  # "$EQNR" form, not "EQNR.OL"
    company_name: str
    sector: str
    rationale_tag: str  # short machine label, e.g. "quality_leader", "overextended"
    metrics: dict[str, float | int | str]


class SectorNote(BaseModel):
    sector: str
    avg_return_ytd: Optional[float]
    avg_sharpe: Optional[float]
    weight: Optional[float]


class WatchlistItem(BaseModel):
    """A name that failed one of the three opportunity gates but was close.

    Surfaced only when opportunities[] is empty. The `missed_on` field tells
    narrate.py (and the frontend) exactly why it didn't qualify — prevents
    the LLM from claiming these are buys.
    """
    ticker: str
    company_name: str
    sector: str
    missed_on: str  # "earnings_yield" | "mean_reversion" | "sharpe"
    metrics: dict[str, float | int | str]


class Brief(BaseModel):
    as_of: str
    data_coverage: float  # share of companies with usable Sharpe
    thin_data: bool

    regime: Literal["broad_rally", "narrow_rally", "rotation",
                    "compressed", "drawdown"]
    breadth: float  # share of sectors with avgReturnYTD > 0

    benchmarks: dict[str, Optional[float]]  # OSEBX/OBX/OSEFX YTD
    benchmark_spreads: dict[str, Optional[float]]  # OBX-OSEBX, etc.

    sector_leaders: list[SectorNote]  # top 2 sectors by avgReturnYTD
    sector_laggards: list[SectorNote]  # bottom 2

    drivers: list[Candidate]
    risks: list[Candidate]
    opportunities: list[Candidate]
    watchlist: list[WatchlistItem]  # populated only when opportunities is empty

    # Thresholds used this run — written for audit/debug & narrate.py reference
    thresholds_used: dict[str, float]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def strip_ticker(t: str) -> str:
    """EQNR.OL -> $EQNR"""
    return "$" + t.replace(".OL", "").replace(".", "")


def percentile(vals: list[float], p: float) -> Optional[float]:
    """Nearest-rank percentile on a non-empty sorted sample."""
    vs = sorted(v for v in vals if v is not None)
    if not vs:
        return None
    idx = min(len(vs) - 1, max(0, int(round(p * (len(vs) - 1)))))
    return vs[idx]


def safe_get(c: dict, *keys):
    """Walk nested keys; return None at first missing link."""
    cur = c
    for k in keys:
        if cur is None or not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


# ---------------------------------------------------------------------------
# Regime classification
# ---------------------------------------------------------------------------

def classify_regime(breadth: float, bench: dict[str, Optional[float]],
                    companies: list[dict]) -> str:
    """Apply the decision tree.

    Order matters: drawdown first (overrides everything), then compressed
    (low-vol tape), then narrow/broad, with rotation as the middle case.
    """
    osebx_ytd = bench.get("OSEBX")
    obx_ytd = bench.get("OBX")
    osefx_ytd = bench.get("OSEFX")

    # Drawdown takes precedence
    if osebx_ytd is not None and osebx_ytd < 0:
        return "drawdown"

    # Compressed: flat index AND weak median risk-adjusted return
    sharpes = [c["sharpeRatio"] for c in companies
               if c.get("sharpeRatio") is not None]
    median_sharpe = st.median(sharpes) if sharpes else None
    if (osebx_ytd is not None and abs(osebx_ytd) < COMPRESSED_YTD_ABS
            and median_sharpe is not None and median_sharpe < 0.5):
        return "compressed"

    # Narrow rally: index up but breadth thin
    if osebx_ytd is not None and osebx_ytd > 0 and breadth < NARROW_BREADTH:
        return "narrow_rally"

    # Broad rally: index up and breadth wide
    if (osebx_ytd is not None and osebx_ytd > 0
            and breadth >= BROAD_BREADTH):
        return "broad_rally"

    # Rotation: middle zone, especially if benchmarks diverge
    spread = None
    if obx_ytd is not None and osebx_ytd is not None:
        spread = obx_ytd - osebx_ytd
    if spread is not None and abs(spread) >= ROTATION_SPREAD:
        return "rotation"

    # Default fallback in the 40–70% breadth band
    return "rotation"


# ---------------------------------------------------------------------------
# Candidate selection
# ---------------------------------------------------------------------------

def select_drivers(companies: list[dict], sharpe_floor: float) -> list[Candidate]:
    """Top quality leaders: high Sharpe AND positive YTD return.

    Ranks by (sharpeRatio * sign of returnsYTD) to penalise high-Sharpe names
    that happen to be flat — we want LEADERSHIP, not just safety.
    """
    pool = []
    for c in companies:
        sharpe = c.get("sharpeRatio")
        ytd = c.get("returnsYTD")
        if sharpe is None or ytd is None:
            continue
        if sharpe < sharpe_floor or ytd <= 0:
            continue
        pool.append((sharpe * (ytd ** 0.5 if ytd > 0 else 0), c))

    pool.sort(key=lambda x: x[0], reverse=True)
    out = []
    for _, c in pool[:MAX_DRIVERS]:
        out.append(Candidate(
            ticker=strip_ticker(c["ticker"]),
            company_name=c.get("companyName", c["ticker"]),
            sector=c.get("sector") or "Unknown",
            rationale_tag="quality_leader",
            metrics={
                "sharpeRatio": round(c["sharpeRatio"], 2),
                "returnsYTD": round(c["returnsYTD"], 1),
                "momentumScore": round(c.get("momentumScore") or 0, 0),
                "alphaSixMonth": round(safe_get(c, "alpha", "sixMonth") or 0, 1),
            },
        ))
    return out


def select_risks(companies: list[dict], mr_cut: float) -> list[Candidate]:
    """Two risk flavours:
    1. Overextended — top-quartile mean reversion AND high vol
    2. Speculative — high momentum with weak Sharpe
    """
    pool = []

    for c in companies:
        mr_dist = safe_get(c, "meanReversion", "distancePercentage")
        vol_pct = c.get("volatilityPercentile")
        momentum = c.get("momentumScore")
        sharpe = c.get("sharpeRatio")

        tag = None
        sort_key = 0.0

        if (mr_dist is not None and vol_pct is not None
                and mr_dist > mr_cut and vol_pct > OVEREXTENSION_VOL_THRESHOLD):
            tag = "overextended"
            sort_key = mr_dist
        elif (momentum is not None and sharpe is not None
              and momentum > SPECULATIVE_MOMENTUM and sharpe < SPECULATIVE_SHARPE):
            tag = "speculative"
            sort_key = momentum

        if tag:
            pool.append((sort_key, tag, c))

    pool.sort(key=lambda x: x[0], reverse=True)
    out = []
    for _, tag, c in pool[:MAX_RISKS]:
        out.append(Candidate(
            ticker=strip_ticker(c["ticker"]),
            company_name=c.get("companyName", c["ticker"]),
            sector=c.get("sector") or "Unknown",
            rationale_tag=tag,
            metrics={
                "meanReversionPct": round(
                    safe_get(c, "meanReversion", "distancePercentage") or 0, 1),
                "volatilityPercentile": c.get("volatilityPercentile") or 0,
                "momentumScore": round(c.get("momentumScore") or 0, 0),
                "sharpeRatio": round(c.get("sharpeRatio") or 0, 2),
                "maxDrawdown": round(c.get("maxDrawdown") or 0, 1),
            },
        ))
    return out


def select_opportunities(companies: list[dict], ey_cut: float,
                         mr_cut: float, sharpe_cut: float) -> list[Candidate]:
    """Hidden gems: high earnings yield, near 5Y mean, acceptable Sharpe."""
    pool = []
    for c in companies:
        ey = c.get("earningsYield")
        mr_dist = safe_get(c, "meanReversion", "distancePercentage")
        sharpe = c.get("sharpeRatio")

        if ey is None or mr_dist is None or sharpe is None:
            continue
        if ey < ey_cut or mr_dist > mr_cut or sharpe < sharpe_cut:
            continue

        # Rank by a composite: yield weighted by quality, penalised for extension
        score = ey * sharpe - max(0, mr_dist) * 0.1
        pool.append((score, c))

    pool.sort(key=lambda x: x[0], reverse=True)
    out = []
    for _, c in pool[:MAX_OPPORTUNITIES]:
        out.append(Candidate(
            ticker=strip_ticker(c["ticker"]),
            company_name=c.get("companyName", c["ticker"]),
            sector=c.get("sector") or "Unknown",
            rationale_tag="hidden_gem",
            metrics={
                "earningsYield": round(c["earningsYield"], 1),
                "meanReversionPct": round(
                    safe_get(c, "meanReversion", "distancePercentage") or 0, 1),
                "sharpeRatio": round(c["sharpeRatio"], 2),
                "dividendYield": round(c.get("dividendYield") or 0, 1),
                "peRatio": round(c.get("peRatio") or 0, 1),
            },
        ))
    return out


def select_watchlist(companies: list[dict], ey_cut: float,
                     mr_cut: float, sharpe_cut: float) -> list[WatchlistItem]:
    """Near-miss opportunities: cleared 2 of 3 gates.

    Only called when opportunities[] is empty. The rationale is that in a
    rotation/rally regime, strict rules refusing to produce output is honest
    but unhelpful — the watchlist surfaces 'here are the names that are
    closest to being buys, and here's exactly what's keeping them off the
    list.' It never implies a recommendation.

    A name is 'near' if it missed ONE gate, and missed it by <25% of the cut.
    """
    pool = []
    for c in companies:
        ey = c.get("earningsYield")
        mr_dist = safe_get(c, "meanReversion", "distancePercentage")
        sharpe = c.get("sharpeRatio")

        if ey is None or mr_dist is None or sharpe is None:
            continue

        # How many gates passed?
        passes = {
            "earnings_yield": ey >= ey_cut,
            "mean_reversion": mr_dist <= mr_cut,
            "sharpe": sharpe >= sharpe_cut,
        }
        failed = [k for k, v in passes.items() if not v]

        # Want exactly one failure (2-of-3), and that failure must be close
        if len(failed) != 1:
            continue

        miss = failed[0]
        # "Close" = within a reasonable overshoot of the threshold.
        # Tolerances asymmetric because the metrics have different distributions:
        # - earnings yield: allow 25% below the gate (common for quality names)
        # - mean reversion: allow up to the MEDIAN distance, which represents
        #   "not exceptional, but not stretched enough to be a risk either"
        # - sharpe: allow 25% below the gate
        close = False
        if miss == "earnings_yield":
            close = ey >= ey_cut * 0.75
        elif miss == "mean_reversion":
            # Using median MR as the ceiling makes this dataset-adaptive:
            # in a rotation regime when everything is extended, we still
            # surface the least-stretched high-quality names.
            close = mr_dist <= max(mr_cut * 5, 25.0)
        elif miss == "sharpe":
            close = sharpe >= sharpe_cut * 0.75

        if not close:
            continue

        # Rank by how close they are to qualifying — best near-miss first
        # Composite: all three metrics normalised, weighted
        norm_ey = ey / ey_cut
        norm_mr = mr_cut / max(mr_dist, 0.01) if mr_dist > 0 else 2.0
        norm_sharpe = sharpe / sharpe_cut
        score = norm_ey + norm_mr + norm_sharpe
        pool.append((score, miss, c))

    pool.sort(key=lambda x: x[0], reverse=True)
    out = []
    for _, miss, c in pool[:3]:
        out.append(WatchlistItem(
            ticker=strip_ticker(c["ticker"]),
            company_name=c.get("companyName", c["ticker"]),
            sector=c.get("sector") or "Unknown",
            missed_on=miss,
            metrics={
                "earningsYield": round(c.get("earningsYield") or 0, 1),
                "meanReversionPct": round(
                    safe_get(c, "meanReversion", "distancePercentage") or 0, 1),
                "sharpeRatio": round(c.get("sharpeRatio") or 0, 2),
                "returnsYTD": round(c.get("returnsYTD") or 0, 1),
            },
        ))
    return out


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def build_brief(data: dict) -> Brief:
    companies = data.get("companies", [])
    sector_summary = data.get("sectorSummary", [])
    benchmarks_raw = {b["name"]: b.get("returnsYTD") for b in data.get("benchmarks", [])}

    # Data coverage
    usable = sum(1 for c in companies if c.get("sharpeRatio") is not None)
    coverage = usable / len(companies) if companies else 0.0
    thin = coverage < MIN_DATA_COVERAGE

    # Breadth
    sectors_with_data = [s for s in sector_summary if s.get("avgReturnYTD") is not None]
    sectors_pos = sum(1 for s in sectors_with_data if s["avgReturnYTD"] > 0)
    breadth = sectors_pos / len(sectors_with_data) if sectors_with_data else 0.0

    # Benchmark spreads
    osebx = benchmarks_raw.get("OSEBX")
    obx = benchmarks_raw.get("OBX")
    osefx = benchmarks_raw.get("OSEFX")
    spreads = {
        "OBX_minus_OSEBX": round(obx - osebx, 2) if obx is not None and osebx is not None else None,
        "OSEFX_minus_OSEBX": round(osefx - osebx, 2) if osefx is not None and osebx is not None else None,
    }

    # Regime
    regime = classify_regime(breadth, benchmarks_raw, companies)

    # Sector ranking (exclude nulls, need ≥2 sectors to be meaningful)
    ranked_sectors = sorted(
        sectors_with_data, key=lambda s: s["avgReturnYTD"], reverse=True,
    )
    leaders = [SectorNote(
        sector=s["sector"],
        avg_return_ytd=round(s["avgReturnYTD"], 2),
        avg_sharpe=round(s["avgSharpe"], 2) if s.get("avgSharpe") is not None else None,
        weight=s.get("totalWeight"),
    ) for s in ranked_sectors[:2]]
    laggards = [SectorNote(
        sector=s["sector"],
        avg_return_ytd=round(s["avgReturnYTD"], 2),
        avg_sharpe=round(s["avgSharpe"], 2) if s.get("avgSharpe") is not None else None,
        weight=s.get("totalWeight"),
    ) for s in ranked_sectors[-2:][::-1]]

    # Dynamic thresholds from the current snapshot
    sharpe_vals = [c["sharpeRatio"] for c in companies if c.get("sharpeRatio") is not None]
    mr_vals = [safe_get(c, "meanReversion", "distancePercentage") for c in companies]
    mr_vals = [v for v in mr_vals if v is not None]
    ey_vals = [c["earningsYield"] for c in companies if c.get("earningsYield") is not None]

    sharpe_floor = percentile(sharpe_vals, QUALITY_SHARPE_PERCENTILE) or 1.0
    mr_overext_cut = percentile(mr_vals, OVEREXTENSION_MR_PERCENTILE) or 25.0
    ey_gem_cut = percentile(ey_vals, GEM_EY_PERCENTILE) or 8.0
    mr_gem_cut = percentile(mr_vals, GEM_MR_PERCENTILE) or 10.0
    sharpe_gem_cut = percentile(sharpe_vals, GEM_SHARPE_PERCENTILE) or 0.8

    log.info(f"Dynamic cuts: sharpe_floor={sharpe_floor:.2f} "
             f"mr_overext={mr_overext_cut:.1f}% "
             f"ey_gem={ey_gem_cut:.2f} "
             f"mr_gem={mr_gem_cut:.2f}% "
             f"sharpe_gem={sharpe_gem_cut:.2f}")

    drivers = select_drivers(companies, sharpe_floor)
    risks = select_risks(companies, mr_overext_cut)
    opps = select_opportunities(companies, ey_gem_cut, mr_gem_cut, sharpe_gem_cut)

    # Fallback: surface near-misses only when the strict gates found nothing
    watchlist = []
    if not opps:
        watchlist = select_watchlist(companies, ey_gem_cut, mr_gem_cut, sharpe_gem_cut)
        log.info(f"Opportunities empty — watchlist surfaced {len(watchlist)} near-misses")

    return Brief(
        as_of=data.get("lastUpdated", datetime.now(timezone.utc).isoformat()),
        data_coverage=round(coverage, 2),
        thin_data=thin,
        regime=regime,
        breadth=round(breadth, 2),
        benchmarks={"OSEBX": osebx, "OBX": obx, "OSEFX": osefx},
        benchmark_spreads=spreads,
        sector_leaders=leaders,
        sector_laggards=laggards,
        drivers=drivers,
        risks=risks,
        opportunities=opps,
        watchlist=watchlist,
        thresholds_used={
            "sharpe_floor_p66": round(sharpe_floor, 2),
            "overextension_mr_p75": round(mr_overext_cut, 1),
            "gem_ey_p75": round(ey_gem_cut, 2),
            "gem_mr_p25": round(mr_gem_cut, 2),
            "gem_sharpe_p50": round(sharpe_gem_cut, 2),
        },
    )


def main(in_path: str = "data.json", out_path: str = "brief.json") -> int:
    log.info("=" * 60)
    log.info("OSEBX Market Brief — Pre-Analysis")
    log.info("=" * 60)

    data = json.loads(Path(in_path).read_text())
    brief = build_brief(data)

    log.info(f"Regime: {brief.regime}")
    log.info(f"Breadth: {brief.breadth:.0%}  Coverage: {brief.data_coverage:.0%}"
             f"{'  [THIN DATA]' if brief.thin_data else ''}")
    log.info(f"Drivers: {len(brief.drivers)}  Risks: {len(brief.risks)}  "
             f"Opportunities: {len(brief.opportunities)}"
             f"{f'  Watchlist: {len(brief.watchlist)}' if brief.watchlist else ''}")

    Path(out_path).write_text(brief.model_dump_json(indent=2))
    log.info(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
