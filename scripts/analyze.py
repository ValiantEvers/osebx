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

# Overextension: top quartile on vol percentile (paired with z-score gate below)
OVEREXTENSION_VOL_THRESHOLD = 75  # volatilityPercentile is already 0–100

# Hidden gems: top quartile earnings yield, above-median Sharpe.
# (The mean-reversion gate is now a fixed z-score range, not a percentile cut.)
GEM_EY_PERCENTILE = 0.75
GEM_SHARPE_PERCENTILE = 0.50

# Speculative flag: high momentum + weak Sharpe
SPECULATIVE_MOMENTUM = 70
SPECULATIVE_SHARPE = 0.5

# Regime thresholds
# Calibrated for COMPANY-level breadth (share of Oslo-listed stocks with
# positive YTD). Company-level breadth runs tighter around 50% than
# sector-level, so the cuts are narrower than the old 0.70/0.40.
BROAD_BREADTH = 0.60
NARROW_BREADTH = 0.35
COMPRESSED_YTD_ABS = 3.0  # |OSEBX YTD| < 3%
ROTATION_SPREAD = 5.0     # OBX vs OSEBX/OSEFX diverge by >5pp

# Z-score thresholds for mean reversion (replaces percentile-based MR cuts).
# 5Y standard-deviation units — roughly normal for diversified equity with
# ~1260 daily observations.
Z_OVEREXTENDED = 2.0   # risks: flag when price is >2σ above 5Y mean
Z_GEM_HIGH = -1.0      # opportunities: cheap side of the range
Z_GEM_LOW = -2.0       # opportunities: crisis side (below this = distressed, not a gem)

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
    breadth: float  # share of Oslo-listed companies with returnsYTD > 0

    benchmarks: dict[str, Optional[float]]  # OSEBX/OBX/OSEFX YTD
    benchmark_spreads: dict[str, Optional[float]]  # OBX-OSEBX, etc.

    sector_leaders: list[SectorNote]  # top 2 sectors by avgReturnYTD
    sector_laggards: list[SectorNote]  # bottom 2

    drivers: list[Candidate]
    risks: list[Candidate]
    opportunities: list[Candidate]
    watchlist: list[WatchlistItem]  # populated only when opportunities is empty
    graduation: dict = Field(default_factory=dict)  # tracks watchlist transitions vs prior run

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

    Ranks by (sharpe + 0.05 * ytd): Sharpe is the primary signal, with a
    small YTD tilt so that among equally risk-adjusted names, the ones
    leading by more get top placement. Each 20pp of YTD adds ~1 unit of
    ranking weight — tuneable.
    """
    pool = []
    for c in companies:
        sharpe = c.get("sharpeRatio")
        ytd = c.get("returnsYTD")
        if sharpe is None or ytd is None:
            continue
        if sharpe < sharpe_floor or ytd <= 0:
            continue
        pool.append((sharpe + 0.05 * ytd, c))

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
    """Two risk flavours, ranked together after within-type normalisation:
    1. Overextended — z-score > Z_OVEREXTENDED (>2σ above 5Y mean) AND high vol
    2. Speculative — high momentum with weak Sharpe

    Because overextended names produce large raw scores (σ units can be big)
    and speculative names produce 0–100 momentum scores, mixing raw values
    would let overextensions always dominate. We rank each sub-type internally
    (0-1 within its pool) before combining, so both types compete fairly.

    The `mr_cut` parameter is retained for interface compatibility but ignored —
    Z_OVEREXTENDED is the new absolute gate.
    """
    overext_pool = []
    spec_pool = []

    for c in companies:
        z = safe_get(c, "meanReversion", "zScore")
        vol_pct = c.get("volatilityPercentile")
        momentum = c.get("momentumScore")
        sharpe = c.get("sharpeRatio")

        if (z is not None and vol_pct is not None
                and z > Z_OVEREXTENDED and vol_pct > OVEREXTENSION_VOL_THRESHOLD):
            overext_pool.append((z, c))
        elif (momentum is not None and sharpe is not None
              and momentum > SPECULATIVE_MOMENTUM and sharpe < SPECULATIVE_SHARPE):
            spec_pool.append((momentum, c))

    # Normalise each pool to rank (1-based) / n so the highest-ranked name
    # in each sub-type gets score 1.0, regardless of absolute value scale.
    def normalise(pool):
        if not pool:
            return []
        pool.sort(key=lambda x: x[0], reverse=True)
        n = len(pool)
        return [((n - i) / n, raw, c) for i, (raw, c) in enumerate(pool)]

    combined = (
        [(norm, raw, c, "overextended") for norm, raw, c in normalise(overext_pool)]
        + [(norm, raw, c, "speculative") for norm, raw, c in normalise(spec_pool)]
    )
    combined.sort(key=lambda x: x[0], reverse=True)

    out = []
    for _, _, c, tag in combined[:MAX_RISKS]:
        out.append(Candidate(
            ticker=strip_ticker(c["ticker"]),
            company_name=c.get("companyName", c["ticker"]),
            sector=c.get("sector") or "Unknown",
            rationale_tag=tag,
            metrics={
                "zScore": round(safe_get(c, "meanReversion", "zScore") or 0, 2),
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
    """Hidden gems: high earnings yield, moderately cheap vs 5Y history,
    acceptable Sharpe.

    Mean-reversion gate uses an absolute z-score range (Z_GEM_LOW to
    Z_GEM_HIGH) rather than a percentile cut. The range is deliberately
    one-to-two σ below the 5Y mean: deep enough to be genuinely cheap,
    but not so deep that it signals distress. Anything below Z_GEM_LOW
    is treated as a crisis name, not a gem.

    The `mr_cut` parameter is kept for interface compatibility but ignored.
    """
    pool = []
    for c in companies:
        ey = c.get("earningsYield")
        z = safe_get(c, "meanReversion", "zScore")
        sharpe = c.get("sharpeRatio")

        if ey is None or z is None or sharpe is None:
            continue
        if ey < ey_cut or sharpe < sharpe_cut:
            continue
        if not (Z_GEM_LOW <= z <= Z_GEM_HIGH):
            continue

        # Rank by yield × quality, with a mild penalty for being closer to
        # the crisis edge of the z-range (i.e., -1.5 is the sweet spot).
        distance_from_centre = abs(z - (Z_GEM_LOW + Z_GEM_HIGH) / 2)
        score = ey * sharpe * (1.0 - 0.2 * distance_from_centre)
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
                "zScore": round(safe_get(c, "meanReversion", "zScore") or 0, 2),
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

    A name is 'near' if it missed ONE gate, and missed it by a small margin.
    Mean-reversion gate is now a z-score range [Z_GEM_LOW, Z_GEM_HIGH];
    'close' means within 0.5σ of either edge.

    The `mr_cut` parameter is kept for interface compatibility but ignored.
    """
    pool = []
    for c in companies:
        ey = c.get("earningsYield")
        z = safe_get(c, "meanReversion", "zScore")
        sharpe = c.get("sharpeRatio")

        if ey is None or z is None or sharpe is None:
            continue

        # Gate pass/fail
        mr_pass = Z_GEM_LOW <= z <= Z_GEM_HIGH
        passes = {
            "earnings_yield": ey >= ey_cut,
            "mean_reversion": mr_pass,
            "sharpe": sharpe >= sharpe_cut,
        }
        failed = [k for k, v in passes.items() if not v]

        # Want exactly one failure (2-of-3), and that failure must be close
        if len(failed) != 1:
            continue

        miss = failed[0]
        close = False
        if miss == "earnings_yield":
            # 25% below gate is close enough for quality names
            close = ey >= ey_cut * 0.75
        elif miss == "mean_reversion":
            # Close = within 0.5σ of either edge of the gem range.
            # This captures "nearly cheap enough" (z just above -1) and
            # "just past distressed" (z just below -2) symmetrically.
            close = (Z_GEM_LOW - 0.5) <= z <= (Z_GEM_HIGH + 0.5)
        elif miss == "sharpe":
            close = sharpe >= sharpe_cut * 0.75

        if not close:
            continue

        # Rank by how close they are to qualifying — best near-miss first.
        # For MR, peak score when z sits at the centre of the gem range.
        norm_ey = ey / ey_cut if ey_cut else 1.0
        z_centre = (Z_GEM_LOW + Z_GEM_HIGH) / 2
        norm_mr = 1.0 / (1.0 + abs(z - z_centre))
        norm_sharpe = sharpe / sharpe_cut if sharpe_cut else 1.0
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
                "zScore": round(safe_get(c, "meanReversion", "zScore") or 0, 2),
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

def build_brief(data: dict, prior_watchlist_tickers: Optional[set[str]] = None) -> Brief:
    companies = data.get("companies", [])
    sector_summary = data.get("sectorSummary", [])
    benchmarks_raw = {b["name"]: b.get("returnsYTD") for b in data.get("benchmarks", [])}

    # Data coverage
    usable = sum(1 for c in companies if c.get("sharpeRatio") is not None)
    coverage = usable / len(companies) if companies else 0.0
    thin = coverage < MIN_DATA_COVERAGE

    # Breadth — share of companies with positive YTD (Oslo market-wide).
    # Previously sector-level, which conflated universe scope with OSEBX
    # benchmarking. Company-level is methodologically cleaner and doesn't
    # depend on small sector buckets.
    with_ytd = [c for c in companies if c.get("returnsYTD") is not None]
    breadth = sum(1 for c in with_ytd if c["returnsYTD"] > 0) / len(with_ytd) if with_ytd else 0.0

    # Sector-level summary is still used below for leader/laggard ranking —
    # just no longer drives the breadth metric.
    sectors_with_data = [s for s in sector_summary if s.get("avgReturnYTD") is not None]

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

    # Dynamic thresholds from the current snapshot.
    # Mean-reversion gates are now fixed z-score thresholds (see constants
    # at top of file), so no MR percentile cuts are computed here anymore.
    sharpe_vals = [c["sharpeRatio"] for c in companies if c.get("sharpeRatio") is not None]
    ey_vals = [c["earningsYield"] for c in companies if c.get("earningsYield") is not None]

    sharpe_floor = percentile(sharpe_vals, QUALITY_SHARPE_PERCENTILE) or 1.0
    ey_gem_cut = percentile(ey_vals, GEM_EY_PERCENTILE) or 8.0
    sharpe_gem_cut = percentile(sharpe_vals, GEM_SHARPE_PERCENTILE) or 0.8

    log.info(f"Dynamic cuts: sharpe_floor={sharpe_floor:.2f} "
             f"ey_gem={ey_gem_cut:.2f} "
             f"sharpe_gem={sharpe_gem_cut:.2f} "
             f"z_overext={Z_OVEREXTENDED} "
             f"z_gem=[{Z_GEM_LOW}, {Z_GEM_HIGH}]")

    drivers = select_drivers(companies, sharpe_floor)
    # mr_cut parameter kept for signature compatibility but unused (z-score is fixed)
    risks = select_risks(companies, 0.0)
    opps = select_opportunities(companies, ey_gem_cut, 0.0, sharpe_gem_cut)

    # Fallback: surface near-misses only when the strict gates found nothing
    watchlist = []
    if not opps:
        watchlist = select_watchlist(companies, ey_gem_cut, 0.0, sharpe_gem_cut)
        log.info(f"Opportunities empty — watchlist surfaced {len(watchlist)} near-misses")

    # Watchlist graduation tracking — compare this week's output to last week's.
    # Prior tickers come from reading last week's committed brief.json, which
    # main() fetches before we overwrite it. This removes the need for a
    # separate prior_watchlist.json file (which wasn't being committed by the
    # workflow and therefore never persisted across runs).
    #
    # Graduation logic:
    #   - "graduated": was on last week's watchlist, now an opportunity
    #   - "fell_to_risk": was on watchlist, now shows up as a risk
    #   - "resolved": was on watchlist, no longer surfaces anywhere
    graduation = {"graduated": [], "fell_to_risk": [], "resolved": []}
    prior_tickers = prior_watchlist_tickers or set()

    if prior_tickers:
        this_opps = {o.ticker for o in opps}
        this_risks = {r.ticker for r in risks}
        this_watch = {w.ticker for w in watchlist}
        for t in prior_tickers:
            if t in this_opps:
                graduation["graduated"].append(t)
            elif t in this_risks:
                graduation["fell_to_risk"].append(t)
            elif t not in this_watch:
                graduation["resolved"].append(t)
        log.info(f"Graduation: {len(graduation['graduated'])} graduated, "
                 f"{len(graduation['fell_to_risk'])} fell to risk, "
                 f"{len(graduation['resolved'])} resolved")

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
        graduation=graduation,
        thresholds_used={
            "sharpe_floor_p66": round(sharpe_floor, 2),
            "z_overextended": Z_OVEREXTENDED,
            "z_gem_low": Z_GEM_LOW,
            "z_gem_high": Z_GEM_HIGH,
            "gem_ey_p75": round(ey_gem_cut, 2),
            "gem_sharpe_p50": round(sharpe_gem_cut, 2),
            # Regime thresholds — surfaced so the frontend tooltip can render
            # these without hardcoding them. Keep naming in sync with the
            # constants at the top of this file.
            "breadth_broad": BROAD_BREADTH,
            "breadth_narrow": NARROW_BREADTH,
            "compressed_ytd_abs": COMPRESSED_YTD_ABS,
            "rotation_spread": ROTATION_SPREAD,
        },
    )


def _load_prior_watchlist_tickers(path: Path) -> set[str]:
    """Read last week's brief.json (if it exists) to extract watchlist tickers."""
    if not path.exists():
        return set()
    try:
        prior = json.loads(path.read_text())
        return {w["ticker"] for w in prior.get("watchlist", [])}
    except Exception as e:
        log.warning(f"Could not read prior {path}: {e}")
        return set()


def main(in_path: str = "data.json", out_path: str = "brief.json") -> int:
    log.info("=" * 60)
    log.info("OSEBX Market Brief — Pre-Analysis")
    log.info("=" * 60)

    data = json.loads(Path(in_path).read_text())

    # Pull last week's watchlist BEFORE we overwrite brief.json. The file
    # is committed by the workflow, so it persists across runs.
    prior_tickers = _load_prior_watchlist_tickers(Path(out_path))

    brief = build_brief(data, prior_watchlist_tickers=prior_tickers)

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
