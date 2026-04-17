#!/usr/bin/env python3
"""
Oslo Børs Heatmap — Data Pipeline
==================================
Fetches fundamental and historical data for all Oslo Børs & Euronext Expand
stocks (ASK-eligible and non-eligible) via yahooquery, computes analytical
metrics (alpha, Sharpe, momentum, drawdown, etc.), validates through Pydantic,
and writes data.json for the frontend dashboard.

Intended to run weekly via GitHub Actions (see .github/workflows/update-data.yml).

IMPORTANT: The constituent list below is manually maintained and should be updated
when the OSEBX index rebalances (typically in June and December each year), or
when new stocks list / delist on Oslo Børs or Euronext Expand.
Last updated: April 2026.
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from yahooquery import Ticker

import random
from yahooquery import utils

# Override the default yahooquery User-Agent to pretend to be a real Chrome browser
utils.USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Norwegian reference rates -- update these periodically when rates change.
RISK_FREE_RATE = 0.035        # 3-month Norwegian government bill yield (approx.)
BOND_YIELD_10Y = 0.035        # 10-year Norwegian government bond yield (approx.)

TRADING_DAYS_1M = 21
TRADING_DAYS_3M = 63
TRADING_DAYS_6M = 126
TRADING_DAYS_1Y = 252
TRADING_DAYS_5Y = 1260

BENCHMARK_TICKERS = ["OSEBX.OL", "OBX.OL", "OSEFX.OL"]
BRENT_TICKER = "BZ=F"

BATCH_SIZE = 12               # Tickers per yahooquery batch for fundamentals
FETCH_DELAY = 0.4             # Seconds between batch fetches

# ---------------------------------------------------------------------------
# Sector Name Normalisation
# ---------------------------------------------------------------------------
# Yahoo Finance returns different GICS sector names than our canonical list.
# This map ensures consistent naming in the output JSON.

SECTOR_NORM = {
    "Financial Services": "Financials",
    "Consumer Defensive": "Consumer Staples",
    "Consumer Cyclical":  "Consumer Discretionary",
    "Basic Materials":    "Materials",
    "Healthcare":         "Health Care",
    "Technology":         "Information Technology",
}

def norm_sector(s: str) -> str:
    """Normalise a sector name to canonical GICS."""
    if not s:
        return "Unknown"
    return SECTOR_NORM.get(s, s)

# ---------------------------------------------------------------------------
# Full Oslo Børs + Euronext Expand Stock List
# ---------------------------------------------------------------------------
# Each entry: (Yahoo ticker, display name, GICS sector, OSEBX weight %, in_osebx, ask_eligible)
#
# in_osebx   = True if the stock is a current OSEBX constituent
# ask_eligible = True if eligible for Nordnet Aksjesparekonto (ASK)
#   ASK rules: must be on a regulated EU/EEA market (Oslo Børs main list or
#   Euronext Expand) AND domiciled within the EEA. Euronext Growth is excluded.
#   Companies registered in Bermuda, Cayman Islands, Marshall Islands,
#   Singapore, etc. are NOT ASK-eligible even if listed on Oslo Børs.
#
# Weights are approximate and manually maintained. Non-OSEBX stocks have weight 0.
# Last updated: April 2026.

CONSTITUENTS = [
    # =========================================================================
    # OSEBX CONSTITUENTS (~69 stocks)
    # =========================================================================

    # --- Energy ---
    ("EQNR.OL",   "Equinor",               "Energy",                  9.50, True,  True),
    ("AKRBP.OL",  "Aker BP",               "Energy",                  3.20, True,  True),
    ("VAR.OL",    "Var Energi",             "Energy",                  1.60, True,  True),
    ("SUBC.OL",   "Subsea 7",              "Energy",                  1.50, True,  True),   # Luxembourg (EEA)
    ("AKSO.OL",   "Aker Solutions",         "Energy",                  0.70, True,  True),
    ("BWE.OL",    "BW Energy",              "Energy",                  0.10, True,  False),  # Bermuda
    ("BWLPG.OL",  "BW LPG",                "Industrials",             0.35, True,  False),  # Bermuda/Singapore
    ("TGS.OL",    "TGS",                    "Energy",                  0.50, True,  True),
    ("BORR.OL",   "Borr Drilling",          "Energy",                  0.25, True,  False),  # Bermuda
    ("HAUTO.OL",  "Hoegh Autoliners",       "Industrials",             0.60, True,  True),
    ("FRO.OL",    "Frontline",              "Energy",                  1.20, True,  False),  # Cyprus/Bermuda
    ("GOGL.OL",   "Golden Ocean Group",     "Energy",                  0.50, True,  False),  # Bermuda
    ("PGS.OL",    "PGS",                    "Energy",                  0.20, True,  True),

    # --- Financials ---
    ("DNB.OL",    "DNB Bank",               "Financials",              8.50, True,  True),
    ("MORG.OL",   "SpareBank 1 SMN",        "Financials",              0.55, True,  True),
    ("SRBNK.OL",  "SpareBank 1 SR-Bank",    "Financials",              0.70, True,  True),
    ("NOFI.OL",   "SpareBank 1 Ostlandet",  "Financials",              0.30, True,  True),
    ("HELG.OL",   "SpareBank 1 Helgeland",  "Financials",              0.10, True,  True),
    ("NONG.OL",   "SpareBank 1 Nord-Norge", "Financials",              0.25, True,  True),
    # Sbanken (SBANK.OL) was delisted and fully merged into DNB in 2022 — removed.
    ("GJF.OL",    "Gjensidige Forsikring",  "Financials",              1.50, True,  True),
    ("STB.OL",    "Storebrand",             "Financials",              1.40, True,  True),
    ("PROTCT.OL", "Protector Forsikring",   "Financials",              0.25, True,  True),
    ("PARB.OL",   "Pareto Bank",            "Financials",              0.10, True,  True),
    ("ABG.OL",    "ABG Sundal Collier",     "Financials",              0.10, True,  True),
    ("ADE.OL",    "Adevinta",               "Financials",              0.60, True,  True),

    # --- Consumer Staples ---
    ("MOWI.OL",   "Mowi",                   "Consumer Staples",        3.00, True,  True),
    ("SALM.OL",   "SalMar",                 "Consumer Staples",        1.60, True,  True),
    ("LSG.OL",    "Leroy Seafood",          "Consumer Staples",        0.70, True,  True),
    ("AUSS.OL",   "Austevoll Seafood",      "Consumer Staples",        0.35, True,  True),
    ("BAKKA.OL",  "Bakkafrost",             "Consumer Staples",        0.60, True,  True),   # Faroe Islands (Danish realm, EEA-ish via Denmark)
    ("ORK.OL",    "Orkla",                  "Consumer Staples",        2.10, True,  True),
    ("GSF.OL",    "Grieg Seafood",          "Consumer Staples",        0.20, True,  True),

    # --- Industrials ---
    ("KOG.OL",    "Kongsberg Gruppen",      "Industrials",             4.50, True,  True),
    ("WILS.OL",   "Wilh. Wilhelmsen Hldg",  "Industrials",             0.50, True,  True),
    ("WWI.OL",    "Wallenius Wilhelmsen",   "Industrials",             1.20, True,  True),
    ("FLNG.OL",   "Flex LNG",              "Industrials",              0.25, True,  False),  # Bermuda
    ("COOL.OL",   "CoolCo",                "Industrials",              0.15, True,  False),  # Bermuda
    ("TOM.OL",    "Tomra Systems",          "Industrials",             1.40, True,  True),
    ("MPCC.OL",   "MPC Container Ships",    "Industrials",             0.20, True,  True),
    ("BELCO.OL",  "Bonheur",               "Industrials",              0.20, True,  True),
    ("NEL.OL",    "Nel",                    "Industrials",              0.40, True,  True),
    ("AKER.OL",   "Aker",                   "Industrials",             1.00, True,  True),
    ("AKAST.OL",  "Akastor",                "Industrials",             0.15, True,  True),

    # --- Information Technology ---
    ("AUTO.OL",   "Autostore Holdings",     "Information Technology",   0.80, True,  False),  # Cayman Islands
    ("KIT.OL",    "Kitron",                 "Information Technology",   0.25, True,  True),
    ("CRAYN.OL",  "Crayon Group",           "Information Technology",   0.60, True,  True),
    ("NOD.OL",    "Nordic Semiconductor",   "Information Technology",   1.20, True,  True),
    ("OPER.OL",   "Opera Ltd",              "Information Technology",   0.25, True,  False),  # Cayman Islands
    ("VOLUE.OL",  "Volue",                  "Information Technology",   0.15, True,  True),
    ("PEXIP.OL",  "Pexip Holding",          "Information Technology",   0.15, True,  True),
    ("B2H.OL",    "B2Holding",              "Information Technology",   0.10, True,  True),
    ("RECSI.OL",  "REC Silicon",            "Information Technology",   0.15, True,  True),
    ("KAHOT.OL",  "Kahoot!",                "Information Technology",   0.25, True,  True),

    # --- Materials ---
    ("NHY.OL",    "Norsk Hydro",            "Materials",               3.00, True,  True),
    ("YAR.OL",    "Yara International",     "Materials",               2.50, True,  True),

    # --- Communication Services ---
    ("TEL.OL",    "Telenor",                "Communication Services",   4.50, True,  True),
    ("ATEA.OL",   "Atea",                   "Communication Services",   0.40, True,  True),

    # --- Utilities ---
    ("SCHA.OL",   "Scatec",                 "Utilities",               0.40, True,  True),
    ("ELMRA.OL",  "Elmera Group",           "Utilities",               0.50, True,  True),

    # --- Consumer Discretionary ---
    ("SCHB.OL",   "Schibsted A",            "Consumer Discretionary",  1.40, True,  True),
    ("EPR.OL",    "Europris",               "Consumer Discretionary",  0.30, True,  True),
    ("HEX.OL",    "Hexagon Composites",     "Consumer Discretionary",  0.20, True,  True),
    ("KOA.OL",    "Kongsberg Automotive",    "Consumer Discretionary",  0.10, True,  True),

    # --- Real Estate ---
    ("ENTRA.OL",  "Entra",                  "Real Estate",             0.60, True,  True),
    ("OLT.OL",    "Olav Thon Eiendom",      "Real Estate",             0.30, True,  True),
    ("SOR.OL",    "Self Storage Group",      "Real Estate",             0.10, True,  True),

    # --- Health Care ---
    ("PHO.OL",    "Photocure",              "Health Care",             0.10, True,  True),
    ("MEDI.OL",   "Medistim",               "Health Care",             0.10, True,  True),


    # =========================================================================
    # ADDITIONAL OSLO BØRS STOCKS (not in OSEBX — weight = 0)
    # =========================================================================

    # --- Energy (non-OSEBX) ---
    ("DNO.OL",    "DNO",                    "Energy",                  0, False, True),
    ("OKEA.OL",   "OKEA",                   "Energy",                  0, False, True),
    ("BNOR.OL",   "BlueNord",               "Energy",                  0, False, True),
    ("PEN.OL",    "Panoro Energy",           "Energy",                  0, False, True),
    ("DOFG.OL",   "DOF Group",              "Energy",                  0, False, True),
    ("ODL.OL",    "Odfjell Drilling",        "Energy",                  0, False, False),  # Bermuda
    ("SOFF.OL",   "Solstad Offshore",        "Energy",                  0, False, True),
    ("SOMA.OL",   "Solstad Maritime",        "Energy",                  0, False, True),
    ("SEA1.OL",   "Sea1 Offshore",           "Energy",                  0, False, False),  # Bermuda
    ("PLSV.OL",   "Paratus Energy",          "Energy",                  0, False, False),  # Bermuda
    ("GKP.OL",    "Gulf Keystone Petroleum", "Energy",                  0, False, False),  # Bermuda
    ("ENH.OL",    "SED Energy Holdings",     "Energy",                  0, False, False),  # Ireland (EEA) — check
    ("COSH.OL",   "Constellation Oil Svcs",  "Energy",                  0, False, False),  # Luxembourg — actually EEA
    ("SHLF.OL",   "Shelf Drilling",          "Energy",                  0, False, False),  # Cayman Islands

    # --- Financials (non-OSEBX) ---
    ("SB1NO.OL",  "SpareBank 1 Sor-Norge",  "Financials",              0, False, True),
    ("SBNOR.OL",  "Sparebanken Norge",       "Financials",              0, False, True),
    ("MORG2.OL",  "Sparebanken More",        "Financials",              0, False, True),
    ("RING.OL",   "SB1 Ringerike Hadeland",  "Financials",              0, False, True),
    ("SOAG.OL",   "SB1 Ostfold Akershus",    "Financials",              0, False, True),
    ("ROGS.OL",   "Rogaland Sparebank",      "Financials",              0, False, True),
    ("B2I.OL",    "B2 Impact",               "Financials",              0, False, True),

    # --- Consumer Staples (non-OSEBX) ---
    ("KID.OL",    "Kid",                     "Consumer Staples",        0, False, True),
    ("AKBM.OL",   "Aker BioMarine",          "Consumer Staples",        0, False, True),
    ("AKVA.OL",   "AKVA Group",              "Consumer Staples",        0, False, True),

    # --- Industrials (non-OSEBX) ---
    ("VEI.OL",    "Veidekke",                "Industrials",             0, False, True),
    ("AFG.OL",    "AF Gruppen",              "Industrials",             0, False, True),
    ("MULTI.OL",  "Multiconsult",            "Industrials",             0, False, True),
    ("BOUV.OL",   "Bouvet",                  "Industrials",             0, False, True),
    ("ENDUR.OL",  "Endur",                   "Industrials",             0, False, True),
    ("NORCO.OL",  "Norconsult",              "Industrials",             0, False, True),
    ("KCC.OL",    "Klaveness Combination",    "Industrials",             0, False, True),
    ("ELO.OL",    "Elopak",                  "Industrials",             0, False, True),
    ("CADLR.OL",  "Cadeler",                 "Industrials",             0, False, True),   # Denmark (EEA)
    ("HAFNI.OL",  "Hafnia",                  "Industrials",             0, False, False),  # Bermuda/Singapore
    ("ODF.OL",    "Odfjell SE",              "Industrials",             0, False, True),
    ("HSHP.OL",   "Himalaya Shipping",       "Industrials",             0, False, False),  # Bermuda
    ("BWO.OL",    "BW Offshore",             "Industrials",             0, False, False),  # Bermuda
    ("SNI.OL",    "Stolt-Nielsen",           "Industrials",             0, False, False),  # Bermuda
    ("SATS.OL",   "Sats",                    "Industrials",             0, False, True),
    ("NAS.OL",    "Norwegian Air Shuttle",   "Industrials",             0, False, True),
    ("CMBTO.OL",  "CMB.Tech",               "Industrials",             0, False, True),   # Belgium (EEA)
    ("TIETO.OL",  "TietoEVRY",              "Information Technology",   0, False, True),   # Finland (EEA)

    # --- Information Technology (non-OSEBX) ---
    ("LINK.OL",   "LINK Mobility",           "Information Technology",   0, False, True),
    ("SMOP.OL",   "Smartoptics",             "Information Technology",   0, False, True),
    ("NAPA.OL",   "Napatech",               "Information Technology",   0, False, True),   # Denmark (EEA)
    ("NORBT.OL",  "Norbit",                  "Information Technology",   0, False, True),
    ("SNTIA.OL",  "Sentia",                  "Information Technology",   0, False, True),
    ("SWON.OL",   "SoftwareOne",             "Information Technology",   0, False, True),   # Switzerland (EEA via EFTA)

    # --- Materials (non-OSEBX) ---
    ("ELK.OL",    "Elkem",                   "Materials",               0, False, True),
    ("BRG.OL",    "Borregaard",              "Materials",               0, False, True),
    ("BEWI.OL",   "BEWI",                    "Materials",               0, False, True),

    # --- Real Estate (non-OSEBX) ---
    ("PUBLI.OL",  "Public Property Invest",  "Real Estate",             0, False, True),
    ("SBO.OL",    "Selvaag Bolig",           "Real Estate",             0, False, True),

    # --- Utilities (non-OSEBX) ---
    ("CLOUD.OL",  "Cloudberry Clean Energy", "Utilities",               0, False, True),
    ("AFK.OL",    "Arendals Fossekompani",   "Utilities",               0, False, True),
    ("ENVIP.OL",  "Envipco Holding",         "Utilities",               0, False, True),   # Netherlands (EEA)

    # --- Consumer Discretionary (non-OSEBX) ---
    ("VEND.OL",   "Vend Marketplaces",       "Consumer Discretionary",  0, False, True),
    ("ANDF.OL",   "Andfjord Salmon",         "Consumer Staples",        0, False, True),

    # --- Misc / Smaller Oslo Børs names ---
    ("BRUT.OL",   "Bruton",                  "Energy",                  0, False, False),  # Check domicile
    ("OET.OL",    "Okeanis Eco Tankers",     "Energy",                  0, False, False),  # Marshall Islands
]


# ---------------------------------------------------------------------------
# Pydantic Models (data validation)
# ---------------------------------------------------------------------------

class Week52(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    low: Optional[float] = None
    high: Optional[float] = None
    current: Optional[float] = None

class Alpha(BaseModel):
    threeMonth: Optional[float] = None
    sixMonth: Optional[float] = None
    oneYear: Optional[float] = None

class MeanReversion(BaseModel):
    fiveYearAverage: Optional[float] = None
    distancePercentage: Optional[float] = None  # kept for display / backward compatibility
    standardDeviation: Optional[float] = None   # 5Y stdev of closing price
    zScore: Optional[float] = None              # (current - mean) / stdev

class DividendConsistency(BaseModel):
    yearsWithDividend: Optional[int] = None
    trend: Optional[str] = None  # "growing" | "stable" | "cut" | "none"

class CompanyRecord(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ticker: str
    companyName: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    osebxWeight: Optional[float] = None
    inOSEBX: bool = True
    askEligible: bool = True
    marketCap: Optional[float] = None
    peRatio: Optional[float] = None
    forwardPE: Optional[float] = None
    pbRatio: Optional[float] = None
    dividendYield: Optional[float] = None
    week52: Optional[Week52] = None
    analystTarget: Optional[float] = None
    recommendation: Optional[str] = None

    returnsYTD: Optional[float] = None
    returns6M: Optional[float] = None
    returns1Y: Optional[float] = None

    alpha: Optional[Alpha] = None
    sharpeRatio: Optional[float] = None
    momentumScore: Optional[float] = None
    maxDrawdown: Optional[float] = None

    # Tier 2
    volatilityPercentile: Optional[int] = None
    correlationToOSEBX: Optional[float] = None
    dividendConsistency: Optional[DividendConsistency] = None
    dividendPayoutRatio: Optional[float] = None
    liquidityScore: Optional[float] = None

    # Tier 1 valuation
    earningsYield: Optional[float] = None
    evToEbitda: Optional[float] = None
    meanReversion: Optional[MeanReversion] = None

    # Tier 3
    betaToBrent: Optional[float] = None
    seasonality: Optional[list[Optional[float]]] = None

    @field_validator("*", mode="before")
    @classmethod
    def sanitise_nan(cls, v):
        """Replace NaN / Inf with None before Pydantic processes."""
        if isinstance(v, float):
            if np.isnan(v) or np.isinf(v):
                return None
        return v


class BenchmarkData(BaseModel):
    ticker: str
    name: Optional[str] = None
    returnsYTD: Optional[float] = None
    returns6M: Optional[float] = None
    returns1Y: Optional[float] = None
    latestClose: Optional[float] = None


class SectorSummary(BaseModel):
    sector: str
    companyCount: int = 0
    totalWeight: Optional[float] = None
    avgReturnYTD: Optional[float] = None
    avgPE: Optional[float] = None
    avgEvToEbitda: Optional[float] = None
    avgDividendYield: Optional[float] = None
    avgSharpe: Optional[float] = None


class HeatmapData(BaseModel):
    lastUpdated: str
    referenceRates: dict
    benchmarks: list[BenchmarkData]
    companies: list[CompanyRecord]
    sectorSummary: list[SectorSummary]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def safe_get(d, *keys, default=None):
    """Safely traverse nested dicts/objects. Returns default on any failure."""
    current = d
    for k in keys:
        if isinstance(current, dict):
            current = current.get(k, default)
        else:
            return default
        if current is None or (isinstance(current, str) and "no data" in current.lower()):
            return default
    return current


def to_py(val):
    """Convert numpy/pandas scalar to Python native (or None)."""
    if val is None:
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        v = float(val)
        if np.isnan(v) or np.isinf(v):
            return None
        return round(v, 6)
    if isinstance(val, float):
        if np.isnan(val) or np.isinf(val):
            return None
        return round(val, 6)
    if isinstance(val, (pd.Timestamp, pd.NaT.__class__)):
        return None
    return val


def clean_series(s: pd.Series) -> pd.Series:
    """Replace inf with NaN in a series."""
    return s.replace([np.inf, -np.inf], np.nan)


def compute_return(hist: pd.DataFrame, days: int) -> Optional[float]:
    """Compute simple return over the last N trading days from a history DataFrame."""
    if hist is None or len(hist) < days + 1:
        return None
    try:
        recent = hist["close"].iloc[-1]
        past = hist["close"].iloc[-days]
        if past == 0 or pd.isna(past) or pd.isna(recent):
            return None
        return to_py(((recent - past) / past) * 100)
    except Exception:
        return None


def compute_ytd_return(hist: pd.DataFrame) -> Optional[float]:
    """Compute YTD return from the first trading day of the current year."""
    if hist is None or hist.empty:
        return None
    try:
        current_year = datetime.now().year
        ytd = hist[hist.index >= f"{current_year}-01-01"]
        if len(ytd) < 2:
            return None
        start = ytd["close"].iloc[0]
        end = ytd["close"].iloc[-1]
        if start == 0 or pd.isna(start) or pd.isna(end):
            return None
        return to_py(((end - start) / start) * 100)
    except Exception:
        return None


def compute_max_drawdown(hist: pd.DataFrame, days: int = TRADING_DAYS_1Y) -> Optional[float]:
    """Largest peak-to-trough decline in trailing N trading days."""
    if hist is None or len(hist) < 10:
        return None
    try:
        closes = hist["close"].iloc[-days:]
        running_max = closes.cummax()
        drawdown = (closes - running_max) / running_max
        dd = drawdown.min()
        return to_py(dd * 100)
    except Exception:
        return None


def compute_sharpe(hist: pd.DataFrame, risk_free: float = RISK_FREE_RATE) -> Optional[float]:
    """Annualised Sharpe ratio over trailing 1 year."""
    if hist is None or len(hist) < TRADING_DAYS_1Y:
        return None
    try:
        closes = hist["close"].iloc[-TRADING_DAYS_1Y:]
        daily_ret = closes.pct_change().dropna()
        if len(daily_ret) < 100:
            return None
        ann_ret = (1 + daily_ret.mean()) ** 252 - 1
        ann_vol = daily_ret.std() * np.sqrt(252)
        if ann_vol == 0 or np.isnan(ann_vol):
            return None
        return to_py((ann_ret - risk_free) / ann_vol)
    except Exception:
        return None


def compute_alpha(stock_hist: pd.DataFrame, bench_hist: pd.DataFrame, days: int) -> Optional[float]:
    """Stock return minus benchmark return over N trading days."""
    sr = compute_return(stock_hist, days)
    br = compute_return(bench_hist, days)
    if sr is None or br is None:
        return None
    return to_py(sr - br)


def compute_mean_reversion(hist: pd.DataFrame) -> Optional[MeanReversion]:
    """Distance from 5-year SMA, plus 5Y standard deviation and z-score.

    Z-score = (current - mean) / stdev, in σ units of the 5Y close distribution.
    This is what analyze.py uses for mean-reversion gating — more robust
    than raw % distance because it adapts to each stock's own volatility.
    """
    if hist is None or len(hist) < TRADING_DAYS_5Y:
        return None
    try:
        window = hist["close"].iloc[-TRADING_DAYS_5Y:]
        sma = window.mean()
        stdev = window.std()
        current = hist["close"].iloc[-1]
        if pd.isna(sma) or sma == 0 or pd.isna(current):
            return None
        dist = ((current - sma) / sma) * 100
        z = None
        if stdev and not pd.isna(stdev) and stdev > 0:
            z = (current - sma) / stdev
        return MeanReversion(
            fiveYearAverage=to_py(sma),
            distancePercentage=to_py(dist),
            standardDeviation=to_py(stdev),
            zScore=to_py(z),
        )
    except Exception:
        return None


def compute_volatility_percentile(hist: pd.DataFrame) -> Optional[int]:
    """30-day realised vol ranked as percentile against 5Y rolling vol history."""
    if hist is None or len(hist) < TRADING_DAYS_1Y:
        return None
    try:
        closes = hist["close"]
        daily_ret = closes.pct_change().dropna()
        rolling_vol = daily_ret.rolling(30).std() * np.sqrt(252)
        rolling_vol = rolling_vol.dropna()
        if len(rolling_vol) < 30:
            return None
        current_vol = rolling_vol.iloc[-1]
        if pd.isna(current_vol):
            return None
        percentile = (rolling_vol < current_vol).sum() / len(rolling_vol) * 100
        return int(round(percentile))
    except Exception:
        return None


def compute_correlation_to_osebx(stock_hist: pd.DataFrame, osebx_hist: pd.DataFrame) -> Optional[float]:
    """Pearson correlation of daily returns, trailing 1Y, aligned by date."""
    if stock_hist is None or osebx_hist is None:
        return None
    try:
        sr = stock_hist["close"].pct_change().dropna()
        br = osebx_hist["close"].pct_change().dropna()
        combined = pd.concat([sr, br], axis=1, join="inner")
        combined.columns = ["stock", "bench"]
        combined = combined.iloc[-TRADING_DAYS_1Y:]
        combined = combined.dropna()
        if len(combined) < 50:
            return None
        corr = combined["stock"].corr(combined["bench"])
        return to_py(corr)
    except Exception:
        return None


def compute_beta_to_brent(stock_hist: pd.DataFrame, brent_hist: pd.DataFrame) -> Optional[float]:
    """OLS beta of stock daily returns vs Brent crude daily returns, trailing 1Y."""
    if stock_hist is None or brent_hist is None:
        return None
    try:
        sr = stock_hist["close"].pct_change().dropna()
        br = brent_hist["close"].pct_change().dropna()
        combined = pd.concat([sr, br], axis=1, join="inner")
        combined.columns = ["stock", "brent"]
        combined = combined.iloc[-TRADING_DAYS_1Y:]
        combined = combined.dropna()
        if len(combined) < 50:
            return None
        cov = combined["stock"].cov(combined["brent"])
        var = combined["brent"].var()
        if var == 0 or np.isnan(var):
            return None
        return to_py(cov / var)
    except Exception:
        return None


def compute_momentum_scores(companies: list[dict], hists: dict) -> dict[str, float]:
    """
    Compute raw momentum for each ticker, then normalise to 0-100.
    Momentum = 0.3*(12M_return_ex_1M) + 0.3*(6M_return) + 0.25*(3M_return) + 0.15*(1M_return)
    """
    raw = {}
    for c in companies:
        ticker = c["ticker"]
        h = hists.get(ticker)
        if h is None or len(h) < TRADING_DAYS_1Y:
            continue
        try:
            closes = h["close"]
            ret_1m = (closes.iloc[-1] / closes.iloc[-TRADING_DAYS_1M] - 1) if len(closes) > TRADING_DAYS_1M else None
            ret_3m = (closes.iloc[-1] / closes.iloc[-TRADING_DAYS_3M] - 1) if len(closes) > TRADING_DAYS_3M else None
            ret_6m = (closes.iloc[-1] / closes.iloc[-TRADING_DAYS_6M] - 1) if len(closes) > TRADING_DAYS_6M else None
            ret_12m = (closes.iloc[-1] / closes.iloc[-TRADING_DAYS_1Y] - 1) if len(closes) > TRADING_DAYS_1Y else None

            if any(v is None or pd.isna(v) for v in [ret_1m, ret_3m, ret_6m, ret_12m]):
                continue

            ret_12m_ex_1m = ret_12m - ret_1m
            score = 0.3 * ret_12m_ex_1m + 0.3 * ret_6m + 0.25 * ret_3m + 0.15 * ret_1m
            if not np.isnan(score) and not np.isinf(score):
                raw[ticker] = float(score)
        except Exception:
            continue

    if not raw:
        return {}

    min_s = min(raw.values())
    max_s = max(raw.values())
    rng = max_s - min_s
    if rng == 0:
        return {t: 50.0 for t in raw}

    return {t: round((v - min_s) / rng * 100, 1) for t, v in raw.items()}


def compute_seasonality(hist: pd.DataFrame) -> Optional[list[Optional[float]]]:
    """Average return for each calendar month over last 5 years."""
    if hist is None or len(hist) < TRADING_DAYS_1Y * 2:
        return None
    try:
        closes = hist["close"].copy()
        monthly = closes.resample("ME").last()
        monthly_ret = monthly.pct_change().dropna()
        cutoff = monthly_ret.index[-1] - pd.DateOffset(years=5)
        monthly_ret = monthly_ret[monthly_ret.index >= cutoff]
        if len(monthly_ret) < 12:
            return None
        avg_by_month = monthly_ret.groupby(monthly_ret.index.month).mean()
        result = []
        for m in range(1, 13):
            if m in avg_by_month.index:
                result.append(to_py(avg_by_month[m] * 100))
            else:
                result.append(None)
        return result
    except Exception:
        return None


def compute_dividend_consistency(ticker_obj, ticker_str: str) -> Optional[DividendConsistency]:
    """Assess dividend payment history over the last 5 calendar years."""
    try:
        # Fetch a 5-year window anchored to the current year so it doesn't
        # silently degrade as years pass. Previously hardcoded to 2021-01-01,
        # which would miss the most recent payment year starting in 2027.
        current_year = datetime.now().year
        start = f"{current_year - 5}-01-01"
        divs = ticker_obj.dividend_history(start=start)
        if isinstance(divs, str) or divs is None or divs.empty:
            return DividendConsistency(yearsWithDividend=0, trend="none")

        divs = divs.reset_index()
        if "date" in divs.columns:
            date_col = "date"
        elif "index" in divs.columns:
            date_col = "index"
        else:
            date_col = divs.columns[0]

        divs["year"] = pd.to_datetime(divs[date_col]).dt.year

        div_col = None
        for candidate in ["dividends", "dividend", "amount"]:
            if candidate in [c.lower() for c in divs.columns]:
                div_col = [c for c in divs.columns if c.lower() == candidate][0]
                break
        if div_col is None:
            numeric_cols = divs.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                return DividendConsistency(yearsWithDividend=0, trend="none")
            div_col = numeric_cols[-1]

        target_years = list(range(current_year - 5, current_year))
        yearly_totals = divs.groupby("year")[div_col].sum()

        years_with = sum(1 for y in target_years if y in yearly_totals.index and yearly_totals[y] > 0)

        paid_years = sorted([y for y in target_years if y in yearly_totals.index and yearly_totals[y] > 0])
        if len(paid_years) < 2:
            trend = "none" if years_with == 0 else "stable"
        else:
            first_val = yearly_totals[paid_years[0]]
            last_val = yearly_totals[paid_years[-1]]
            if last_val > first_val * 1.05:
                trend = "growing"
            elif last_val < first_val * 0.90:
                trend = "cut"
            else:
                trend = "stable"

        return DividendConsistency(yearsWithDividend=years_with, trend=trend)
    except Exception:
        return DividendConsistency(yearsWithDividend=0, trend="none")


# ---------------------------------------------------------------------------
# Main data fetching logic
# ---------------------------------------------------------------------------

def fetch_history(ticker_str: str, period: str = "10y") -> Optional[pd.DataFrame]:
    """Fetch price history for a single ticker. Returns DataFrame or None."""
    try:
        t = Ticker(ticker_str)
        h = t.history(period=period)
        if isinstance(h, str) or h is None or h.empty:
            return None
        if isinstance(h.index, pd.MultiIndex):
            h = h.droplevel("symbol")
        h.index = pd.to_datetime(h.index)
        h = h.sort_index()
        h.columns = [c.lower() for c in h.columns]
        return h
    except Exception as e:
        log.warning(f"  History fetch failed for {ticker_str}: {e}")
        return None


def fetch_benchmarks() -> tuple[dict[str, pd.DataFrame], list[BenchmarkData]]:
    """Fetch history for OSEBX, OBX, OSEFX, and Brent crude.

    OSEFX has patchy coverage on yahooquery — if `OSEFX.OL` returns nothing,
    we retry with `^OSEFX`. If both fail, the record is kept with null
    returns and the frontend should hide the pill (see index.html).
    """
    histories = {}
    benchmark_records = []

    # Primary fetch attempt
    for tk in BENCHMARK_TICKERS + [BRENT_TICKER]:
        log.info(f"  Fetching benchmark: {tk}")
        h = fetch_history(tk, period="10y")
        if h is not None:
            histories[tk] = h
            log.info(f"    Got {len(h)} data points")
        else:
            log.warning(f"    No data for {tk}")
        time.sleep(0.3)

    # OSEFX fallback — try the alternate symbol if the primary fetch came back empty
    if histories.get("OSEFX.OL") is None:
        log.info("  OSEFX.OL empty — retrying with ^OSEFX")
        h_alt = fetch_history("^OSEFX", period="10y")
        if h_alt is not None:
            histories["OSEFX.OL"] = h_alt  # store under canonical key so downstream works
            log.info(f"    Got {len(h_alt)} data points from ^OSEFX")
        else:
            log.warning("    ^OSEFX also empty — OSEFX will be null this run")
        time.sleep(0.3)

    name_map = {"OSEBX.OL": "OSEBX", "OBX.OL": "OBX", "OSEFX.OL": "OSEFX"}
    for tk in BENCHMARK_TICKERS:
        h = histories.get(tk)
        rec = BenchmarkData(
            ticker=tk,
            name=name_map.get(tk, tk),
            returnsYTD=compute_ytd_return(h) if h is not None else None,
            returns6M=compute_return(h, TRADING_DAYS_6M) if h is not None else None,
            returns1Y=compute_return(h, TRADING_DAYS_1Y) if h is not None else None,
            latestClose=to_py(h["close"].iloc[-1]) if h is not None and len(h) > 0 else None,
        )
        benchmark_records.append(rec)

    return histories, benchmark_records


def fetch_fundamentals_batch(tickers: list[str]) -> dict:
    """Fetch fundamental data for a batch of tickers. Returns a dict keyed by ticker."""
    joined = " ".join(tickers)
    t = Ticker(joined)

    result = {}
    try:
        prices = t.price
        summaries = t.summary_detail
        profiles = t.summary_profile
        keystats = t.key_stats
        fin_data = t.financial_data
    except Exception as e:
        log.warning(f"  Batch fundamental fetch failed: {e}")
        return result

    for tk in tickers:
        data = {}
        p = safe_get(prices, tk)
        if p and isinstance(p, dict):
            data["companyName"] = p.get("shortName") or p.get("longName")
            data["marketCap"] = p.get("marketCap")
            data["currentPrice"] = p.get("regularMarketPrice")

        sd = safe_get(summaries, tk)
        if sd and isinstance(sd, dict):
            data["peRatio"] = sd.get("trailingPE")
            data["forwardPE"] = sd.get("forwardPE")
            data["dividendYield"] = sd.get("dividendYield")
            if data["dividendYield"] is not None:
                data["dividendYield"] = data["dividendYield"] * 100
            data["week52Low"] = sd.get("fiftyTwoWeekLow")
            data["week52High"] = sd.get("fiftyTwoWeekHigh")
            data["dividendRate"] = sd.get("dividendRate")
            data["trailingEPS_from_PE"] = None
            if data.get("peRatio") and data.get("currentPrice") and data["peRatio"] > 0:
                data["trailingEPS_from_PE"] = data["currentPrice"] / data["peRatio"]

        sp = safe_get(profiles, tk)
        if sp and isinstance(sp, dict):
            data["sector"] = sp.get("sector")
            data["industry"] = sp.get("industry")

        ks = safe_get(keystats, tk)
        if ks and isinstance(ks, dict):
            data["pbRatio"] = ks.get("priceToBook")
            data["enterpriseValue"] = ks.get("enterpriseValue")
            data["trailingEPS"] = ks.get("trailingEps")

        fd = safe_get(fin_data, tk)
        if fd and isinstance(fd, dict):
            data["analystTarget"] = fd.get("targetMeanPrice")
            data["recommendation"] = fd.get("recommendationKey")
            data["ebitda"] = fd.get("ebitda")

        result[tk] = data

    return result


def process_company(
    ticker_str: str,
    display_name: str,
    sector: str,
    weight: float,
    in_osebx: bool,
    ask_eligible: bool,
    fundamentals: dict,
    hist: Optional[pd.DataFrame],
    osebx_hist: Optional[pd.DataFrame],
    brent_hist: Optional[pd.DataFrame],
) -> Optional[CompanyRecord]:
    """Build a CompanyRecord for one company, computing all derived metrics."""
    f = fundamentals or {}

    company_name = f.get("companyName") or display_name
    current_price = f.get("currentPrice")

    w52 = Week52(
        low=to_py(f.get("week52Low")),
        high=to_py(f.get("week52High")),
        current=to_py(current_price),
    )

    ytd = compute_ytd_return(hist)
    r6m = compute_return(hist, TRADING_DAYS_6M)
    r1y = compute_return(hist, TRADING_DAYS_1Y)

    alpha = Alpha(
        threeMonth=compute_alpha(hist, osebx_hist, TRADING_DAYS_3M),
        sixMonth=compute_alpha(hist, osebx_hist, TRADING_DAYS_6M),
        oneYear=compute_alpha(hist, osebx_hist, TRADING_DAYS_1Y),
    )

    sharpe = compute_sharpe(hist)
    mdd = compute_max_drawdown(hist)
    mr = compute_mean_reversion(hist)
    vol_pct = compute_volatility_percentile(hist)
    corr = compute_correlation_to_osebx(hist, osebx_hist)
    beta_brent = compute_beta_to_brent(hist, brent_hist)
    season = compute_seasonality(hist)

    eps = f.get("trailingEPS") or f.get("trailingEPS_from_PE")
    earnings_yield = None
    if eps and current_price and current_price > 0:
        earnings_yield = to_py((eps / current_price) * 100)

    ev_ebitda = None
    ev = f.get("enterpriseValue")
    ebitda = f.get("ebitda")
    if ev and ebitda and ebitda > 0:
        ev_ebitda = to_py(ev / ebitda)

    div_payout = None
    div_rate = f.get("dividendRate")
    if div_rate and eps and eps > 0:
        div_payout = to_py((div_rate / eps) * 100)

    liquidity = None
    if hist is not None and len(hist) > 30 and "volume" in hist.columns and current_price:
        try:
            avg_vol = hist["volume"].iloc[-30:].mean()
            if not pd.isna(avg_vol):
                liquidity = to_py(avg_vol * current_price)
        except Exception:
            pass

    # Normalise sector: prefer Yahoo's sector, fall back to hardcoded. Apply
    # norm_sector to BOTH so a typo in the CONSTITUENTS list also gets mapped
    # to a canonical GICS name (previously only Yahoo's value was normalised).
    yahoo_sector = f.get("sector")
    actual_sector = norm_sector(yahoo_sector or sector)

    record = CompanyRecord(
        ticker=ticker_str,
        companyName=company_name,
        sector=actual_sector,
        industry=f.get("industry"),
        osebxWeight=weight if weight > 0 else None,
        inOSEBX=in_osebx,
        askEligible=ask_eligible,
        marketCap=to_py(f.get("marketCap")),
        peRatio=to_py(f.get("peRatio")),
        forwardPE=to_py(f.get("forwardPE")),
        pbRatio=to_py(f.get("pbRatio")),
        dividendYield=to_py(f.get("dividendYield")),
        week52=w52,
        analystTarget=to_py(f.get("analystTarget")),
        recommendation=f.get("recommendation"),
        returnsYTD=ytd,
        returns6M=r6m,
        returns1Y=r1y,
        alpha=alpha,
        sharpeRatio=sharpe,
        momentumScore=None,
        maxDrawdown=mdd,
        volatilityPercentile=vol_pct,
        correlationToOSEBX=corr,
        dividendConsistency=None,
        dividendPayoutRatio=div_payout,
        liquidityScore=liquidity,
        earningsYield=earnings_yield,
        evToEbitda=ev_ebitda,
        meanReversion=mr,
        betaToBrent=beta_brent,
        seasonality=season,
    )

    return record


def compute_sector_summary(companies: list[CompanyRecord]) -> list[SectorSummary]:
    """Compute aggregate stats per sector, requiring at least 2 companies with data.

    Sorted by totalWeight descending so the sector tabs appear heaviest-first
    in the frontend (matching prior client-side behaviour, now that the
    frontend no longer rebuilds this).
    """
    sectors: dict[str, list[CompanyRecord]] = {}
    for c in companies:
        s = c.sector or "Unknown"
        sectors.setdefault(s, []).append(c)

    summaries = []
    for sector_name, members in sectors.items():
        def avg(vals):
            # Round consistently regardless of sample size — previously the
            # single-entry branch skipped rounding, producing JSON with mixed
            # precision across sectors.
            clean = [v for v in vals if v is not None]
            if not clean:
                return None
            return round(sum(clean) / len(clean), 2)

        summaries.append(SectorSummary(
            sector=sector_name,
            companyCount=len(members),
            totalWeight=round(sum(c.osebxWeight or 0 for c in members), 2),
            avgReturnYTD=avg([c.returnsYTD for c in members]),
            avgPE=avg([c.peRatio for c in members]),
            avgEvToEbitda=avg([c.evToEbitda for c in members]),
            avgDividendYield=avg([c.dividendYield for c in members]),
            avgSharpe=avg([c.sharpeRatio for c in members]),
        ))

    summaries.sort(key=lambda s: s.totalWeight or 0, reverse=True)
    return summaries


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("Oslo Børs Heatmap Data Pipeline -- Starting")
    log.info(f"  Total stocks: {len(CONSTITUENTS)}")
    log.info(f"  OSEBX members: {sum(1 for c in CONSTITUENTS if c[4])}")
    log.info(f"  ASK-eligible: {sum(1 for c in CONSTITUENTS if c[5])}")
    log.info("=" * 60)

    # Step 1: Fetch benchmarks
    log.info("\n[Step 1/5] Fetching benchmark indices...")
    bench_histories, benchmark_records = fetch_benchmarks()
    osebx_hist = bench_histories.get("OSEBX.OL")
    brent_hist = bench_histories.get(BRENT_TICKER)

    if osebx_hist is None:
        log.warning("OSEBX history unavailable -- alpha calculations will return null")

    # Step 2: Fetch fundamentals in batches
    log.info(f"\n[Step 2/5] Fetching fundamentals for {len(CONSTITUENTS)} companies...")
    all_fundamentals: dict[str, dict] = {}
    tickers_only = [c[0] for c in CONSTITUENTS]

    for i in range(0, len(tickers_only), BATCH_SIZE):
        batch = tickers_only[i : i + BATCH_SIZE]
        log.info(f"  Batch {i // BATCH_SIZE + 1}: {', '.join(batch)}")
        try:
            batch_data = fetch_fundamentals_batch(batch)
            all_fundamentals.update(batch_data)
        except Exception as e:
            log.warning(f"  Batch failed: {e}")
        time.sleep(FETCH_DELAY)

    # Step 3: Fetch individual histories + compute per-company metrics
    log.info(f"\n[Step 3/5] Fetching price histories and computing metrics...")
    all_histories: dict[str, pd.DataFrame] = {}
    company_records: list[CompanyRecord] = []

    for idx, (ticker, name, sector, weight, in_osebx, ask_eligible) in enumerate(CONSTITUENTS, 1):
        log.info(f"  [{idx}/{len(CONSTITUENTS)}] {ticker} -- {name}")
        try:
            hist = fetch_history(ticker, period="10y")
            if hist is not None:
                all_histories[ticker] = hist
                log.info(f"    {len(hist)} data points")
            else:
                log.warning(f"    No history data")

            fund = all_fundamentals.get(ticker, {})
            record = process_company(
                ticker, name, sector, weight, in_osebx, ask_eligible, fund,
                hist, osebx_hist, brent_hist,
            )

            if record:
                try:
                    t_obj = Ticker(ticker)
                    record.dividendConsistency = compute_dividend_consistency(t_obj, ticker)
                except Exception:
                    pass

                company_records.append(record)
            else:
                log.warning(f"    Skipped (no usable data)")

        except Exception as e:
            log.warning(f"    Failed: {e}")

        if idx % 5 == 0:
            time.sleep(0.3)

    # Step 4: Compute momentum scores (requires all companies)
    log.info(f"\n[Step 4/5] Computing momentum scores...")
    momentum_scores = compute_momentum_scores(
        [{"ticker": c.ticker} for c in company_records],
        all_histories,
    )
    for record in company_records:
        if record.ticker in momentum_scores:
            record.momentumScore = momentum_scores[record.ticker]

    # Step 5: Build output
    log.info(f"\n[Step 5/5] Building output JSON...")
    sector_summary = compute_sector_summary(company_records)

    output = HeatmapData(
        lastUpdated=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        referenceRates={
            "riskFreeRate": RISK_FREE_RATE,
            "bondYield10Y": BOND_YIELD_10Y,
        },
        benchmarks=benchmark_records,
        companies=company_records,
        sectorSummary=sector_summary,
    )

    output_dict = output.model_dump(mode="json")
    json_str = json.dumps(output_dict, indent=2, ensure_ascii=False, default=str)

    with open("data.json", "w", encoding="utf-8") as f:
        f.write(json_str)

    log.info(f"\n{'=' * 60}")
    log.info(f"Done! {len(company_records)} companies written to data.json")
    log.info(f"  Sectors: {len(sector_summary)}")
    log.info(f"  Benchmarks: {len(benchmark_records)}")
    log.info(f"  File size: {len(json_str):,} bytes")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
