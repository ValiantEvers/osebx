#!/usr/bin/env python3
"""
OSEBX Sector Heatmap — Data Pipeline
Fetches sector index data from Yahoo Finance via yfinance,
computes return metrics, and writes data.json for the frontend.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf


# ── Configuration ──────────────────────────────────────────────────────────────

BENCHMARKS = {
    "OSEBX": {"ticker": "OSEBX.OL", "name": "Oslo Børs Benchmark Index"},
    "OBX":   {"ticker": "OBX.OL",   "name": "OBX Total Return Index"},
    "OSEFX": {"ticker": "OSEFX.OL", "name": "Oslo Børs Mutual Fund Index"},
}

# GICS sector sub-indices on Oslo Børs
# Format: (ticker_suffix, sectorName, osebxWeight placeholder, peRatio, pbRatio,
#          dividendYield, analystConsensus, topHoldings, marketCapBreakdown)
SECTORS = [
    {
        "ticker_yf": "OSE10GI.OL",
        "ticker_display": "OSE10GI",
        "sectorName": "Energy",
        "osebxWeight": 32.5,
        "peRatio": 8.2, "pbRatio": 1.9, "dividendYield": 5.8,
        "analystConsensus": "Buy",
        "topHoldings": [
            {"ticker": "EQNR", "name": "Equinor", "weight": 58.2},
            {"ticker": "AKRBP", "name": "Aker BP", "weight": 14.1},
            {"ticker": "VAR", "name": "Vår Energi", "weight": 9.8},
            {"ticker": "AKSO", "name": "Aker Solutions", "weight": 5.3},
            {"ticker": "TGS", "name": "TGS", "weight": 4.1},
        ],
        "marketCapBreakdown": {"large": 78, "mid": 17, "small": 5},
    },
    {
        "ticker_yf": "OSE40GI.OL",
        "ticker_display": "OSE40GI",
        "sectorName": "Financials",
        "osebxWeight": 18.7,
        "peRatio": 10.5, "pbRatio": 1.2, "dividendYield": 6.1,
        "analystConsensus": "Buy",
        "topHoldings": [
            {"ticker": "DNB", "name": "DNB Bank", "weight": 52.3},
            {"ticker": "STB", "name": "Storebrand", "weight": 16.8},
            {"ticker": "GJFS", "name": "Gjensidige Forsikring", "weight": 11.2},
            {"ticker": "SBANK", "name": "SpareBank 1 SR-Bank", "weight": 7.4},
            {"ticker": "PARB", "name": "Pareto Bank", "weight": 3.1},
        ],
        "marketCapBreakdown": {"large": 70, "mid": 22, "small": 8},
    },
    {
        "ticker_yf": "OSE30GI.OL",
        "ticker_display": "OSE30GI",
        "sectorName": "Consumer Staples",
        "osebxWeight": 12.3,
        "peRatio": 14.8, "pbRatio": 2.8, "dividendYield": 3.2,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "MOWI", "name": "Mowi", "weight": 35.6},
            {"ticker": "SALM", "name": "SalMar", "weight": 18.4},
            {"ticker": "LSG", "name": "Lerøy Seafood", "weight": 12.9},
            {"ticker": "BAKKA", "name": "Bakkafrost", "weight": 10.2},
            {"ticker": "ORK", "name": "Orkla", "weight": 9.8},
        ],
        "marketCapBreakdown": {"large": 55, "mid": 35, "small": 10},
    },
    {
        "ticker_yf": "OSE20GI.OL",
        "ticker_display": "OSE20GI",
        "sectorName": "Industrials",
        "osebxWeight": 13.1,
        "peRatio": 18.3, "pbRatio": 2.4, "dividendYield": 2.1,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "KOG", "name": "Kongsberg Gruppen", "weight": 22.4},
            {"ticker": "KAHOT", "name": "Kahoot!", "weight": 14.2},
            {"ticker": "TOMRA", "name": "Tomra Systems", "weight": 12.8},
            {"ticker": "WILS", "name": "Wilh. Wilhelmsen", "weight": 9.1},
            {"ticker": "FLNG", "name": "Flex LNG", "weight": 7.5},
        ],
        "marketCapBreakdown": {"large": 45, "mid": 40, "small": 15},
    },
    {
        "ticker_yf": "OSE15GI.OL",
        "ticker_display": "OSE15GI",
        "sectorName": "Materials",
        "osebxWeight": 6.8,
        "peRatio": 22.1, "pbRatio": 1.6, "dividendYield": 1.4,
        "analystConsensus": "Sell",
        "topHoldings": [
            {"ticker": "NHY", "name": "Norsk Hydro", "weight": 62.4},
            {"ticker": "YAR", "name": "Yara International", "weight": 24.1},
            {"ticker": "BOR", "name": "Borregaard", "weight": 8.3},
            {"ticker": "ELMRA", "name": "Elkem", "weight": 3.8},
            {"ticker": "RECSI", "name": "REC Silicon", "weight": 1.4},
        ],
        "marketCapBreakdown": {"large": 88, "mid": 10, "small": 2},
    },
    {
        "ticker_yf": "OSE45GI.OL",
        "ticker_display": "OSE45GI",
        "sectorName": "IT",
        "osebxWeight": 5.4,
        "peRatio": 34.2, "pbRatio": 5.1, "dividendYield": 0.8,
        "analystConsensus": "Buy",
        "topHoldings": [
            {"ticker": "NOD", "name": "Nordic Semiconductor", "weight": 22.4},
            {"ticker": "CRAYN", "name": "Crayon Group", "weight": 28.6},
            {"ticker": "OPER", "name": "Opera", "weight": 18.5},
            {"ticker": "LINK", "name": "Link Mobility", "weight": 11.3},
            {"ticker": "VOLUE", "name": "Volue", "weight": 8.2},
        ],
        "marketCapBreakdown": {"large": 30, "mid": 45, "small": 25},
    },
    {
        "ticker_yf": "OSE50GI.OL",
        "ticker_display": "OSE50GI",
        "sectorName": "Telecom",
        "osebxWeight": 5.9,
        "peRatio": 16.4, "pbRatio": 3.2, "dividendYield": 4.5,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "TEL", "name": "Telenor", "weight": 82.3},
            {"ticker": "TEKNA", "name": "Tekna Holding", "weight": 6.1},
            {"ticker": "NTEL", "name": "NextGenTel", "weight": 4.8},
            {"ticker": "CXENSE", "name": "Cxense", "weight": 3.9},
            {"ticker": "ITERA", "name": "Itera", "weight": 2.9},
        ],
        "marketCapBreakdown": {"large": 85, "mid": 10, "small": 5},
    },
    {
        "ticker_yf": "OSE55GI.OL",
        "ticker_display": "OSE55GI",
        "sectorName": "Utilities",
        "osebxWeight": 3.2,
        "peRatio": 20.6, "pbRatio": 2.0, "dividendYield": 3.8,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "HAFNI", "name": "Hafslund", "weight": 42.1},
            {"ticker": "SCATC", "name": "Scatec", "weight": 28.4},
            {"ticker": "AGAS", "name": "Aker Horizons", "weight": 12.3},
            {"ticker": "CLOUDBERRY", "name": "Cloudberry Clean Energy", "weight": 9.8},
            {"ticker": "SMSN", "name": "SmallCap Green", "weight": 7.4},
        ],
        "marketCapBreakdown": {"large": 40, "mid": 42, "small": 18},
    },
    {
        "ticker_yf": "OSE60GI.OL",
        "ticker_display": "OSE60GI",
        "sectorName": "Real Estate",
        "osebxWeight": 2.1,
        "peRatio": 12.9, "pbRatio": 0.8, "dividendYield": 5.2,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "ENTRA", "name": "Entra", "weight": 38.6},
            {"ticker": "OBOS", "name": "OBOS BBL", "weight": 22.4},
            {"ticker": "SELVA", "name": "Selvaag Bolig", "weight": 15.8},
            {"ticker": "SBO", "name": "Self Storage Group", "weight": 12.3},
            {"ticker": "NWRE", "name": "Norwegian Property", "weight": 10.9},
        ],
        "marketCapBreakdown": {"large": 40, "mid": 38, "small": 22},
    },
]

# Minimum trading days for 5-year SMA
SMA_WINDOW = 1260
HISTORY_PERIOD = "10y"

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data.json"


# ── Helpers ────────────────────────────────────────────────────────────────────

def compute_return(hist: pd.DataFrame, days_ago: int) -> float | None:
    """Compute percentage return over the last N trading days."""
    if hist.empty or len(hist) < 2:
        return None
    try:
        current = float(hist["Close"].iloc[-1])
        if len(hist) >= days_ago:
            past = float(hist["Close"].iloc[-days_ago])
        else:
            past = float(hist["Close"].iloc[0])
        if past == 0:
            return None
        return round((current - past) / past * 100, 2)
    except Exception:
        return None


def compute_ytd(hist: pd.DataFrame) -> float | None:
    """Compute year-to-date return."""
    if hist.empty or len(hist) < 2:
        return None
    try:
        current = float(hist["Close"].iloc[-1])
        year_start = hist.index[0].year
        current_year = datetime.now().year
        # Find first trading day of current year
        this_year = hist[hist.index.year == current_year]
        if this_year.empty:
            return None
        first_price = float(this_year["Close"].iloc[0])
        if first_price == 0:
            return None
        return round((current - first_price) / first_price * 100, 2)
    except Exception:
        return None


def compute_52w(hist: pd.DataFrame) -> dict | None:
    """Compute 52-week high, low, and current price."""
    if hist.empty:
        return None
    try:
        last_252 = hist.tail(252)
        return {
            "low": round(float(last_252["Close"].min()), 2),
            "high": round(float(last_252["Close"].max()), 2),
            "current": round(float(hist["Close"].iloc[-1]), 2),
        }
    except Exception:
        return None


def compute_mean_reversion(hist: pd.DataFrame) -> dict | None:
    """Compute 5-year SMA and distance from it."""
    if hist.empty or len(hist) < SMA_WINDOW:
        return None
    try:
        sma = float(hist["Close"].tail(SMA_WINDOW).mean())
        current = float(hist["Close"].iloc[-1])
        if sma == 0:
            return None
        distance = round((current - sma) / sma * 100, 2)
        return {
            "fiveYearAverage": round(sma, 2),
            "distancePercentage": distance,
        }
    except Exception:
        return None


def fetch_ticker_data(ticker_yf: str) -> pd.DataFrame | None:
    """Fetch historical data for a ticker, returning None on failure."""
    try:
        t = yf.Ticker(ticker_yf)
        hist = t.history(period=HISTORY_PERIOD)
        if hist.empty:
            print(f"  ⚠  No data returned for {ticker_yf}, skipping.")
            return None
        print(f"  ✓  {ticker_yf}: {len(hist)} rows fetched.")
        return hist
    except Exception as e:
        print(f"  ✗  Error fetching {ticker_yf}: {e}")
        return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("OSEBX Heatmap — Data Update")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    # ── Benchmarks ─────────────────────────────────────────────
    print("\n→ Fetching benchmarks...")
    benchmarks_out = {}
    for key, meta in BENCHMARKS.items():
        hist = fetch_ticker_data(meta["ticker"])
        if hist is not None:
            ytd = compute_ytd(hist)
            current = round(float(hist["Close"].iloc[-1]), 2) if not hist.empty else None
            benchmarks_out[key] = {
                "ticker": meta["ticker"],
                "name": meta["name"],
                "ytd": ytd,
                "current": current,
            }
        else:
            # Keep previous data if available
            benchmarks_out[key] = {
                "ticker": meta["ticker"],
                "name": meta["name"],
                "ytd": None,
                "current": None,
            }

    # ── Sectors ────────────────────────────────────────────────
    print("\n→ Fetching sector indices...")
    sectors_out = []
    for sector in SECTORS:
        print(f"\n  [{sector['sectorName']}]")
        hist = fetch_ticker_data(sector["ticker_yf"])

        if hist is not None:
            ytd = compute_ytd(hist)
            r6m = compute_return(hist, 126)   # ~6 months
            r1y = compute_return(hist, 252)   # ~1 year
            w52 = compute_52w(hist)
            mr = compute_mean_reversion(hist)
        else:
            print(f"    → Using placeholder values for {sector['sectorName']}")
            ytd = None
            r6m = None
            r1y = None
            w52 = None
            mr = None

        sectors_out.append({
            "sectorName": sector["sectorName"],
            "ticker": sector["ticker_display"],
            "osebxWeight": sector["osebxWeight"],
            "returnsYTD": ytd,
            "returns6M": r6m,
            "returns1Y": r1y,
            # TODO: These are static placeholders. Enrich from individual
            # stock tickers' fundamentals if needed in the future.
            "peRatio": sector["peRatio"],
            "pbRatio": sector["pbRatio"],
            "dividendYield": sector["dividendYield"],
            "analystConsensus": sector["analystConsensus"],
            "topHoldings": sector["topHoldings"],
            "marketCapBreakdown": sector["marketCapBreakdown"],
            "week52": w52,
            "meanReversion": mr,
        })

    # ── Write JSON ─────────────────────────────────────────────
    output = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "benchmarks": benchmarks_out,
        "sectors": sectors_out,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n✓ data.json written to {OUTPUT_PATH}")
    print(f"  Sectors: {len(sectors_out)}")
    print(f"  Timestamp: {output['lastUpdated']}")


if __name__ == "__main__":
    main()
