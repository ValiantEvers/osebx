#!/usr/bin/env python3
"""
update_data.py — Fetch OSEBX sector index data via yfinance and write data.json.
Runs weekly via GitHub Actions. Zero API costs.
"""

import json
import datetime
import yfinance as yf

# ── Sector definitions ──────────────────────────────────────────────
# Tickers for Oslo Børs GICS sector indices (GI = Gross Index)
SECTOR_TICKERS = {
    "OSE10GI.OL": "Energy",
    "OSE15GI.OL": "Materials",
    "OSE20GI.OL": "Industrials",
    "OSE25GI.OL": "Healthcare",
    "OSE30GI.OL": "Consumer Staples",
    "OSE35GI.OL": "Real Estate",
    "OSE40GI.OL": "Financials",
    "OSE45GI.OL": "IT",
    "OSE50GI.OL": "Comm. Services",
    "OSE55GI.OL": "Utilities",
}

BENCHMARK_TICKERS = ["OSEBX.OL", "OBX.OL", "OSEFX.OL"]

# ── Placeholder data yfinance cannot provide at sector-index level ──
# TODO: Enrich these from a proper data source in the future
SECTOR_META = {
    "Energy": {
        "osebxWeight": 27.8, "peRatio": 9.8, "dividendYield": 6.2,
        "analystConsensus": "Buy",
        "topHoldings": [
            {"ticker": "EQNR", "name": "Equinor", "weight": 52.1},
            {"ticker": "AKRBP", "name": "Aker BP", "weight": 18.3},
            {"ticker": "VAR", "name": "Vår Energi", "weight": 11.7},
            {"ticker": "SUBC", "name": "Subsea 7", "weight": 8.2},
            {"ticker": "TGS", "name": "TGS", "weight": 4.5},
        ],
        "marketCapBreakdown": {"large": 82, "mid": 14, "small": 4},
    },
    "Financials": {
        "osebxWeight": 16.5, "peRatio": 11.2, "dividendYield": 5.8,
        "analystConsensus": "Buy",
        "topHoldings": [
            {"ticker": "DNB", "name": "DNB Bank", "weight": 48.6},
            {"ticker": "MORG", "name": "SpareBank 1 SMN", "weight": 12.4},
            {"ticker": "SRBNK", "name": "SpareBank 1 SR-Bank", "weight": 10.8},
            {"ticker": "GJF", "name": "Gjensidige", "weight": 15.5},
            {"ticker": "SBANK", "name": "Sbanken", "weight": 7.2},
        ],
        "marketCapBreakdown": {"large": 65, "mid": 28, "small": 7},
    },
    "Consumer Staples": {
        "osebxWeight": 12.3, "peRatio": 18.5, "dividendYield": 3.1,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "MOWI", "name": "Mowi", "weight": 32.8},
            {"ticker": "SALM", "name": "SalMar", "weight": 22.1},
            {"ticker": "LSG", "name": "Lerøy Seafood", "weight": 14.6},
            {"ticker": "BAKKA", "name": "Bakkafrost", "weight": 11.3},
            {"ticker": "ORK", "name": "Orkla", "weight": 12.5},
        ],
        "marketCapBreakdown": {"large": 55, "mid": 35, "small": 10},
    },
    "Industrials": {
        "osebxWeight": 14.2, "peRatio": 22.4, "dividendYield": 1.9,
        "analystConsensus": "Buy",
        "topHoldings": [
            {"ticker": "KVAER", "name": "Kongsberg Gruppen", "weight": 28.5},
            {"ticker": "TEL", "name": "Telenor", "weight": 22.3},
            {"ticker": "TOM", "name": "Tomra Systems", "weight": 14.1},
            {"ticker": "WWI", "name": "Wallenius Wilhelmsen", "weight": 12.8},
            {"ticker": "HAFNI", "name": "Hafnia", "weight": 8.9},
        ],
        "marketCapBreakdown": {"large": 50, "mid": 38, "small": 12},
    },
    "Materials": {
        "osebxWeight": 8.7, "peRatio": 15.7, "dividendYield": 2.8,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "NHY", "name": "Norsk Hydro", "weight": 55.2},
            {"ticker": "YAR", "name": "Yara International", "weight": 28.4},
            {"ticker": "BORR", "name": "Borregaard", "weight": 8.1},
            {"ticker": "ELK", "name": "Elkem", "weight": 5.5},
            {"ticker": "AUSS", "name": "Austevoll Seafood", "weight": 2.8},
        ],
        "marketCapBreakdown": {"large": 72, "mid": 22, "small": 6},
    },
    "IT": {
        "osebxWeight": 6.8, "peRatio": 34.6, "dividendYield": 0.8,
        "analystConsensus": "Buy",
        "topHoldings": [
            {"ticker": "CRAYN", "name": "Crayon Group", "weight": 24.5},
            {"ticker": "NOD", "name": "Nordic Semiconductor", "weight": 22.8},
            {"ticker": "OPERA", "name": "Opera", "weight": 15.3},
            {"ticker": "KID", "name": "Kahoot!", "weight": 12.1},
            {"ticker": "ATEA", "name": "Atea", "weight": 10.6},
        ],
        "marketCapBreakdown": {"large": 30, "mid": 45, "small": 25},
    },
    "Real Estate": {
        "osebxWeight": 4.2, "peRatio": 28.3, "dividendYield": 4.5,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "ENTRA", "name": "Entra", "weight": 38.2},
            {"ticker": "OLT", "name": "Olav Thon", "weight": 25.6},
            {"ticker": "SCHB", "name": "Self Storage Group", "weight": 14.3},
            {"ticker": "SBO", "name": "Selvaag Bolig", "weight": 12.1},
            {"ticker": "FREST", "name": "Fredensborg", "weight": 9.8},
        ],
        "marketCapBreakdown": {"large": 40, "mid": 42, "small": 18},
    },
    "Healthcare": {
        "osebxWeight": 3.5, "peRatio": 42.1, "dividendYield": 0.4,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "PHO", "name": "Photocure", "weight": 22.5},
            {"ticker": "MEDI", "name": "Medistim", "weight": 20.8},
            {"ticker": "NAVA", "name": "Navamedic", "weight": 18.3},
            {"ticker": "ARC", "name": "Arcus", "weight": 15.6},
            {"ticker": "GIG", "name": "Gaming Innovation", "weight": 12.2},
        ],
        "marketCapBreakdown": {"large": 15, "mid": 45, "small": 40},
    },
    "Utilities": {
        "osebxWeight": 3.1, "peRatio": 19.8, "dividendYield": 5.1,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "SCATC", "name": "Scatec", "weight": 35.2},
            {"ticker": "AKER", "name": "Aker Horizons", "weight": 22.4},
            {"ticker": "AGAS", "name": "Aker Clean Hydrogen", "weight": 15.8},
            {"ticker": "VOLUE", "name": "Volue", "weight": 14.3},
            {"ticker": "NEL", "name": "Nel", "weight": 12.3},
        ],
        "marketCapBreakdown": {"large": 35, "mid": 40, "small": 25},
    },
    "Comm. Services": {
        "osebxWeight": 2.9, "peRatio": 16.3, "dividendYield": 3.4,
        "analystConsensus": "Hold",
        "topHoldings": [
            {"ticker": "SCHIBSTED", "name": "Schibsted", "weight": 42.1},
            {"ticker": "AMEDIA", "name": "Adevinta", "weight": 25.8},
            {"ticker": "REC", "name": "REC Silicon", "weight": 12.4},
            {"ticker": "OTELLO", "name": "Otello", "weight": 10.5},
            {"ticker": "LINK", "name": "Link Mobility", "weight": 9.2},
        ],
        "marketCapBreakdown": {"large": 45, "mid": 35, "small": 20},
    },
}


def compute_returns(hist):
    """Compute YTD, 6M, and 1Y returns from a price history DataFrame."""
    if hist.empty or len(hist) < 2:
        return None, None, None

    current = hist["Close"].iloc[-1]
    today = hist.index[-1]

    # YTD: from first trading day of the year
    ytd_start = hist.loc[hist.index >= f"{today.year}-01-01"]
    ytd_ret = ((current / ytd_start["Close"].iloc[0]) - 1) * 100 if len(ytd_start) > 0 else None

    # 6M
    six_m_ago = today - datetime.timedelta(days=182)
    six_m = hist.loc[hist.index >= six_m_ago]
    ret_6m = ((current / six_m["Close"].iloc[0]) - 1) * 100 if len(six_m) > 0 else None

    # 1Y
    one_y_ago = today - datetime.timedelta(days=365)
    one_y = hist.loc[hist.index >= one_y_ago]
    ret_1y = ((current / one_y["Close"].iloc[0]) - 1) * 100 if len(one_y) > 0 else None

    return (
        round(ytd_ret, 2) if ytd_ret is not None else 0,
        round(ret_6m, 2) if ret_6m is not None else 0,
        round(ret_1y, 2) if ret_1y is not None else 0,
    )


def get_52_week(hist):
    """Get 52-week high, low, and current price."""
    if hist.empty:
        return {"low": 0, "high": 0, "current": 0}
    today = hist.index[-1]
    one_y_ago = today - datetime.timedelta(days=365)
    recent = hist.loc[hist.index >= one_y_ago]
    if recent.empty:
        recent = hist
    return {
        "low": round(float(recent["Close"].min()), 2),
        "high": round(float(recent["Close"].max()), 2),
        "current": round(float(recent["Close"].iloc[-1]), 2),
    }


def main():
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # ── Benchmarks ──────────────────────────────────────────────────
    benchmarks = []
    for ticker in BENCHMARK_TICKERS:
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="1y")
            if hist.empty:
                continue
            ytd, _, _ = compute_returns(hist)
            price = round(float(hist["Close"].iloc[-1]), 2)
            name = ticker.replace(".OL", "")
            benchmarks.append({"name": name, "ticker": ticker, "price": price, "ytd": ytd})
        except Exception as e:
            print(f"Warning: Could not fetch {ticker}: {e}")

    # ── Sectors ─────────────────────────────────────────────────────
    sectors = []
    for ticker, sector_name in SECTOR_TICKERS.items():
        meta = SECTOR_META.get(sector_name, {})
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="1y")
            ytd, ret6m, ret1y = compute_returns(hist)
            w52 = get_52_week(hist)
        except Exception as e:
            print(f"Warning: Could not fetch {ticker}: {e}")
            ytd, ret6m, ret1y = 0, 0, 0
            w52 = {"low": 0, "high": 0, "current": 0}

        sectors.append({
            "sectorName": sector_name,
            "ticker": ticker.replace(".OL", ""),
            "osebxWeight": meta.get("osebxWeight", 0),
            "returnsYTD": ytd if ytd is not None else 0,
            "returns6M": ret6m if ret6m is not None else 0,
            "returns1Y": ret1y if ret1y is not None else 0,
            "peRatio": meta.get("peRatio", 0),        # TODO: enrich from real source
            "dividendYield": meta.get("dividendYield", 0),  # TODO: enrich
            "analystConsensus": meta.get("analystConsensus", "Hold"),  # TODO: enrich
            "topHoldings": meta.get("topHoldings", []),  # TODO: enrich
            "marketCapBreakdown": meta.get("marketCapBreakdown", {"large": 33, "mid": 34, "small": 33}),
            "week52": w52,
        })

    # ── Write JSON ──────────────────────────────────────────────────
    output = {"lastUpdated": now, "benchmarks": benchmarks, "sectors": sectors}
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✓ data.json updated at {now} with {len(sectors)} sectors and {len(benchmarks)} benchmarks.")


if __name__ == "__main__":
    main()
