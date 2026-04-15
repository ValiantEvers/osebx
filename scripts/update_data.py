import json
import time
import datetime
import numpy as np
import pandas as pd
from yahooquery import Ticker
from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional, Dict, Any

import random
from yahooquery import utils

# Override the default yahooquery User-Agent to pretend to be a real Chrome browser
utils.USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

# ==========================================
# PYDANTIC MODELS (Validation Layer)
# ==========================================
class Week52(BaseModel):
    low: Optional[float] = None
    high: Optional[float] = None
    current: Optional[float] = None

class MeanReversion(BaseModel):
    fiveYearAverage: Optional[float] = None
    distancePercentage: Optional[float] = None

class Alpha(BaseModel):
    threeMonth: Optional[float] = None
    sixMonth: Optional[float] = None
    oneYear: Optional[float] = None

class DividendConsistency(BaseModel):
    yearsWithDividend: Optional[int] = None
    trend: Optional[str] = None

class CompanyRecord(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=False)
    
    ticker: str
    companyName: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    osebxWeight: float
    
    # Fundamentals
    marketCap: Optional[float] = None
    peRatio: Optional[float] = None
    forwardPE: Optional[float] = None
    pbRatio: Optional[float] = None
    dividendYield: Optional[float] = None
    earningsYield: Optional[float] = None
    evToEbitda: Optional[float] = None
    dividendPayoutRatio: Optional[float] = None
    liquidityScore: Optional[float] = None
    analystTarget: Optional[float] = None
    recommendation: Optional[str] = None
    
    # Nested & Arrays
    week52: Optional[Week52] = None
    meanReversion: Optional[MeanReversion] = None
    alpha: Optional[Alpha] = None
    dividendConsistency: Optional[DividendConsistency] = None
    seasonality: Optional[List[Optional[float]]] = None
    
    # Computed Performance & Risk
    returnsYTD: Optional[float] = None
    returns6M: Optional[float] = None
    returns1Y: Optional[float] = None
    sharpeRatio: Optional[float] = None
    momentumScore: Optional[float] = None
    maxDrawdown: Optional[float] = None
    betaToBrent: Optional[float] = None
    volatilityPercentile: Optional[int] = None
    correlationToOSEBX: Optional[float] = None

class HeatmapData(BaseModel):
    lastUpdated: str
    referenceRates: Dict[str, float]
    benchmarks: Dict[str, Optional[float]]
    sectorSummary: Dict[str, Dict[str, Optional[float]]]
    companies: List[CompanyRecord]

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================
REFERENCE_RATES = {
    "riskFreeRate": 0.035, # Norwegian 3-month bill proxy
    "bondYield10Y": 0.035  # Norwegian 10-year bond proxy
}

# The hardcoded source of truth (Update manually during June/Dec rebalancing)
# Format: "TICKER": Estimated Weight %
OSEBX_CONSTITUENTS = {
    "EQNR.OL": 14.5,
    "DNB.OL": 10.2,
    "MOWI.OL": 4.8,
    # Add the remaining ~66 constituents here...
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def clean_float(val: Any) -> Optional[float]:
    """Cleans Pandas NaN, NaT, and Inf to Python None for JSON/Pydantic."""
    if pd.isna(val) or val in [np.inf, -np.inf]:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def calc_return(series: pd.Series, periods: int) -> Optional[float]:
    if len(series) < periods + 1: return None
    return clean_float((series.iloc[-1] / series.iloc[-(periods+1)]) - 1)

def get_ytd_return(series: pd.Series) -> Optional[float]:
    if series.empty: return None
    current_year = datetime.datetime.now().year
    ytd_data = series[series.index.year == current_year]
    if ytd_data.empty: return None
    return clean_float((series.iloc[-1] / ytd_data.iloc[0]) - 1)

def compute_drawdown(series: pd.Series, periods=252) -> Optional[float]:
    recent = series.tail(periods)
    if recent.empty: return None
    roll_max = recent.cummax()
    drawdown = (recent - roll_max) / roll_max
    return clean_float(drawdown.min())

def compute_beta(stock_returns: pd.Series, benchmark_returns: pd.Series) -> Optional[float]:
    # Inner join aligns trading days exactly (e.g., Oslo Børs vs ICE Brent)
    df = pd.concat([stock_returns, benchmark_returns], axis=1, join='inner').dropna()
    if len(df) < 50: return None
    cov = df.cov().iloc[0, 1]
    var = df.iloc[:, 1].var()
    return clean_float(cov / var) if var != 0 else None

def compute_seasonality(returns: pd.Series) -> List[Optional[float]]:
    if len(returns) < 252 * 2: # Need at least 2 years for meaningful averages
        return [None] * 12
    monthly_returns = returns.resample('M').apply(lambda x: (x + 1).prod() - 1)
    seasonality = monthly_returns.groupby(monthly_returns.index.month).mean()
    return [clean_float(seasonality.get(i, None)) for i in range(1, 13)]

# ==========================================
# MAIN PIPELINE
# ==========================================
def main():
    print("Starting OSEBX data pipeline...")
    
    # Sleep for a random amount of time between 5 and 20 seconds to avoid GitHub Actions IP clustering
    sleep_time = random.randint(5, 20)
    print(f"Jitter delay: sleeping for {sleep_time} seconds to avoid rate limits...")
    time.sleep(sleep_time)
    
    # 1. Fetch Benchmarks & Oil
    print("Fetching benchmark histories...")
    b_tickers = Ticker("OSEBX.OL OBX.OL OSEFX.OL BZ=F")
    b_hist = b_tickers.history(period="5y")
    
    try:
        osebx_close = b_hist.loc["OSEBX.OL", "close"]
        osebx_returns = osebx_close.pct_change().dropna()
        bz_close = b_hist.loc["BZ=F", "close"]
        bz_returns = bz_close.pct_change().dropna()
        
        benchmarks = {
            "OSEBX": get_ytd_return(osebx_close),
            "OBX": get_ytd_return(b_hist.loc["OBX.OL", "close"] if "OBX.OL" in b_hist.index else pd.Series()),
            "OSEFX": get_ytd_return(b_hist.loc["OSEFX.OL", "close"] if "OSEFX.OL" in b_hist.index else pd.Series())
        }
    except Exception as e:
        print(f"Error processing benchmarks: {e}")
        osebx_close, osebx_returns, bz_returns = pd.Series(), pd.Series(), pd.Series()
        benchmarks = {"OSEBX": None, "OBX": None, "OSEFX": None}

    companies_data = []
    tickers = list(OSEBX_CONSTITUENTS.keys())
    
    # 2. Fetch Companies
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker}...", end=" ")
        try:
            t = Ticker(ticker)
            
            # Use dict.get() safely because yahooquery can return string errors instead of dicts
            info_summary = t.summary_detail.get(ticker, {})
            info_summary = info_summary if isinstance(info_summary, dict) else {}
            
            info_profile = t.summary_profile.get(ticker, {})
            info_profile = info_profile if isinstance(info_profile, dict) else {}
            
            info_price = t.price.get(ticker, {})
            info_price = info_price if isinstance(info_price, dict) else {}
            
            info_fin = t.financial_data.get(ticker, {})
            info_fin = info_fin if isinstance(info_fin, dict) else {}
            
            info_stats = t.key_stats.get(ticker, {})
            info_stats = info_stats if isinstance(info_stats, dict) else {}

            # Core fields
            current_price = info_price.get("regularMarketPrice")
            market_cap = info_price.get("marketCap")
            eps = info_stats.get("trailingEps")
            pe = info_summary.get("trailingPE")
            
            # Calculated Valuation
            earnings_yield = None
            if eps and current_price and current_price > 0:
                earnings_yield = (eps / current_price) * 100
            elif pe and pe > 0:
                earnings_yield = (1 / pe) * 100

            ev = info_stats.get("enterpriseValue")
            ebitda = info_fin.get("ebitda")
            ev_ebitda = (ev / ebitda) if ev and ebitda and ebitda > 0 else None

            # Dividends
            div_yield = info_summary.get("dividendYield")
            if div_yield: div_yield *= 100
            
            div_rate = info_summary.get("dividendRate")
            payout_ratio = (div_rate / eps * 100) if div_rate and eps and eps > 0 else None

            # Historical processing
            hist = t.history(period="10y")
            
            ytd, ret_6m, ret_1y, sharpe, max_dd, mom_score = None, None, None, None, None, None
            beta, vol_pct, corr = None, None, None
            alpha = Alpha()
            mean_rev = MeanReversion()
            seasonality_arr = [None]*12
            
            if not isinstance(hist, dict) and not hist.empty and "close" in hist.columns:
                close = hist.reset_index().set_index('date')["close"]
                returns = close.pct_change().dropna()
                
                ytd = get_ytd_return(close)
                ret_1m = calc_return(close, 21)
                ret_3m = calc_return(close, 63)
                ret_6m = calc_return(close, 126)
                ret_1y = calc_return(close, 252)
                ret_12m_ex_1m = calc_return(close.iloc[:-21] if len(close) > 21 else close, 231)
                
                # Sharpe (1Y)
                if ret_1y is not None and len(returns) >= 252:
                    ann_vol = returns.tail(252).std() * np.sqrt(252)
                    if ann_vol > 0:
                        sharpe = (ret_1y - REFERENCE_RATES["riskFreeRate"]) / ann_vol

                # Momentum Score
                if all(v is not None for v in [ret_12m_ex_1m, ret_6m, ret_3m, ret_1m]):
                    mom_score = (0.3*ret_12m_ex_1m) + (0.3*ret_6m) + (0.25*ret_3m) + (0.15*ret_1m)
                
                # Mean Reversion (5Y = 1260 days)
                if len(close) >= 1260:
                    sma5y = close.tail(1260).mean()
                    mean_rev = MeanReversion(
                        fiveYearAverage=clean_float(sma5y),
                        distancePercentage=clean_float((current_price - sma5y)/sma5y * 100) if current_price else None
                    )

                max_dd = compute_drawdown(close, 252)
                
                # Aligned metrics (Alpha, Beta, Corr)
                if not osebx_returns.empty:
                    df_aligned = pd.concat([returns, osebx_returns], axis=1, join='inner').dropna()
                    if len(df_aligned) >= 252:
                        corr = clean_float(df_aligned.iloc[-252:, 0].corr(df_aligned.iloc[-252:, 1]))
                        
                        osebx_3m = calc_return(osebx_close, 63)
                        osebx_6m = calc_return(osebx_close, 126)
                        osebx_1y = calc_return(osebx_close, 252)
                        
                        alpha = Alpha(
                            threeMonth=clean_float(ret_3m - osebx_3m) if ret_3m and osebx_3m else None,
                            sixMonth=clean_float(ret_6m - osebx_6m) if ret_6m and osebx_6m else None,
                            oneYear=clean_float(ret_1y - osebx_1y) if ret_1y and osebx_1y else None
                        )

                if not bz_returns.empty:
                    beta = compute_beta(returns.tail(252), bz_returns.tail(252))

                # Volatility Percentile
                if len(returns) > 252:
                    rolling_vol = returns.rolling(30).std() * np.sqrt(252)
                    current_vol = rolling_vol.iloc[-1]
                    vol_pct = int((rolling_vol.rank(pct=True).iloc[-1]) * 100) if not pd.isna(current_vol) else None

                seasonality_arr = compute_seasonality(returns)

            avg_vol = info_summary.get("averageVolume") or info_price.get("averageDailyVolume3Month")
            liq_score = (avg_vol * current_price) if avg_vol and current_price else None

            # Construct Pydantic Model
            record = CompanyRecord(
                ticker=ticker,
                companyName=info_price.get("shortName") or info_price.get("longName"),
                sector=info_profile.get("sector"),
                industry=info_profile.get("industry"),
                osebxWeight=OSEBX_CONSTITUENTS[ticker],
                marketCap=clean_float(market_cap),
                peRatio=clean_float(pe),
                forwardPE=clean_float(info_summary.get("forwardPE")),
                pbRatio=clean_float(info_stats.get("priceToBook")),
                dividendYield=clean_float(div_yield),
                earningsYield=clean_float(earnings_yield),
                evToEbitda=clean_float(ev_ebitda),
                dividendPayoutRatio=clean_float(payout_ratio),
                liquidityScore=clean_float(liq_score),
                analystTarget=clean_float(info_fin.get("targetMeanPrice")),
                recommendation=info_fin.get("recommendationKey"),
                week52=Week52(
                    low=clean_float(info_summary.get("fiftyTwoWeekLow")),
                    high=clean_float(info_summary.get("fiftyTwoWeekHigh")),
                    current=clean_float(current_price)
                ),
                meanReversion=mean_rev,
                alpha=alpha,
                seasonality=seasonality_arr,
                returnsYTD=clean_float(ytd),
                returns6M=clean_float(ret_6m),
                returns1Y=clean_float(ret_1y),
                sharpeRatio=clean_float(sharpe),
                momentumScore=clean_float(mom_score),
                maxDrawdown=clean_float(max_dd),
                betaToBrent=clean_float(beta),
                volatilityPercentile=vol_pct,
                correlationToOSEBX=clean_float(corr)
            )
            
            companies_data.append(record)
            print("✓")
            time.sleep(0.4) # Rate limit safety
            
        except Exception as e:
            print(f"Failed: {e}")

    # 3. Normalise Momentum
    mom_scores = [c.momentumScore for c in companies_data if c.momentumScore is not None]
    if mom_scores:
        mom_min, mom_max = min(mom_scores), max(mom_scores)
        for c in companies_data:
            if c.momentumScore is not None:
                if mom_max > mom_min:
                    c.momentumScore = round(((c.momentumScore - mom_min) / (mom_max - mom_min)) * 100, 1)
                else:
                    c.momentumScore = 50.0

    # 4. Sector Summary
    sector_summary = {}
    for c in companies_data:
        s = c.sector or "Unknown"
        if s not in sector_summary:
            sector_summary[s] = {"count": 0, "weight": 0, "ytd_sum": 0, "pe_sum": 0}
        sector_summary[s]["count"] += 1
        sector_summary[s]["weight"] += c.osebxWeight
        if c.returnsYTD: sector_summary[s]["ytd_sum"] += c.returnsYTD
        if c.peRatio: sector_summary[s]["pe_sum"] += c.peRatio

    final_summary = {}
    for s, data in sector_summary.items():
        count = data["count"]
        final_summary[s] = {
            "weight": clean_float(data["weight"]),
            "averageYTD": clean_float(data["ytd_sum"] / count) if count else None,
            "averagePE": clean_float(data["pe_sum"] / count) if count else None
        }

    # 5. Export
    output = HeatmapData(
        lastUpdated=datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        referenceRates=REFERENCE_RATES,
        benchmarks=benchmarks,
        sectorSummary=final_summary,
        companies=companies_data
    )

    with open("data.json", "w") as f:
        json.dump(output.model_dump(), f, indent=2)
        
    print(f"Done. Exported {len(companies_data)} companies.")

if __name__ == "__main__":
    main()
