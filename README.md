# Oslo Børs Heatmap

A dark-themed, responsive dashboard displaying stocks on **Oslo Børs** and **Euronext Expand** — OSEBX index members plus a broader universe of Norwegian equities. Each card shows real fundamentals, computed analytical metrics, analyst ratings, and interactive filters.

**[Live demo →](https://evers.no/osebx)**

![Screenshot](screenshot.png)

---

## Features

- **Full Oslo Børs + Euronext Expand coverage** — ~130 stocks, filterable by OSEBX membership, ASK eligibility (Aksjesparekonto), or analyst rating
- **Real data** — fundamentals (P/E, P/B, dividend yield, market cap) and analyst recommendations fetched from Yahoo Finance via `yahooquery`
- **Computed analytics** — alpha vs OSEBX, Sharpe ratio, momentum score, max drawdown, volatility percentile, correlation, mean reversion (with z-score), earnings yield, EV/EBITDA, dividend consistency, liquidity, seasonality, and beta to Brent crude
- **Weekly Market Snapshot** — a separate pipeline (`analyze.py` → `narrate.py`) classifies the market regime, ranks drivers / risks / opportunities, and renders a compact briefing above the heatmap. All analytical judgement is deterministic Python; the LLM only writes human prose around pre-computed facts
- **Zero API cost for data** — free `yahooquery` library, weekly updates via GitHub Actions; the narration step is the only paid API call and is cost-bounded by a strict Pydantic schema
- **Pydantic-validated pipelines** — every record in `data.json`, `brief.json`, and `insights.json` passes through typed models; corrupted fields become `null`, not crashes
- **Interactive UI** — sector filter tabs, universe filter (All / ASK-eligible / OSEBX), rating filter (Strong Buy / Buy / Hold / Underperform / Sell), multi-metric sorting with toggle direction, real-time search, expandable cards with tabbed detail views
- **Norwegian number formatting** — `nb-NO` locale throughout (comma decimals, space thousands, Norwegian long-scale `mrd.` / `bill.`)
- **Accessibility** — keyboard navigation, ARIA attributes, focus trapping in the Strategy Guide modal, WCAG AA contrast ratios
- **Progressive loading** — 30 cards per page; intersection-observer fade-in
- **Strategy Guide** — in-app modal explaining every metric in plain language

---

## Architecture

```
osebx-heatmap/
├── index.html                          # Single-page dashboard (HTML + CSS + JS)
├── heatmap-upgrade.js                  # Adds universe/rating filters, ASK badges, benchmark tooltips
├── data.json                           # Market data (auto-generated weekly)
├── brief.json                          # Pre-computed analytical brief (weekly)
├── insights.json                       # Human-readable market snapshot (weekly)
├── scripts/
│   ├── update_data.py                  # Python data pipeline
│   ├── analyze.py                      # Deterministic pre-analysis → brief.json
│   └── narrate.py                      # LLM narration → insights.json
├── requirements.txt                    # Python dependencies
├── .github/
│   └── workflows/
│       └── update-data.yml             # Weekly GitHub Actions workflow
└── README.md
```

### Data flow

1. **GitHub Actions** runs weekly (Sundays around 20:17 UTC).
2. `update_data.py` fetches fundamentals and 10-year price history for all Oslo Børs + Euronext Expand stocks via `yahooquery`. Analytical metrics (alpha, Sharpe, momentum, drawdown, z-score, etc.) are computed in Python and validated through Pydantic. Output: `data.json`.
3. `analyze.py` reads `data.json` and produces `brief.json`: regime classification, breadth, benchmark spreads, sector leaders/laggards, and ranked driver/risk/opportunity candidates. The watchlist fallback and graduation tracking (comparing against the previously committed `brief.json`) both live here. **All analytical judgement happens in this step, with no LLM involved.**
4. `narrate.py` passes `brief.json` to Claude with a tight system prompt. The model writes the headline, the market-take paragraph, and one short claim per item. It cannot change numbers or swap tickers — the output is schema-validated and ticker-cross-checked before writing `insights.json`. If the model returns malformed output, the workflow keeps the previous `insights.json` so the dashboard never shows broken prose.
5. The frontend (`index.html`) fetches `data.json` once on load and renders everything. `heatmap-upgrade.js` reads from the same dataset via `window.osebxApp` to add universe/rating filter rows, ASK badges, and the rec-source tooltip — no second fetch. A separate script reads `insights.json` and renders the weekly market snapshot at the top of the page.

---

## Metrics Computed

### Tier 1 (Core — always present)
| Metric | Description |
|--------|-------------|
| YTD / 6M / 1Y returns | Simple price returns over each period |
| Market Cap | Total market value from Yahoo Finance |
| P/E, Forward P/E, P/B | Standard valuation ratios |
| Dividend Yield | Annual dividend as % of price |
| Alpha (3M, 6M, 1Y) | Stock return minus OSEBX return |
| Sharpe Ratio | Risk-adjusted return (annualised) |
| Max Drawdown | Worst peak-to-trough decline in trailing 1Y |
| Mean Reversion | % distance from 5-year SMA, plus z-score in σ units |
| EV/EBITDA | Enterprise value to EBITDA |
| Earnings Yield | Inverse of P/E, comparable to bond yields |
| Analyst Recommendation | Consensus buy / hold / sell |

### Tier 2 (High value)
| Metric | Description |
|--------|-------------|
| Momentum Score | Multi-timeframe composite, normalised 0–100 |
| Volatility Percentile | Current vol ranked against own 5Y history |
| Correlation to OSEBX | Pearson correlation of daily returns |
| Dividend Consistency | Years paid (out of 5) and trend direction |
| Dividend Payout Ratio | % of earnings distributed as dividends |
| Liquidity Score | Average daily traded value in NOK |

### Tier 3 (Included when data permits)
| Metric | Description |
|--------|-------------|
| Beta to Brent Crude | OLS regression slope vs BZ=F daily returns |
| Monthly Seasonality | 12-month sparkline of average returns |

---

## Setup

### Prerequisites

- Python 3.10+
- A GitHub repository with Actions enabled
- An Anthropic API key (stored as the `ANTHROPIC_API_KEY` secret) — only needed for the narration step; the rest of the pipeline runs without it

### Local development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the data pipeline
python scripts/update_data.py

# (optional) Run the analysis + narration pipeline
python scripts/analyze.py data.json brief.json
ANTHROPIC_API_KEY=... python scripts/narrate.py brief.json insights.json

# Serve locally
python -m http.server 8000
# Open http://localhost:8000
```

### Deploy to GitHub Pages

1. Push the repository to GitHub.
2. **Settings → Pages → Source**: `main` branch, root `/`.
3. The site will be live at `https://<username>.github.io/<repo-name>/` (or your custom domain — this project is deployed at `evers.no/osebx`).
4. Enable the GitHub Actions workflow for automatic weekly updates.

### Manual data update

**Actions → Update OSEBX Heatmap Data → Run workflow** to trigger a manual refresh.

---

## Updating the Universe

Oslo Børs listings change, and the OSEBX index rebalances twice a year (June and December). When this happens:

1. Open `scripts/update_data.py`.
2. Edit the `CONSTITUENTS` list — add new tickers, remove delisted ones, update sectors, weights, OSEBX membership, and ASK eligibility.
3. Commit and push.
4. Run the workflow manually to regenerate `data.json`.

Every row is a tuple of `(ticker, display_name, sector, osebx_weight, in_osebx, ask_eligible)`. The `osebxWeight` values are approximate and manually maintained — Yahoo Finance does not expose index weights. Set weight to `0` for non-OSEBX stocks.

ASK eligibility rule of thumb: the stock must be on a regulated EEA market *and* the issuer must be domiciled in the EEA. Companies listed on Oslo Børs but registered in Bermuda, Cayman, Marshall Islands, etc. are not ASK-eligible.

---

## Reference Rates

`scripts/update_data.py` uses hardcoded Norwegian reference rates for Sharpe ratio and earnings-yield comparison:

- **Risk-free rate:** 3.5% (Norwegian 3-month government bill yield)
- **10Y bond yield:** 3.5% (Norwegian government bond)

Update these at the top of the file when rates shift meaningfully.

---

## Regime thresholds

`scripts/analyze.py` classifies the market into one of five regimes (`broad_rally`, `narrow_rally`, `rotation`, `compressed`, `drawdown`) using breadth (share of names with positive YTD), benchmark spreads, and median Sharpe. The thresholds live as constants at the top of the file and are surfaced in `brief.json` / `insights.json` under `thresholds_used`, so the frontend tooltip renders them from data instead of duplicating them in JS.

---

## Disclaimer

Market data is delayed and updated weekly via Yahoo Finance. This dashboard is for **informational and educational purposes only** — not financial analysis or investment advice.

---

## Tech Stack

- **Frontend:** Vanilla HTML, CSS, JavaScript (no frameworks, no build step)
- **Data pipeline:** Python, yahooquery, pandas, Pydantic
- **Narration:** `anthropic` Python SDK, Claude Sonnet 4.6
- **Hosting:** GitHub Pages (free)
- **CI/CD:** GitHub Actions (free tier)
- **Fonts:** Playfair Display, DM Sans, JetBrains Mono (Google Fonts)

---

## License

MIT
