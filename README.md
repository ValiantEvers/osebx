# OSEBX Company Heatmap

A dark-themed, responsive dashboard displaying all ~69 constituents of the **OSEBX (Oslo Børs Benchmark Index)** with real fundamental data, computed analytical metrics, and interactive filtering.

**[Live Demo →](https://your-username.github.io/osebx-heatmap/)**

![Screenshot](screenshot.png)

---

## Features

- **Real data** — fundamentals (P/E, P/B, dividend yield, market cap) and analyst recommendations fetched from Yahoo Finance via `yahooquery`
- **Computed analytics** — alpha vs OSEBX, Sharpe ratio, momentum score, max drawdown, volatility percentile, correlation, mean reversion, earnings yield, EV/EBITDA, dividend consistency, and more
- **Zero API cost** — uses the free `yahooquery` library; data updated weekly via GitHub Actions
- **Pydantic-validated pipeline** — every record passes through typed models before hitting the JSON
- **Interactive UI** — sector filter tabs, multi-metric sorting with toggle direction, real-time search, expandable cards with tabbed detail views
- **Norwegian locale formatting** — all numbers use `nb-NO` formatting (comma decimals, space thousands)
- **Fully accessible** — keyboard navigation, ARIA attributes, focus trapping in modals, WCAG AA contrast ratios
- **Progressive loading** — 20 cards at a time on mobile; all at once on desktop
- **Strategy Guide** — built-in modal explaining every metric in plain language

---

## Architecture

```
osebx-heatmap/
├── index.html                          # Single-page dashboard (HTML + CSS + JS)
├── data.json                           # Market data (auto-generated weekly)
├── scripts/
│   └── update_data.py                  # Python data pipeline
├── requirements.txt                    # Python dependencies
├── .github/
│   └── workflows/
│       └── update-data.yml             # Weekly GitHub Actions workflow
└── README.md
```

### Data flow

1. **GitHub Actions** runs `scripts/update_data.py` every Sunday at 20:00 UTC
2. The script fetches fundamentals and 10-year price history for all OSEBX constituents via `yahooquery`
3. Analytical metrics (alpha, Sharpe, momentum, drawdown, etc.) are computed in Python
4. Every record is validated through **Pydantic models** — corrupted fields become `null`, not crashes
5. Output is written to `data.json` and committed to the repository
6. The frontend (`index.html`) fetches `data.json` and renders everything dynamically — no data is hardcoded in the HTML

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
| Mean Reversion | % distance from 5-year SMA |
| EV/EBITDA | Enterprise value to EBITDA |
| Earnings Yield | Inverse of P/E, comparable to bond yields |
| Analyst Recommendation | Consensus buy/hold/sell |

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

### Local development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the data pipeline
python scripts/update_data.py

# Serve locally
python -m http.server 8000
# Open http://localhost:8000
```

### Deploy to GitHub Pages

1. Push the repository to GitHub
2. Go to **Settings → Pages → Source** and select `main` branch, root `/`
3. The site will be live at `https://<username>.github.io/<repo-name>/`
4. Enable the GitHub Actions workflow for automatic weekly updates

### Manual data update

Go to **Actions → Update OSEBX Heatmap Data → Run workflow** to trigger a manual refresh.

---

## Updating the Constituent List

The OSEBX index rebalances **twice per year** (June and December). When this happens:

1. Open `scripts/update_data.py`
2. Update the `CONSTITUENTS` list — add new tickers, remove delisted ones, update sector classifications and approximate weights
3. Commit and push
4. Run the workflow manually to regenerate `data.json`

The `osebxWeight` values are approximate and manually maintained — Yahoo Finance does not expose index weights.

---

## Reference Rates

The script uses hardcoded Norwegian reference rates for Sharpe ratio and earnings yield comparison:

- **Risk-free rate:** 3.5% (Norwegian 3-month government bill yield)
- **10Y bond yield:** 3.5% (Norwegian government bond)

Update these in `scripts/update_data.py` when rates change significantly.

---

## Disclaimer

Market data is delayed and updated weekly via Yahoo Finance. This dashboard is for **informational and educational purposes only** — not to be used for financial analysis or investment decisions.

---

## Tech Stack

- **Frontend:** Vanilla HTML, CSS, JavaScript (no frameworks, no build step)
- **Data pipeline:** Python, yahooquery, pandas, Pydantic
- **Hosting:** GitHub Pages (free)
- **CI/CD:** GitHub Actions (free)
- **Fonts:** Playfair Display, DM Sans, JetBrains Mono (Google Fonts)

---

## License

MIT
