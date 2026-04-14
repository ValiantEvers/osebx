# OSEBX Sector Heatmap

A dark-themed, responsive heatmap dashboard for the Oslo Børs Benchmark Index (OSEBX), showing sector performance, fundamentals, and market structure.

Data updates weekly via GitHub Actions using [yfinance](https://github.com/ranaroussi/yfinance) — **zero API costs**.

## Quick Start

### 1. Create the Repository

Create a new public repository on GitHub (e.g., `osebx-heatmap` or add to your existing GitHub Pages repo).

### 2. Add All Files

Upload or push these files to the repository:

```
├── .github/
│   └── workflows/
│       └── update-data.yml      ← GitHub Actions workflow
├── scripts/
│   └── update_data.py           ← Python data pipeline
├── data.json                     ← Seed data (works immediately)
├── index.html                    ← The heatmap dashboard
├── requirements.txt              ← Python dependencies
└── README.md
```

### 3. Enable GitHub Pages

1. Go to **Settings → Pages**
2. Under **Source**, select **Deploy from a branch**
3. Choose the `main` branch and `/ (root)` folder
4. Click **Save**

Your site will be live at `https://<your-username>.github.io/<repo-name>/` within a few minutes.

### 4. Enable the Actions Workflow

1. Go to the **Actions** tab in your repository
2. You should see the "Update OSEBX Data" workflow
3. Click **Enable** if prompted
4. To run it immediately: click the workflow → **Run workflow** → **Run workflow**

The workflow runs automatically every **Sunday at 20:00 UTC**. You can also trigger it manually at any time.

## How It Works

### Data Pipeline

The GitHub Actions workflow runs `scripts/update_data.py` weekly:

1. Fetches historical price data for OSEBX sector indices via Yahoo Finance (using `yfinance`)
2. Computes YTD, 6M, and 1Y returns from price history
3. Computes 52-week high/low and 5-year mean reversion metrics
4. Writes everything to `data.json` with a timestamp
5. Commits and pushes the updated file

**Note:** Fundamental metrics (P/E, P/B, dividend yield, analyst consensus, top holdings, market cap breakdown) are realistic placeholders hardcoded in the Python script. These can be enriched in the future by fetching individual stock data.

### Frontend

The `index.html` page fetches `data.json` on load and dynamically renders:

- Market benchmark pills (OSEBX, OBX, OSEFX)
- A sortable, colour-coded sector heatmap grid
- Expandable cards with detailed metrics
- A metrics dictionary modal
- A collapsible beginner explainer

No build step, no framework, no dependencies — just static HTML/CSS/JS.

## Disclaimer

Market data is delayed and updated weekly. Certain fundamental metrics are illustrative placeholders. This dashboard is for informational and educational purposes only — not to be used for financial analysis or investment decisions.

## Tech Stack

- **Frontend:** Vanilla HTML, CSS, JavaScript
- **Data:** Python + yfinance + GitHub Actions
- **Hosting:** GitHub Pages (free)
- **Fonts:** Playfair Display, DM Sans, JetBrains Mono (Google Fonts)

## License

Personal use. Data sourced from Yahoo Finance via yfinance (intended for personal/educational use per Yahoo's terms of service).
