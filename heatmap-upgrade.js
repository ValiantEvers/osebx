/**
 * Oslo Børs Heatmap — Upgrade Script v1.0
 * ========================================
 * Drop this <script> tag into the existing heatmap index.html, 
 * right BEFORE the closing </body> tag.
 *
 * It adds:
 *  1. Børsen hero image behind the header
 *  2. OSEBX / OBX / OSEFX hover tooltips on benchmark pills
 *  3. Recommendation source info (? icon on rec badges)
 *  4. Market universe filter (All / ASK-eligible / OSEBX Only)
 *
 * Requires: borsen-hero.jpeg in the same directory as index.html.
 * Requires: data.json with inOSEBX and askEligible fields per company.
 */
(function () {
  'use strict';

  // ===================================================================
  // 1. INJECT CSS
  // ===================================================================
  const upgradeCSS = `
/* ===== [UPGRADE] Hero Børsen Background ===== */
.site-header {
  position: relative;
  overflow: hidden;
}
.site-header::before {
  content: '';
  position: absolute;
  inset: 0;
  background: url('borsen-hero.jpeg') center top / cover no-repeat;
  opacity: 0.50;
  filter: grayscale(40%) brightness(0.8);
  z-index: 0;
  pointer-events: none;
}
.site-header::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(180deg, transparent 0%, var(--bg) 100%);
  z-index: 0;
  pointer-events: none;
}
.site-header > .container {
  position: relative;
  z-index: 1;
}

/* ===== [UPGRADE] Benchmark Tooltips ===== */
.bench-pill {
  position: relative;
}
.bench-tooltip {
  position: absolute;
  bottom: calc(100% + 10px);
  left: 50%;
  transform: translateX(-50%) scale(0.95);
  background: var(--bg-surface);
  border: 1px solid var(--border-hover);
  border-radius: var(--radius-md, 10px);
  padding: 10px 14px;
  font-size: 0.72rem;
  font-family: var(--font-body);
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.55;
  width: 280px;
  opacity: 0;
  pointer-events: none;
  transition: opacity 0.2s ease, transform 0.2s ease;
  z-index: 100;
  box-shadow: 0 8px 24px rgba(0,0,0,0.3);
  text-align: left;
}
.bench-tooltip::after {
  content: '';
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border: 6px solid transparent;
  border-top-color: var(--bg-surface);
}
.bench-pill:hover .bench-tooltip {
  opacity: 1;
  transform: translateX(-50%) scale(1);
  pointer-events: auto;
}
.bench-tooltip strong {
  color: var(--text-primary);
  font-weight: 700;
}

/* ===== [UPGRADE] Recommendation Source Icon ===== */
.rec-source-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 14px;
  height: 14px;
  border-radius: 50%;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  font-size: 0.55rem;
  color: var(--text-muted);
  cursor: help;
  position: relative;
  vertical-align: middle;
  margin-left: 3px;
  flex-shrink: 0;
}
.rec-source-icon .rec-tip {
  position: absolute;
  bottom: calc(100% + 8px);
  left: 50%;
  transform: translateX(-50%);
  background: var(--bg-surface);
  border: 1px solid var(--border-hover);
  border-radius: var(--radius-sm, 6px);
  padding: 8px 12px;
  font-size: 0.68rem;
  color: var(--text-secondary);
  line-height: 1.5;
  width: 220px;
  opacity: 0;
  pointer-events: none;
  transition: opacity 0.2s;
  z-index: 100;
  box-shadow: 0 4px 16px rgba(0,0,0,0.3);
  text-align: left;
  font-weight: 400;
}
.rec-source-icon:hover .rec-tip {
  opacity: 1;
  pointer-events: auto;
}

/* ===== [UPGRADE] Market Filter ===== */
.market-filter-row {
  display: flex;
  gap: 6px;
  align-items: center;
  flex-wrap: wrap;
}
.market-filter-label {
  font-size: 0.72rem;
  color: var(--text-muted);
  font-weight: 600;
  margin-right: 4px;
  white-space: nowrap;
}
.market-btn {
  padding: 4px 12px;
  border-radius: 100px;
  font-size: 0.72rem;
  font-weight: 600;
  background: var(--bg-elevated);
  border: 1px solid var(--border);
  color: var(--text-muted);
  transition: all 0.2s ease;
  white-space: nowrap;
  cursor: pointer;
}
.market-btn:hover {
  color: var(--text-secondary);
  border-color: var(--border-hover);
}
.market-btn.active {
  background: rgba(59,130,246,0.12);
  border-color: var(--blue, #3b82f6);
  color: var(--blue, #3b82f6);
}
.ask-badge {
  display: inline-block;
  font-size: 0.58rem;
  font-weight: 700;
  padding: 1px 5px;
  border-radius: 3px;
  background: rgba(59,130,246,0.12);
  color: var(--blue, #3b82f6);
  margin-left: 4px;
  vertical-align: middle;
  letter-spacing: 0.5px;
}
.ask-badge.not-ask {
  background: rgba(239,68,68,0.10);
  color: var(--red, #ef4444);
}
@media (max-width: 640px) {
  .bench-tooltip { width: 220px; font-size: 0.65rem; }
}
`;

  const styleEl = document.createElement('style');
  styleEl.textContent = upgradeCSS;
  document.head.appendChild(styleEl);

  // ===================================================================
  // 2. TOOLTIP DEFINITIONS
  // ===================================================================
  const BENCH_TOOLTIPS = {
    'OSEBX': '<strong>OSEBX</strong> — Oslo Børs Benchmark Index. The main benchmark for Norwegian equities. Contains ~69 of the most traded companies, free-float adjusted and rebalanced semi-annually (June & December).',
    'OBX':   '<strong>OBX</strong> — Oslo Børs Total Return Index. The top 25 most liquid stocks. Primarily used for derivatives (futures/options). More concentrated than OSEBX.',
    'OSEFX': '<strong>OSEFX</strong> — Oslo Børs Mutual Fund Index. A capped version of OSEBX where no single stock can exceed 10% weight. Used as benchmark for Norwegian equity funds.',
  };

  // ===================================================================
  // 3. MONKEY-PATCH renderBenchmarks to add tooltips
  // ===================================================================
  // We watch for benchmark pills to appear, then inject tooltips.
  function injectBenchmarkTooltips() {
    const row = document.getElementById('benchmarkRow');
    if (!row) return;

    const observer = new MutationObserver(() => {
      const pills = row.querySelectorAll('.bench-pill');
      pills.forEach(pill => {
        if (pill.querySelector('.bench-tooltip')) return; // already injected
        const nameEl = pill.querySelector('.bench-name');
        if (!nameEl) return;
        const name = nameEl.textContent.trim();
        const tip = BENCH_TOOLTIPS[name];
        if (tip) {
          const tooltipEl = document.createElement('div');
          tooltipEl.className = 'bench-tooltip';
          tooltipEl.innerHTML = tip;
          pill.prepend(tooltipEl);
        }
      });
    });

    observer.observe(row, { childList: true, subtree: true });
    // Also try immediately in case pills are already rendered
    setTimeout(() => {
      const pills = row.querySelectorAll('.bench-pill');
      pills.forEach(pill => {
        if (pill.querySelector('.bench-tooltip')) return;
        const nameEl = pill.querySelector('.bench-name');
        if (!nameEl) return;
        const name = nameEl.textContent.trim();
        const tip = BENCH_TOOLTIPS[name];
        if (tip) {
          const tooltipEl = document.createElement('div');
          tooltipEl.className = 'bench-tooltip';
          tooltipEl.innerHTML = tip;
          pill.prepend(tooltipEl);
        }
      });
    }, 2000);
  }

  // ===================================================================
  // 4. INJECT MARKET FILTER ROW
  // ===================================================================
  let currentMarket = 'all';

  function injectMarketFilter() {
    const controlsBar = document.querySelector('.controls-bar');
    if (!controlsBar) return;

    const filterRow = document.createElement('div');
    filterRow.className = 'market-filter-row';
    filterRow.id = 'marketFilterRow';
    filterRow.innerHTML = `
      <span class="market-filter-label">Universe:</span>
      <button class="market-btn active" data-market="all" title="All Oslo Børs + Euronext Expand stocks">All Stocks</button>
      <button class="market-btn" data-market="ask" title="Only stocks eligible for Nordnet ASK (EEA-domiciled, regulated market)">ASK-Eligible</button>
      <button class="market-btn" data-market="osebx" title="Only OSEBX index members (~69 stocks)">OSEBX Only</button>
    `;

    // Insert before sector tabs
    const sectorTabs = controlsBar.querySelector('.sector-tabs') || controlsBar.firstChild;
    controlsBar.insertBefore(filterRow, sectorTabs);

    // Click handlers
    filterRow.addEventListener('click', (e) => {
      const btn = e.target.closest('.market-btn');
      if (!btn) return;
      currentMarket = btn.dataset.market;
      filterRow.querySelectorAll('.market-btn').forEach(b =>
        b.classList.toggle('active', b.dataset.market === currentMarket)
      );

      // Update subtitle
      const subtitle = document.querySelector('.site-header .subtitle');
      if (subtitle) {
        const subs = {
          'all':   'All stocks on Oslo Børs + Euronext Expand — fundamentals, momentum, risk & valuation',
          'ask':   'ASK-eligible stocks (EEA-domiciled, regulated market) — fundamentals, momentum, risk & valuation',
          'osebx': 'OSEBX index constituents — fundamentals, momentum, risk & valuation',
        };
        subtitle.textContent = subs[currentMarket] || subs.all;
      }

      // Trigger re-filter (dispatch input event on search to re-trigger the existing filter chain)
      const searchInput = document.getElementById('searchInput');
      if (searchInput) {
        searchInput.dispatchEvent(new Event('input', { bubbles: true }));
      }
    });
  }

  // ===================================================================
  // 5. MONKEY-PATCH THE FILTER LOGIC
  // ===================================================================
  // We intercept the existing applyFiltersAndRender by wrapping the
  // data.companies array with a getter that pre-filters by market.
  function patchFilterLogic() {
    // Wait for DATA to be loaded
    const checkData = setInterval(() => {
      // Look for the script's DATA variable via the cards grid being populated
      const grid = document.getElementById('cardsGrid');
      if (!grid || grid.children.length === 0) return;

      clearInterval(checkData);

      // We patch by observing mutation on cardsGrid and re-hiding non-matching cards.
      // This is a lightweight approach that doesn't require modifying the source IIFE.
      const filterCards = new MutationObserver(() => {
        if (currentMarket === 'all') return;

        const cards = grid.querySelectorAll('.company-card');
        // We need to figure out which company each card represents
        // The card contains the ticker in a .ticker-tag element
        // We need data from data.json to know inOSEBX and askEligible
        // Fetch data.json ourselves
        if (!window.__heatmapData) {
          fetch('./data.json').then(r => r.json()).then(d => {
            window.__heatmapData = {};
            (d.companies || []).forEach(c => {
              window.__heatmapData[c.ticker.replace('.OL', '')] = c;
            });
            applyMarketFilter(cards);
          });
        } else {
          applyMarketFilter(cards);
        }
      });

      filterCards.observe(grid, { childList: true });

      // Also load data immediately
      fetch('./data.json').then(r => r.json()).then(d => {
        window.__heatmapData = {};
        (d.companies || []).forEach(c => {
          window.__heatmapData[c.ticker.replace('.OL', '')] = c;
        });
      });
    }, 500);
  }

  function applyMarketFilter(cards) {
    if (!window.__heatmapData) return;
    cards.forEach(card => {
      const tickerEl = card.querySelector('.ticker-tag');
      if (!tickerEl) return;
      const ticker = tickerEl.textContent.trim();
      const data = window.__heatmapData[ticker];
      if (!data) return;

      let visible = true;
      if (currentMarket === 'osebx') visible = data.inOSEBX !== false; // default true if field missing
      else if (currentMarket === 'ask') visible = data.askEligible !== false; // default true if field missing

      card.style.display = visible ? '' : 'none';
    });

    // Update count
    const countEl = document.getElementById('companyCount');
    if (countEl) {
      const allCards = document.querySelectorAll('#cardsGrid .company-card');
      const visibleCards = Array.from(allCards).filter(c => c.style.display !== 'none');
      const label = currentMarket === 'osebx'
        ? `${visibleCards.length} OSEBX members`
        : currentMarket === 'ask'
          ? `${visibleCards.length} ASK-eligible of ${allCards.length}`
          : `${visibleCards.length} of ${allCards.length} companies`;
      countEl.textContent = label;
    }
  }

  // ===================================================================
  // 6. INJECT ASK BADGES + REC SOURCE TOOLTIPS ON CARDS
  // ===================================================================
  function injectCardBadges() {
    // Observe new cards being added
    const grid = document.getElementById('cardsGrid');
    if (!grid) return;

    const observer = new MutationObserver((mutations) => {
      mutations.forEach(mutation => {
        mutation.addedNodes.forEach(node => {
          if (node.nodeType !== 1 || !node.classList.contains('company-card')) return;
          upgradeCard(node);
        });
      });
    });

    observer.observe(grid, { childList: true });

    // Also upgrade existing cards
    grid.querySelectorAll('.company-card').forEach(upgradeCard);
  }

  function upgradeCard(card) {
    if (card.dataset.upgraded) return;
    card.dataset.upgraded = 'true';

    const meta = card.querySelector('.card-meta');
    if (!meta) return;

    // Get ticker
    const tickerEl = meta.querySelector('.ticker-tag');
    if (!tickerEl) return;
    const ticker = tickerEl.textContent.trim();

    // Wait for data
    const tryUpgrade = () => {
      if (!window.__heatmapData) {
        setTimeout(tryUpgrade, 300);
        return;
      }

      const data = window.__heatmapData[ticker];
      if (!data) return;

      // Add ASK badge — only if the fields actually exist in data.json
      if (data.askEligible === false) {
        const badge = document.createElement('span');
        badge.className = 'ask-badge not-ask';
        badge.textContent = 'Not ASK';
        badge.title = 'Not eligible for Aksjesparekonto — company is domiciled outside the EEA';
        meta.appendChild(badge);
      } else if (data.inOSEBX === false && data.askEligible === true) {
        const badge = document.createElement('span');
        badge.className = 'ask-badge';
        badge.textContent = 'ASK';
        badge.title = 'Eligible for Aksjesparekonto at Nordnet';
        meta.appendChild(badge);
      }

      // Add rec source tooltip next to existing rec badge
      const recBadge = meta.querySelector('.rec-badge');
      if (recBadge && !meta.querySelector('.rec-source-icon')) {
        const recKey = (data.recommendation || '').toLowerCase().replace(/\s+/g, '_');
        const recExplain = {
          'strong_buy': '<strong>Strong Buy</strong> — Analysts expect the stock to significantly outperform the market. High conviction that the price will rise.',
          'buy':        '<strong>Buy</strong> — Analysts expect the stock to outperform. A majority of covering brokers recommend purchasing.',
          'hold':       '<strong>Hold</strong> — Analysts see the stock as fairly valued at current levels. Neither a clear buy nor sell.',
          'underperform':'<strong>Underperform</strong> — Analysts expect the stock to lag the market. Some brokers recommend reducing exposure.',
          'sell':       '<strong>Sell</strong> — Analysts expect the price to decline. Strong consensus to exit the position.',
        };
        const explanation = recExplain[recKey] || '<strong>' + recBadge.textContent.trim() + '</strong>';

        const icon = document.createElement('span');
        icon.className = 'rec-source-icon';
        icon.innerHTML = `?<span class="rec-tip">${explanation}<br><br><span style="color:var(--text-muted);font-size:0.6rem">Source: Yahoo Finance consensus, aggregated from broker research by S&P Capital IQ.</span></span>`;
        recBadge.insertAdjacentElement('afterend', icon);
      }
    };

    tryUpgrade();
  }

  // ===================================================================
  // 7. UPDATE HEADER TITLE
  // ===================================================================
  function updateHeaderTitle() {
    const h1 = document.querySelector('.site-header h1');
    if (h1) h1.textContent = 'Oslo Børs Heatmap';

    const subtitle = document.querySelector('.site-header .subtitle');
    if (subtitle) subtitle.textContent = 'All stocks on Oslo Børs + Euronext Expand — fundamentals, momentum, risk & valuation';
  }

  // ===================================================================
  // 8. UPDATE ABOUT SECTION
  // ===================================================================
  function updateAboutSection() {
    const aboutContent = document.querySelector('.about-content');
    if (!aboutContent) return;

    aboutContent.innerHTML = `
      <p><strong>The Oslo Stock Exchange (Oslo Børs)</strong> was founded in 1819 and is now
      part of Euronext. It is the primary marketplace for Norwegian equities.</p>
      <p><strong>This dashboard</strong> shows all stocks listed on Oslo Børs (main list)
      and Euronext Expand — both regulated markets in Norway. You can filter by:</p>
      <p><strong>OSEBX</strong> — the benchmark index (~69 most-traded companies),
      <strong>ASK-eligible</strong> — stocks you can hold in a Nordnet Aksjesparekonto
      (must be EEA-domiciled and listed on a regulated market; Euronext Growth is excluded),
      or <strong>All</strong> — every listed stock including non-EEA-domiciled companies.</p>
      <p>Data is <strong>delayed and updated weekly</strong> via Yahoo Finance.
      Analyst recommendations (Buy, Hold, etc.) are consensus ratings aggregated
      from broker research by S&P Capital IQ, distributed through Yahoo Finance.
      This dashboard is for informational and educational purposes only.</p>
    `;
  }

  // ===================================================================
  // BOOT
  // ===================================================================
  function boot() {
    updateHeaderTitle();
    injectBenchmarkTooltips();
    injectMarketFilter();
    patchFilterLogic();
    injectCardBadges();
    updateAboutSection();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    // DOM already ready, but data might not be loaded yet
    // Wait a tick for the main IIFE to initialize
    setTimeout(boot, 100);
  }
})();
