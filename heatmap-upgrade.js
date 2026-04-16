/**
 * Oslo Børs Heatmap — Upgrade Script v1.1
 * Drop this <script> right BEFORE </body> in your heatmap index.html.
 */
(function () {
  'use strict';

  const SECTOR_SHORT = {
    'Communication Services': 'Com. Services',
    'Information Technology': 'IT',
    'Consumer Staples': 'Cons. Stapl.',
    'Consumer Discretionary': 'Cons. Discr.',
  };

  const BENCH_TOOLTIPS = {
    'OSEBX': '<strong>OSEBX</strong> — Oslo Børs Benchmark Index. ~69 most-traded companies, free-float adjusted, rebalanced semi-annually (June & December).',
    'OBX':   '<strong>OBX</strong> — Top 25 most liquid stocks. Used for derivatives (futures/options). More concentrated than OSEBX.',
    'OSEFX': '<strong>OSEFX</strong> — Capped OSEBX where no stock exceeds 10% weight. Benchmark for Norwegian equity funds.',
  };

  const REC_EXPLAIN = {
    'strong_buy':   '<strong>Strong Buy</strong> — Analysts expect the stock to significantly outperform. High conviction.',
    'buy':          '<strong>Buy</strong> — Analysts expect the stock to outperform. Majority of brokers recommend purchasing.',
    'hold':         '<strong>Hold</strong> — Analysts see the stock as fairly valued. Neither a clear buy nor sell.',
    'underperform': '<strong>Underperform</strong> — Analysts expect the stock to lag the market.',
    'sell':         '<strong>Sell</strong> — Analysts expect the price to decline.',
  };
  const REC_SOURCE = '<span style="color:var(--text-muted);font-size:0.6rem">Source: Yahoo Finance consensus, aggregated from broker research by S&amp;P Capital IQ.</span>';

  // === CSS ===
  const css = `
.site-header{position:relative;overflow:hidden}
.site-header::before{content:'';position:absolute;inset:0;background:url('borsen-hero.jpeg') center top/cover no-repeat;opacity:.10;filter:grayscale(40%) brightness(.8);z-index:0;pointer-events:none}
.site-header::after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,transparent 0%,var(--bg) 100%);z-index:0;pointer-events:none}
.site-header>.container{position:relative;z-index:1}
.bench-pill{position:relative}
.bench-tooltip{position:absolute;bottom:calc(100% + 10px);left:50%;transform:translateX(-50%) scale(.95);background:var(--bg-surface);border:1px solid var(--border-hover);border-radius:10px;padding:10px 14px;font-size:.72rem;font-family:var(--font-body);font-weight:400;color:var(--text-secondary);line-height:1.55;width:280px;opacity:0;pointer-events:none;transition:opacity .2s,transform .2s;z-index:100;box-shadow:0 8px 24px rgba(0,0,0,.3);text-align:left}
.bench-tooltip::after{content:'';position:absolute;top:100%;left:50%;transform:translateX(-50%);border:6px solid transparent;border-top-color:var(--bg-surface)}
.bench-pill:hover .bench-tooltip{opacity:1;transform:translateX(-50%) scale(1);pointer-events:auto}
.bench-tooltip strong{color:var(--text-primary);font-weight:700}
.floating-tip{position:fixed;z-index:10000;background:var(--bg-surface,#1a1e28);border:1px solid var(--border-hover,rgba(255,255,255,.12));border-radius:8px;padding:10px 14px;font-size:.7rem;font-family:var(--font-body,'DM Sans',sans-serif);font-weight:400;color:var(--text-secondary,rgba(232,236,244,.55));line-height:1.55;width:250px;box-shadow:0 8px 32px rgba(0,0,0,.4);text-align:left;pointer-events:none;opacity:0;transition:opacity .15s}
.floating-tip.visible{opacity:1}
.floating-tip strong{color:var(--text-primary,#e8ecf4);font-weight:700}
.rec-source-icon{display:inline-flex;align-items:center;justify-content:center;width:14px;height:14px;border-radius:50%;background:var(--bg-elevated);border:1px solid var(--border);font-size:.55rem;color:var(--text-muted);cursor:help;vertical-align:middle;margin-left:3px;flex-shrink:0}
.market-filter-row,.rec-filter-row{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.filter-label{font-size:.72rem;color:var(--text-muted);font-weight:600;margin-right:4px;white-space:nowrap}
.filter-btn{padding:4px 12px;border-radius:100px;font-size:.72rem;font-weight:600;background:var(--bg-elevated);border:1px solid var(--border);color:var(--text-muted);transition:all .2s;white-space:nowrap;cursor:pointer}
.filter-btn:hover{color:var(--text-secondary);border-color:var(--border-hover)}
.filter-btn.active{background:rgba(59,130,246,.12);border-color:var(--blue,#3b82f6);color:var(--blue,#3b82f6)}
.filter-btn.active-green{background:rgba(34,197,94,.12);border-color:var(--green,#22c55e);color:var(--green,#22c55e)}
.filter-btn.active-amber{background:rgba(245,158,11,.12);border-color:var(--amber,#f59e0b);color:var(--amber,#f59e0b)}
.filter-btn.active-red{background:rgba(239,68,68,.12);border-color:var(--red,#ef4444);color:var(--red,#ef4444)}
.ask-badge{display:inline-block;font-size:.58rem;font-weight:700;padding:1px 5px;border-radius:3px;background:rgba(59,130,246,.12);color:var(--blue,#3b82f6);margin-left:4px;vertical-align:middle;letter-spacing:.5px}
.ask-badge.not-ask{background:rgba(239,68,68,.1);color:var(--red,#ef4444)}
@media(max-width:640px){.bench-tooltip{width:220px;font-size:.65rem}.floating-tip{width:200px}}
`;
  document.head.appendChild(Object.assign(document.createElement('style'), { textContent: css }));

  // === FLOATING TOOLTIP ===
  let fTip = null;
  function showTip(anchor, html) {
    if (!fTip) { fTip = document.createElement('div'); fTip.className = 'floating-tip'; document.body.appendChild(fTip); }
    fTip.innerHTML = html;
    fTip.classList.add('visible');
    const r = anchor.getBoundingClientRect(), tw = 250;
    let l = r.left + r.width / 2 - tw / 2;
    if (l < 8) l = 8; if (l + tw > innerWidth - 8) l = innerWidth - tw - 8;
    fTip.style.left = l + 'px'; fTip.style.top = '0px';
    let t = r.top - fTip.offsetHeight - 8;
    if (t < 8) t = r.bottom + 8;
    fTip.style.top = t + 'px';
  }
  function hideTip() { if (fTip) fTip.classList.remove('visible'); }

  // === STATE ===
  let curMarket = 'all', curRec = 'all';

  // === BENCHMARK TOOLTIPS ===
  function addBenchTips() {
    const row = document.getElementById('benchmarkRow');
    if (!row) return;
    function inject() {
      row.querySelectorAll('.bench-pill').forEach(p => {
        if (p.querySelector('.bench-tooltip')) return;
        const n = p.querySelector('.bench-name');
        if (!n) return;
        const tip = BENCH_TOOLTIPS[n.textContent.trim()];
        if (tip) { const d = document.createElement('div'); d.className = 'bench-tooltip'; d.innerHTML = tip; p.prepend(d); }
      });
    }
    new MutationObserver(inject).observe(row, { childList: true, subtree: true });
    setTimeout(inject, 2000);
  }

  // === FILTER ROWS ===
  function addFilters() {
    const bar = document.querySelector('.controls-bar');
    if (!bar) return;
    const before = bar.querySelector('.sector-tabs') || bar.firstChild;

    // Market
    const mRow = document.createElement('div'); mRow.className = 'market-filter-row';
    mRow.innerHTML = '<span class="filter-label">Universe:</span><button class="filter-btn active" data-market="all">All Stocks</button><button class="filter-btn" data-market="ask" title="EEA-domiciled, regulated market">ASK-Eligible</button><button class="filter-btn" data-market="osebx" title="~69 OSEBX index members">OSEBX Only</button>';
    bar.insertBefore(mRow, before);
    mRow.addEventListener('click', e => {
      const b = e.target.closest('.filter-btn'); if (!b) return;
      curMarket = b.dataset.market;
      mRow.querySelectorAll('.filter-btn').forEach(x => x.classList.toggle('active', x.dataset.market === curMarket));
      updSub(); applyFilters();
    });

    // Rec
    const rRow = document.createElement('div'); rRow.className = 'rec-filter-row';
    rRow.innerHTML = '<span class="filter-label">Rating:</span><button class="filter-btn active" data-rec="all">All</button><button class="filter-btn" data-rec="strong_buy">Strong Buy</button><button class="filter-btn" data-rec="buy">Buy</button><button class="filter-btn" data-rec="hold">Hold</button><button class="filter-btn" data-rec="underperform">Underperform</button><button class="filter-btn" data-rec="sell">Sell</button>';
    bar.insertBefore(rRow, before);
    const rcol = { all:'active', strong_buy:'active-green', buy:'active-green', hold:'active-amber', underperform:'active-red', sell:'active-red' };
    rRow.addEventListener('click', e => {
      const b = e.target.closest('.filter-btn'); if (!b) return;
      curRec = b.dataset.rec;
      rRow.querySelectorAll('.filter-btn').forEach(x => { x.className = 'filter-btn' + (x.dataset.rec === curRec ? ' ' + (rcol[curRec]||'active') : ''); });
      applyFilters();
    });
  }

  function updSub() {
    const el = document.querySelector('.site-header .subtitle'); if (!el) return;
    const m = { all:'All stocks on Oslo Børs + Euronext Expand', ask:'ASK-eligible stocks (EEA-domiciled, regulated market)', osebx:'OSEBX index constituents' };
    el.textContent = (m[curMarket]||m.all) + ' \u2014 fundamentals, momentum, risk & valuation';
  }

  // === APPLY FILTERS ===
  function applyFilters() {
    const grid = document.getElementById('cardsGrid');
    if (!grid || !window.__heatmapData) return;
    let vis = 0;
    const cards = grid.querySelectorAll('.company-card');
    cards.forEach(card => {
      const te = card.querySelector('.ticker-tag'); if (!te) return;
      const d = window.__heatmapData[te.textContent.trim()]; if (!d) return;
      let ok = true;
      if (curMarket === 'osebx') ok = d.inOSEBX !== false;
      else if (curMarket === 'ask') ok = d.askEligible !== false;
      if (ok && curRec !== 'all') { ok = (d.recommendation||'').toLowerCase().replace(/\s+/g,'_') === curRec; }
      card.style.display = ok ? '' : 'none';
      if (ok) vis++;
    });
    const ce = document.getElementById('companyCount');
    if (ce) ce.textContent = curMarket==='osebx' ? vis+' OSEBX members' : curMarket==='ask' ? vis+' ASK-eligible of '+cards.length : vis+' of '+cards.length+' companies';
  }

  // === LOAD DATA + OBSERVE ===
  function loadAndObserve() {
    fetch('./data.json').then(r=>r.json()).then(d => {
      window.__heatmapData = {};
      (d.companies||[]).forEach(c => { window.__heatmapData[c.ticker.replace('.OL','')] = c; });
      const grid = document.getElementById('cardsGrid');
      if (grid) {
        grid.querySelectorAll('.company-card').forEach(upgradeCard);
        new MutationObserver(muts => {
          muts.forEach(m => m.addedNodes.forEach(n => { if (n.nodeType===1 && n.classList.contains('company-card')) upgradeCard(n); }));
          applyFilters();
        }).observe(grid, { childList: true });
      }
    }).catch(()=>{});
    document.addEventListener('click', () => setTimeout(applyFilters, 80), true);
    document.addEventListener('input', () => setTimeout(applyFilters, 80), true);
  }

  // === UPGRADE CARD ===
  function upgradeCard(card) {
    if (card.dataset.upgraded) return;
    card.dataset.upgraded = 'true';
    const meta = card.querySelector('.card-meta'); if (!meta) return;
    const te = meta.querySelector('.ticker-tag'); if (!te) return;
    const data = window.__heatmapData?.[te.textContent.trim()];

    // Shorten sector
    const sb = meta.querySelector('.sector-badge');
    if (sb) { const f = sb.textContent.trim(); if (SECTOR_SHORT[f]) sb.textContent = SECTOR_SHORT[f]; }

    if (!data) return;

    // ASK badge
    if (data.askEligible === false) {
      const b = document.createElement('span'); b.className='ask-badge not-ask'; b.textContent='Not ASK';
      b.title='Not eligible for Aksjesparekonto \u2014 domiciled outside the EEA'; meta.appendChild(b);
    } else if (data.inOSEBX === false && data.askEligible === true) {
      const b = document.createElement('span'); b.className='ask-badge'; b.textContent='ASK';
      b.title='Eligible for Aksjesparekonto'; meta.appendChild(b);
    }

    // Rec ? icon
    const rb = meta.querySelector('.rec-badge');
    if (rb && !meta.querySelector('.rec-source-icon')) {
      const rk = (data.recommendation||'').toLowerCase().replace(/\s+/g,'_');
      const ex = REC_EXPLAIN[rk] || '<strong>'+rb.textContent.trim()+'</strong>';
      const html = ex + '<br><br>' + REC_SOURCE;
      const icon = document.createElement('span'); icon.className='rec-source-icon'; icon.textContent='?'; icon.tabIndex=0;
      icon.addEventListener('mouseenter', () => showTip(icon, html));
      icon.addEventListener('mouseleave', hideTip);
      icon.addEventListener('focus', () => showTip(icon, html));
      icon.addEventListener('blur', hideTip);
      rb.insertAdjacentElement('afterend', icon);
    }
  }

  // === HEADER + ABOUT ===
  function updHeader() {
    const h = document.querySelector('.site-header h1'); if (h) h.textContent = 'Oslo Børs Heatmap';
    updSub();
  }
  function updAbout() {
    const el = document.querySelector('.about-content'); if (!el) return;
    el.innerHTML = '<p><strong>The Oslo Stock Exchange (Oslo Børs)</strong> was founded in 1819 and is now part of Euronext.</p><p><strong>This dashboard</strong> shows all stocks on Oslo Børs (main list) and Euronext Expand. Filter by <strong>OSEBX</strong> (~69 index members), <strong>ASK-eligible</strong> (EEA-domiciled, Nordnet Aksjesparekonto), or <strong>All</strong>.</p><p>Analyst recommendations are consensus ratings from Yahoo Finance, aggregated by S&amp;P Capital IQ from broker research. Data is delayed and updated weekly. For informational purposes only.</p>';
  }

  // === BOOT ===
  function boot() { updHeader(); addBenchTips(); addFilters(); loadAndObserve(); updAbout(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else setTimeout(boot, 100);
})();
