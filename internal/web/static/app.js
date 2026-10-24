/**
 * TRML Binance USDⓈ-M Futures Intraday Dashboard
 * Ultra-Lightweight Vanilla JS State & Controller
 */

// Global State
const state = {
  trades: {
    page: 1,
    limit: 10,
    filter: 'ALL',
    totalPages: 1,
    total: 0
  },
  signals: {
    page: 1,
    limit: 15,
    decision: 'ALL',
    symbol: 'ALL',
    totalPages: 1,
    total: 0
  },
  knownSymbols: new Set(),
  isPollingActive: true,
  lastSummary: null
};

// Utilities
function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function formatUSD(val) {
  if (isNaN(val)) return '$0.00';
  return '$' + Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatSignedUSD(val) {
  if (isNaN(val)) return '$0.00';
  const prefix = val > 0 ? '+$' : (val < 0 ? '-$' : '$');
  return prefix + Math.abs(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatSignedPct(val) {
  if (isNaN(val)) return '0.00%';
  const prefix = val > 0 ? '+' : '';
  return prefix + Number(val).toFixed(2) + '%';
}

function formatTimeUTC(isoStr) {
  if (!isoStr) return '--';
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return isoStr;
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const min = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `${mm}-${dd} ${hh}:${min}:${ss}`;
}

function formatDuration(sec) {
  if (!sec || sec < 0) return '0s';
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  const remM = m % 60;
  return `${h}h ${remM}m`;
}

function updateConnectionStatus(isOnline) {
  const dot = document.getElementById('status-dot');
  const txt = document.getElementById('status-text');
  if (dot && txt) {
    if (isOnline) {
      dot.classList.remove('offline');
      txt.textContent = 'ONLINE';
    } else {
      dot.classList.add('offline');
      txt.textContent = 'OFFLINE';
    }
  }
}

// 1. Fetch Summary & KPIs
async function fetchSummary() {
  try {
    const res = await fetch('/api/summary');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    state.lastSummary = data;
    updateConnectionStatus(true);
    renderSummary(data);
  } catch (err) {
    console.warn('Failed to fetch summary:', err);
    updateConnectionStatus(false);
  }
}

function renderSummary(data) {
  // Environment Badge
  const envBadge = document.getElementById('env-badge');
  if (envBadge) {
    envBadge.textContent = data.env || 'DRY-RUN';
    if (data.env === 'LIVE') {
      envBadge.className = 'env-badge env-live';
    } else {
      envBadge.className = 'env-badge env-dryrun';
    }
  }

  // Equity & Balance
  const elEquity = document.getElementById('kpi-equity');
  const elBalance = document.getElementById('kpi-balance');
  if (elEquity) elEquity.textContent = formatUSD(data.equity);
  if (elBalance) elBalance.textContent = `Avail: ${formatUSD(data.balance)}`;

  // Net PnL & Win Rate
  const stats = data.stats || {};
  const elPnL = document.getElementById('kpi-pnl');
  const elTradesCount = document.getElementById('kpi-trades-count');
  const elWinRate = document.getElementById('kpi-winrate');
  if (elPnL) {
    elPnL.textContent = formatSignedUSD(stats.total_net_pnl_usdt || 0);
    elPnL.className = 'kpi-value ' + (stats.total_net_pnl_usdt > 0 ? 'val-green' : (stats.total_net_pnl_usdt < 0 ? 'val-red' : ''));
  }
  if (elTradesCount) elTradesCount.textContent = `${stats.total_trades || 0} Closed Trades`;
  if (elWinRate) elWinRate.textContent = `${(stats.win_rate_pct || 0).toFixed(1)}% WR`;

  // Realized R:R
  const elRR = document.getElementById('kpi-rr');
  const elRRSub = document.getElementById('kpi-rr-sub');
  if (elRR) elRR.textContent = `${(stats.realized_rr || 0).toFixed(2)} : 1`;
  if (elRRSub) elRRSub.textContent = `Avg +${formatUSD(stats.avg_win_usdt || 0)} / -${formatUSD(stats.avg_loss_usdt || 0)}`;

  // Daily Drawdown
  const elDD = document.getElementById('kpi-drawdown');
  const elDDSub = document.getElementById('kpi-drawdown-sub');
  const ddPct = (data.daily_drawdown_pct || 0) * 100;
  if (elDD) {
    elDD.textContent = formatSignedPct(ddPct);
    elDD.className = 'kpi-value ' + (ddPct < -2.0 ? 'val-red' : (ddPct < 0 ? 'val-yellow' : 'val-green'));
  }
  if (elDDSub) {
    elDDSub.textContent = data.circuit_breaker ? 'CIRCUIT BREAKER TRIPPED' : 'Breaker: Max 3.00%';
    if (data.circuit_breaker) elDDSub.classList.add('val-red');
  }

  // Active Slots
  const elSlots = document.getElementById('kpi-slots');
  const elSlotsSub = document.getElementById('kpi-slots-sub');
  if (elSlots) elSlots.textContent = `${data.active_slots || 0} / ${data.max_slots || 3}`;
  if (elSlotsSub) {
    const avail = (data.max_slots || 3) - (data.active_slots || 0);
    elSlotsSub.textContent = `${avail} Slots Available`;
  }

  // Outcomes breakdown
  const elOutcomes = document.getElementById('kpi-outcomes');
  if (elOutcomes) {
    elOutcomes.innerHTML = `
      <span class="val-green">${stats.wins_count || 0} TP</span> • 
      <span class="val-yellow">${stats.break_even_count || 0} BE</span> • 
      <span class="val-red">${stats.losses_count || 0} SL</span>
    `;
  }

  // Render Active Positions
  renderActivePositions(data.positions || []);
}

function renderActivePositions(positions) {
  const container = document.getElementById('positions-container');
  const badge = document.getElementById('pos-count-badge');
  if (badge) badge.textContent = `${positions.length} Open`;

  if (!container) return;

  if (!positions || positions.length === 0) {
    container.classList.add('is-empty');
    container.innerHTML = `
      <div class="empty-state">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <circle cx="12" cy="12" r="10"/>
          <path d="M12 6v6l4 2"/>
        </svg>
        <div>No active positions open. Scanner is monitoring top liquid coins for high-conviction retests.</div>
      </div>
    `;
    return;
  }

  container.classList.remove('is-empty');

  let html = '';
  for (const pos of positions) {
    const sideClass = pos.side === 'LONG' ? 'pos-side-long' : 'pos-side-short';
    const pnlClass = pos.unrealized_pnl >= 0 ? 'val-green' : 'val-red';
    const roeClass = pos.unrealized_roe_pct >= 0 ? 'val-green' : 'val-red';
    const beBadge = pos.is_break_even ? '<span class="badge badge-be" style="margin-left: 6px;">BE MOVED</span>' : '';

    html += `
      <div class="position-card">
        <div class="pos-header">
          <div class="pos-symbol">
            <span>${escapeHtml(pos.symbol)}</span>
            <span class="pos-side-pill ${sideClass}">${escapeHtml(pos.side)}</span>
            ${beBadge}
          </div>
          <div class="pos-pnl-pill ${pnlClass}">
            ${formatSignedUSD(pos.unrealized_pnl)} (${formatSignedPct(pos.unrealized_roe_pct)})
          </div>
        </div>

        <div class="pos-details-grid">
          <div class="pos-metric">
            <span class="pos-metric-label">Entry Price</span>
            <span class="pos-metric-val">${pos.entry_price}</span>
          </div>
          <div class="pos-metric">
            <span class="pos-metric-label">Mark Price</span>
            <span class="pos-metric-val">${pos.mark_price > 0 ? pos.mark_price : '--'}</span>
          </div>
          <div class="pos-metric">
            <span class="pos-metric-label">Size / Qty</span>
            <span class="pos-metric-val">${pos.qty}</span>
          </div>
          <div class="pos-metric">
            <span class="pos-metric-label">Hold Duration</span>
            <span class="pos-metric-val">${formatDuration(pos.duration_sec)}</span>
          </div>
        </div>

        <div class="pos-targets">
          <span class="target-badge">SL: <span class="val-red">${pos.sl_price || '--'}</span></span>
          <span class="target-badge">TP: <span class="val-green">${pos.tp_price || '--'}</span></span>
          <span class="target-badge" style="margin-left: auto; color: var(--text-secondary); font-size: 10px;">
            ${formatTimeUTC(pos.open_time_utc)}
          </span>
        </div>
      </div>
    `;
  }
  container.innerHTML = html;
}

// 2. Fetch Scanner Radar
async function fetchScanner() {
  try {
    const res = await fetch('/api/scanner');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    renderScanner(data);
  } catch (err) {
    console.warn('Failed to fetch scanner radar:', err);
  }
}

function renderScanner(data) {
  const tbody = document.getElementById('radar-tbody');
  const lastScanEl = document.getElementById('radar-last-scan');
  if (lastScanEl && data.scanned_at_utc) {
    lastScanEl.textContent = `Last Scan: ${formatTimeUTC(data.scanned_at_utc)}`;
  }

  const countBadge = document.getElementById('radar-count-badge');
  if (countBadge && data.coins && data.coins.length > 0) {
    countBadge.textContent = `Top ${data.coins.length} Scanner`;
  }

  if (!tbody) return;

  const coins = data.coins || [];
  if (coins.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" style="text-align: center; color: var(--text-secondary); padding: 24px;">
          No watched coins available yet. Scanner is bootstrapping...
        </td>
      </tr>
    `;
    return;
  }

  let html = '';
  for (const c of coins) {
    // Add symbol to known list for signals filter
    if (c.symbol && !state.knownSymbols.has(c.symbol)) {
      state.knownSymbols.add(c.symbol);
      updateSymbolDropdown();
    }

    const dist = c.dist_vwap_pct || 0;
    const distClass = dist >= 0 ? 'val-green' : 'val-red';
    const barFillWidth = Math.min(Math.abs(dist) * 20, 100);
    const barFillColor = dist >= 0 ? 'var(--buy-green)' : 'var(--sell-red)';
    const rvolBadge = c.rvol >= 1.8 ? '<span class="badge badge-tp" style="font-size:10px;">HOT</span>' : '';

    html += `
      <tr>
        <td class="col-sticky-symbol sticky-col"><strong>${escapeHtml(c.symbol)}</strong></td>
        <td>${c.mark_price ? c.mark_price.toFixed(4) : '--'}</td>

        <td>${c.session_vwap ? c.session_vwap.toFixed(4) : '--'}</td>
        <td>
          <div class="vwap-bar-cell">
            <div class="vwap-bar-track">
              <div class="vwap-bar-fill" style="width: ${barFillWidth}%; background: ${barFillColor};"></div>
            </div>
            <span class="${distClass}">${formatSignedPct(dist)}</span>
          </div>
        </td>
        <td>${c.natr_pct ? c.natr_pct.toFixed(2) + '%' : '--'}</td>
        <td>${c.rvol ? c.rvol.toFixed(2) : '--'} ${rvolBadge}</td>
        <td>${c.spread_pct ? (c.spread_pct * 100).toFixed(3) + '%' : '--'}</td>
        <td class="${(c.funding_rate || 0) > 0 ? 'val-red' : 'val-green'}">
          ${c.funding_rate ? (c.funding_rate * 100).toFixed(4) + '%' : '0.0000%'}
        </td>
      </tr>
    `;
  }
  tbody.innerHTML = html;
}

function updateSymbolDropdown() {
  const select = document.getElementById('signal-symbol-select');
  if (!select) return;
  const currentVal = select.value;

  let opts = '<option value="ALL">All Symbols</option>';
  const sorted = Array.from(state.knownSymbols).sort();
  for (const sym of sorted) {
    opts += `<option value="${escapeHtml(sym)}">${escapeHtml(sym)}</option>`;
  }
  select.innerHTML = opts;
  select.value = currentVal;
}

// 3. Fetch Paginated Trade Ledger
async function fetchTrades() {
  try {
    const params = new URLSearchParams({
      page: state.trades.page,
      limit: state.trades.limit,
      status: state.trades.filter
    });
    const res = await fetch(`/api/trades?${params.toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    state.trades.total = data.total || 0;
    state.trades.totalPages = Math.max(1, Math.ceil(state.trades.total / state.trades.limit));
    renderTrades(data.trades || []);
  } catch (err) {
    console.warn('Failed to fetch trades:', err);
  }
}

function renderTrades(trades) {
  const tbody = document.getElementById('trades-tbody');
  const mobileStream = document.getElementById('trades-mobile-stream');
  const pageInfo = document.getElementById('trades-page-info');
  const totalBadge = document.getElementById('trades-total-badge');
  const btnPrev = document.getElementById('trades-btn-prev');
  const btnNext = document.getElementById('trades-btn-next');

  if (totalBadge) totalBadge.textContent = `${state.trades.total} Records`;
  if (pageInfo) pageInfo.textContent = `Page ${state.trades.page} of ${state.trades.totalPages} (${state.trades.total} Total)`;
  if (btnPrev) btnPrev.disabled = state.trades.page <= 1;
  if (btnNext) btnNext.disabled = state.trades.page >= state.trades.totalPages;

  if (!trades || trades.length === 0) {
    const emptyRow = `
      <tr>
        <td colspan="10" style="text-align: center; color: var(--text-secondary); padding: 24px;">
          No trade records match current filter.
        </td>
      </tr>
    `;
    if (tbody) tbody.innerHTML = emptyRow;
    if (mobileStream) {
      mobileStream.innerHTML = `<div class="empty-state">No trade records match current filter.</div>`;
    }
    return;
  }

  let tableHtml = '';
  let streamHtml = '';

  for (const t of trades) {
    const pnlClass = t.net_pnl_usdt > 0 ? 'val-green' : (t.net_pnl_usdt < 0 ? 'val-red' : '');
    const sideClass = t.side === 'LONG' ? 'pos-side-long' : 'pos-side-short';

    let reasonBadge = '<span class="badge badge-time">' + escapeHtml(t.exit_reason) + '</span>';
    if (t.exit_reason === 'TAKE_PROFIT') {
      reasonBadge = '<span class="badge badge-tp">TAKE PROFIT</span>';
    } else if (t.exit_reason === 'BREAK_EVEN') {
      reasonBadge = '<span class="badge badge-be">BREAK EVEN</span>';
    } else if (t.exit_reason === 'STOP_LOSS') {
      reasonBadge = '<span class="badge badge-sl">STOP LOSS</span>';
    }

    const roeVal = typeof t.roe_pct === 'number' && !isNaN(t.roe_pct) ? t.roe_pct : 0;

    // Desktop Row
    tableHtml += `
      <tr>
        <td class="col-sticky-symbol sticky-col"><strong>${escapeHtml(t.symbol)}</strong></td>
        <td>${formatTimeUTC(t.closed_at_utc)}</td>
        <td><span class="pos-side-pill ${sideClass}">${escapeHtml(t.side)}</span></td>
        <td>${escapeHtml(t.qty)}</td>
        <td>${escapeHtml(t.entry_price)}</td>
        <td>${escapeHtml(t.exit_price)}</td>
        <td class="${pnlClass}">
          <strong>${formatSignedUSD(t.net_pnl_usdt)}</strong>
          <span style="font-size: 11px; opacity: 0.85; margin-left: 3px;">(${formatSignedPct(roeVal)})</span>
        </td>
        <td>-${(t.mae_pct || 0).toFixed(2)}% / +${(t.mfe_pct || 0).toFixed(2)}%</td>
        <td>${formatDuration(t.duration_sec)}</td>
        <td>${reasonBadge}</td>
      </tr>
    `;

    // Mobile Card Stream
    streamHtml += `
      <div class="stream-card">
        <div class="stream-card-row">
          <div style="display:flex; align-items:center; gap:6px;">
            <strong>${escapeHtml(t.symbol)}</strong>
            <span class="pos-side-pill ${sideClass}">${escapeHtml(t.side)}</span>
          </div>
          <div>${reasonBadge}</div>
        </div>

        <div class="stream-card-row">
          <span class="stream-card-label">Net PnL</span>
          <span class="${pnlClass}" style="font-size:15px; font-weight:700;">
            ${formatSignedUSD(t.net_pnl_usdt)} <span style="font-size:12px; font-weight:500; opacity:0.85;">(${formatSignedPct(roeVal)})</span>
          </span>
        </div>

        <div class="stream-card-row">
          <span class="stream-card-label">Entry &rarr; Exit</span>
          <span>${escapeHtml(t.entry_price)} &rarr; ${escapeHtml(t.exit_price)}</span>
        </div>

        <div class="stream-card-row">
          <span class="stream-card-label">Hold / Excursions</span>
          <span>${formatDuration(t.duration_sec)} (MAE: -${(t.mae_pct||0).toFixed(2)}% / MFE: +${(t.mfe_pct||0).toFixed(2)}%)</span>
        </div>

        <div class="stream-card-row" style="font-size:11px; color:var(--text-secondary); border-top:1px dashed var(--border-subtle); padding-top:6px;">
          <span>Closed: ${formatTimeUTC(t.closed_at_utc)}</span>
          <span>Qty: ${escapeHtml(t.qty)}</span>
        </div>
      </div>
    `;
  }

  if (tbody) tbody.innerHTML = tableHtml;
  if (mobileStream) mobileStream.innerHTML = streamHtml;
}

// 4. Fetch Paginated Signal Audit Log
async function fetchSignals() {
  try {
    const params = new URLSearchParams({
      page: state.signals.page,
      limit: state.signals.limit,
      decision: state.signals.decision,
      symbol: state.signals.symbol
    });
    const res = await fetch(`/api/signals?${params.toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    state.signals.total = data.total || 0;
    state.signals.totalPages = Math.max(1, Math.ceil(state.signals.total / state.signals.limit));
    renderSignals(data.signals || []);
  } catch (err) {
    console.warn('Failed to fetch signals:', err);
  }
}

function formatAuditReason(decision, code, subReason) {
  const isApp = decision === 'APPROVE' || decision === 'APPROVED';
  if (isApp) {
    return `
      <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
        <span class="badge badge-approved" style="font-size:11px;">✓ CRITERIA MET</span>
        <span class="audit-pill audit-pill-app">Retest &amp; Delta Validated</span>
      </div>
    `;
  }

  // Strip internal telemetry tags like |OI_BOOTSTRAPPED
  let cleaned = (subReason || '')
    .replace(/\|?OI_BOOTSTRAPPED/g, '')
    .trim();

  const labelMap = {
    'NO_PRIOR_IMPULSE': 'No Prior Impulse (18b)',
    'VWAP_NOT_TESTED': 'VWAP Not Retested',
    'CANDLE_NOT_BULLISH': 'Candle Not Bullish',
    'CANDLE_NOT_BEARISH': 'Candle Not Bearish',
    'WEAK_DELTA_ZSCORE': 'Weak Delta Z-Score',
    'LOW_OI_DELTA': 'Low OI Delta (< +0.05%)',
    'ADVERSE_FUNDING': 'Adverse Funding Rate',
    'CLOSE_ABOVE_VWAP': 'Close Above VWAP',
    'CLOSE_BELOW_VWAP': 'Close Below VWAP',
    'BTC_GATE_VETO': 'BTC Macro Veto Active',
    'SLOTS_FULL': 'Slots Full (3/3)',
    'SYMBOL_ALREADY_ACTIVE': 'Symbol Already Active',
    'NATR_OUT_OF_BOUNDS': 'NATR Out of Bounds',
    'SPREAD_EXCEEDED': 'Spread Exceeded',
    'ENTRY_TOO_FAR_FROM_VWAP': 'Entry Too Far From VWAP',
    'STALE_MARK_PRICE_FEED': 'Stale Mark Price Feed',
    'INVALID_OI_INTERVAL': 'Invalid OI Interval',
    'MISSING_OI_DATA': 'Missing OI Data'
  };

  if (cleaned.includes('|')) {
    const parts = cleaned.split('|').map(p => p.trim()).filter(p => p.length > 0);
    const pills = parts.map(p => {
      const label = labelMap[p] || formatSubParam(p);
      return `<span class="audit-pill">${escapeHtml(label)}</span>`;
    }).join('');
    return `<div class="audit-reason-wrap">${pills}</div>`;
  }

  if (cleaned.length > 0) {
    const label = labelMap[cleaned] || formatSubParam(cleaned);
    return `<div class="audit-reason-wrap"><span class="audit-pill">${escapeHtml(label)}</span></div>`;
  }

  const codeLabel = labelMap[code] || (code ? code.replace(/_/g, ' ') : 'Setup Incomplete');
  return `<div class="audit-reason-wrap"><span class="audit-pill">${escapeHtml(codeLabel)}</span></div>`;
}

function formatSubParam(str) {
  if (str.startsWith('DIST_')) {
    const match = str.match(/DIST_([0-9.]+)_ATR_MAX_([0-9.]+)_ATR/);
    if (match) {
      return `Dist: ${match[1]} ATR (Max ${match[2]})`;
    }
  }
  if (str.startsWith('SPREAD_')) {
    const match = str.match(/SPREAD_([0-9.]+%?)_MAX_([0-9.]+%?)/);
    if (match) {
      return `Spread: ${match[1]} > Max ${match[2]}`;
    }
  }
  if (str.startsWith('NATR_')) {
    const match = str.match(/NATR_([0-9.]+%?)_OUTSIDE_([0-9.]+%?)_([0-9.]+%?)/);
    if (match) {
      return `NATR: ${match[1]} outside [${match[2]}, ${match[3]}]`;
    }
  }
  return str.replace(/_/g, ' ');
}

function renderSignals(signals) {
  const tbody = document.getElementById('signals-tbody');
  const mobileStream = document.getElementById('signals-mobile-stream');
  const pageInfo = document.getElementById('signals-page-info');
  const totalBadge = document.getElementById('signals-total-badge');
  const btnPrev = document.getElementById('signals-btn-prev');
  const btnNext = document.getElementById('signals-btn-next');

  if (totalBadge) totalBadge.textContent = `${state.signals.total} Signals`;
  if (pageInfo) pageInfo.textContent = `Page ${state.signals.page} of ${state.signals.totalPages} (${state.signals.total} Total)`;
  if (btnPrev) btnPrev.disabled = state.signals.page <= 1;
  if (btnNext) btnNext.disabled = state.signals.page >= state.signals.totalPages;

  if (!signals || signals.length === 0) {
    const emptyRow = `
      <tr>
        <td colspan="10" style="text-align: center; color: var(--text-secondary); padding: 24px;">
          No signals match current filter.
        </td>
      </tr>
    `;
    if (tbody) tbody.innerHTML = emptyRow;
    if (mobileStream) {
      mobileStream.innerHTML = `<div class="empty-state">No signals match current filter.</div>`;
    }
    return;
  }

  let tableHtml = '';
  let streamHtml = '';

  for (const s of signals) {
    const isApp = s.decision === 'APPROVE' || s.decision === 'APPROVED';
    const decBadge = isApp 
      ? '<span class="badge badge-approved">APPROVED</span>' 
      : '<span class="badge badge-rejected">REJECTED</span>';
    const sideClass = s.side === 'LONG' ? 'pos-side-long' : 'pos-side-short';
    const zScoreClass = Math.abs(s.bar_delta_zscore) >= 1.5 ? 'val-green' : '';

    const reasonHtml = formatAuditReason(s.decision, s.reject_reason_code, s.reject_sub_reason);

    // Also populate known symbols
    if (s.symbol && !state.knownSymbols.has(s.symbol)) {
      state.knownSymbols.add(s.symbol);
      updateSymbolDropdown();
    }

    // Desktop Row
    tableHtml += `
      <tr>
        <td class="col-sticky-symbol sticky-col"><strong>${escapeHtml(s.symbol)}</strong></td>
        <td>${formatTimeUTC(s.candle_open_time_utc)}</td>
        <td><span class="pos-side-pill ${sideClass}">${escapeHtml(s.side)}</span></td>
        <td>${s.close_price ? s.close_price.toFixed(4) : '--'}</td>
        <td>${s.session_vwap ? s.session_vwap.toFixed(4) : '--'}</td>
        <td class="${zScoreClass}">${s.bar_delta_zscore ? s.bar_delta_zscore.toFixed(2) : '--'}</td>
        <td>${s.oi_delta_pct ? formatSignedPct(s.oi_delta_pct) : '--'}</td>
        <td>${s.rvol ? s.rvol.toFixed(2) : '--'}</td>
        <td>${decBadge}</td>
        <td class="col-audit-reason">${reasonHtml}</td>
      </tr>
    `;

    // Mobile Card Stream
    streamHtml += `
      <div class="stream-card">
        <div class="stream-card-row">
          <div style="display:flex; align-items:center; gap:6px;">
            <strong style="font-size:15px;">${escapeHtml(s.symbol)}</strong>
            <span class="pos-side-pill ${sideClass}">${escapeHtml(s.side)}</span>
          </div>
          <div>${decBadge}</div>
        </div>

        <div class="stream-card-row">
          <span class="stream-card-label">Price / VWAP</span>
          <span style="font-family:var(--font-mono); font-weight:600;">
            ${s.close_price ? s.close_price.toFixed(4) : '--'} <span style="color:var(--text-secondary);">&bull;</span> VWAP: ${s.session_vwap ? s.session_vwap.toFixed(4) : '--'}
          </span>
        </div>

        <div class="stream-card-row">
          <span class="stream-card-label">Metrics</span>
          <span style="font-family:var(--font-mono); font-size:12px;">
            Z: <strong class="${zScoreClass}">${s.bar_delta_zscore ? s.bar_delta_zscore.toFixed(2) : '--'}</strong> &bull; 
            OI: <strong>${s.oi_delta_pct ? formatSignedPct(s.oi_delta_pct) : '--'}</strong> &bull; 
            RVOL: <strong>${s.rvol ? s.rvol.toFixed(2) : '--'}</strong>
          </span>
        </div>

        <div style="border-top:1px dashed var(--border-subtle); padding-top:8px; margin-top:2px;">
          <div class="stream-card-label" style="margin-bottom:4px;">Evaluation:</div>
          <div style="display:flex; flex-wrap:wrap; gap:4px;">${reasonHtml}</div>
        </div>

        <div class="stream-card-row" style="font-size:11px; color:var(--text-secondary); padding-top:4px;">
          <span>Candle: ${formatTimeUTC(s.candle_open_time_utc)}</span>
          <span>Signal #${s.id}</span>
        </div>
      </div>
    `;
  }

  if (tbody) tbody.innerHTML = tableHtml;
  if (mobileStream) mobileStream.innerHTML = streamHtml;
}


// 5. Setup Event Listeners
function setupEvents() {
  // Manual Refresh
  const btnRefresh = document.getElementById('btn-refresh');
  if (btnRefresh) {
    btnRefresh.addEventListener('click', async () => {
      btnRefresh.classList.add('spinning');
      await Promise.all([fetchSummary(), fetchScanner(), fetchTrades(), fetchSignals()]);
      setTimeout(() => btnRefresh.classList.remove('spinning'), 500);
    });
  }

  // Trade Filter Pills
  document.querySelectorAll('[data-filter-trade]').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-filter-trade]').forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      state.trades.filter = btn.getAttribute('data-filter-trade');
      state.trades.page = 1;
      fetchTrades();
    });
  });

  // Trade Limit
  const tradeLimitSelect = document.getElementById('trade-limit-select');
  if (tradeLimitSelect) {
    tradeLimitSelect.addEventListener('change', (e) => {
      state.trades.limit = parseInt(e.target.value, 10) || 10;
      state.trades.page = 1;
      fetchTrades();
    });
  }

  // Trade Pagination Buttons
  const tradePrev = document.getElementById('trades-btn-prev');
  const tradeNext = document.getElementById('trades-btn-next');
  if (tradePrev) {
    tradePrev.addEventListener('click', () => {
      if (state.trades.page > 1) {
        state.trades.page--;
        fetchTrades();
      }
    });
  }
  if (tradeNext) {
    tradeNext.addEventListener('click', () => {
      if (state.trades.page < state.trades.totalPages) {
        state.trades.page++;
        fetchTrades();
      }
    });
  }

  // Signal Decision Pills
  document.querySelectorAll('[data-filter-signal]').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-filter-signal]').forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      state.signals.decision = btn.getAttribute('data-filter-signal');
      state.signals.page = 1;
      fetchSignals();
    });
  });

  // Signal Symbol Dropdown
  const signalSymbolSelect = document.getElementById('signal-symbol-select');
  if (signalSymbolSelect) {
    signalSymbolSelect.addEventListener('change', (e) => {
      state.signals.symbol = e.target.value;
      state.signals.page = 1;
      fetchSignals();
    });
  }

  // Signal Limit
  const signalLimitSelect = document.getElementById('signal-limit-select');
  if (signalLimitSelect) {
    signalLimitSelect.addEventListener('change', (e) => {
      state.signals.limit = parseInt(e.target.value, 10) || 15;
      state.signals.page = 1;
      fetchSignals();
    });
  }

  // Signal Pagination Buttons
  const signalPrev = document.getElementById('signals-btn-prev');
  const signalNext = document.getElementById('signals-btn-next');
  if (signalPrev) {
    signalPrev.addEventListener('click', () => {
      if (state.signals.page > 1) {
        state.signals.page--;
        fetchSignals();
      }
    });
  }
  if (signalNext) {
    signalNext.addEventListener('click', () => {
      if (state.signals.page < state.signals.totalPages) {
        state.signals.page++;
        fetchSignals();
      }
    });
  }
}

// 6. Real-time UTC Clock
function startClock() {
  const clockEl = document.getElementById('utc-clock');
  function tick() {
    const now = new Date();
    const hh = String(now.getUTCHours()).padStart(2, '0');
    const mm = String(now.getUTCMinutes()).padStart(2, '0');
    const ss = String(now.getUTCSeconds()).padStart(2, '0');
    if (clockEl) {
      clockEl.textContent = `${hh}:${mm}:${ss} UTC`;
    }
  }
  tick();
  setInterval(tick, 1000);
}

// 7. Initialize Application
document.addEventListener('DOMContentLoaded', () => {
  startClock();
  setupEvents();

  // Initial fetches
  fetchSummary();
  fetchScanner();
  fetchTrades();
  fetchSignals();

  // Background Polling
  // High-frequency polling (every 3 seconds for summary & positions)
  setInterval(() => {
    if (state.isPollingActive) {
      fetchSummary();
    }
  }, 3000);

  // Scanner radar polling (every 5 seconds)
  setInterval(() => {
    if (state.isPollingActive) {
      fetchScanner();
    }
  }, 5000);

  // Register PWA Service Worker
  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('/sw.js?v=2').then((reg) => {
        reg.update();
      }).catch((err) => {
        console.warn('Service worker registration failed:', err);
      });
    });
  }
});
