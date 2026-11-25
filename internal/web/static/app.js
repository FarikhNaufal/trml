'use strict';

// =========================================================================
// TRML Quant Execution Desk Controller (Basecoat v1.3.0 + Vanilla JS)
// =========================================================================

const POLL_SUMMARY_IDLE_MS = 5000;
const POLL_SUMMARY_ACTIVE_MS = 3000;
const POLL_SCANNER_MS = 5000;

const state = {
  pinRequired: false,
  pinVerified: false,
  storedPin: '',
  pinSubmitting: false,

  activeTab: 'desk', // 'desk' | 'ledger' | 'audit'
  activeSlotsCount: 0,
  consecutiveErrors: 0,

  lastSummaryData: null,
  lastScannerCoins: [],
  scannerFilter: '',

  tradesPage: 1,
  tradesLimit: 20,
  tradesTotal: 0,
  tradesStrategy: 'ALL',

  signalsPage: 1,
  signalsLimit: 20,
  signalsTotal: 0,
  signalsFilter: 'ALL',
  signalRecordsById: new Map(),

  summaryTimer: null,
  scannerTimer: null,
  summaryInFlight: false,
  scannerInFlight: false,

  aiRequestSeq: 0,
  aiRawMarkdown: '',
  highlightSymbol: ''
};

// -------------------------------------------------------------------------
// Security & Format Utilities
// -------------------------------------------------------------------------
function escapeHtml(val) {
  if (val === null || val === undefined) return '';
  return String(val)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function showToast(category, title, description = '') {
  const toaster = document.getElementById('toaster');
  if (!toaster || typeof toaster.toast !== 'function') return;
  toaster.toast({ category, title, description });
}

function fmtMoney(val, showSign = false) {
  const n = Number(val || 0);
  const abs = Math.abs(n).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
  if (n < 0) return `-$${abs}`;
  if (showSign && n > 0) return `+$${abs}`;
  return `$${abs}`;
}

function fmtPrice(val) {
  const n = Number(val || 0);
  if (n === 0 || !Number.isFinite(n)) return '0';
  if (Math.abs(n) >= 1000) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (Math.abs(n) >= 10) return n.toFixed(3);
  if (Math.abs(n) >= 0.1) return n.toFixed(4);
  return n.toFixed(5);
}

function fmtPct(val, decimals = 2, showSign = false) {
  const n = Number(val || 0);
  if (!Number.isFinite(n)) return '0.00%';
  const prefix = showSign && n > 0 ? '+' : '';
  return `${prefix}${n.toFixed(decimals)}%`;
}

function fmtFunding(val) {
  const pct = Number(val || 0) * 100;
  const prefix = pct > 0 ? '+' : '';
  return `${prefix}${pct.toFixed(4)}%`;
}

function fmtDuration(sec) {
  const s = Math.max(0, Math.floor(Number(sec || 0)));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const r = s % 60;
  if (m < 60) return `${m}m ${r}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function fmtUTC(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return escapeHtml(iso);
  const pad = (x) => String(x).padStart(2, '0');
  return `${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

function formatStrategyLabel(raw) {
  const map = {
    VWAP_PULLBACK: 'VWAP',
    VOL_BREAKOUT: 'VOL',
    REL_RESIDUAL: 'REL',
    LIQ_SWEEP: 'SWEEP',
    EMA_PULLBACK: 'EMA',
    VWAP_BREAKDOWN: 'BRK',
    STRUCTURE_BREAK: 'STRUCT'
  };
  const key = String(raw || 'VWAP_PULLBACK').trim().toUpperCase();
  return map[key] || key.slice(0, 8);
}

function sanitizeMarkdown(md) {
  if (!md) return '';
  const escaped = escapeHtml(md);
  const blocks = [];
  const withPlaceholders = escaped.replace(/```([\s\S]*?)```/g, (_, code) => {
    const idx = blocks.length;
    blocks.push(`<pre><code>${code.trim()}</code></pre>`);
    return `\n@@CODEBLOCK_${idx}@@\n`;
  });

  const lines = withPlaceholders.split('\n');
  let html = '';
  let inList = false;

  const closeList = () => {
    if (inList) {
      html += '</ul>';
      inList = false;
    }
  };

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      closeList();
      continue;
    }
    if (line.startsWith('@@CODEBLOCK_') && line.endsWith('@@')) {
      closeList();
      const idx = Number(line.slice(12, -2));
      html += blocks[idx] || '';
      continue;
    }
    if (line.startsWith('### ')) {
      closeList();
      html += `<h3>${inlineMd(line.slice(4))}</h3>`;
      continue;
    }
    if (line.startsWith('## ')) {
      closeList();
      html += `<h2>${inlineMd(line.slice(3))}</h2>`;
      continue;
    }
    if (line.startsWith('# ')) {
      closeList();
      html += `<h1>${inlineMd(line.slice(2))}</h1>`;
      continue;
    }
    if (line.startsWith('- ') || line.startsWith('* ')) {
      if (!inList) {
        html += '<ul>';
        inList = true;
      }
      html += `<li>${inlineMd(line.slice(2))}</li>`;
      continue;
    }
    closeList();
    html += `<p>${inlineMd(line)}</p>`;
  }
  closeList();
  return html;
}

function inlineMd(s) {
  return s
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/\*([^*]+)\*/g, '<em>$1</em>');
}

function parseSignalReasonTokens(raw) {
  const labelMap = {
    EXECUTED: 'Order Executed',
    COOLDOWN_ACTIVE: 'Post-Exit Cooldown',
    SLOT_FULL: 'Max Slots Reached',
    CIRCUIT_BREAKER_ACTIVE: 'Circuit Breaker Halt',
    DAILY_DD_EXCEEDED: 'Daily Drawdown Limit',
    CLUSTER_SL_COOLDOWN: 'Cluster SL Cooldown',
    BTC_DUMP_BLOCK: 'BTC Dump Gate',
    BTC_PUMP_BLOCK: 'BTC Pump Gate',
    BTC_CHOP_BLOCK: 'BTC Chop Filter',
    SPREAD_TOO_WIDE: 'Wide Spread Gate',
    LOW_NATR: 'Insufficient NATR',
    HIGH_NATR: 'Excessive NATR',
    LOW_RVOL: 'Low Relative Volume',
    FRENZY_RVOL_BLOCK: 'Frenzy RVOL Cap',
    HIGH_ADVERSE_FUNDING: 'Adverse Funding Gate',
    CROWDED_FUNDING_BLOCK: 'Crowded Funding Filter',
    NEGATIVE_OI_DELTA: 'OI Delta Gate',
    STALE_BOOK_TICKER: 'Stale Book Ticker',
    STALE_MARK_PRICE: 'Stale Mark Price',
    OUTSIDE_UTC_HOURS: 'Blocked UTC Hour',
    MACRO_REGIME_BLOCK: 'Macro Regime Gate',
    WEAK_HTF_TREND: 'Weak HTF Trend',
    NO_PULLBACK_SETUP: 'No Pullback Setup',
    WEAK_BOUNCE_DELTA: 'Weak Orderflow Delta'
  };
  return String(raw || '')
    .split('|')
    .map((p) => p.trim())
    .filter(Boolean)
    .map((part) => {
      const colonIdx = part.indexOf(':');
      const code = colonIdx > 0 ? part.slice(0, colonIdx).trim() : part;
      const detail = colonIdx > 0 ? part.slice(colonIdx + 1).trim() : '';
      return {
        raw: part,
        code,
        label: labelMap[code] || code.replace(/_/g, ' '),
        detail
      };
    });
}

// -------------------------------------------------------------------------
// Network Layer (with PIN auth handling)
// -------------------------------------------------------------------------
async function apiFetch(url, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.pinRequired && state.storedPin) {
    headers.set('X-TRML-PIN', state.storedPin);
  }
  const resp = await fetch(url, { ...options, headers });
  if (resp.status === 401 && state.pinRequired) {
    state.pinVerified = false;
    state.storedPin = '';
    try { localStorage.removeItem('trml_pin'); } catch (e) {}
    showPinScreen('Session locked or PIN rejected. Re-enter PIN.');
    throw new Error('Unauthorized (PIN required)');
  }
  if (!resp.ok) {
    const errText = await resp.text().catch(() => resp.statusText);
    throw new Error(errText || `HTTP ${resp.status}`);
  }
  return resp.json();
}

function setConnectionState(online, label) {
  const badge = document.getElementById('conn-badge');
  const text = document.getElementById('conn-text');
  if (badge) badge.dataset.state = online ? 'online' : 'error';
  if (text) text.textContent = label;
}

// -------------------------------------------------------------------------
// Theme Management (Light Default, Synced with window.basecoat.theme)
// -------------------------------------------------------------------------
function syncThemeUI() {
  const isDark = document.documentElement.classList.contains('dark');
  const sun = document.getElementById('theme-icon-sun');
  const moon = document.getElementById('theme-icon-moon');
  if (sun) sun.classList.toggle('hidden', !isDark);
  if (moon) moon.classList.toggle('hidden', isDark);
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.setAttribute('content', isDark ? '#0b1017' : '#f6f8fa');
}

function toggleTheme() {
  if (window.basecoat?.theme) {
    window.basecoat.theme.toggle();
  } else {
    const isDark = !document.documentElement.classList.contains('dark');
    document.documentElement.classList.toggle('dark', isDark);
    try { localStorage.setItem('themeMode', isDark ? 'dark' : 'light'); } catch (e) {}
  }
  syncThemeUI();
}

// -------------------------------------------------------------------------
// Auth / PIN Screen (Basecoat .input-otp + Touch Keypad)
// -------------------------------------------------------------------------
function showPinScreen(errorMsg = '') {
  stopPolling();
  const pinScreen = document.getElementById('pin-screen');
  const appShell = document.getElementById('app-shell');
  const errEl = document.getElementById('pin-error-msg');
  const input = document.getElementById('pin-otp-input');

  if (appShell) appShell.classList.add('hidden');
  if (pinScreen) {
    pinScreen.classList.remove('hidden');
    pinScreen.setAttribute('aria-hidden', 'false');
  }
  if (errEl) errEl.textContent = errorMsg;
  if (input) {
    input.value = '';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    requestAnimationFrame(() => input.focus());
  }
}

function showAppShell() {
  const pinScreen = document.getElementById('pin-screen');
  const appShell = document.getElementById('app-shell');
  if (pinScreen) {
    pinScreen.classList.add('hidden');
    pinScreen.setAttribute('aria-hidden', 'true');
  }
  if (appShell) appShell.classList.remove('hidden');
}

async function verifyPIN(pin, fromStored = false) {
  if (state.pinSubmitting || !pin || pin.length !== 6) return;
  state.pinSubmitting = true;
  const errEl = document.getElementById('pin-error-msg');
  if (errEl && !fromStored) errEl.textContent = 'Verifying…';

  try {
    const resp = await fetch('/api/auth/verify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pin })
    });
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      if (fromStored) {
        try { localStorage.removeItem('trml_pin'); } catch (e) {}
        showPinScreen('');
      } else {
        showPinScreen(body.error || 'Invalid PIN. Try again.');
      }
      return;
    }

    state.pinVerified = true;
    state.storedPin = pin;
    try { localStorage.setItem('trml_pin', pin); } catch (e) {}
    showAppShell();
    startDeskDataCycle(true);
  } catch (err) {
    if (fromStored) {
      showPinScreen('Failed to connect for session check.');
    } else {
      showPinScreen('Network error verifying PIN.');
    }
  } finally {
    state.pinSubmitting = false;
  }
}

function initPinControls() {
  const input = document.getElementById('pin-otp-input');
  const keypad = document.getElementById('pin-keypad');

  if (input) {
    input.addEventListener('input', () => {
      const clean = input.value.replace(/\D/g, '').slice(0, 6);
      if (clean !== input.value) {
        input.value = clean;
      }
      const errEl = document.getElementById('pin-error-msg');
      if (errEl && errEl.textContent && clean.length < 6) errEl.textContent = '';
      if (clean.length === 6) {
        verifyPIN(clean, false);
      }
    });
  }

  if (keypad && input) {
    keypad.addEventListener('click', (ev) => {
      const btn = ev.target.closest('button[data-key]');
      if (!btn || state.pinSubmitting) return;
      const key = btn.dataset.key;
      let val = input.value.replace(/\D/g, '');
      if (key === 'CLEAR') {
        val = '';
      } else if (key === 'DEL') {
        val = val.slice(0, -1);
      } else if (val.length < 6) {
        val += key;
      }
      input.value = val;
      input.dispatchEvent(new Event('input', { bubbles: true }));
    });
  }
}

// -------------------------------------------------------------------------
// Render Summary, Risk Bars, Strategy Stats & Point 3 Telemetry
// -------------------------------------------------------------------------
function renderSummary(data) {
  state.lastSummaryData = data;
  state.activeSlotsCount = Number(data.active_slots || 0);

  // Env & Model badges
  const envBadge = document.getElementById('env-badge');
  if (envBadge) {
    const isLive = String(data.env || '').toUpperCase() === 'LIVE';
    envBadge.textContent = isLive ? 'LIVE' : 'DRY-RUN';
    envBadge.className = `badge ${isLive ? 'trml-badge-neg' : 'trml-badge-info'}`;
  }
  const sheetModel = document.getElementById('ai-sheet-model');
  if (sheetModel && data.gemini_model) {
    sheetModel.textContent = data.gemini_model;
  }

  // Account Capital
  const equityEl = document.getElementById('kpi-equity');
  const balEl = document.getElementById('kpi-balance');
  const deltaEl = document.getElementById('kpi-equity-delta');
  if (equityEl) equityEl.textContent = fmtMoney(data.equity);
  if (balEl) balEl.textContent = fmtMoney(data.balance);

  const realizedNet = Number(data.stats?.total_net_pnl_usdt || 0);
  if (deltaEl) {
    deltaEl.textContent = `${fmtMoney(realizedNet, true)} Realized`;
    deltaEl.className = `badge trml-mono ${
      realizedNet > 0 ? 'trml-badge-pos' : realizedNet < 0 ? 'trml-badge-neg' : ''
    }`;
  }

  // Daily Drawdown Bar (daily_drawdown_pct is negative or 0; max_daily_drawdown_pct e.g. 0.05)
  const ddAbsPct = Math.abs(Number(data.daily_drawdown_pct || 0)) * 100;
  const maxDdRaw = Number(data.max_daily_drawdown_pct || 0);
  const maxDdPct = maxDdRaw > 1 ? maxDdRaw : maxDdRaw * 100;
  const ddUtil = maxDdPct > 0 ? Math.min(100, (ddAbsPct / maxDdPct) * 100) : 0;

  const ddValEl = document.getElementById('kpi-dd-val');
  const ddMaxEl = document.getElementById('kpi-dd-max');
  const ddBar = document.getElementById('kpi-dd-bar');
  const ddWrap = ddBar?.parentElement;
  if (ddValEl) ddValEl.textContent = `-${ddAbsPct.toFixed(2)}%`;
  if (ddMaxEl && maxDdPct > 0) ddMaxEl.textContent = `/ -${maxDdPct.toFixed(1)}% cap`;
  if (ddBar) ddBar.style.width = `${ddUtil.toFixed(1)}%`;
  if (ddWrap) {
    ddWrap.setAttribute('aria-valuenow', ddUtil.toFixed(0));
    ddWrap.dataset.tone = ddUtil >= 75 ? 'neg' : ddUtil >= 45 ? 'warn' : 'pos';
  }

  // Slots Bar
  const activeSlots = Number(data.active_slots || 0);
  const maxSlots = Math.max(1, Number(data.max_slots || 1));
  const slotPct = Math.min(100, (activeSlots / maxSlots) * 100);
  const slotsValEl = document.getElementById('kpi-slots-val');
  const slotsPctEl = document.getElementById('kpi-slots-pct');
  const slotsBar = document.getElementById('kpi-slots-bar');
  const slotsWrap = slotsBar?.parentElement;
  if (slotsValEl) slotsValEl.textContent = `${activeSlots} / ${data.max_slots || 0}`;
  if (slotsPctEl) slotsPctEl.textContent = `(${slotPct.toFixed(0)}%)`;
  if (slotsBar) slotsBar.style.width = `${slotPct.toFixed(1)}%`;
  if (slotsWrap) {
    slotsWrap.setAttribute('aria-valuenow', slotPct.toFixed(0));
    slotsWrap.dataset.tone = slotPct >= 100 ? 'warn' : 'info';
  }

  // Outcomes
  const stats = data.stats || {};
  const setText = (id, txt) => {
    const el = document.getElementById(id);
    if (el) el.textContent = txt;
  };
  setText('stat-trades', String(stats.total_trades || 0));
  setText('stat-winrate', fmtPct(stats.win_rate_pct || 0, 1));
  const pf = Number(stats.profit_factor || 0);
  setText('stat-pf', pf > 99 ? '99+' : pf.toFixed(2));
  setText('stat-avgwin', fmtMoney(stats.avg_win_usdt || 0, true));
  setText('stat-avgloss', fmtMoney(-Math.abs(Number(stats.avg_loss_usdt || 0))));
  const avgR = Number(stats.avg_r || 0);
  const avgREl = document.getElementById('stat-avgr');
  if (avgREl) {
    avgREl.textContent = `${avgR >= 0 ? '+' : ''}${avgR.toFixed(2)} R`;
    avgREl.className = `badge trml-mono ${avgR > 0 ? 'trml-badge-pos' : avgR < 0 ? 'trml-badge-neg' : ''}`;
  }

  // Strategy Attribution
  renderStrategyStats(data.strategy_stats || []);

  // Point 3 Telemetry & Alerts
  renderTelemetry(data.telemetry || null, Boolean(data.circuit_breaker));

  // Active Positions
  renderPositions(data.positions || []);

  // Tab counter
  const deskBadge = document.getElementById('tab-desk-count');
  if (deskBadge) deskBadge.textContent = String(activeSlots);

  refreshCommandSymbols();
}

function renderStrategyStats(items) {
  const host = document.getElementById('strategy-stats-list');
  if (!host) return;
  if (!items || items.length === 0) {
    host.innerHTML = '<div class="trml-muted-text">No strategy trades recorded in this mode yet.</div>';
    return;
  }
  host.innerHTML = items
    .map((item) => {
      const net = Number(item.total_net_pnl_usdt || 0);
      const cls = net > 0 ? 'trml-pos' : net < 0 ? 'trml-neg' : '';
      return `
        <div class="trml-strat-row">
          <span class="trml-strat-name" title="${escapeHtml(item.strategy_id)}">${escapeHtml(item.strategy_id)}</span>
          <span class="trml-mono trml-muted-text">${Number(item.total_trades || 0)} trd</span>
          <span class="trml-mono">${Number(item.win_rate_pct || 0).toFixed(1)}%</span>
          <span class="trml-mono trml-strat-pnl ${cls}">${fmtMoney(net, true)}</span>
        </div>
      `;
    })
    .join('');
}

function renderTelemetry(telem, circuitBreaker) {
  const chipLabel = document.getElementById('telemetry-chip-label');
  const popWrap = document.getElementById('telemetry-popover');
  const popBody = document.getElementById('telemetry-popover-body');
  const inlineSummary = document.getElementById('telemetry-inline-summary');
  const alertStack = document.getElementById('alert-stack');

  const regime = telem?.macro_regime || 'UNKNOWN';
  const breadthTxt = telem?.market_breadth_valid ? `${Number(telem.market_breadth_pct || 0).toFixed(0)}% B` : 'B:n/a';
  const tfTxt = telem?.timeframe || '—';

  if (popWrap) popWrap.dataset.regime = regime;
  if (chipLabel) chipLabel.textContent = telem ? `${regime} · ${tfTxt} · ${breadthTxt}` : 'Engine: RAM n/a';
  if (inlineSummary) inlineSummary.textContent = telem ? `${regime} / ${tfTxt}` : '—';

  if (popBody) {
    if (!telem) {
      popBody.innerHTML = '<span class="trml-muted-text">Telemetry callback unavailable in standalone mode.</span>';
    } else {
      const kvs = [
        ['Macro Regime', escapeHtml(regime)],
        ['BTC 24h Return', fmtPct(telem.macro_return_24h_pct, 2, true)],
        ['Efficiency Ratio', Number(telem.macro_er || 0).toFixed(3)],
        ['Vol Ratio (1h)', Number(telem.macro_vol_ratio || 0).toFixed(2) + 'x'],
        ['Market Breadth', telem.market_breadth_valid ? fmtPct(telem.market_breadth_pct, 1) : 'Warmup'],
        ['Range Risk Mult', Number(telem.range_risk_mult || 0).toFixed(2) + 'x'],
        ['Vol Exp Ratio / Mult', `${Number(telem.vol_expansion_ratio || 0).toFixed(2)} / ${Number(telem.vol_expansion_risk_mult || 0).toFixed(2)}x`],
        ['Cadence / Side', `${escapeHtml(telem.timeframe)} · ${escapeHtml(telem.allowed_side || 'BOTH')}`],
        ['Scan Interval', escapeHtml(telem.scanner_interval || '—')],
        ['Cluster SL Guard', telem.cluster_sl_paused ? `PAUSED (${fmtUTC(telem.cluster_sl_until_utc)})` : 'ARMED']
      ];
      popBody.innerHTML = kvs
        .map(([k, v]) => `<div class="trml-telem-kv"><span>${k}</span><strong>${v}</strong></div>`)
        .join('');
    }
  }

  // Conditional Alerts
  if (alertStack) {
    const alerts = [];
    if (circuitBreaker) {
      alerts.push(`
        <div class="alert" data-variant="destructive" role="alert">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>
          <h2>Circuit Breaker Tripped</h2>
          <section><p>New entries halted after daily drawdown limit triggered.</p></section>
        </div>
      `);
    }
    if (telem?.cluster_sl_paused) {
      alerts.push(`
        <div class="alert" role="status">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          <h2>Portfolio Cluster SL Cooldown Active</h2>
          <section><p>Correlated entries temporarily paused until ${escapeHtml(fmtUTC(telem.cluster_sl_until_utc))} UTC.</p></section>
        </div>
      `);
    }
    if (state.consecutiveErrors >= 2) {
      alerts.push(`
        <div class="alert" data-variant="destructive" role="alert">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m2 2 20 20"/><path d="M8.5 16.5a5 5 0 0 1 7 0"/></svg>
          <h2>Telemetry Sync Interrupted</h2>
          <section><p>Retrying connection to bot HTTP endpoint automatically.</p></section>
        </div>
      `);
    }
    alertStack.innerHTML = alerts.join('');
    alertStack.classList.toggle('hidden', alerts.length === 0);
  }
}

// -------------------------------------------------------------------------
// Active Positions Renderer
// -------------------------------------------------------------------------
function calcSlTpTrackPct(pos) {
  const sl = Number(pos.sl_price || 0);
  const tp = Number(pos.tp_price || 0);
  const mark = Number(pos.mark_price || 0);
  if (sl <= 0 || tp <= 0 || mark <= 0 || sl === tp) return { pct: 50, tone: 'info' };
  const isLong = String(pos.side).toUpperCase() === 'LONG';
  const raw = isLong ? ((mark - sl) / (tp - sl)) * 100 : ((sl - mark) / (sl - tp)) * 100;
  const pct = Math.max(2, Math.min(98, raw));
  const tone = pct >= 50 ? 'pos' : pct <= 28 ? 'neg' : 'warn';
  return { pct, tone };
}

function renderPositions(positions) {
  const countBadge = document.getElementById('positions-count-badge');
  const tableWrap = document.getElementById('positions-table-wrap');
  const tbody = document.getElementById('positions-tbody');
  const mobileList = document.getElementById('positions-mobile-list');
  const emptyEl = document.getElementById('positions-empty');

  if (countBadge) countBadge.textContent = `${positions.length} open`;

  if (!positions || positions.length === 0) {
    tableWrap?.classList.add('hidden');
    mobileList?.classList.add('hidden');
    emptyEl?.classList.remove('hidden');
    if (tbody) tbody.innerHTML = '';
    if (mobileList) mobileList.innerHTML = '';
    return;
  }

  emptyEl?.classList.add('hidden');
  tableWrap?.classList.remove('hidden');
  mobileList?.classList.remove('hidden');

  if (tbody) {
    tbody.innerHTML = positions
      .map((p) => {
        const isLong = String(p.side).toUpperCase() === 'LONG';
        const pnl = Number(p.unrealized_pnl || 0);
        const roe = Number(p.unrealized_roe_pct || 0);
        const pnlCls = pnl > 0 ? 'trml-pos' : pnl < 0 ? 'trml-neg' : '';
        const track = calcSlTpTrackPct(p);
        const beChip = p.is_break_even ? '<span class="badge trml-badge-info">BE</span>' : '';
        const stratShort = formatStrategyLabel(p.strategy_id);
        const hlCls = state.highlightSymbol === p.symbol ? 'trml-row-highlight' : '';

        return `
          <tr class="${hlCls}" data-symbol="${escapeHtml(p.symbol)}">
            <td class="col-sticky trml-mono">${escapeHtml(p.symbol)} ${beChip}</td>
            <td>
              <span class="badge ${isLong ? 'trml-badge-pos' : 'trml-badge-neg'}">${escapeHtml(p.side)}</span>
              <span class="badge" data-variant="secondary" title="${escapeHtml(p.strategy_id)}">${escapeHtml(stratShort)}</span>
            </td>
            <td class="num trml-mono">${escapeHtml(p.qty)} @ ${fmtPrice(p.entry_price)}</td>
            <td class="num trml-mono">${fmtPrice(p.mark_price)}</td>
            <td>
              <div class="trml-range-track">
                <div class="trml-range-nums trml-mono">
                  <span class="trml-neg">SL ${fmtPrice(p.sl_price)}</span>
                  <span class="trml-pos">TP ${fmtPrice(p.tp_price)}</span>
                </div>
                <div class="progress trml-progress" data-tone="${track.tone}">
                  <span style="width: ${track.pct.toFixed(1)}%"></span>
                </div>
              </div>
            </td>
            <td class="num trml-mono ${pnlCls}">
              <strong>${fmtMoney(pnl, true)}</strong>
              <div class="trml-muted-text">(${fmtPct(roe, 2, true)})</div>
            </td>
            <td class="num trml-mono trml-muted-text">${fmtDuration(p.duration_sec)}</td>
            <td class="num">
              <button type="button" class="btn" data-variant="outline" data-size="xs" onclick="inspectPositionAI('${escapeHtml(p.symbol)}')">Health</button>
            </td>
          </tr>
        `;
      })
      .join('');
  }

  if (mobileList) {
    mobileList.innerHTML = positions
      .map((p) => {
        const isLong = String(p.side).toUpperCase() === 'LONG';
        const pnl = Number(p.unrealized_pnl || 0);
        const roe = Number(p.unrealized_roe_pct || 0);
        const pnlCls = pnl > 0 ? 'trml-pos' : pnl < 0 ? 'trml-neg' : '';
        const track = calcSlTpTrackPct(p);
        return `
          <div class="trml-mobile-item">
            <div class="trml-mobile-head">
              <div class="trml-panel-title-wrap">
                <strong class="trml-mono">${escapeHtml(p.symbol)}</strong>
                <span class="badge ${isLong ? 'trml-badge-pos' : 'trml-badge-neg'}">${escapeHtml(p.side)}</span>
                <span class="badge" data-variant="secondary">${escapeHtml(formatStrategyLabel(p.strategy_id))}</span>
                ${p.is_break_even ? '<span class="badge trml-badge-info">BE</span>' : ''}
              </div>
              <strong class="trml-mono ${pnlCls}">${fmtMoney(pnl, true)} (${fmtPct(roe, 2, true)})</strong>
            </div>
            <div class="trml-mobile-grid trml-mono">
              <div class="trml-mobile-kv"><span>Entry / Qty</span><strong>${fmtPrice(p.entry_price)} (${escapeHtml(p.qty)})</strong></div>
              <div class="trml-mobile-kv"><span>Mark</span><strong>${fmtPrice(p.mark_price)}</strong></div>
              <div class="trml-mobile-kv"><span>Hold</span><strong>${fmtDuration(p.duration_sec)}</strong></div>
              <div class="trml-mobile-kv"><span>SL</span><strong class="trml-neg">${fmtPrice(p.sl_price)}</strong></div>
              <div class="trml-mobile-kv"><span>TP</span><strong class="trml-pos">${fmtPrice(p.tp_price)}</strong></div>
              <div class="trml-mobile-kv">
                <span>AI Check</span>
                <button type="button" class="btn" data-variant="outline" data-size="xs" onclick="inspectPositionAI('${escapeHtml(p.symbol)}')">Health</button>
              </div>
            </div>
            <div class="progress trml-progress" data-tone="${track.tone}">
              <span style="width: ${track.pct.toFixed(1)}%"></span>
            </div>
          </div>
        `;
      })
      .join('');
  }
}

// -------------------------------------------------------------------------
// Scanner Radar Renderer (Tab-Aware)
// -------------------------------------------------------------------------
function renderScanner(coins, scannedAtUTC) {
  state.lastScannerCoins = coins || [];
  const countBadge = document.getElementById('scanner-count-badge');
  const updatedEl = document.getElementById('scanner-updated');
  const tbody = document.getElementById('scanner-tbody');
  const mobileList = document.getElementById('scanner-mobile-list');
  const tableWrap = document.getElementById('scanner-table-wrap');
  const emptyEl = document.getElementById('scanner-empty');

  const query = state.scannerFilter.trim().toUpperCase();
  const filtered = query
    ? state.lastScannerCoins.filter((c) => String(c.symbol).toUpperCase().includes(query))
    : state.lastScannerCoins;

  if (countBadge) {
    countBadge.textContent = query
      ? `${filtered.length} / ${state.lastScannerCoins.length} coins`
      : `${state.lastScannerCoins.length} coins`;
  }
  if (updatedEl && scannedAtUTC) updatedEl.textContent = `Sync ${fmtUTC(scannedAtUTC)}`;

  if (filtered.length === 0) {
    tableWrap?.classList.add('hidden');
    mobileList?.classList.add('hidden');
    emptyEl?.classList.remove('hidden');
    if (tbody) tbody.innerHTML = '';
    if (mobileList) mobileList.innerHTML = '';
    return;
  }

  emptyEl?.classList.add('hidden');
  tableWrap?.classList.remove('hidden');
  mobileList?.classList.remove('hidden');

  if (tbody) {
    tbody.innerHTML = filtered
      .map((c) => {
        const dist = Number(c.dist_vwap_pct || 0);
        const distCls = dist > 0 ? 'trml-pos' : dist < 0 ? 'trml-neg' : '';
        const hlCls = state.highlightSymbol === c.symbol ? 'trml-row-highlight' : '';
        return `
          <tr class="${hlCls}" data-symbol="${escapeHtml(c.symbol)}">
            <td class="col-sticky trml-mono">${escapeHtml(c.symbol)}</td>
            <td class="num trml-mono">${fmtPrice(c.mark_price)}</td>
            <td class="num trml-mono trml-muted-text">${fmtPrice(c.session_vwap)}</td>
            <td class="num trml-mono ${distCls}">${fmtPct(dist, 2, true)}</td>
            <td class="num trml-mono">${Number(c.spread_pct || 0).toFixed(3)}%</td>
            <td class="num trml-mono">${fmtFunding(c.funding_rate)}</td>
            <td class="num trml-mono">${Number(c.natr_pct || 0).toFixed(2)}%</td>
            <td class="num trml-mono">${Number(c.rvol || 0).toFixed(2)}x</td>
          </tr>
        `;
      })
      .join('');
  }

  if (mobileList) {
    mobileList.innerHTML = filtered
      .map((c) => {
        const dist = Number(c.dist_vwap_pct || 0);
        const distCls = dist > 0 ? 'trml-pos' : dist < 0 ? 'trml-neg' : '';
        return `
          <div class="trml-mobile-item">
            <div class="trml-mobile-head">
              <strong class="trml-mono">${escapeHtml(c.symbol)}</strong>
              <span class="trml-mono ${distCls}">VWAP ${fmtPct(dist, 2, true)}</span>
            </div>
            <div class="trml-mobile-grid trml-mono">
              <div class="trml-mobile-kv"><span>Mark</span><strong>${fmtPrice(c.mark_price)}</strong></div>
              <div class="trml-mobile-kv"><span>NATR / RVOL</span><strong>${Number(c.natr_pct || 0).toFixed(2)}% / ${Number(c.rvol || 0).toFixed(2)}x</strong></div>
              <div class="trml-mobile-kv"><span>Funding</span><strong>${fmtFunding(c.funding_rate)}</strong></div>
            </div>
          </div>
        `;
      })
      .join('');
  }
  refreshCommandSymbols();
}

// -------------------------------------------------------------------------
// Trade Ledger & Pagination
// -------------------------------------------------------------------------
async function fetchTrades(page = state.tradesPage) {
  state.tradesPage = Math.max(1, page);
  try {
    const url = `/api/trades?page=${state.tradesPage}&limit=${state.tradesLimit}&strategy=${encodeURIComponent(state.tradesStrategy)}`;
    const data = await apiFetch(url);
    renderTrades(data);
  } catch (err) {
    showToast('error', 'Failed to load Trade Ledger', err.message);
  }
}

function renderTrades(data) {
  const trades = data.trades || [];
  state.tradesTotal = Number(data.total || 0);

  const summaryEl = document.getElementById('trades-summary');
  const tableWrap = document.getElementById('trades-table-wrap');
  const tbody = document.getElementById('trades-tbody');
  const mobileList = document.getElementById('trades-mobile-list');
  const emptyEl = document.getElementById('trades-empty');
  const pagEl = document.getElementById('trades-pagination');

  if (summaryEl) summaryEl.textContent = `${state.tradesTotal} trades (${state.tradesStrategy})`;

  if (trades.length === 0) {
    tableWrap?.classList.add('hidden');
    mobileList?.classList.add('hidden');
    emptyEl?.classList.remove('hidden');
    if (tbody) tbody.innerHTML = '';
    if (mobileList) mobileList.innerHTML = '';
  } else {
    emptyEl?.classList.add('hidden');
    tableWrap?.classList.remove('hidden');
    mobileList?.classList.remove('hidden');

    if (tbody) {
      tbody.innerHTML = trades
        .map((t) => {
          const isLong = String(t.side).toUpperCase() === 'LONG';
          const pnl = Number(t.net_pnl_usdt || 0);
          const pnlCls = pnl > 0 ? 'trml-pos' : pnl < 0 ? 'trml-neg' : '';
          const cachedTag = t.has_autopsy ? 'Cached' : 'Autopsy';
          const btnVariant = t.has_autopsy ? 'secondary' : 'outline';
          return `
            <tr>
              <td class="trml-mono trml-muted-text">${fmtUTC(t.closed_at_utc)}</td>
              <td class="col-sticky trml-mono">${escapeHtml(t.symbol)}</td>
              <td>
                <span class="badge ${isLong ? 'trml-badge-pos' : 'trml-badge-neg'}">${escapeHtml(t.side)}</span>
                <span class="badge" data-variant="secondary" title="${escapeHtml(t.strategy_id)}">${escapeHtml(formatStrategyLabel(t.strategy_id))}</span>
              </td>
              <td class="num trml-mono">${fmtPrice(t.entry_price)} → ${fmtPrice(t.exit_price)}</td>
              <td><span class="badge" data-variant="outline">${escapeHtml(t.exit_reason)}</span></td>
              <td class="num trml-mono trml-muted-text">${Number(t.mae_pct || 0).toFixed(2)}% / ${Number(t.mfe_pct || 0).toFixed(2)}%</td>
              <td class="num trml-mono ${pnlCls}"><strong>${fmtMoney(pnl, true)}</strong></td>
              <td class="num">
                <button type="button" class="btn" data-variant="${btnVariant}" data-size="xs" onclick="inspectTradeAutopsy('${escapeHtml(t.client_order_id)}', '${escapeHtml(t.symbol)}')">${cachedTag}</button>
              </td>
            </tr>
          `;
        })
        .join('');
    }

    if (mobileList) {
      mobileList.innerHTML = trades
        .map((t) => {
          const isLong = String(t.side).toUpperCase() === 'LONG';
          const pnl = Number(t.net_pnl_usdt || 0);
          const pnlCls = pnl > 0 ? 'trml-pos' : pnl < 0 ? 'trml-neg' : '';
          return `
            <div class="trml-mobile-item">
              <div class="trml-mobile-head">
                <div class="trml-panel-title-wrap">
                  <strong class="trml-mono">${escapeHtml(t.symbol)}</strong>
                  <span class="badge ${isLong ? 'trml-badge-pos' : 'trml-badge-neg'}">${escapeHtml(t.side)}</span>
                  <span class="badge" data-variant="outline">${escapeHtml(t.exit_reason)}</span>
                </div>
                <strong class="trml-mono ${pnlCls}">${fmtMoney(pnl, true)}</strong>
              </div>
              <div class="trml-mobile-grid trml-mono">
                <div class="trml-mobile-kv"><span>Entry → Exit</span><strong>${fmtPrice(t.entry_price)} → ${fmtPrice(t.exit_price)}</strong></div>
                <div class="trml-mobile-kv"><span>MAE / MFE</span><strong>${Number(t.mae_pct || 0).toFixed(2)}% / ${Number(t.mfe_pct || 0).toFixed(2)}%</strong></div>
                <div class="trml-mobile-kv"><span>AI</span><button type="button" class="btn" data-variant="outline" data-size="xs" onclick="inspectTradeAutopsy('${escapeHtml(t.client_order_id)}', '${escapeHtml(t.symbol)}')">${t.has_autopsy ? 'Cached' : 'Autopsy'}</button></div>
              </div>
            </div>
          `;
        })
        .join('');
    }
  }

  renderPagination(pagEl, state.tradesPage, state.tradesLimit, state.tradesTotal, 'fetchTrades');
}

// -------------------------------------------------------------------------
// Signal Audit Log & Detail Inspector Dialog
// -------------------------------------------------------------------------
async function fetchSignals(page = state.signalsPage) {
  state.signalsPage = Math.max(1, page);
  try {
    const url = `/api/signals?page=${state.signalsPage}&limit=${state.signalsLimit}&status=${encodeURIComponent(state.signalsFilter)}`;
    const data = await apiFetch(url);
    renderSignals(data);
  } catch (err) {
    showToast('error', 'Failed to load Signal Audit', err.message);
  }
}

function renderSignals(data) {
  const signals = data.signals || [];
  state.signalsTotal = Number(data.total || 0);
  state.signalRecordsById.clear();
  signals.forEach((s) => state.signalRecordsById.set(String(s.id), s));

  const summaryEl = document.getElementById('signals-summary');
  const tableWrap = document.getElementById('signals-table-wrap');
  const tbody = document.getElementById('signals-tbody');
  const mobileList = document.getElementById('signals-mobile-list');
  const emptyEl = document.getElementById('signals-empty');
  const pagEl = document.getElementById('signals-pagination');

  if (summaryEl) summaryEl.textContent = `${state.signalsTotal} signals (${state.signalsFilter})`;

  if (signals.length === 0) {
    tableWrap?.classList.add('hidden');
    mobileList?.classList.add('hidden');
    emptyEl?.classList.remove('hidden');
    if (tbody) tbody.innerHTML = '';
    if (mobileList) mobileList.innerHTML = '';
  } else {
    emptyEl?.classList.add('hidden');
    tableWrap?.classList.remove('hidden');
    mobileList?.classList.remove('hidden');

    if (tbody) {
      tbody.innerHTML = signals
        .map((sig) => {
          const isLong = String(sig.side).toUpperCase() === 'LONG';
          const tokens = parseSignalReasonTokens(sig.reject_reason);
          const summaryNote = tokens.map((t) => (t.detail ? `${t.label} (${t.detail})` : t.label)).join(' · ') || '—';
          return `
            <tr>
              <td class="trml-mono trml-muted-text">${fmtUTC(sig.candle_open_time_utc)}</td>
              <td class="col-sticky trml-mono">${escapeHtml(sig.symbol)}</td>
              <td>
                <span class="badge ${isLong ? 'trml-badge-pos' : 'trml-badge-neg'}">${escapeHtml(sig.side)}</span>
                <span class="badge" data-variant="secondary">${escapeHtml(formatStrategyLabel(sig.strategy_id))}</span>
              </td>
              <td>
                <span class="badge ${sig.is_executed ? 'trml-badge-pos' : 'trml-badge-warn'}">${sig.is_executed ? 'EXECUTED' : 'REJECTED'}</span>
              </td>
              <td><div class="trml-audit-note" title="${escapeHtml(summaryNote)}">${escapeHtml(summaryNote)}</div></td>
              <td class="num">
                <button type="button" class="btn" data-variant="outline" data-size="xs" onclick="openSignalInspector('${escapeHtml(String(sig.id))}')">Metrics</button>
              </td>
            </tr>
          `;
        })
        .join('');
    }

    if (mobileList) {
      mobileList.innerHTML = signals
        .map((sig) => {
          const isLong = String(sig.side).toUpperCase() === 'LONG';
          const tokens = parseSignalReasonTokens(sig.reject_reason);
          const summaryNote = tokens.map((t) => t.label).join(' · ') || '—';
          return `
            <div class="trml-mobile-item">
              <div class="trml-mobile-head">
                <div class="trml-panel-title-wrap">
                  <strong class="trml-mono">${escapeHtml(sig.symbol)}</strong>
                  <span class="badge ${isLong ? 'trml-badge-pos' : 'trml-badge-neg'}">${escapeHtml(sig.side)}</span>
                  <span class="badge ${sig.is_executed ? 'trml-badge-pos' : 'trml-badge-warn'}">${sig.is_executed ? 'EXEC' : 'REJ'}</span>
                </div>
                <button type="button" class="btn" data-variant="outline" data-size="xs" onclick="openSignalInspector('${escapeHtml(String(sig.id))}')">Metrics</button>
              </div>
              <div class="trml-muted-text">${fmtUTC(sig.candle_open_time_utc)} · ${escapeHtml(summaryNote)}</div>
            </div>
          `;
        })
        .join('');
    }
  }

  renderPagination(pagEl, state.signalsPage, state.signalsLimit, state.signalsTotal, 'fetchSignals');
}

function openSignalInspector(signalId) {
  const sig = state.signalRecordsById.get(String(signalId));
  const dialog = document.getElementById('signal-detail-dialog');
  const titleEl = document.getElementById('signal-detail-title');
  const subEl = document.getElementById('signal-detail-subtitle');
  const bodyEl = document.getElementById('signal-detail-body');
  if (!sig || !dialog || !bodyEl) return;

  if (titleEl) titleEl.textContent = `${sig.symbol} · ${sig.side} (${formatStrategyLabel(sig.strategy_id)})`;
  if (subEl) subEl.textContent = `${fmtUTC(sig.candle_open_time_utc)} · ${sig.is_executed ? 'EXECUTED' : 'REJECTED'}`;

  const tokens = parseSignalReasonTokens(sig.reject_reason);
  const chipsHtml = tokens
    .map((t) => {
      const text = t.detail ? `${t.label}: ${t.detail}` : t.label;
      return `<span class="badge ${sig.is_executed ? 'trml-badge-pos' : 'trml-badge-warn'}">${escapeHtml(text)}</span>`;
    })
    .join('');

  const metrics = [
    ['Close Price', fmtPrice(sig.close_price)],
    ['EMA 200', fmtPrice(sig.ema200)],
    ['Session VWAP', fmtPrice(sig.session_vwap)],
    ['ATR (14)', fmtPrice(sig.atr14)],
    ['Bar Delta Z', Number(sig.bar_delta_zscore || 0).toFixed(2)],
    ['OI Delta %', fmtPct(sig.oi_delta_pct, 2, true)],
    ['Funding Rate', fmtFunding(sig.funding_rate)],
    ['Strategy Module', escapeHtml(sig.strategy_id || 'VWAP_PULLBACK')]
  ];

  bodyEl.innerHTML = `
    <div class="trml-reason-chips">${chipsHtml || '<span class="badge">No reason tags</span>'}</div>
    <div class="trml-signal-metrics-grid">
      ${metrics.map(([k, v]) => `<div class="trml-telem-kv"><span>${k}</span><strong>${v}</strong></div>`).join('')}
    </div>
  `;

  if (!dialog.open) dialog.showModal();
}

function renderPagination(container, page, limit, total, fnName) {
  if (!container) return;
  const totalPages = Math.max(1, Math.ceil(total / limit));
  const start = total === 0 ? 0 : (page - 1) * limit + 1;
  const end = Math.min(total, page * limit);
  container.innerHTML = `
    <span class="trml-mono trml-muted-text">Showing ${start}–${end} of ${total} (Page ${page}/${totalPages})</span>
    <div class="trml-page-btns">
      <button type="button" class="btn" data-variant="outline" data-size="xs" ${page <= 1 ? 'disabled' : ''} onclick="${fnName}(${page - 1})">Prev</button>
      <button type="button" class="btn" data-variant="outline" data-size="xs" ${page >= totalPages ? 'disabled' : ''} onclick="${fnName}(${page + 1})">Next</button>
    </div>
  `;
}

// -------------------------------------------------------------------------
// AI Copilot Slide-Over Sheet
// -------------------------------------------------------------------------
function openAISheetLoading(title, subtitle) {
  const sheet = document.getElementById('ai-sheet');
  const titleEl = document.getElementById('ai-sheet-title');
  const subEl = document.getElementById('ai-sheet-subtitle');
  const bodyEl = document.getElementById('ai-sheet-body');
  if (!sheet || !bodyEl) return;

  state.aiRawMarkdown = '';
  if (titleEl) titleEl.textContent = title;
  if (subEl) subEl.textContent = subtitle;

  bodyEl.innerHTML = `
    <div class="trml-skeleton-stack">
      <div class="trml-ai-loading-head">
        <span class="trml-spinner" aria-hidden="true"></span>
        <span>Analyzing execution telemetry &amp; trade history…</span>
      </div>
      <div class="skeleton" style="height: 16px; width: 70%;"></div>
      <div class="skeleton" style="height: 14px; width: 100%;"></div>
      <div class="skeleton" style="height: 14px; width: 92%;"></div>
      <div class="skeleton" style="height: 84px; width: 100%;"></div>
      <div class="skeleton" style="height: 14px; width: 85%;"></div>
    </div>
  `;

  if (!sheet.open) sheet.showModal();
}

function finishAISheet(seq, title, subtitle, markdown, cached = false) {
  if (seq !== state.aiRequestSeq) return;
  const titleEl = document.getElementById('ai-sheet-title');
  const subEl = document.getElementById('ai-sheet-subtitle');
  const bodyEl = document.getElementById('ai-sheet-body');
  if (!bodyEl) return;

  state.aiRawMarkdown = markdown || '';
  if (titleEl) titleEl.textContent = title;
  if (subEl) subEl.textContent = cached ? `${subtitle} · Cached` : subtitle;
  bodyEl.innerHTML = `<div class="trml-ai-markdown">${sanitizeMarkdown(markdown)}</div>`;
}

function failAISheet(seq, errMsg) {
  if (seq !== state.aiRequestSeq) return;
  const bodyEl = document.getElementById('ai-sheet-body');
  if (!bodyEl) return;
  bodyEl.innerHTML = `
    <div class="alert" data-variant="destructive" role="alert">
      <h2>AI Evaluation Failed</h2>
      <section><p>${escapeHtml(errMsg)}</p></section>
    </div>
  `;
}

async function runExecutiveSummaryAI() {
  const seq = ++state.aiRequestSeq;
  openAISheetLoading('Executive Daily Debrief', 'Account performance & macro alignment');
  try {
    const res = await apiFetch('/api/ai/executive-summary');
    finishAISheet(seq, 'Executive Daily Debrief', 'Generated by Gemini', res.analysis || res.markdown || res.summary || 'No summary returned.');
  } catch (err) {
    failAISheet(seq, err.message);
  }
}

async function inspectPositionAI(symbol) {
  const seq = ++state.aiRequestSeq;
  openAISheetLoading(`Position Health: ${symbol}`, 'Live mark-to-market & structure check');
  try {
    const res = await apiFetch(`/api/ai/position-health?symbol=${encodeURIComponent(symbol)}`);
    finishAISheet(seq, `Position Health: ${symbol}`, 'Real-time position assessment', res.analysis || res.markdown || 'No assessment returned.');
  } catch (err) {
    failAISheet(seq, err.message);
  }
}

async function inspectTradeAutopsy(clientOrderId, symbol) {
  const seq = ++state.aiRequestSeq;
  openAISheetLoading(`Trade Autopsy: ${symbol}`, `Order ${clientOrderId}`);
  try {
    const res = await apiFetch(`/api/ai/trade-autopsy?client_order_id=${encodeURIComponent(clientOrderId)}`);
    finishAISheet(seq, `Trade Autopsy: ${symbol}`, `Order ${clientOrderId}`, res.analysis || res.autopsy || res.markdown || 'No autopsy available.', Boolean(res.cached));
  } catch (err) {
    failAISheet(seq, err.message);
  }
}

async function runPatternMinerAI() {
  const seq = ++state.aiRequestSeq;
  openAISheetLoading('AI Pattern Miner', 'Cluster audit of recent losing & edge trades');
  try {
    const res = await apiFetch('/api/ai/pattern-miner');
    finishAISheet(seq, 'AI Pattern Miner', 'Recurring failure & setup attribution', res.analysis || res.markdown || 'No patterns detected.');
  } catch (err) {
    failAISheet(seq, err.message);
  }
}

// -------------------------------------------------------------------------
// Command Palette (⌘K) & Dialog Backdrop Wiring
// -------------------------------------------------------------------------
function refreshCommandSymbols() {
  const grp = document.getElementById('cmd-symbols-group');
  const menu = document.getElementById('cmd-menu');
  if (!grp) return;

  const syms = new Set();
  (state.lastSummaryData?.positions || []).forEach((p) => syms.add(p.symbol));
  (state.lastScannerCoins || []).forEach((c) => syms.add(c.symbol));

  const heading = '<div role="heading" id="cmd-grp-symbols">Active &amp; Scanned Symbols</div>';
  const items = Array.from(syms)
    .sort()
    .slice(0, 25)
    .map((sym) => `<div role="menuitem" data-cmd="symbol:${escapeHtml(sym)}" data-keywords="coin pair radar">${escapeHtml(sym)}</div>`)
    .join('');

  grp.innerHTML = heading + items;
  if (menu && typeof menu.refresh === 'function') menu.refresh();
}

function handleCommandAction(cmd) {
  if (!cmd) return;
  if (cmd === 'tab:desk') selectTab('tab-desk');
  else if (cmd === 'tab:ledger') selectTab('tab-ledger');
  else if (cmd === 'tab:audit') selectTab('tab-audit');
  else if (cmd === 'action:refresh') forceRefreshNow();
  else if (cmd === 'action:theme') toggleTheme();
  else if (cmd === 'action:ai-debrief') runExecutiveSummaryAI();
  else if (cmd === 'action:ai-miner') runPatternMinerAI();
  else if (cmd.startsWith('symbol:')) {
    const sym = cmd.slice(7);
    state.highlightSymbol = sym;
    state.scannerFilter = '';
    const filterEl = document.getElementById('scanner-filter');
    if (filterEl) filterEl.value = '';
    selectTab('tab-desk');
    if (state.lastSummaryData) renderPositions(state.lastSummaryData.positions || []);
    renderScanner(state.lastScannerCoins, null);
    setTimeout(() => {
      const targetRow = document.querySelector(`tr[data-symbol="${CSS.escape(sym)}"]`);
      targetRow?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 60);
  }
}

function setupDialogDismiss(dialogEl) {
  if (!dialogEl) return;
  let pressedOutside = false;
  dialogEl.addEventListener('pointerdown', (e) => {
    pressedOutside = e.target === dialogEl;
  });
  dialogEl.addEventListener('click', (e) => {
    if (pressedOutside && e.target === dialogEl) {
      dialogEl.close();
    }
    pressedOutside = false;
  });
}

// -------------------------------------------------------------------------
// Tab-Aware Polling Engine
// -------------------------------------------------------------------------
function selectTab(tabBtnId) {
  const tabsEl = document.getElementById('workspace-tabs');
  const btn = document.getElementById(tabBtnId);
  if (tabsEl && btn && typeof tabsEl.select === 'function') {
    tabsEl.select(btn, false);
  }
  onTabActivated(tabBtnId);
}

function onTabActivated(tabBtnId) {
  const map = { 'tab-desk': 'desk', 'tab-ledger': 'ledger', 'tab-audit': 'audit' };
  const nextTab = map[tabBtnId] || 'desk';
  const changed = state.activeTab !== nextTab;
  state.activeTab = nextTab;

  if (nextTab === 'desk') {
    if (changed) pollScannerOnce();
    scheduleNextScannerPoll();
  } else {
    clearTimeout(state.scannerTimer);
    if (nextTab === 'ledger') fetchTrades(state.tradesPage);
    if (nextTab === 'audit') fetchSignals(state.signalsPage);
  }
}

async function pollSummaryOnce() {
  if (state.summaryInFlight || (state.pinRequired && !state.pinVerified)) return;
  state.summaryInFlight = true;
  try {
    const data = await apiFetch('/api/summary');
    state.consecutiveErrors = 0;
    setConnectionState(true, 'Live');
    renderSummary(data);
  } catch (err) {
    state.consecutiveErrors++;
    setConnectionState(false, 'Reconnecting');
    if (state.lastSummaryData) {
      renderTelemetry(state.lastSummaryData.telemetry || null, Boolean(state.lastSummaryData.circuit_breaker));
    }
  } finally {
    state.summaryInFlight = false;
  }
}

async function pollScannerOnce() {
  if (state.scannerInFlight || state.activeTab !== 'desk' || (state.pinRequired && !state.pinVerified)) return;
  state.scannerInFlight = true;
  try {
    const data = await apiFetch('/api/scanner');
    renderScanner(data.coins || [], data.scanned_at_utc);
  } catch (err) {
    // Summary poller owns connection alert state
  } finally {
    state.scannerInFlight = false;
  }
}

function scheduleNextSummaryPoll() {
  clearTimeout(state.summaryTimer);
  if (document.visibilityState === 'hidden' || (state.pinRequired && !state.pinVerified)) return;
  const interval = state.activeSlotsCount > 0 ? POLL_SUMMARY_ACTIVE_MS : POLL_SUMMARY_IDLE_MS;
  state.summaryTimer = setTimeout(async () => {
    await pollSummaryOnce();
    scheduleNextSummaryPoll();
  }, interval);
}

function scheduleNextScannerPoll() {
  clearTimeout(state.scannerTimer);
  if (document.visibilityState === 'hidden' || state.activeTab !== 'desk' || (state.pinRequired && !state.pinVerified)) return;
  state.scannerTimer = setTimeout(async () => {
    await pollScannerOnce();
    scheduleNextScannerPoll();
  }, POLL_SCANNER_MS);
}

function stopPolling() {
  clearTimeout(state.summaryTimer);
  clearTimeout(state.scannerTimer);
}

function startDeskDataCycle(immediate = true) {
  stopPolling();
  if (immediate) {
    pollSummaryOnce().then(scheduleNextSummaryPoll);
    if (state.activeTab === 'desk') {
      pollScannerOnce().then(scheduleNextScannerPoll);
    } else if (state.activeTab === 'ledger') {
      fetchTrades(state.tradesPage);
    } else if (state.activeTab === 'audit') {
      fetchSignals(state.signalsPage);
    }
  } else {
    scheduleNextSummaryPoll();
    scheduleNextScannerPoll();
  }
}

function forceRefreshNow() {
  startDeskDataCycle(true);
  if (state.activeTab === 'ledger') fetchTrades(state.tradesPage);
  if (state.activeTab === 'audit') fetchSignals(state.signalsPage);
  showToast('info', 'Telemetry refreshed', 'Synchronized with bot state.');
}

// -------------------------------------------------------------------------
// Bootstrap & Event Bindings
// -------------------------------------------------------------------------
function startClock() {
  const el = document.getElementById('utc-clock');
  if (!el) return;
  const tick = () => {
    const now = new Date();
    const pad = (x) => String(x).padStart(2, '0');
    el.textContent = `${pad(now.getUTCHours())}:${pad(now.getUTCMinutes())}:${pad(now.getUTCSeconds())} UTC`;
  };
  tick();
  setInterval(tick, 1000);
}

async function initAuthAndBoot() {
  try {
    const status = await fetch('/api/auth/status').then((r) => r.json());
    state.pinRequired = Boolean(status.pin_required);

    if (!state.pinRequired) {
      state.pinVerified = true;
      showAppShell();
      startDeskDataCycle(true);
      return;
    }

    let saved = '';
    try { saved = localStorage.getItem('trml_pin') || ''; } catch (e) {}
    if (/^\d{6}$/.test(saved)) {
      await verifyPIN(saved, true);
    } else {
      showPinScreen('');
    }
  } catch (err) {
    showAppShell();
    setConnectionState(false, 'Offline');
    showToast('error', 'Server unreachable', 'Could not fetch /api/auth/status.');
  }
}

document.addEventListener('DOMContentLoaded', () => {
  syncThemeUI();
  startClock();
  initPinControls();

  // Keep icons & meta in sync when Basecoat toggles theme
  document.addEventListener('basecoat:themechange', syncThemeUI);

  document.getElementById('theme-toggle-btn')?.addEventListener('click', toggleTheme);
  document.getElementById('refresh-btn')?.addEventListener('click', forceRefreshNow);
  document.getElementById('ai-exec-btn')?.addEventListener('click', runExecutiveSummaryAI);
  document.getElementById('ai-miner-btn')?.addEventListener('click', runPatternMinerAI);

  // Sheet copy & close
  document.getElementById('ai-sheet-close-btn')?.addEventListener('click', () => {
    document.getElementById('ai-sheet')?.close();
  });
  document.getElementById('ai-sheet-copy-btn')?.addEventListener('click', async () => {
    if (!state.aiRawMarkdown) return;
    try {
      await navigator.clipboard.writeText(state.aiRawMarkdown);
      showToast('success', 'Copied to clipboard', 'AI report copied as Markdown.');
    } catch (e) {
      showToast('warning', 'Copy unavailable', 'Clipboard permission denied.');
    }
  });

  setupDialogDismiss(document.getElementById('ai-sheet'));
  setupDialogDismiss(document.getElementById('signal-detail-dialog'));
  setupDialogDismiss(document.getElementById('cmd-dialog'));

  // Command palette open triggers (Button & Cmd+K / Ctrl+K)
  const openCmdPalette = () => {
    const dlg = document.getElementById('cmd-dialog');
    const inp = document.getElementById('cmd-input');
    if (!dlg) return;
    refreshCommandSymbols();
    if (inp) {
      inp.value = '';
      document.getElementById('cmd-menu')?.refresh?.();
    }
    if (!dlg.open) dlg.showModal();
    requestAnimationFrame(() => inp?.focus());
  };

  document.getElementById('cmd-trigger-btn')?.addEventListener('click', openCmdPalette);
  document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      if (!state.pinRequired || state.pinVerified) openCmdPalette();
    }
  });
  document.getElementById('cmd-menu-list')?.addEventListener('click', (e) => {
    const item = e.target.closest('[role="menuitem"][data-cmd]');
    if (item) handleCommandAction(item.dataset.cmd);
  });

  // Tab switch hook
  document.getElementById('workspace-tabs')?.addEventListener('click', (e) => {
    const tabBtn = e.target.closest('[role="tab"]');
    if (tabBtn?.id) onTabActivated(tabBtn.id);
  });

  // Scanner live filter
  document.getElementById('scanner-filter')?.addEventListener('input', (e) => {
    state.scannerFilter = e.target.value || '';
    renderScanner(state.lastScannerCoins, null);
  });

  // Trades strategy segmented control
  document.getElementById('trades-strat-group')?.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-strategy]');
    if (!btn) return;
    state.tradesStrategy = btn.dataset.strategy || 'ALL';
    document.querySelectorAll('#trades-strat-group button[data-strategy]').forEach((b) => {
      b.setAttribute('aria-pressed', String(b === btn));
    });
    fetchTrades(1);
  });

  // Signals decision segmented control
  document.getElementById('signals-filter-group')?.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-status]');
    if (!btn) return;
    state.signalsFilter = btn.dataset.status || 'ALL';
    document.querySelectorAll('#signals-filter-group button[data-status]').forEach((b) => {
      b.setAttribute('aria-pressed', String(b === btn));
    });
    fetchSignals(1);
  });

  // Pause background polling when tab is hidden
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      stopPolling();
    } else if (!state.pinRequired || state.pinVerified) {
      startDeskDataCycle(true);
    }
  });

  // Register Service Worker
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
  }

  initAuthAndBoot();
});
