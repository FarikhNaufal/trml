'use strict';

// =========================================================================
// TRML_DESK — WebTUI Fixed-Viewport Controller (Pure Vanilla JS)
// =========================================================================

const POLL_SUMMARY_IDLE_MS = 5000;
const POLL_SUMMARY_ACTIVE_MS = 3000;
const POLL_SCANNER_MS = 5000;
const THEMES = ['obsidian', 'catppuccin-mocha', 'nord', 'gruvbox-dark-hard'];

const state = {
  pinRequired: false,
  pinVerified: false,
  storedPin: '',
  pinSubmitting: false,

  activeTab: 'desk', // 'desk' | 'signals' | 'ledger' | 'copilot'
  activeSlotsCount: 0,
  maxSlots: 3,
  selectedPosSymbol: '',

  lastSummaryData: null,
  lastScannerCoins: [],
  lastScanDiagnostics: null,
  scannerFilter: '',
  scannerPodFilter: 'ALL',

  signalsPage: 1,
  signalsLimit: 25,
  signalsTotal: 0,
  signalsTotalPages: 1,
  signalsDecision: 'ALL',
  signalsStrategy: 'ALL',
  signalsSymbol: '',
  signalRecordsById: new Map(),

  tradesPage: 1,
  tradesLimit: 25,
  tradesTotal: 0,
  tradesTotalPages: 1,
  tradesStatus: 'ALL',
  tradesStrategy: 'ALL',
  tradesByClientId: new Map(),

  summaryTimer: null,
  scannerTimer: null,
  summaryInFlight: false,
  scannerInFlight: false,
  signalsInFlight: false,
  tradesInFlight: false,
  consecutiveErrors: 0,

  aiRequestSeq: 0,
  aiRawMarkdown: ''
};

// -------------------------------------------------------------------------
// Security & Formatting Helpers
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

function showToast(level, title, msg = '') {
  const host = document.getElementById('tui-toasts');
  if (!host) return;
  const el = document.createElement('div');
  el.className = `tui-toast${level === 'error' ? ' is-error' : level === 'warn' ? ' is-warn' : ''}`;
  el.innerHTML = `<div class="c-cyan"><strong>${escapeHtml(title)}</strong></div>${msg ? `<div class="tui-muted">${escapeHtml(msg)}</div>` : ''}`;
  host.appendChild(el);
  setTimeout(() => {
    el.remove();
  }, 4200);
}

function fmtMoney(val, showSign = false) {
  const n = Number(val || 0);
  if (!Number.isFinite(n)) return '$0.00';
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
  if (!Number.isFinite(n) || n === 0) return '0';
  const abs = Math.abs(n);
  if (abs >= 1000) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (abs >= 10) return n.toFixed(3);
  if (abs >= 0.1) return n.toFixed(4);
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
  if (!Number.isFinite(pct)) return '0.0000%';
  return `${pct > 0 ? '+' : ''}${pct.toFixed(4)}%`;
}

function fmtDuration(sec) {
  const s = Math.max(0, Math.floor(Number(sec || 0)));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function fmtUTC(iso) {
  if (!iso) return '--:--:--';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return escapeHtml(iso);
  const pad = (x) => String(x).padStart(2, '0');
  return `${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

function fmtShortTimeUTC(iso) {
  if (!iso) return '--:--:--';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '--:--:--';
  const pad = (x) => String(x).padStart(2, '0');
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

const STRATEGY_META = {
  VWAP_PULLBACK:         { short: 'VWAP',   name: 'VWAP Pullback & Orderflow Bounce', pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-core' },
  VOL_BREAKOUT:          { short: 'VOL_BO', name: 'Volatility Compression Breakout',  pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-bo' },
  REL_RESIDUAL:          { short: 'REL',    name: 'Relative Residual vs BTC',         pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-core' },
  LIQUIDITY_SWEEP_SHORT: { short: 'SWEEP',  name: '1-Bar Liquidity Sweep SHORT',      pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-bo' },
  LIQ_SWEEP:             { short: 'SWEEP',  name: '1-Bar Liquidity Sweep SHORT',      pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-bo' },
  EMA_PULLBACK:          { short: 'EMA',    name: 'Stacked EMA Continuation Pullback',pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-core' },
  VWAP_BREAKDOWN:        { short: 'BRK',    name: 'Failed Relief VWAP Breakdown',     pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-bo' },
  STRUCTURE_BREAK:       { short: 'STRUCT', name: 'Pivot Structure Breakdown',        pod: 'CORE',  tf: '15m', tagCls: 'tag-strat-bo' },
  MAJOR_RSMOM_SHORT:     { short: 'MAJ_S',  name: 'Major 1h RS-Momentum SHORT',       pod: 'MAJOR', tf: '1h',  tagCls: 'tag-pod-maj' },
  MAJOR_RSMOM_LONG:      { short: 'MAJ_L',  name: 'Major 1h RS-Momentum LONG',        pod: 'MAJOR', tf: '1h',  tagCls: 'tag-pod-maj' },
  CTA_4H_SHORT:          { short: 'CTA_S',  name: 'CTA 4h Trend Following SHORT',     pod: 'CTA',   tf: '4h',  tagCls: 'tag-pod-cta' },
  CTA_4H_LONG:           { short: 'CTA_L',  name: 'CTA 4h Trend Following LONG',      pod: 'CTA',   tf: '4h',  tagCls: 'tag-pod-cta' }
};

function getStrategyMeta(raw) {
  const key = String(raw || 'VWAP_PULLBACK').trim().toUpperCase();
  if (STRATEGY_META[key]) return { id: key, ...STRATEGY_META[key] };
  return { id: key, short: key.slice(0, 10), name: key, pod: 'CORE', tf: '15m', tagCls: 'tag-muted' };
}

function fmtStrategyShort(raw) {
  return getStrategyMeta(raw).short;
}

function renderStrategyTag(raw, showTF = false) {
  const m = getStrategyMeta(raw);
  const label = showTF ? `${m.short} ${m.tf}` : m.short;
  return `<span class="tui-tag ${m.tagCls}" title="${escapeHtml(`${m.name} (${m.tf})`)}">${escapeHtml(label)}</span>`;
}

function fmtPriceOrDash(val) {
  const n = Number(val || 0);
  if (!Number.isFinite(n) || n <= 0) return '<span class="tui-muted">--</span>';
  return fmtPrice(n);
}

function renderPodTags(pods, primaryPod, showScanBadge) {
  const list = Array.isArray(pods) && pods.length > 0 ? pods : [primaryPod || 'SCAN'];
  return list.map((rawPod) => {
    const p = String(rawPod || 'SCAN').toUpperCase();
    if (p === 'MAJOR') return '<span class="tui-tag tag-pod-maj">MAJ</span>';
    if (p === 'CTA') return '<span class="tui-tag tag-pod-cta">CTA</span>';
    return showScanBadge ? '<span class="tui-tag tag-pod-scan">SCAN</span>' : '';
  }).join('');
}

function fmtTierLabel(pos) {
  const tier = Number(pos.protection_tier || 0);
  if (tier >= 3) return '[TRAIL]';
  if (tier === 2) return '[LOCK]';
  if (tier === 1 || pos.is_break_even) return '[BE]';
  return '';
}

function fmtQty(val) {
  const s = String(val ?? '').trim();
  if (!s) return '0';
  const n = Number(s);
  if (!Number.isFinite(n)) return escapeHtml(s);
  return escapeHtml(s.includes('.') ? s.replace(/\.?0+$/, '') : s);
}

function fmtExitReasonTag(raw) {
  const key = String(raw || '--').trim().toUpperCase();
  const map = {
    TAKE_PROFIT: { label: 'TP', cls: 'tag-win' },
    PARTIAL_TP1: { label: 'TP1', cls: 'tag-win' },
    TRAILING_STOP: { label: 'TRAIL', cls: 'tag-win' },
    STOP_LOSS: { label: 'SL', cls: 'tag-loss' },
    BREAK_EVEN: { label: 'BE', cls: 'tag-be' },
    TIME_STOP_MAX_HOLDING: { label: 'TIME_STOP', cls: 'tag-muted' },
    STALE_LOSS_EXIT: { label: 'STALE_EXIT', cls: 'tag-muted' }
  };
  return map[key] || { label: key.slice(0, 14), cls: 'tag-muted' };
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
  const out = [];
  let inList = false;

  const formatInline = (text) =>
    text
      .replace(/`([^`]+)`/g, '<code>$1</code>')
      .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
      .replace(/\*([^*]+)\*/g, '<em>$1</em>');

  for (const rawLine of lines) {
    const line = rawLine.trim();
    const cbMatch = line.match(/^@@CODEBLOCK_(\d+)@@$/);
    if (cbMatch) {
      if (inList) {
        out.push('</ul>');
        inList = false;
      }
      out.push(blocks[Number(cbMatch[1])]);
      continue;
    }
    if (!line) {
      if (inList) {
        out.push('</ul>');
        inList = false;
      }
      continue;
    }
    if (line.startsWith('### ')) {
      if (inList) { out.push('</ul>'); inList = false; }
      out.push(`<h3>${formatInline(line.slice(4))}</h3>`);
    } else if (line.startsWith('## ')) {
      if (inList) { out.push('</ul>'); inList = false; }
      out.push(`<h2>${formatInline(line.slice(3))}</h2>`);
    } else if (line.startsWith('# ')) {
      if (inList) { out.push('</ul>'); inList = false; }
      out.push(`<h1>${formatInline(line.slice(2))}</h1>`);
    } else if (line.startsWith('- ') || line.startsWith('* ')) {
      if (!inList) {
        out.push('<ul>');
        inList = true;
      }
      out.push(`<li>${formatInline(line.slice(2))}</li>`);
    } else {
      if (inList) {
        out.push('</ul>');
        inList = false;
      }
      out.push(`<p>${formatInline(line)}</p>`);
    }
  }
  if (inList) out.push('</ul>');
  return out.join('');
}

// -------------------------------------------------------------------------
// Theme Management
// -------------------------------------------------------------------------
function applyTheme(theme) {
  const chosen = THEMES.includes(theme) ? theme : 'obsidian';
  document.documentElement.setAttribute('data-webtui-theme', chosen);
  const sel = document.getElementById('theme-select');
  if (sel && sel.value !== chosen) sel.value = chosen;
  try {
    localStorage.setItem('trml_theme', chosen);
  } catch (_) {}
}

function cycleTheme() {
  const cur = document.documentElement.getAttribute('data-webtui-theme') || 'obsidian';
  const idx = THEMES.indexOf(cur);
  const next = THEMES[(idx + 1) % THEMES.length];
  applyTheme(next);
  showToast('info', `THEME: ${next.toUpperCase()}`);
}

// -------------------------------------------------------------------------
// Network & PIN Auth
// -------------------------------------------------------------------------
async function apiFetch(url, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.storedPin) {
    headers.set('X-TRML-PIN', state.storedPin);
  }
  const res = await fetch(url, {
    ...options,
    headers,
    credentials: 'same-origin'
  });

  if (res.status === 401 && state.pinRequired) {
    handleUnauthorized();
    const err = new Error('UNAUTHORIZED');
    err.status = 401;
    throw err;
  }

  let data = null;
  const text = await res.text();
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (_) {
      data = { error: text };
    }
  }

  if (!res.ok) {
    const err = new Error((data && (data.message || data.error)) || `HTTP ${res.status}`);
    err.status = res.status;
    err.payload = data;
    throw err;
  }
  return data;
}

function getSavedPin() {
  try {
    const lsPin = localStorage.getItem('trml_pin') || '';
    if (lsPin) return lsPin;
    const ssPin = sessionStorage.getItem('trml_pin') || '';
    if (ssPin) {
      localStorage.setItem('trml_pin', ssPin);
      return ssPin;
    }
  } catch (_) {}
  return '';
}

function savePin(pin) {
  try {
    localStorage.setItem('trml_pin', pin);
  } catch (_) {}
  try {
    sessionStorage.setItem('trml_pin', pin);
  } catch (_) {}
}

function clearSavedPin() {
  try {
    localStorage.removeItem('trml_pin');
  } catch (_) {}
  try {
    sessionStorage.removeItem('trml_pin');
  } catch (_) {}
}

function handleUnauthorized() {
  state.pinVerified = false;
  state.storedPin = '';
  clearSavedPin();
  stopPolling();
  showPinOverlay('Session locked or PIN invalid.');
}

function showPinOverlay(errMsg = '') {
  const ov = document.getElementById('pin-overlay');
  const errEl = document.getElementById('pin-error');
  const inp = document.getElementById('pin-input');
  if (ov) ov.classList.remove('hidden');
  if (errEl) {
    if (errMsg) {
      errEl.textContent = errMsg;
      errEl.classList.remove('hidden');
    } else {
      errEl.classList.add('hidden');
    }
  }
  if (inp) {
    inp.value = '';
    setTimeout(() => inp.focus(), 40);
  }
}

function hidePinOverlay() {
  const ov = document.getElementById('pin-overlay');
  if (ov) ov.classList.add('hidden');
}

async function initAuth() {
  try {
    const status = await apiFetch('/api/auth/status');
    state.pinRequired = Boolean(status && status.pin_required);
  } catch (_) {
    state.pinRequired = false;
  }

  const lockBtn = document.getElementById('btn-lock');
  if (lockBtn) lockBtn.classList.toggle('hidden', !state.pinRequired);

  if (!state.pinRequired) {
    state.pinVerified = true;
    hidePinOverlay();
    return true;
  }

  const cached = getSavedPin();
  if (cached) {
    state.storedPin = cached;
    try {
      await apiFetch('/api/summary');
      state.pinVerified = true;
      hidePinOverlay();
      return true;
    } catch (err) {
      if (err && err.status === 401) {
        state.storedPin = '';
        clearSavedPin();
      } else {
        state.pinVerified = true;
        hidePinOverlay();
        return true;
      }
    }
  }

  state.pinVerified = false;
  showPinOverlay('');
  return false;
}

// -------------------------------------------------------------------------
// Render: Header, Ticker, Overview Panels (DESK Tab)
// -------------------------------------------------------------------------
function updateConnectionState(ok, errMsg = '') {
  const sysPill = document.getElementById('pill-sys');
  const footerState = document.getElementById('footer-sync-state');

  if (ok) {
    state.consecutiveErrors = 0;
    if (footerState) {
      footerState.textContent = 'LIVE SYNC';
      footerState.className = 'c-green';
    }
  } else {
    state.consecutiveErrors += 1;
    if (footerState) {
      footerState.textContent = errMsg ? `ERR: ${errMsg.slice(0, 28)}` : 'SYNC RETRYING';
      footerState.className = 'c-red';
    }
    if (sysPill && state.consecutiveErrors >= 3) {
      sysPill.textContent = 'SYS: ERR';
      sysPill.className = 'tui-pill pill-red';
    }
  }
}

function renderSummary(data) {
  if (!data) return;
  state.lastSummaryData = data;
  state.activeSlotsCount = Number(data.active_slots || 0);
  state.maxSlots = Math.max(1, Number(data.max_slots || 3));

  const equity = Number(data.equity || 0);
  const avail = Number(data.balance || 0);
  const ddPct = Number(data.daily_drawdown_pct || 0) * 100;
  const maxDdPct = Number(data.max_daily_drawdown_pct || -0.05) * 100;
  const peakDdPct = Number(data.drawdown_from_peak_pct || 0) * 100;
  const stats = data.stats || {};
  const netPnL = Number(stats.total_net_pnl_usdt || 0);
  const telem = data.telemetry || {};

  // Header Pills
  const modePill = document.getElementById('pill-mode');
  if (modePill) {
    const env = String(data.env || 'DRY-RUN').toUpperCase();
    modePill.textContent = `MODE: ${env}`;
    modePill.className = `tui-pill ${env === 'LIVE' ? 'pill-amber' : 'pill-cyan'}`;
  }
  const sysPill = document.getElementById('pill-sys');
  if (sysPill) {
    if (data.circuit_breaker) {
      sysPill.textContent = 'SYS: BREAKER';
      sysPill.className = 'tui-pill pill-red';
    } else {
      sysPill.textContent = 'SYS: OK';
      sysPill.className = 'tui-pill pill-green';
    }
  }
  const sidePill = document.getElementById('pill-side');
  if (sidePill) {
    const side = String(telem.allowed_side || 'AUTO').toUpperCase();
    const majShort = Boolean(telem.major_short_enabled);
    const majLong = Boolean(telem.major_long_enabled);
    const ctaShort = Boolean(telem.cta_short_enabled);
    const ctaLong = Boolean(telem.cta_long_enabled);
    const ctaOn = ctaShort || ctaLong;
    if (ctaOn) {
      const hasShort = side === 'SHORT_ONLY' || side === 'AUTO' || majShort || ctaShort;
      const hasLong = side === 'LONG_ONLY' || side === 'AUTO' || majLong || ctaLong;
      const dirLabel = hasShort && hasLong ? 'S+L' : hasShort ? 'SHORT' : 'LONG';
      const pods = 1 + (majShort ? 1 : 0) + (majLong ? 1 : 0) + 1;
      sidePill.textContent = `SIDE: ${dirLabel} (${pods}-POD)`;
      sidePill.className = `tui-pill ${dirLabel === 'S+L' ? 'pill-cyan' : dirLabel === 'SHORT' ? 'pill-red' : 'pill-green'}`;
    } else if (majShort && majLong) {
      sidePill.textContent = 'SIDE: S+L (3-POD)';
      sidePill.className = 'tui-pill pill-cyan';
    } else if (majLong && side === 'SHORT_ONLY') {
      sidePill.textContent = 'SIDE: S+L (2-POD)';
      sidePill.className = 'tui-pill pill-cyan';
    } else if (majShort) {
      sidePill.textContent = `SIDE: ${side === 'SHORT_ONLY' ? 'SHORT' : side} (2-POD)`;
      sidePill.className = `tui-pill ${side === 'SHORT_ONLY' ? 'pill-red' : 'pill-cyan'}`;
    } else {
      sidePill.textContent = `SIDE: ${side}`;
      sidePill.className = `tui-pill ${side === 'SHORT_ONLY' ? 'pill-red' : side === 'LONG_ONLY' ? 'pill-green' : 'pill-muted'}`;
    }
  }

  // DESK Panel 1: Account & Performance
  const activePods = Array.isArray(telem.active_pods) ? telem.active_pods : [];
  const totalTrades = Number(stats.total_trades || 0);
  const winsCnt = Number(stats.wins_count || 0);
  const beCnt = Number(stats.break_even_count || 0);
  const lossesCnt = Number(stats.losses_count || 0);
  setText('perf-trades-badge', `${totalTrades} TRADES`);
  setText('perf-tally', `${winsCnt}W·${beCnt}B·${lossesCnt}L`);

  setText('perf-equity', fmtMoney(equity));
  const marginUsedPct = equity > 0 ? Math.min(100, Math.max(0, Math.round(((equity - avail) / equity) * 100))) : 0;
  setText('perf-avail-sub', `FREE ${fmtMoney(avail)} · ${marginUsedPct}% USED`);

  const baselineEq = equity - netPnL;
  const roiPct = baselineEq > 0 ? (netPnL / baselineEq) * 100 : 0;
  const perfNetEl = document.getElementById('perf-net-pnl');
  if (perfNetEl) {
    perfNetEl.textContent = fmtMoney(netPnL, true);
    perfNetEl.className = `perf-delta ${netPnL >= 0 ? 'c-green' : 'c-red'}`;
  }
  const perfRoiEl = document.getElementById('perf-roi-pct');
  if (perfRoiEl) {
    perfRoiEl.textContent = `${fmtPct(roiPct, 2, true)} ROI`;
    perfRoiEl.className = `perf-roi-pct ${netPnL >= 0 ? 'c-green' : 'c-red'}`;
  }
  const peakDdEl = document.getElementById('perf-peak-dd');
  if (peakDdEl) {
    const signedPeakDd = peakDdPct > 0 ? -peakDdPct : peakDdPct;
    const isDd = signedPeakDd < -0.005;
    peakDdEl.textContent = `PEAK DD ${isDd ? fmtPct(signedPeakDd, 2, true) : '0.00%'}`;
    peakDdEl.className = `perf-dd-pill ${isDd ? 'dd-warn' : ''}`.trim();
  }

  const wrVal = Number(stats.win_rate_pct || 0);
  const wrEl = document.getElementById('perf-winrate');
  if (wrEl) {
    wrEl.textContent = fmtPct(wrVal, 1);
    wrEl.className = `kpi-v ${totalTrades > 0 ? (wrVal >= 50 ? 'c-green' : 'c-red') : ''}`.trim();
  }
  const pfVal = Number(stats.profit_factor || 0);
  const pfEl = document.getElementById('perf-pf');
  if (pfEl) {
    pfEl.textContent = pfVal.toFixed(2);
    pfEl.className = `kpi-v ${pfVal >= 1.2 ? 'c-green' : pfVal > 0 && pfVal < 1 ? 'c-red' : 'c-cyan'}`;
  }
  setText('perf-rr', Number(stats.realized_rr || 0).toFixed(2));
  const avgWlEl = document.getElementById('perf-avg-wl');
  if (avgWlEl) {
    const avgW = escapeHtml(fmtMoney(stats.avg_win_usdt || 0));
    const avgL = escapeHtml(fmtMoney(Math.abs(stats.avg_loss_usdt || 0)));
    avgWlEl.innerHTML = `<span class="c-green">${avgW}</span><span class="tui-muted">/</span><span class="c-red">${avgL}</span>`;
  }

  const podGroups = activePods.length > 0
    ? activePods.map((p) => {
        const id = String(p.id || 'CORE').toUpperCase();
        return {
          id,
          short: id === 'MAJOR' ? 'M' : id === 'CTA' ? 'T' : 'C',
          used: Math.max(0, Number(p.used_slots || 0)),
          max: Math.max(0, Number(p.max_slots || 0)),
          cls: id === 'MAJOR' ? 'seg-maj' : id === 'CTA' ? 'seg-cta' : 'seg-core',
        };
      })
    : [{ id: 'CORE', short: 'C', used: state.activeSlotsCount, max: state.maxSlots, cls: 'seg-core' }];

  setText('val-slots', `${state.activeSlotsCount} / ${state.maxSlots}`);
  const slotMeterEl = document.getElementById('slot-seg-meter');
  if (slotMeterEl) {
    let segHtml = '';
    podGroups.forEach((g) => {
      if (g.max <= 0) return;
      segHtml += `<div class="slot-seg-group" style="flex:${g.max}" title="${escapeHtml(`${g.id}: ${g.used}/${g.max}`)}">`;
      for (let i = 0; i < g.max; i++) {
        segHtml += `<span class="slot-seg ${i < g.used ? `is-used ${g.cls}` : ''}"></span>`;
      }
      segHtml += '</div>';
    });
    slotMeterEl.innerHTML = segHtml;
  }
  setText('val-slots-pods', podGroups.map((g) => `${g.short}:${g.used}/${g.max}`).join(' · '));

  const ddUsedPct = maxDdPct < 0 && ddPct < 0 ? Math.min(100, Math.round((Math.abs(ddPct) / Math.abs(maxDdPct)) * 100)) : 0;
  setBar('bar-dd', ddUsedPct, ddUsedPct >= 75 ? 'fill-red' : ddUsedPct >= 40 ? 'fill-amber' : 'fill-green');
  const valDdEl = document.getElementById('val-dd');
  if (valDdEl) {
    valDdEl.textContent = ddPct < 0 ? fmtPct(ddPct, 2, true) : fmtPct(ddPct, 2);
    valDdEl.className = `gauge-v ${ddPct <= maxDdPct * 0.7 ? 'c-red' : ddPct < 0 ? 'c-amber' : 'c-green'}`;
  }
  setText('val-dd-limit', `LIM ${fmtPct(maxDdPct, 1)}`);

  setBar('bar-margin', marginUsedPct, marginUsedPct >= 75 ? 'fill-red' : marginUsedPct >= 40 ? 'fill-amber' : 'fill-cyan');
  const valMarginPctEl = document.getElementById('val-margin-pct');
  if (valMarginPctEl) {
    valMarginPctEl.textContent = `${marginUsedPct}%`;
    valMarginPctEl.className = `gauge-v ${marginUsedPct >= 75 ? 'c-red' : marginUsedPct >= 40 ? 'c-amber' : ''}`.trim();
  }
  setText('val-margin', `${fmtMoney(avail)} FREE`);

  // DESK Panel 2: Equity Curve
  renderEquityCurve(Array.isArray(data.equity_curve) ? data.equity_curve : [], equity);

  // DESK Panel 3: Macro & Scanner Pulse
  const regBadge = document.getElementById('macro-regime-badge');
  const regime = String(telem.macro_regime || 'UNKNOWN').toUpperCase();
  if (regBadge) {
    regBadge.textContent = regime;
    regBadge.className = `tui-pill ${
      regime === 'TREND_UP' ? 'pill-green' :
      regime === 'TREND_DOWN' ? 'pill-red' :
      regime === 'RANGE' ? 'pill-lavender' : 'pill-muted'
    }`;
  }
  const erVal = Number(telem.macro_er || 0);
  const vrVal = Number(telem.macro_vol_ratio || 0);
  setText('macro-er-vol', `${erVal.toFixed(2)} / ${vrVal.toFixed(2)}x`);

  const ret24 = Number(telem.macro_return_24h_pct || 0);
  const retEl = document.getElementById('macro-ret24');
  if (retEl) {
    retEl.textContent = fmtPct(ret24, 2, true);
    retEl.className = `mono-val ${ret24 >= 0 ? 'c-green' : 'c-red'}`;
  }

  const breadthPct = telem.market_breadth_valid ? Math.min(100, Math.max(0, Number(telem.market_breadth_pct || 0) * 100)) : 0;
  setBar('bar-breadth', Math.round(breadthPct), breadthPct < 25 ? 'fill-amber' : 'fill-cyan');
  setText('val-breadth', telem.market_breadth_valid ? `${breadthPct.toFixed(1)}%` : '--%');

  const clusterEl = document.getElementById('macro-cluster');
  if (clusterEl) {
    if (telem.cluster_sl_paused) {
      clusterEl.textContent = `PAUSED ${telem.cluster_sl_until_utc ? fmtShortTimeUTC(telem.cluster_sl_until_utc) : ''}`;
      clusterEl.className = 'mono-val c-red';
    } else {
      clusterEl.textContent = 'CLEAR';
      clusterEl.className = 'mono-val c-green';
    }
  }

  const podsEl = document.getElementById('macro-pods');
  if (podsEl) {
    if (activePods.length > 0) {
      const totalPodCount = activePods.reduce((acc, p) => {
        const id = String(p.id || '').toUpperCase();
        const sideStr = String(p.side || '').toUpperCase();
        return acc + (id === 'MAJOR' && sideStr === 'S+L' ? 2 : 1);
      }, 0);
      podsEl.textContent = `${totalPodCount} PODS · ${state.maxSlots} SLOTS`;
      podsEl.className = 'mono-val c-cyan';
    } else {
      const majS = Boolean(telem.major_short_enabled);
      const majL = Boolean(telem.major_long_enabled);
      const ctaS = Boolean(telem.cta_short_enabled);
      const ctaL = Boolean(telem.cta_long_enabled);
      const coreSlots = Number(telem.core_max_slots || 6);
      const parts = [`CORE(${coreSlots})`];
      if (majS || majL) {
        const majSlots = Number(telem.major_max_slots || 3);
        const majCount = Number(telem.major_pod_count || 14);
        const majDir = majS && majL ? 'S/L' : majL ? 'L' : 'S';
        parts.push(`MAJ ${majDir}(${majSlots})·${majCount}M`);
      }
      if (ctaS || ctaL) {
        const ctaSlots = Number(telem.cta_max_slots || 3);
        const ctaCount = Number(telem.cta_pod_count || 30);
        const ctaDir = ctaS && ctaL ? 'S/L' : ctaL ? 'L' : 'S';
        parts.push(`CTA ${ctaDir}(${ctaSlots})·${ctaCount}C`);
      }
      if (parts.length > 1) {
        podsEl.textContent = parts.join(' + ');
        podsEl.className = 'mono-val c-cyan';
      } else {
        podsEl.textContent = `CORE ONLY (${state.maxSlots})`;
        podsEl.className = 'mono-val';
      }
    }
  }

  const podsListEl = document.getElementById('macro-pods-list');
  if (podsListEl) {
    if (activePods.length > 0) {
      podsListEl.innerHTML = activePods.map((pod) => {
        const pid = String(pod.id || 'CORE').toUpperCase();
        const badgeCls = pid === 'MAJOR' ? 'tag-pod-maj' : pid === 'CTA' ? 'tag-pod-cta' : 'tag-pod-scan';
        const strats = Array.isArray(pod.strategies) ? pod.strategies : [];
        const stratHtml = strats.map((s) => renderStrategyTag(s, false)).join('');
        const used = Number(pod.used_slots || 0);
        const maxS = Number(pod.max_slots || 0);
        const univ = Number(pod.universe || 0);
        return `<div class="macro-pod-row">
          <div class="macro-pod-head">
            <span class="tui-tag ${badgeCls}">${escapeHtml(pid)}</span>
            <span class="tui-muted-xs">${escapeHtml(pod.timeframe || '--')} ${escapeHtml(pod.side || '--')}</span>
            <span class="tui-muted-xs">${used}/${maxS} · ${univ}c</span>
          </div>
          <div class="macro-pod-strats">${stratHtml}</div>
        </div>`;
      }).join('');
    } else {
      podsListEl.innerHTML = '';
    }
  }

  // Active Positions Table & Copilot Selector
  const positions = Array.isArray(data.positions) ? data.positions : [];
  renderActivePositions(positions, telem.scanner_interval || '15m');
  syncCopilotPositionSelector(positions);

  // Copilot Model Pill
  const modelPill = document.getElementById('copilot-model-pill');
  if (modelPill && data.gemini_model) {
    modelPill.textContent = `MODEL: ${String(data.gemini_model).toUpperCase()}`;
  }

  // Ledger Strip KPIs & Strategy Breakdown
  renderLedgerStrip(stats, Array.isArray(data.strategy_stats) ? data.strategy_stats : []);

  // Signals Audit Summary Strip
  if (data.audit_summary) {
    renderSignalsSummaryStrip(data.audit_summary);
  }

  const clockEl = document.getElementById('footer-clock');
  if (clockEl) {
    clockEl.textContent = `${fmtShortTimeUTC(new Date().toISOString())} UTC`;
  }
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function setBar(id, pct, colorClass) {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.width = `${Math.max(0, Math.min(100, pct))}%`;
  el.className = `bar-fill ${colorClass}`;
}

function renderEquityCurve(points, fallbackEq) {
  const wrap = document.getElementById('equity-curve-wrap');
  const metaEl = document.getElementById('curve-meta');
  if (!wrap) return;

  if (!points || points.length < 2) {
    if (metaEl) metaEl.textContent = 'NO CLOSED TRADES';
    wrap.innerHTML = `<div class="tui-empty-center">Baseline ${escapeHtml(fmtMoney(fallbackEq))} · Equity curve activates after first closed trade.</div>`;
    return;
  }

  const equities = points.map((p) => Number(p.equity || 0));
  let minEq = Math.min(...equities);
  let maxEq = Math.max(...equities);
  if (maxEq - minEq < 0.01) {
    minEq -= 1;
    maxEq += 1;
  } else {
    const pad = (maxEq - minEq) * 0.08;
    minEq -= pad;
    maxEq += pad;
  }

  const firstEq = equities[0];
  const lastEq = equities[equities.length - 1];
  const roiPct = firstEq > 0 ? ((lastEq - firstEq) / firstEq) * 100 : 0;
  if (metaEl) {
    const lastTrades = points[points.length - 1].trade_count || (points.length - 1);
    metaEl.textContent = `${lastTrades} TRADES · ${fmtPct(roiPct, 1, true)}`;
  }

  const W = 420;
  const H = 132;
  const padL = 6;
  const padR = 54;
  const padT = 8;
  const padB = 14;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  const mapX = (i) => padL + (i / (points.length - 1)) * plotW;
  const mapY = (v) => padT + ((maxEq - v) / (maxEq - minEq)) * plotH;

  const coords = points.map((p, i) => [mapX(i), mapY(Number(p.equity || 0))]);
  const linePath = coords.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  const areaPath = `${linePath} L${coords[coords.length - 1][0].toFixed(1)},${(padT + plotH).toFixed(1)} L${coords[0][0].toFixed(1)},${(padT + plotH).toFixed(1)} Z`;

  const gridLines = [0, 0.5, 1].map((t) => {
    const y = padT + t * plotH;
    const val = maxEq - t * (maxEq - minEq);
    return `<line x1="${padL}" y1="${y.toFixed(1)}" x2="${(padL + plotW).toFixed(1)}" y2="${y.toFixed(1)}" stroke="var(--border-color)" stroke-width="1" stroke-dasharray="2 3" />
      <text x="${W - 4}" y="${(y + 3).toFixed(1)}" text-anchor="end" fill="var(--foreground2)" font-size="9">${escapeHtml(fmtMoney(val))}</text>`;
  }).join('');

  const [lx, ly] = coords[coords.length - 1];
  wrap.innerHTML = `
    <svg class="curve-svg" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img" aria-label="Equity Curve">
      <defs>
        <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--accent)" stop-opacity="0.28" />
          <stop offset="100%" stop-color="var(--accent)" stop-opacity="0.02" />
        </linearGradient>
      </defs>
      ${gridLines}
      <path d="${areaPath}" fill="url(#eqGrad)" />
      <path d="${linePath}" fill="none" stroke="var(--accent)" stroke-width="1.8" />
      <circle cx="${lx.toFixed(1)}" cy="${ly.toFixed(1)}" r="2.8" fill="var(--accent)" />
    </svg>
  `;
}

function renderActivePositions(positions, scanInterval) {
  const tbody = document.getElementById('positions-tbody');
  const badge = document.getElementById('pos-count-badge');
  const meta = document.getElementById('pos-meta-note');

  if (badge) badge.textContent = `[${positions.length}/${state.maxSlots}]`;
  if (meta) {
    const rem = Math.max(0, state.maxSlots - positions.length);
    meta.textContent = `${rem} SLOTS FREE · SCAN ${String(scanInterval).toUpperCase()}`;
  }
  if (!tbody) return;

  if (positions.length === 0) {
    state.selectedPosSymbol = '';
    tbody.innerHTML = '<tr><td colspan="12" class="tui-empty-cell">No open positions. Capital standing by.</td></tr>';
    return;
  }

  if (!positions.some((p) => p.symbol === state.selectedPosSymbol)) {
    state.selectedPosSymbol = positions[0].symbol;
  }

  tbody.innerHTML = positions.map((pos) => {
    const sym = escapeHtml(pos.symbol);
    const side = String(pos.side || '').toUpperCase();
    const isSelected = pos.symbol === state.selectedPosSymbol;
    const pnl = Number(pos.unrealized_pnl || 0);
    const roe = Number(pos.unrealized_roe_pct || 0);
    const tier = fmtTierLabel(pos);
    const pnlCls = pnl > 0 ? 'c-green' : pnl < 0 ? 'c-red' : 'tui-muted';
    const sideTagCls = side === 'LONG' ? 'tag-long' : side === 'SHORT' ? 'tag-short' : 'tag-muted';

    return `<tr data-pos-symbol="${sym}" class="${isSelected ? 'is-selected' : ''}">
      <td class="cell-symbol">${sym}</td>
      <td><span class="tui-tag ${sideTagCls}">${escapeHtml(side)}</span></td>
      <td>${renderStrategyTag(pos.strategy_id, false)}</td>
      <td class="num">${fmtQty(pos.qty)}</td>
      <td class="num">${fmtPrice(pos.entry_price)}</td>
      <td class="num">${fmtPrice(pos.mark_price)}</td>
      <td>${fmtPrice(pos.sl_price)}${tier ? ` <span class="c-amber">${escapeHtml(tier)}</span>` : ''}</td>
      <td class="num">${Number(pos.tp_price || 0) <= 0 ? '<span class="tui-muted">TRAIL</span>' : fmtPrice(pos.tp_price)}</td>
      <td class="num ${pnlCls}">${fmtMoney(pnl, true)}</td>
      <td class="num ${pnlCls}">${fmtPct(roe, 1, true)}</td>
      <td class="num tui-muted">${fmtDuration(pos.duration_sec)}</td>
      <td class="act"><button type="button" class="tui-btn tui-btn-ghost" data-ai-health="${sym}">AI HEALTH</button></td>
    </tr>`;
  }).join('');
}

function syncCopilotPositionSelector(positions) {
  const sel = document.getElementById('copilot-pos-select');
  if (!sel) return;
  if (!positions || positions.length === 0) {
    sel.innerHTML = '<option value="">NO OPEN POS</option>';
    return;
  }
  const prev = sel.value || state.selectedPosSymbol;
  sel.innerHTML = positions.map((p) => {
    const s = escapeHtml(p.symbol);
    return `<option value="${s}">${s} (${escapeHtml(String(p.side || '').toUpperCase())})</option>`;
  }).join('');
  if (positions.some((p) => p.symbol === prev)) {
    sel.value = prev;
  }
}

// -------------------------------------------------------------------------
// Render: Scanner & Diagnostics (DESK Tab)
// -------------------------------------------------------------------------
function renderScanner(data) {
  const coins = Array.isArray(data && data.coins) ? data.coins : [];
  state.lastScannerCoins = coins;
  state.lastScanDiagnostics = (data && data.diagnostics) || null;

  renderScanDiagnostics(state.lastScanDiagnostics, data && data.scanned_at_utc);
  renderWatchlistTable();
}

function renderScanDiagnostics(diag, fallbackAt) {
  const timeEl = document.getElementById('scan-diag-time');

  if (!diag) {
    if (timeEl) timeEl.textContent = fallbackAt ? `${fmtShortTimeUTC(fallbackAt)} UTC` : '--:--:-- UTC';
    ['diag-pre', 'diag-natr', 'diag-side', 'diag-vwap', 'diag-err', 'diag-sel'].forEach((id) => setText(id, '--'));
    return;
  }

  if (timeEl) timeEl.textContent = `${fmtShortTimeUTC(diag.scanned_at_utc || fallbackAt)} UTC`;
  const errs = Number(diag.kline_fetch_failed || 0) + Number(diag.kline_invalid_or_gap || 0);
  const sel = Number(diag.selected_count || 0);

  setText('diag-pre', String(Number(diag.pre_filter_passed || 0)));
  setText('diag-natr', String(Number(diag.natr_rejected || 0)));
  setText('diag-side', String(Number(diag.side_rejected || 0)));
  setText('diag-vwap', String(Number(diag.vwap_dist_rejected || 0)));

  const errEl = document.getElementById('diag-err');
  if (errEl) {
    errEl.textContent = String(errs);
    errEl.className = `metric-v ${errs > 0 ? 'c-red' : 'c-green'}`;
  }
  const selEl = document.getElementById('diag-sel');
  if (selEl) {
    selEl.textContent = String(sel);
    selEl.className = `metric-v ${sel > 0 ? 'c-cyan' : 'tui-muted'}`;
  }
}

function renderWatchlistTable() {
  const tbody = document.getElementById('scanner-tbody');
  const badge = document.getElementById('scan-count-badge');
  const filter = state.scannerFilter.trim().toUpperCase();
  const podFilter = String(state.scannerPodFilter || 'ALL').toUpperCase();

  let scanCount = 0;
  let majCount = 0;
  let ctaCount = 0;
  for (const c of state.lastScannerCoins) {
    const podUp = String(c.pod || 'SCAN').toUpperCase();
    if (podUp === 'MAJOR') majCount += 1;
    else if (podUp === 'CTA') ctaCount += 1;
    else scanCount += 1;
  }

  const list = state.lastScannerCoins.filter((c) => {
    const primaryPod = String(c.pod || 'SCAN').toUpperCase();
    const pods = Array.isArray(c.pods) && c.pods.length > 0 ? c.pods.map((p) => String(p).toUpperCase()) : [primaryPod];
    if (podFilter !== 'ALL' && !pods.includes(podFilter)) {
      return false;
    }
    if (!filter) return true;
    const sym = String(c.symbol || '').toUpperCase();
    const strats = Array.isArray(c.strategies) ? c.strategies.join(' ').toUpperCase() : '';
    return sym.includes(filter) || pods.join(' ').includes(filter) || strats.includes(filter);
  });

  if (badge) {
    if (majCount > 0 || ctaCount > 0) {
      const suffix = [];
      if (majCount > 0) suffix.push(`${majCount}M`);
      if (ctaCount > 0) suffix.push(`${ctaCount}C`);
      badge.textContent = `[${scanCount}+${suffix.join('+')}]`;
    } else {
      badge.textContent = `[${state.lastScannerCoins.length}]`;
    }
  }
  if (!tbody) return;

  if (list.length === 0) {
    const emptyMsg = filter || podFilter !== 'ALL'
      ? `No watched coins matching ${podFilter !== 'ALL' ? `pod=${escapeHtml(podFilter)}` : ''}${filter ? ` "${escapeHtml(filter)}"` : ''}.`
      : 'No watched coins on current scan.';
    tbody.innerHTML = `<tr><td colspan="7" class="tui-empty-cell">${emptyMsg}</td></tr>`;
    return;
  }

  tbody.innerHTML = list.map((c) => {
    const dist = Number(c.dist_vwap_pct || 0);
    const natr = Number(c.natr_pct || 0);
    const fund = Number(c.funding_rate || 0);
    const pod = String(c.pod || 'SCAN').toUpperCase();

    const strats = Array.isArray(c.strategies) && c.strategies.length > 0
      ? c.strategies
      : [pod === 'MAJOR' ? 'MAJOR_RSMOM_SHORT' : pod === 'CTA' ? 'CTA_4H_SHORT' : 'VWAP_PULLBACK'];
    const tf = escapeHtml(c.timeframe || (pod === 'MAJOR' ? '1h' : pod === 'CTA' ? '4h' : '15m'));
    const tfTip = escapeHtml(`Primary signal timeframe (${tf})`);
    const stratHtml = `<div class="strat-chip-group">${strats.map((s) => renderStrategyTag(s, false)).join('')}<span class="strat-tf" title="${tfTip}">${tf}</span></div>`;

    let metricHtml = '<span class="tui-muted">--</span>';
    if (c.metric_value) {
      const toneCls = c.metric_tone === 'neg' ? 'c-red' : c.metric_tone === 'pos' ? 'c-green' : 'tui-muted';
      metricHtml = `<span class="metric-k-inline">${escapeHtml(c.metric_label || '')}</span> <span class="${toneCls}">${escapeHtml(c.metric_value)}</span>`;
    } else if (pod === 'SCAN' && Number(c.session_vwap || 0) > 0) {
      metricHtml = `<span class="metric-k-inline">VWAP</span> <span class="${dist < 0 ? 'c-red' : 'c-green'}">${fmtPct(dist, 2, true)}</span>`;
    }

    let volHtml = '<span class="tui-muted">--</span>';
    if (c.vol_label && c.vol_label !== '--') {
      const cls = c.vol_label.includes('--') ? 'tui-muted' : 'c-green';
      volHtml = `<span class="${cls}">${escapeHtml(c.vol_label)}</span>`;
    } else if (pod === 'SCAN' && natr > 0) {
      volHtml = `<span class="c-green">${fmtPct(natr, 2)}</span> <span class="tui-muted">· ${Number(c.rvol || 0).toFixed(2)}x</span>`;
    }

    return `<tr>
      <td class="cell-symbol">${escapeHtml(c.symbol)}</td>
      <td>${stratHtml}</td>
      <td class="num">${fmtPrice(c.mark_price)}</td>
      <td class="num">${metricHtml}</td>
      <td class="num">${volHtml}</td>
      <td class="num tui-muted">${fmtPct(c.spread_pct || 0, 3)}</td>
      <td class="num ${fund > 0 ? 'c-red' : 'c-green'}">${fmtFunding(fund)}</td>
    </tr>`;
  }).join('');
}

// -------------------------------------------------------------------------
// Render: Signals (DESK Stream + Full SIGNALS Tab)
// -------------------------------------------------------------------------
function renderSignalsSummaryStrip(summary) {
  if (!summary) return;
  setText('sig-sum-total', String(Number(summary.total_evaluated || 0)));
  setText('sig-sum-appr', String(Number(summary.total_approved || 0)));
  setText('sig-sum-rej', String(Number(summary.total_rejected || 0)));
  setText('sig-sum-rate', fmtPct(summary.rejection_rate_pct || 0, 1));

  const tagsEl = document.getElementById('sig-top-rejects');
  if (!tagsEl) return;
  const tops = Array.isArray(summary.top_rejections) ? summary.top_rejections.slice(0, 3) : [];
  tagsEl.innerHTML = tops.map((t) =>
    `<span class="tui-pill pill-muted">${escapeHtml(t.reason_code)} (${Number(t.count || 0)} · ${Number(t.pct || 0).toFixed(0)}%)</span>`
  ).join('');
}

function renderDeskSignalStream(signals) {
  const el = document.getElementById('desk-signal-stream');
  if (!el) return;
  if (!signals || signals.length === 0) {
    el.innerHTML = '<div class="tui-empty-center">Awaiting closed candle evaluations...</div>';
    return;
  }
  const latest = signals.slice(0, 12);
  el.innerHTML = latest.map((s) => {
    state.signalRecordsById.set(String(s.id), s);
    const isAppr = String(s.decision || '').toUpperCase().startsWith('APPROV');
    const tag = isAppr ? '[APPR]' : '[REJ ]';
    const cls = isAppr ? 'c-green' : 'c-red';
    const reason = s.reject_reason_code || (isAppr ? 'SIGNAL_APPROVED' : '--');
    return `<div class="log-line" data-signal-id="${escapeHtml(s.id)}">
      <span class="tui-muted">${fmtShortTimeUTC(s.candle_open_time_utc || s.created_at_utc)}</span>
      <span class="${cls}">${tag}</span>
      <strong>${escapeHtml(s.symbol)}</strong>
      <span class="log-strat">${renderStrategyTag(s.strategy_id, false)}</span>
      <span class="log-reason">${escapeHtml(reason)}</span>
    </div>`;
  }).join('');
}

function renderSignalsTable(data) {
  const tbody = document.getElementById('signals-tbody');
  const pageEl = document.getElementById('sig-page-info');
  const signals = Array.isArray(data && data.signals) ? data.signals : [];
  state.signalsTotal = Number((data && data.total) || 0);
  state.signalsTotalPages = Math.max(1, Number((data && data.total_pages) || 1));

  if (pageEl) {
    pageEl.textContent = `PAGE ${state.signalsPage} / ${state.signalsTotalPages} (${state.signalsTotal} ROWS)`;
  }
  const prevBtn = document.getElementById('sig-prev');
  const nextBtn = document.getElementById('sig-next');
  if (prevBtn) prevBtn.disabled = state.signalsPage <= 1;
  if (nextBtn) nextBtn.disabled = state.signalsPage >= state.signalsTotalPages;

  if (!tbody) return;
  if (signals.length === 0) {
    tbody.innerHTML = '<tr><td colspan="12" class="tui-empty-cell">No signal records matching current filters.</td></tr>';
    return;
  }

  tbody.innerHTML = signals.map((s) => {
    state.signalRecordsById.set(String(s.id), s);
    const isAppr = String(s.decision || '').toUpperCase().startsWith('APPROV');
    const decTagCls = isAppr ? 'tag-appr' : 'tag-rej';
    const decLabel = isAppr ? 'APPROVE' : 'REJECT';
    const side = String(s.side || '').toUpperCase() || '--';
    const sideTagCls = side === 'LONG' ? 'tag-long' : side === 'SHORT' ? 'tag-short' : 'tag-muted';
    const reason = s.reject_reason_code ? `${s.reject_reason_code}${s.reject_sub_reason ? ` (${s.reject_sub_reason})` : ''}` : 'SIGNAL_APPROVED';
    const meta = getStrategyMeta(s.strategy_id);
    const refVal = Number(s.session_vwap || 0);
    const dzVal = Number(s.bar_delta_zscore || 0);
    const rvolVal = Number(s.rvol || 0);
    const impVal = Number(s.impulse_atr_mult || 0);

    let refCell = fmtPriceOrDash(refVal);
    let sigCell = dzVal.toFixed(2);
    let actCell = `${rvolVal.toFixed(2)}x`;
    if (meta.pod === 'CTA') {
      refCell = refVal > 0 ? `<span class="metric-k-inline">E36</span> ${fmtPrice(refVal)}` : '<span class="tui-muted">--</span>';
      sigCell = impVal !== 0 ? `<span class="metric-k-inline">SC</span> ${impVal.toFixed(2)}` : '<span class="tui-muted">--</span>';
      actCell = '<span class="tui-muted">--</span>';
    } else if (meta.pod === 'MAJOR') {
      refCell = '<span class="tui-muted">--</span>';
      sigCell = `<span class="metric-k-inline">ΔZ</span> ${dzVal.toFixed(2)}`;
      actCell = impVal !== 0
        ? `<span class="metric-k-inline">RS</span> <span class="${impVal < 0 ? 'c-red' : 'c-green'}">${fmtPct(impVal, 2, true)}</span>`
        : '<span class="tui-muted">--</span>';
    }

    return `<tr data-signal-id="${escapeHtml(s.id)}">
      <td class="cell-time">${fmtUTC(s.candle_open_time_utc || s.created_at_utc)}</td>
      <td class="cell-symbol">${escapeHtml(s.symbol)}</td>
      <td><span class="tui-tag ${sideTagCls}">${escapeHtml(side)}</span></td>
      <td>${renderStrategyTag(s.strategy_id, true)}</td>
      <td class="num">${fmtPriceOrDash(s.close_price)}</td>
      <td class="num tui-muted">${fmtPriceOrDash(s.ema200)}</td>
      <td class="num tui-muted">${refCell}</td>
      <td class="num">${sigCell}</td>
      <td class="num">${actCell}</td>
      <td class="num tui-muted">${fmtPct(s.spread_pct || 0, 3)}</td>
      <td><span class="tui-tag ${decTagCls}">${decLabel}</span></td>
      <td class="cell-reason" title="${escapeHtml(reason)}">${escapeHtml(reason)}</td>
    </tr>`;
  }).join('');
}

// -------------------------------------------------------------------------
// Render: Ledger Tab
// -------------------------------------------------------------------------
function renderLedgerStrip(stats, stratStats) {
  setText('led-total', String(Number(stats.total_trades || 0)));
  const netPnL = Number(stats.total_net_pnl_usdt || 0);
  const pnlEl = document.getElementById('led-pnl');
  if (pnlEl) {
    pnlEl.textContent = fmtMoney(netPnL, true);
    pnlEl.className = netPnL >= 0 ? 'c-green' : 'c-red';
  }
  setText('led-wr', fmtPct(stats.win_rate_pct || 0, 1));
  setText('led-pf', Number(stats.profit_factor || 0).toFixed(2));
  setText('led-rr', Number(stats.realized_rr || 0).toFixed(2));

  const chipsEl = document.getElementById('led-strategy-chips');
  if (!chipsEl) return;
  chipsEl.innerHTML = (stratStats || []).map((st) => {
    const p = Number(st.total_net_pnl_usdt || 0);
    const cls = p >= 0 ? 'pill-green' : 'pill-red';
    return `<span class="tui-pill ${cls}">${escapeHtml(fmtStrategyShort(st.strategy_id))} ${Number(st.total_trades || 0)}T · ${fmtMoney(p, true)} · PF ${Number(st.profit_factor || 0).toFixed(2)}</span>`;
  }).join('');
}

function renderLedgerTable(data) {
  const tbody = document.getElementById('ledger-tbody');
  const pageEl = document.getElementById('led-page-info');
  const trades = Array.isArray(data && data.trades) ? data.trades : [];
  state.tradesTotal = Number((data && data.total) || 0);
  state.tradesTotalPages = Math.max(1, Number((data && data.total_pages) || 1));

  if (pageEl) {
    pageEl.textContent = `PAGE ${state.tradesPage} / ${state.tradesTotalPages} (${state.tradesTotal} TRADES)`;
  }
  const prevBtn = document.getElementById('led-prev');
  const nextBtn = document.getElementById('led-next');
  if (prevBtn) prevBtn.disabled = state.tradesPage <= 1;
  if (nextBtn) nextBtn.disabled = state.tradesPage >= state.tradesTotalPages;

  if (!tbody) return;
  if (trades.length === 0) {
    tbody.innerHTML = '<tr><td colspan="13" class="tui-empty-cell">No executed trades matching current filters.</td></tr>';
    return;
  }

  tbody.innerHTML = trades.map((t) => {
    state.tradesByClientId.set(t.client_order_id, t);
    const pnl = Number(t.net_pnl_usdt || 0);
    const roe = Number(t.roe_pct || 0);
    const pnlCls = pnl > 0 ? 'c-green' : pnl < 0 ? 'c-red' : 'tui-muted';
    const side = String(t.side || '').toUpperCase();
    const sideTagCls = side === 'LONG' ? 'tag-long' : side === 'SHORT' ? 'tag-short' : 'tag-muted';
    const exitTag = fmtExitReasonTag(t.exit_reason);
    const cidEsc = escapeHtml(t.client_order_id);

    return `<tr data-trade-cid="${cidEsc}">
      <td class="cell-time">${fmtUTC(t.closed_at_utc)}</td>
      <td class="cell-symbol">${escapeHtml(t.symbol)}</td>
      <td><span class="tui-tag ${sideTagCls}">${escapeHtml(side)}</span></td>
      <td>${renderStrategyTag(t.strategy_id, false)}</td>
      <td class="num">${fmtQty(t.qty)}</td>
      <td class="num">${fmtPrice(t.entry_price)}</td>
      <td class="num">${fmtPrice(t.exit_price)}</td>
      <td class="num ${pnlCls}"><strong>${fmtMoney(pnl, true)}</strong></td>
      <td class="num ${pnlCls}">${fmtPct(roe, 1, true)}</td>
      <td class="num tui-muted">-${Number(t.mae_pct || 0).toFixed(2)}% / +${Number(t.mfe_pct || 0).toFixed(2)}%</td>
      <td title="${escapeHtml(t.exit_reason)}"><span class="tui-tag ${exitTag.cls}">${escapeHtml(exitTag.label)}</span></td>
      <td class="num tui-muted">${fmtDuration(t.duration_sec)}</td>
      <td class="act"><button type="button" class="tui-btn tui-btn-ghost" data-autopsy-cid="${cidEsc}">AUTOPSY</button></td>
    </tr>`;
  }).join('');
}

// -------------------------------------------------------------------------
// Modal Inspectors (Signal Detail & AI Trade Autopsy)
// -------------------------------------------------------------------------
function openSignalDialog(signalId) {
  const sig = state.signalRecordsById.get(String(signalId));
  if (!sig) return;
  const dlg = document.getElementById('detail-dialog');
  const titleEl = document.getElementById('dialog-title');
  const bodyEl = document.getElementById('dialog-body');
  const refreshBtn = document.getElementById('dialog-refresh-btn');
  if (!dlg || !bodyEl) return;

  if (refreshBtn) refreshBtn.classList.add('hidden');
  if (titleEl) titleEl.textContent = `SIGNAL_AUDIT // ${sig.symbol} (${sig.decision})`;

  const isAppr = String(sig.decision || '').toUpperCase().startsWith('APPROV');
  const meta = getStrategyMeta(sig.strategy_id);
  const coreTF = String(state.lastSummaryData?.telemetry?.timeframe || meta.tf || '15m').toUpperCase();
  const displayTF = meta.pod === 'CORE' ? coreTF.toLowerCase() : meta.tf;
  const closeVal = Number(sig.close_price || 0);
  const atrVal = Number(sig.atr14 || 0);
  const atrPctStr = closeVal > 0 && atrVal > 0 ? ` (${((atrVal / closeVal) * 100).toFixed(2)}%)` : '';

  let stratRows = `
      <div class="detail-kv"><span class="k">EMA200 (${escapeHtml(coreTF)})</span><span class="v">${fmtPriceOrDash(sig.ema200)}</span></div>
      <div class="detail-kv"><span class="k">SESSION VWAP</span><span class="v">${fmtPriceOrDash(sig.session_vwap)}</span></div>
      <div class="detail-kv"><span class="k">ATR14</span><span class="v">${fmtPriceOrDash(atrVal)}${escapeHtml(atrPctStr)}</span></div>
      <div class="detail-kv"><span class="k">DELTA Z-SCORE</span><span class="v">${Number(sig.bar_delta_zscore || 0).toFixed(3)}</span></div>
      <div class="detail-kv"><span class="k">RVOL / OI DELTA</span><span class="v">${Number(sig.rvol || 0).toFixed(2)}x / ${fmtPct(sig.oi_delta_pct || 0, 2, true)}</span></div>`;
  if (meta.pod === 'CTA') {
    stratRows = `
      <div class="detail-kv"><span class="k">EMA200 (4H)</span><span class="v">${fmtPriceOrDash(sig.ema200)}</span></div>
      <div class="detail-kv"><span class="k">EMA36 (4H)</span><span class="v">${fmtPriceOrDash(sig.session_vwap)}</span></div>
      <div class="detail-kv"><span class="k">ATR14 (4H)</span><span class="v">${fmtPriceOrDash(atrVal)}${escapeHtml(atrPctStr)}</span></div>
      <div class="detail-kv"><span class="k">EMA CROSS SCORE</span><span class="v">${Number(sig.impulse_atr_mult || 0).toFixed(3)} ATR</span></div>`;
  } else if (meta.pod === 'MAJOR') {
    stratRows = `
      <div class="detail-kv"><span class="k">EMA200 (1H)</span><span class="v">${fmtPriceOrDash(sig.ema200)}</span></div>
      <div class="detail-kv"><span class="k">ATR14 (1H)</span><span class="v">${fmtPriceOrDash(atrVal)}${escapeHtml(atrPctStr)}</span></div>
      <div class="detail-kv"><span class="k">RS 24H VS BTC</span><span class="v">${fmtPct(sig.impulse_atr_mult || 0, 2, true)}</span></div>
      <div class="detail-kv"><span class="k">DELTA Z (1H)</span><span class="v">${Number(sig.bar_delta_zscore || 0).toFixed(3)}</span></div>`;
  }

  bodyEl.innerHTML = `
    <div class="detail-kv-grid">
      <div class="detail-kv"><span class="k">CANDLE OPEN UTC</span><span class="v">${fmtUTC(sig.candle_open_time_utc || sig.created_at_utc)}</span></div>
      <div class="detail-kv"><span class="k">POD / STRATEGY</span><span class="v">${escapeHtml(`${meta.pod} · ${displayTF} · ${meta.id}`)}</span></div>
      <div class="detail-kv"><span class="k">DECISION</span><span class="v ${isAppr ? 'c-green' : 'c-red'}">${escapeHtml(sig.decision)}</span></div>
      <div class="detail-kv"><span class="k">REASON</span><span class="v">${escapeHtml(sig.reject_reason_code || 'SIGNAL_APPROVED')}</span></div>
      <div class="detail-kv"><span class="k">CLOSE</span><span class="v">${fmtPriceOrDash(sig.close_price)}</span></div>
      ${stratRows}
      <div class="detail-kv"><span class="k">SPREAD / FUNDING</span><span class="v">${fmtPct(sig.spread_pct || 0, 3)} / ${fmtFunding(sig.funding_rate)}</span></div>
    </div>
    ${sig.reject_sub_reason ? `<div class="scan-diag-box"><div class="tui-muted-xs">SUB-REASON DETAIL</div><div>${escapeHtml(sig.reject_sub_reason)}</div></div>` : ''}
  `;
  if (!dlg.open) dlg.showModal();
}

async function openTradeAutopsyDialog(cid, forceRefresh = false) {
  const trade = state.tradesByClientId.get(cid);
  const dlg = document.getElementById('detail-dialog');
  const titleEl = document.getElementById('dialog-title');
  const bodyEl = document.getElementById('dialog-body');
  const refreshBtn = document.getElementById('dialog-refresh-btn');
  if (!dlg || !bodyEl) return;

  if (titleEl) titleEl.textContent = `TRADE_AUTOPSY // ${trade ? trade.symbol : cid}`;
  if (refreshBtn) {
    refreshBtn.classList.remove('hidden');
    refreshBtn.dataset.cid = cid;
    refreshBtn.disabled = true;
  }

  const pnl = trade ? Number(trade.net_pnl_usdt || 0) : 0;
  const summaryHtml = trade ? `
    <div class="detail-kv-grid">
      <div class="detail-kv"><span class="k">SYMBOL / SIDE</span><span class="v">${escapeHtml(trade.symbol)} · ${escapeHtml(trade.side)}</span></div>
      <div class="detail-kv"><span class="k">STRATEGY</span><span class="v">${escapeHtml(trade.strategy_id)}</span></div>
      <div class="detail-kv"><span class="k">NET PNL / ROE</span><span class="v ${pnl >= 0 ? 'c-green' : 'c-red'}">${fmtMoney(pnl, true)} (${fmtPct(trade.roe_pct || 0, 1, true)})</span></div>
      <div class="detail-kv"><span class="k">ENTRY → EXIT</span><span class="v">${fmtPrice(trade.entry_price)} → ${fmtPrice(trade.exit_price)}</span></div>
      <div class="detail-kv"><span class="k">MAE / MFE</span><span class="v">-${Number(trade.mae_pct || 0).toFixed(2)}% / +${Number(trade.mfe_pct || 0).toFixed(2)}%</span></div>
      <div class="detail-kv"><span class="k">EXIT REASON</span><span class="v">${escapeHtml(trade.exit_reason)} (${fmtDuration(trade.duration_sec)})</span></div>
    </div>
  ` : '';

  bodyEl.innerHTML = `${summaryHtml}<div class="tui-copilot-scroll" id="autopsy-markdown"><p class="c-cyan">$ analyzing trade ${escapeHtml(cid)}...</p></div>`;
  if (!dlg.open) dlg.showModal();

  try {
    const url = `/api/ai/trade-autopsy?client_order_id=${encodeURIComponent(cid)}${forceRefresh ? '&refresh=true' : ''}`;
    const res = await apiFetch(url);
    const mdEl = document.getElementById('autopsy-markdown');
    if (mdEl) {
      if (res && res.text) {
        mdEl.innerHTML = `<div class="tui-muted-xs">SOURCE: ${escapeHtml(String(res.source || 'gemini').toUpperCase())}</div>${sanitizeMarkdown(res.text)}`;
      } else {
        mdEl.innerHTML = `<p class="c-red">${escapeHtml((res && res.error) || 'No AI autopsy output returned.')}</p>`;
      }
    }
  } catch (err) {
    const mdEl = document.getElementById('autopsy-markdown');
    if (mdEl) mdEl.innerHTML = `<p class="c-red">AUTOPSY ERROR: ${escapeHtml(err.message)}</p>`;
  } finally {
    if (refreshBtn) refreshBtn.disabled = false;
  }
}

// -------------------------------------------------------------------------
// AI Copilot Console (TAB 4)
// -------------------------------------------------------------------------
async function runCopilotCommand(label, url) {
  switchTab('copilot');
  const seq = ++state.aiRequestSeq;
  const titleEl = document.getElementById('copilot-pane-title');
  const tsEl = document.getElementById('copilot-pane-ts');
  const srcPill = document.getElementById('copilot-source-pill');
  const outEl = document.getElementById('copilot-output');

  if (titleEl) titleEl.textContent = `COPILOT_OUTPUT // ${label}`;
  if (srcPill) {
    srcPill.textContent = 'RUNNING';
    srcPill.className = 'tui-pill pill-amber';
  }
  if (outEl) {
    outEl.innerHTML = `<p class="c-cyan">$ trml ${escapeHtml(label)} ...</p><p class="tui-muted">Querying Gemini analysis engine (timeout 30s)...</p>`;
  }

  try {
    const res = await apiFetch(url);
    if (seq !== state.aiRequestSeq) return;
    if (res && res.text) {
      state.aiRawMarkdown = res.text;
      if (outEl) outEl.innerHTML = sanitizeMarkdown(res.text);
      if (srcPill) {
        srcPill.textContent = String(res.source || 'GEMINI').toUpperCase();
        srcPill.className = 'tui-pill pill-green';
      }
      if (tsEl) tsEl.textContent = `${fmtShortTimeUTC(new Date().toISOString())} UTC`;
    } else {
      const msg = (res && (res.error || res.message)) || 'Empty AI response';
      if (outEl) outEl.innerHTML = `<p class="c-red">AI ERROR: ${escapeHtml(msg)}</p>`;
      if (srcPill) {
        srcPill.textContent = 'ERROR';
        srcPill.className = 'tui-pill pill-red';
      }
    }
  } catch (err) {
    if (seq !== state.aiRequestSeq) return;
    if (outEl) outEl.innerHTML = `<p class="c-red">REQUEST FAILED: ${escapeHtml(err.message)}</p>`;
    if (srcPill) {
      srcPill.textContent = err.status === 429 ? 'RATE_LIMIT' : 'FAILED';
      srcPill.className = 'tui-pill pill-red';
    }
    showToast('error', 'COPILOT ERROR', err.message);
  }
}

// -------------------------------------------------------------------------
// Data Fetching & Adaptive Polling
// -------------------------------------------------------------------------
async function fetchSummary() {
  if (!state.pinVerified || state.summaryInFlight) return;
  state.summaryInFlight = true;
  try {
    const data = await apiFetch('/api/summary');
    renderSummary(data);
    updateConnectionState(true);
  } catch (err) {
    if (err.status !== 401) updateConnectionState(false, err.message);
  } finally {
    state.summaryInFlight = false;
  }
}

async function fetchScanner() {
  if (!state.pinVerified || state.scannerInFlight) return;
  state.scannerInFlight = true;
  try {
    const data = await apiFetch('/api/scanner');
    renderScanner(data);
  } catch (_) {
  } finally {
    state.scannerInFlight = false;
  }
}

async function fetchSignals(forDeskStream = false) {
  if (!state.pinVerified || state.signalsInFlight) return;
  state.signalsInFlight = true;
  try {
    const params = new URLSearchParams();
    if (forDeskStream) {
      params.set('page', '1');
      params.set('limit', '12');
    } else {
      params.set('page', String(state.signalsPage));
      params.set('limit', String(state.signalsLimit));
      if (state.signalsDecision && state.signalsDecision !== 'ALL') {
        params.set('decision', state.signalsDecision);
      }
      if (state.signalsStrategy && state.signalsStrategy !== 'ALL') {
        params.set('strategy', state.signalsStrategy);
      }
      if (state.signalsSymbol.trim()) {
        params.set('symbol', state.signalsSymbol.trim().toUpperCase());
      }
    }
    const data = await apiFetch(`/api/signals?${params.toString()}`);
    if (forDeskStream) {
      renderDeskSignalStream((data && data.signals) || []);
    } else {
      renderSignalsTable(data);
      if (state.signalsPage === 1 && state.signalsDecision === 'ALL' && state.signalsStrategy === 'ALL' && !state.signalsSymbol) {
        renderDeskSignalStream((data && data.signals) || []);
      }
    }
  } catch (err) {
    if (!forDeskStream && err.status !== 401) {
      showToast('error', 'SIGNALS LOAD FAILED', err.message);
    }
  } finally {
    state.signalsInFlight = false;
  }
}

async function fetchTrades() {
  if (!state.pinVerified || state.tradesInFlight) return;
  state.tradesInFlight = true;
  try {
    const params = new URLSearchParams({
      page: String(state.tradesPage),
      limit: String(state.tradesLimit)
    });
    if (state.tradesStatus && state.tradesStatus !== 'ALL') {
      params.set('status', state.tradesStatus);
    }
    if (state.tradesStrategy && state.tradesStrategy !== 'ALL') {
      params.set('strategy', state.tradesStrategy);
    }
    const data = await apiFetch(`/api/trades?${params.toString()}`);
    renderLedgerTable(data);
  } catch (err) {
    if (err.status !== 401) showToast('error', 'LEDGER LOAD FAILED', err.message);
  } finally {
    state.tradesInFlight = false;
  }
}

function schedulePolling() {
  stopPolling();
  const runSummaryTick = async () => {
    if (!state.pinVerified) return;
    if (!document.hidden) {
      await fetchSummary();
      if (state.activeTab === 'desk') {
        await fetchSignals(true);
      }
    }
    const delay = state.activeSlotsCount > 0 ? POLL_SUMMARY_ACTIVE_MS : POLL_SUMMARY_IDLE_MS;
    state.summaryTimer = setTimeout(runSummaryTick, delay);
  };
  const runScannerTick = async () => {
    if (!state.pinVerified) return;
    if (!document.hidden && state.activeTab === 'desk') {
      await fetchScanner();
    }
    state.scannerTimer = setTimeout(runScannerTick, POLL_SCANNER_MS);
  };
  state.summaryTimer = setTimeout(runSummaryTick, state.activeSlotsCount > 0 ? POLL_SUMMARY_ACTIVE_MS : POLL_SUMMARY_IDLE_MS);
  state.scannerTimer = setTimeout(runScannerTick, POLL_SCANNER_MS);
}

function stopPolling() {
  if (state.summaryTimer) clearTimeout(state.summaryTimer);
  if (state.scannerTimer) clearTimeout(state.scannerTimer);
  state.summaryTimer = null;
  state.scannerTimer = null;
}

async function refreshActiveView() {
  await fetchSummary();
  if (state.activeTab === 'desk') {
    await Promise.all([fetchScanner(), fetchSignals(true)]);
  } else if (state.activeTab === 'signals') {
    await fetchSignals(false);
  } else if (state.activeTab === 'ledger') {
    await fetchTrades();
  }
}

// -------------------------------------------------------------------------
// Tabs & Event Wiring
// -------------------------------------------------------------------------
function switchTab(tabName) {
  if (!['desk', 'signals', 'ledger', 'copilot'].includes(tabName)) return;
  state.activeTab = tabName;

  document.querySelectorAll('.tui-tab').forEach((btn) => {
    const active = btn.dataset.tab === tabName;
    btn.classList.toggle('is-active', active);
    btn.setAttribute('aria-selected', active ? 'true' : 'false');
  });

  document.querySelectorAll('.tui-view').forEach((view) => {
    view.classList.toggle('is-active', view.id === `view-${tabName}`);
  });

  if (tabName === 'signals') fetchSignals(false);
  if (tabName === 'ledger') fetchTrades();
  if (tabName === 'desk') {
    fetchScanner();
    fetchSignals(true);
  }
}

function wireEvents() {
  // Theme Switcher
  const themeSel = document.getElementById('theme-select');
  if (themeSel) {
    themeSel.addEventListener('change', (e) => applyTheme(e.target.value));
  }

  // Tabs
  document.querySelectorAll('.tui-tab').forEach((btn) => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  // Topbar Refresh & Lock
  const btnRefresh = document.getElementById('btn-refresh');
  if (btnRefresh) btnRefresh.addEventListener('click', () => refreshActiveView());

  const btnLock = document.getElementById('btn-lock');
  if (btnLock) {
    btnLock.addEventListener('click', () => {
      handleUnauthorized();
    });
  }

  // PIN Form Submit
  const pinForm = document.getElementById('pin-form');
  if (pinForm) {
    pinForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (state.pinSubmitting) return;
      const inp = document.getElementById('pin-input');
      const pin = inp ? inp.value.trim() : '';
      if (!pin) return;

      state.pinSubmitting = true;
      try {
        const res = await fetch('/api/auth/verify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pin }),
          credentials: 'same-origin'
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          showPinOverlay(data.message || data.error || 'Invalid PIN.');
          return;
        }
        state.storedPin = pin;
        state.pinVerified = true;
        savePin(pin);
        hidePinOverlay();
        await refreshActiveView();
        schedulePolling();
      } catch (err) {
        showPinOverlay(err.message || 'Authentication failed.');
      } finally {
        state.pinSubmitting = false;
      }
    });
  }

  // Active Positions Row Selection + Inline AI Health
  const posTbody = document.getElementById('positions-tbody');
  if (posTbody) {
    posTbody.addEventListener('click', (e) => {
      const healthBtn = e.target.closest('[data-ai-health]');
      if (healthBtn) {
        e.stopPropagation();
        const sym = healthBtn.dataset.aiHealth;
        if (sym) {
          state.selectedPosSymbol = sym;
          runCopilotCommand(`position-health ${sym}`, `/api/ai/position-health?symbol=${encodeURIComponent(sym)}`);
        }
        return;
      }
      const row = e.target.closest('tr[data-pos-symbol]');
      if (row) {
        state.selectedPosSymbol = row.dataset.posSymbol;
        posTbody.querySelectorAll('tr').forEach((tr) => tr.classList.toggle('is-selected', tr === row));
        const sel = document.getElementById('copilot-pos-select');
        if (sel) sel.value = state.selectedPosSymbol;
      }
    });
  }

  // Scanner inline filter & Pod filter
  document.querySelectorAll('[data-scan-pod]').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-scan-pod]').forEach((b) => b.classList.toggle('is-active', b === btn));
      state.scannerPodFilter = btn.dataset.scanPod || 'ALL';
      renderWatchlistTable();
    });
  });

  const scanFilter = document.getElementById('scanner-filter');
  if (scanFilter) {
    scanFilter.addEventListener('input', (e) => {
      state.scannerFilter = e.target.value || '';
      renderWatchlistTable();
    });
  }

  // DESK Signal Stream Click -> Modal
  const deskStream = document.getElementById('desk-signal-stream');
  if (deskStream) {
    deskStream.addEventListener('click', (e) => {
      const item = e.target.closest('[data-signal-id]');
      if (item) openSignalDialog(item.dataset.signalId);
    });
  }

  // DESK Quick Copilot Buttons
  const qcExec = document.getElementById('btn-qc-exec');
  if (qcExec) qcExec.addEventListener('click', () => runCopilotCommand('executive-summary', '/api/ai/executive-summary'));

  const qcHealth = document.getElementById('btn-qc-health');
  if (qcHealth) {
    qcHealth.addEventListener('click', () => {
      const sym = state.selectedPosSymbol || (document.getElementById('copilot-pos-select')?.value || '');
      if (!sym) {
        showToast('warn', 'NO ACTIVE POSITION', 'Open a position first or switch to Copilot.');
        return;
      }
      runCopilotCommand(`position-health ${sym}`, `/api/ai/position-health?symbol=${encodeURIComponent(sym)}`);
    });
  }

  const qcPattern = document.getElementById('btn-qc-pattern');
  if (qcPattern) qcPattern.addEventListener('click', () => runCopilotCommand('pattern-miner', '/api/ai/pattern-miner'));

  // Signals Tab Filters, Click & Pagination
  document.querySelectorAll('[data-sig-dec]').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-sig-dec]').forEach((b) => b.classList.toggle('is-active', b === btn));
      state.signalsDecision = btn.dataset.sigDec || 'ALL';
      state.signalsPage = 1;
      fetchSignals(false);
    });
  });

  const sigStrat = document.getElementById('sig-strategy-filter');
  if (sigStrat) {
    sigStrat.addEventListener('change', (e) => {
      state.signalsStrategy = e.target.value || 'ALL';
      state.signalsPage = 1;
      fetchSignals(false);
    });
  }

  let sigSearchTimer = null;
  const sigSym = document.getElementById('sig-symbol-filter');
  if (sigSym) {
    sigSym.addEventListener('input', (e) => {
      state.signalsSymbol = e.target.value || '';
      state.signalsPage = 1;
      clearTimeout(sigSearchTimer);
      sigSearchTimer = setTimeout(() => fetchSignals(false), 240);
    });
  }

  const sigTbody = document.getElementById('signals-tbody');
  if (sigTbody) {
    sigTbody.addEventListener('click', (e) => {
      const row = e.target.closest('tr[data-signal-id]');
      if (row) openSignalDialog(row.dataset.signalId);
    });
  }

  document.getElementById('sig-prev')?.addEventListener('click', () => {
    if (state.signalsPage > 1) {
      state.signalsPage -= 1;
      fetchSignals(false);
    }
  });
  document.getElementById('sig-next')?.addEventListener('click', () => {
    if (state.signalsPage < state.signalsTotalPages) {
      state.signalsPage += 1;
      fetchSignals(false);
    }
  });

  // Ledger Tab Filters, Click & Pagination
  document.querySelectorAll('[data-led-status]').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-led-status]').forEach((b) => b.classList.toggle('is-active', b === btn));
      state.tradesStatus = btn.dataset.ledStatus || 'ALL';
      state.tradesPage = 1;
      fetchTrades();
    });
  });

  const ledStrat = document.getElementById('led-strategy-filter');
  if (ledStrat) {
    ledStrat.addEventListener('change', (e) => {
      state.tradesStrategy = e.target.value || 'ALL';
      state.tradesPage = 1;
      fetchTrades();
    });
  }

  const ledTbody = document.getElementById('ledger-tbody');
  if (ledTbody) {
    ledTbody.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-autopsy-cid]');
      if (btn) {
        e.stopPropagation();
        openTradeAutopsyDialog(btn.dataset.autopsyCid, false);
        return;
      }
      const row = e.target.closest('tr[data-trade-cid]');
      if (row) openTradeAutopsyDialog(row.dataset.tradeCid, false);
    });
  }

  document.getElementById('led-prev')?.addEventListener('click', () => {
    if (state.tradesPage > 1) {
      state.tradesPage -= 1;
      fetchTrades();
    }
  });
  document.getElementById('led-next')?.addEventListener('click', () => {
    if (state.tradesPage < state.tradesTotalPages) {
      state.tradesPage += 1;
      fetchTrades();
    }
  });

  // Copilot Tab Buttons
  document.getElementById('cmd-exec-summary')?.addEventListener('click', () =>
    runCopilotCommand('executive-summary', '/api/ai/executive-summary')
  );
  document.getElementById('cmd-pos-health')?.addEventListener('click', () => {
    const sym = document.getElementById('copilot-pos-select')?.value || state.selectedPosSymbol;
    if (!sym) {
      showToast('warn', 'NO ACTIVE POSITION', 'Select an open position to analyze.');
      return;
    }
    runCopilotCommand(`position-health ${sym}`, `/api/ai/position-health?symbol=${encodeURIComponent(sym)}`);
  });
  document.getElementById('cmd-pattern-miner')?.addEventListener('click', () =>
    runCopilotCommand('pattern-miner', '/api/ai/pattern-miner')
  );
  document.getElementById('cmd-copy-ai')?.addEventListener('click', async () => {
    if (!state.aiRawMarkdown) return;
    try {
      await navigator.clipboard.writeText(state.aiRawMarkdown);
      showToast('info', 'COPIED', 'Copilot output copied to clipboard.');
    } catch (_) {
      showToast('warn', 'COPY FAILED', 'Clipboard permission unavailable.');
    }
  });

  // Dialog Modal Controls
  const dlg = document.getElementById('detail-dialog');
  document.getElementById('dialog-close-btn')?.addEventListener('click', () => dlg?.close());
  document.getElementById('dialog-refresh-btn')?.addEventListener('click', (e) => {
    const cid = e.target.dataset.cid;
    if (cid) openTradeAutopsyDialog(cid, true);
  });
  dlg?.addEventListener('click', (e) => {
    if (e.target === dlg) dlg.close();
  });

  // Global Keyboard Shortcuts
  document.addEventListener('keydown', (e) => {
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    const tag = (e.target && e.target.tagName) || '';
    const isTyping = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';

    if (e.key === 'Escape') {
      if (dlg && dlg.open) {
        dlg.close();
      } else if (isTyping && e.target.blur) {
        e.target.blur();
      }
      return;
    }
    if (isTyping || !state.pinVerified) return;

    if (e.key === '1') switchTab('desk');
    else if (e.key === '2') switchTab('signals');
    else if (e.key === '3') switchTab('ledger');
    else if (e.key === '4') switchTab('copilot');
    else if (e.key === 'r' || e.key === 'R') refreshActiveView();
    else if (e.key === 't' || e.key === 'T') cycleTheme();
    else if (e.key === '/') {
      e.preventDefault();
      if (state.activeTab === 'desk') document.getElementById('scanner-filter')?.focus();
      else if (state.activeTab === 'signals') document.getElementById('sig-symbol-filter')?.focus();
    }
  });

  // Visibility Pause/Resume
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && state.pinVerified) {
      refreshActiveView();
    }
  });
}

// -------------------------------------------------------------------------
// Bootstrap
// -------------------------------------------------------------------------
async function bootstrap() {
  try {
    const savedTheme = localStorage.getItem('trml_theme') || 'obsidian';
    applyTheme(savedTheme);
  } catch (_) {
    applyTheme('obsidian');
  }

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
  }

  wireEvents();
  const ok = await initAuth();
  if (ok) {
    await refreshActiveView();
    schedulePolling();
  }
}

document.addEventListener('DOMContentLoaded', bootstrap);
