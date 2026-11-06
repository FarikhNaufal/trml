import subprocess
import os
import re

configs = [
    {
        "name": "Iter_1_ShortFocus_Slots8",
        "params": {
            "ALLOWED_SIDE": "SHORT_ONLY",
            "MAX_CONCURRENT_SLOTS": "8",
            "MIN_BOUNCE_ZSCORE_SHORT": "0.65",
            "PULLBACK_TOLERANCE_ATR": "0.40",
            "MIN_IMPULSE_ATR": "0.60",
            "MIN_NATR_PCT": "0.55",
            "MAX_SPREAD_PCT": "0.06",
            "MIN_SIGNAL_RVOL": "0.60",
            "COOLDOWN_AFTER_EXIT_MINUTES": "5",
            "COOLDOWN_AFTER_LOSS_MINUTES": "15",
            "COOLDOWN_AFTER_WIN_MINUTES": "3",
            "PARTIAL_TP1_RATIO": "0.40",
            "PARTIAL_TP1_ATR_MULT": "1.25",
            "TRAILING_ACTIVATE_ATR": "1.50",
            "TRAILING_DISTANCE_ATR": "0.35",
            "SL_ATR_MULTIPLIER": "1.65",
            "TP_ATR_MULTIPLIER": "3.50",
            "MAX_HOLDING_DURATION": "3h"
        }
    },
    {
        "name": "Iter_2_ShortUltra_Slots10",
        "params": {
            "ALLOWED_SIDE": "SHORT_ONLY",
            "MAX_CONCURRENT_SLOTS": "10",
            "MIN_BOUNCE_ZSCORE_SHORT": "0.60",
            "PULLBACK_TOLERANCE_ATR": "0.42",
            "MIN_IMPULSE_ATR": "0.55",
            "MIN_NATR_PCT": "0.50",
            "MAX_SPREAD_PCT": "0.06",
            "MIN_SIGNAL_RVOL": "0.55",
            "COOLDOWN_AFTER_EXIT_MINUTES": "3",
            "COOLDOWN_AFTER_LOSS_MINUTES": "10",
            "COOLDOWN_AFTER_WIN_MINUTES": "2",
            "PARTIAL_TP1_RATIO": "0.45",
            "PARTIAL_TP1_ATR_MULT": "1.20",
            "TRAILING_ACTIVATE_ATR": "1.45",
            "TRAILING_DISTANCE_ATR": "0.35",
            "SL_ATR_MULTIPLIER": "1.60",
            "TP_ATR_MULTIPLIER": "3.50",
            "MAX_HOLDING_DURATION": "3h"
        }
    },
    {
        "name": "Iter_3_AsymStrictLong_Slots8",
        "params": {
            "ALLOWED_SIDE": "AUTO",
            "MAX_CONCURRENT_SLOTS": "8",
            "MIN_BOUNCE_ZSCORE_LONG": "1.50",
            "MIN_BOUNCE_ZSCORE_SHORT": "0.65",
            "PULLBACK_TOLERANCE_ATR": "0.40",
            "MIN_IMPULSE_ATR": "0.60",
            "MIN_NATR_PCT": "0.55",
            "MAX_SPREAD_PCT": "0.06",
            "MIN_SIGNAL_RVOL": "0.60",
            "COOLDOWN_AFTER_EXIT_MINUTES": "5",
            "COOLDOWN_AFTER_LOSS_MINUTES": "15",
            "COOLDOWN_AFTER_WIN_MINUTES": "3",
            "PARTIAL_TP1_RATIO": "0.40",
            "PARTIAL_TP1_ATR_MULT": "1.25",
            "TRAILING_ACTIVATE_ATR": "1.50",
            "TRAILING_DISTANCE_ATR": "0.35",
            "SL_ATR_MULTIPLIER": "1.65",
            "TP_ATR_MULTIPLIER": "3.50",
            "MAX_HOLDING_DURATION": "3h"
        }
    },
    {
        "name": "Iter_4_ShortRunner_Slots8",
        "params": {
            "ALLOWED_SIDE": "SHORT_ONLY",
            "MAX_CONCURRENT_SLOTS": "8",
            "MIN_BOUNCE_ZSCORE_SHORT": "0.65",
            "PULLBACK_TOLERANCE_ATR": "0.38",
            "MIN_IMPULSE_ATR": "0.60",
            "MIN_NATR_PCT": "0.60",
            "MAX_SPREAD_PCT": "0.05",
            "MIN_SIGNAL_RVOL": "0.60",
            "COOLDOWN_AFTER_EXIT_MINUTES": "5",
            "COOLDOWN_AFTER_LOSS_MINUTES": "15",
            "COOLDOWN_AFTER_WIN_MINUTES": "3",
            "PARTIAL_TP1_RATIO": "0.50",
            "PARTIAL_TP1_ATR_MULT": "1.20",
            "TRAILING_ACTIVATE_ATR": "1.45",
            "TRAILING_DISTANCE_ATR": "0.35",
            "SL_ATR_MULTIPLIER": "1.65",
            "TP_ATR_MULTIPLIER": "3.80",
            "MAX_HOLDING_DURATION": "3h"
        }
    }
]

# Read base env
with open('opt_test1.env') as f:
    base_lines = f.readlines()

symbols_file = 'pure_crypto_symbols.txt'
with open(symbols_file) as f:
    symbols = f.read().strip()

print(f"{'Config Name':<28} | {'Trades':<6} | {'Win Rate':<8} | {'Profit Factor':<13} | {'Net PnL':<12} | {'Exp/Tr':<10} | {'Max DD':<10}")
print("-" * 105)

results = []

for cfg in configs:
    env_name = f"{cfg['name']}.env"
    report_name = f"report_{cfg['name']}.md"
    
    # Generate custom env
    custom_lines = []
    for line in base_lines:
        matched = False
        for k, v in cfg['params'].items():
            if line.startswith(f"{k}="):
                custom_lines.append(f"{k}={v}\n")
                matched = True
                break
        if not matched:
            custom_lines.append(line)
            
    with open(env_name, 'w') as f:
        f.writelines(custom_lines)
        
    cmd = [
        './bin/backtest', 'run',
        '--from', '2026-05-01',
        '--to', '2026-10-31',
        '--env', env_name,
        '--symbols', symbols,
        '--out', report_name
    ]
    
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out_text = proc.stdout
    
    # Parse metrics from stdout / report
    t_m = re.search(r'Total Trade Selesai\*\* \| `([0-9]+)` setup', out_text)
    wr_m = re.search(r'Win Rate \(All-Inclusive\)\*\* \| `([0-9.]+)%`', out_text)
    pf_m = re.search(r'Profit Factor\*\* \| `([0-9.]+)`', out_text)
    pnl_m = re.search(r'Total Net PnL\*\* \| `([+-]?\$?[0-9.]+) USDT`', out_text)
    exp_m = re.search(r'Net Expectancy per Trade\*\* \| `([+-]?\$?[0-9.]+) USDT`', out_text)
    dd_m = re.search(r'Max Drawdown \(Peak-to-Trough\)\*\* \| `-?([0-9.]+)%`', out_text)
    
    trades = int(t_m.group(1)) if t_m else 0
    wr = float(wr_m.group(1)) if wr_m else 0.0
    pf = float(pf_m.group(1)) if pf_m else 0.0
    pnl = pnl_m.group(1) if pnl_m else "$0.00"
    exp = exp_m.group(1) if exp_m else "$0.00"
    dd = dd_m.group(1) if dd_m else "0.0"
    
    print(f"{cfg['name']:<28} | {trades:<6} | {wr:<7.1f}% | {pf:<13.2f} | {pnl:<12} | {exp:<10} | -{dd:<9}%")
    results.append({
        "name": cfg['name'],
        "trades": trades,
        "wr": wr,
        "pf": pf,
        "pnl": pnl,
        "exp": exp,
        "dd": dd,
        "report": report_name,
        "env": env_name
    })

