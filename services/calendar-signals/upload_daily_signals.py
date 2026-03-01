#!/usr/bin/env python3
"""
upload_daily_signals.py
Investment OS NEW PLATFORM — Supabase uploader for daily signals

Location: /opt/investment-os/services/calendar-signals/upload_daily_signals.py

USAGE (called by run_daily_signals.sh):
    PYTHONPATH=/opt/investment-os/packages python3 \
        /opt/investment-os/services/calendar-signals/upload_daily_signals.py \
        --signal-json /opt/investment-os/signals/investment_os_signals_YYYYMMDD_HHMMSS.json \
        --date $(date +%Y-%m-%d)

CREDENTIALS: reads SUPABASE_URL + SUPABASE_KEY from /opt/investment-os/.env
             via common.config (get_config) + common.database (get_supabase_client)

TABLE: cse_daily_signals
  PK: signal_date
  Columns: signal_date, overall_signal, confidence, stocks_analyzed,
           high_confidence_count, notes, raw_payload (JSONB), created_at

NEW PLATFORM SIGNAL JSON FORMAT (investment_os_signals_YYYYMMDD_HHMMSS.json):
  {
    "generated_at": "...",
    "stocks_analyzed": 296,
    "manipulation_contamination": "...",
    "signals": [
      {"stock": "RCH.N0000", "signal": "BUY", "confidence": 76.08, ...},
      ...
    ]
  }
  overall_signal is derived by majority vote across signals[].signal
  high_confidence_count = stocks where confidence >= 60
"""

import json
import os
import re
import sys
import argparse
from datetime import date
from pathlib import Path
from typing import Any

# ── New platform: use common library for DB connection ────────────────────────
# PYTHONPATH must include /opt/investment-os/packages (set in shell wrapper)
from common.database import get_supabase_client


# ─── Parse latest_signal.json ────────────────────────────────────────────────

def parse_signal_json(json_path: str, signal_date: str) -> dict[str, Any]:
    """
    Parse signal JSON produced by the new platform tier1_signal_generator.py.

    New platform format (investment_os_signals_YYYYMMDD_HHMMSS.json):
      {
        "generated_at": "...",
        "stocks_analyzed": 296,
        "signals": [{"stock": "RCH.N0000", "signal": "BUY", "confidence": 76.08}, ...]
      }
      overall_signal → majority vote across signals[].signal
      high_confidence_count → stocks where confidence (numeric) >= 60

    Also handles legacy flat/nested formats for backward compatibility.
    """
    from collections import Counter

    with open(json_path) as f:
        data = json.load(f)

    if isinstance(data, dict):
        # ── New platform: dict with 'signals' array ────────────────────────────
        if 'signals' in data and isinstance(data['signals'], list):
            per_stock = data['signals']
            stock_signals = [s.get('signal', 'HOLD').upper() for s in per_stock if 'signal' in s]
            signal   = Counter(stock_signals).most_common(1)[0][0] if stock_signals else 'HOLD'
            stocks   = data.get('stocks_analyzed') or len(per_stock)
            # confidence is numeric (0–100) in new platform format
            hc_count = sum(
                1 for s in per_stock
                if isinstance(s.get('confidence'), (int, float)) and float(s['confidence']) >= 60
            )
            conf     = 'HIGH' if hc_count > 0 else 'LOW'
            buy_count  = stock_signals.count('BUY')
            sell_count = stock_signals.count('SELL')
            notes    = (
                f"{stocks} stocks analyzed; {hc_count} high-confidence (≥60%); "
                f"{buy_count} BUY, {sell_count} SELL"
            )

        # ── Legacy flat/nested format ──────────────────────────────────────────
        else:
            overall  = data.get('overall') or data
            signal   = (overall.get('signal') or overall.get('overall_signal') or 'HOLD').upper()
            conf     = overall.get('confidence')
            stocks   = overall.get('stocks') or overall.get('stocks_analyzed') or 0
            hc_count = overall.get('high_confidence_count') or overall.get('high_count') or 0
            notes    = overall.get('notes') or overall.get('message') or overall.get('summary') or ''

    elif isinstance(data, list):
        # ── Top-level list of per-stock signals ───────────────────────────────
        stock_signals = [s.get('signal', 'HOLD').upper() for s in data if 'signal' in s]
        signal   = Counter(stock_signals).most_common(1)[0][0] if stock_signals else 'HOLD'
        stocks   = len(data)
        hc_count = sum(
            1 for s in data
            if isinstance(s.get('confidence'), (int, float)) and float(s['confidence']) >= 60
        )
        conf     = 'HIGH' if hc_count > 0 else None
        notes    = f'{stocks} stocks analyzed'
    else:
        signal, conf, stocks, hc_count, notes = 'HOLD', None, 0, 0, ''

    return {
        'signal_date':           signal_date,
        'overall_signal':        signal,             # ← column is overall_signal, NOT signal
        'confidence':            str(conf).upper() if conf else None,
        'stocks_analyzed':       int(stocks) if stocks else 0,
        'high_confidence_count': int(hc_count),
        'notes':                 str(notes)[:1000] if notes else None,
        'raw_payload':           data,               # stored as JSONB
    }


# ─── Parse text report (fallback) ────────────────────────────────────────────

def parse_signal_txt(txt_path: str, signal_date: str) -> dict[str, Any]:
    """Parse report_YYYY-MM-DD.txt as fallback when no JSON available."""
    with open(txt_path) as f:
        content = f.read()

    signal, stocks, hc_count, notes = 'HOLD', 0, 0, ''

    m = re.search(r'Stocks:\s*(\d+)', content)
    if m: stocks = int(m.group(1))

    if 'NO HIGH-CONFIDENCE SIGNALS' in content:
        signal, notes = 'HOLD', 'No high-confidence signals today'
    m2 = re.search(r'Signal:\s*(BUY|SELL|HOLD)', content, re.IGNORECASE)
    if m2: signal = m2.group(1).upper()

    return {
        'signal_date':           signal_date,
        'overall_signal':        signal,
        'confidence':            'HIGH' if hc_count > 0 else None,
        'stocks_analyzed':       stocks,
        'high_confidence_count': hc_count,
        'notes':                 notes or None,
        'raw_payload':           {'parsed_from': 'txt', 'content_preview': content[:500]},
    }


# ─── Supabase upsert via common.database ─────────────────────────────────────

def upload_to_supabase(row: dict[str, Any]) -> bool:
    try:
        sb = get_supabase_client()   # ← singleton from common.database

        sb.table('cse_daily_signals') \
          .upsert(row, on_conflict='signal_date') \
          .execute()

        print(f"[upload_daily_signals] ✅ Upserted signal for {row['signal_date']}: "
              f"{row['overall_signal']} → cse_daily_signals")
        return True

    except Exception as e:
        print(f"[upload_daily_signals] ❌ ERROR: {e}")
        return False


# ─── Entry points ─────────────────────────────────────────────────────────────

def upload_signal(signal_json_path: str, signal_date: str | None = None) -> bool:
    signal_date = signal_date or date.today().isoformat()
    row = parse_signal_json(signal_json_path, signal_date)
    return upload_to_supabase(row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload daily signal to Supabase')
    parser.add_argument('--signal-json', help='Path to latest_signal.json')
    parser.add_argument('--signal-txt',  help='Path to report txt (fallback)')
    parser.add_argument('--date', default=date.today().isoformat(), help='Signal date YYYY-MM-DD')
    parser.add_argument('--signal',  help='Override: BUY|SELL|HOLD')
    parser.add_argument('--stocks', type=int, default=0, help='Override: stocks analyzed count')
    args = parser.parse_args()

    if args.signal_json and Path(args.signal_json).exists():
        row = parse_signal_json(args.signal_json, args.date)
    elif args.signal_txt and Path(args.signal_txt).exists():
        row = parse_signal_txt(args.signal_txt, args.date)
    elif args.signal:
        row = {
            'signal_date':           args.date,
            'overall_signal':        args.signal.upper(),
            'confidence':            None,
            'stocks_analyzed':       args.stocks,
            'high_confidence_count': 0,
            'notes':                 None,
            'raw_payload':           None,
        }
    else:
        print("ERROR: provide --signal-json, --signal-txt, or --signal")
        sys.exit(1)

    success = upload_to_supabase(row)
    sys.exit(0 if success else 1)
