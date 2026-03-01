#!/usr/bin/env python3
"""
upload_manipulation_signals.py
Investment OS NEW PLATFORM — Supabase uploader for v5.0 Manipulation Detector

Location: /opt/investment-os/services/manipulation-detector/upload_manipulation_signals.py

USAGE (called by run_manipulation_detector.sh):
    PYTHONPATH=/opt/investment-os/packages python3 \
        /opt/investment-os/services/manipulation-detector/upload_manipulation_signals.py \
        --report /opt/investment-os/v5_reports/v5_report_$(date +%Y-%m-%d).json \
        --date $(date +%Y-%m-%d)

CREDENTIALS: reads SUPABASE_URL + SUPABASE_KEY from /opt/investment-os/.env
             via common.config (get_config) + common.database (get_supabase_client)
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

# ─── Parse JSON report produced by v5.0 detector ─────────────────────────────

def parse_v5_json(json_path: str, run_date: str) -> list[dict[str, Any]]:
    """
    Parse v5_report_YYYY-MM-DD.json → list of rows ready for upsert.

    Confirmed JSON structure (2026-02-28):
    [
      {
        "stock_symbol": "HARI.N0000",
        "priority": "HIGH",
        "is_empire_stock": false,
        "best_pattern": {
          "pattern_name": "stairstep",
          "confidence": 0.9,        ← decimal 0-1, multiply ×100 for _pct columns
          "expected_return": 0.68,  ← decimal 0-1, multiply ×100
          "win_rate": 0.75,         ← decimal 0-1, multiply ×100
          "action": "BUY",
          "entry_price": 5309.75,
          "stop_loss": 5317.11,
          "target_price": 8925.0,
          "time_horizon": "3_months",
          "explanation": "..."
        }
      }, ...
    ]
    """
    with open(json_path) as f:
        data = json.load(f)

    rows = []
    skipped = 0

    def pct(v) -> float | None:
        """Convert decimal fraction (0–1) to percentage (0–100)."""
        if v is None: return None
        try: return round(float(v) * 100, 1)
        except (ValueError, TypeError): return None

    def to_num(v) -> float | None:
        """Parse a numeric value (int/float or formatted string)."""
        if v is None: return None
        if isinstance(v, (int, float)): return float(v)
        m = re.search(r'[\d,]+\.?\d*', str(v).replace(',', ''))
        return float(m.group()) if m else None

    items = data if isinstance(data, list) else []

    for opp in items:
        symbol = str(opp.get('stock_symbol', '') or '').strip()
        if not symbol:
            skipped += 1
            continue

        best = opp.get('best_pattern', {}) or {}
        priority = str(opp.get('priority', 'MEDIUM')).upper()
        is_empire = bool(opp.get('is_empire_stock', False))

        rows.append({
            'run_date':             run_date,
            'symbol':               symbol,
            'is_empire':            is_empire,
            'pattern_type':         str(best.get('pattern_name', 'STAIRSTEP') or 'STAIRSTEP').upper(),
            'priority':             priority,
            'confidence_pct':       pct(best.get('confidence')),
            'expected_return_pct':  pct(best.get('expected_return')),
            'win_rate_pct':         pct(best.get('win_rate')),
            'action':               str(best.get('action', 'BUY') or 'BUY').upper(),
            'price_lkr':            to_num(best.get('entry_price')),
            'stop_loss_lkr':        to_num(best.get('stop_loss')),
            'target_lkr':           to_num(best.get('target_price')),
            'time_horizon':         str(best.get('time_horizon', '') or ''),
            'explanation':          str(best.get('explanation', '') or ''),
        })

    if skipped:
        print(f"[upload_manipulation] ⚠  Skipped {skipped} rows with missing stock_symbol")

    print(f"[upload_manipulation] Parsed {len(rows)} opportunities from JSON")
    return rows


# ─── Parse text report (fallback if no JSON available) ────────────────────────

def parse_v5_txt(txt_path: str, run_date: str) -> list[dict[str, Any]]:
    """Parse v5_report_YYYY-MM-DD.txt text fallback."""
    rows = []
    current_priority = 'MEDIUM'
    current_opp: dict | None = None

    with open(txt_path) as f:
        lines = f.readlines()

    for line in lines:
        line = line.rstrip()

        if 'HIGH PRIORITY' in line:
            current_priority = 'HIGH'
        elif 'MEDIUM PRIORITY' in line:
            current_priority = 'MEDIUM'
        elif 'LOW PRIORITY' in line:
            current_priority = 'LOW'

        m = re.match(r'^\d+\.\s+([\w.]+)(\s+\[EMPIRE\])?\s+-\s+(\w+)', line)
        if m and current_priority == 'HIGH':
            if current_opp:
                rows.append(current_opp)
            symbol, is_empire, pattern = m.group(1), bool(m.group(2)), m.group(3)
            current_opp = {
                'run_date': run_date, 'symbol': symbol, 'is_empire': is_empire,
                'pattern_type': pattern.upper(), 'priority': 'HIGH',
                'confidence_pct': None, 'expected_return_pct': None, 'win_rate_pct': None,
                'action': 'BUY', 'price_lkr': None, 'stop_loss_lkr': None,
                'target_lkr': None, 'time_horizon': None, 'explanation': None,
            }
            continue

        if current_opp and current_priority == 'HIGH':
            if 'Confidence:' in line:
                m2 = re.search(r'([\d.]+)%', line)
                if m2: current_opp['confidence_pct'] = float(m2.group(1))
            elif 'Expected Return:' in line:
                m2 = re.search(r'\+([\d.]+)%', line)
                if m2: current_opp['expected_return_pct'] = float(m2.group(1))
            elif 'Win Rate:' in line:
                m2 = re.search(r'([\d.]+)%', line)
                if m2: current_opp['win_rate_pct'] = float(m2.group(1))
            elif 'Action:' in line:
                m2 = re.search(r'Action:\s*(BUY|SELL|HOLD)', line)
                if m2: current_opp['action'] = m2.group(1)
            elif 'BUY at Rs' in line:
                m2 = re.search(r'Rs\s*([\d,]+\.?\d*)', line)
                if m2: current_opp['price_lkr'] = float(m2.group(1).replace(',', ''))
            elif 'Stop Loss:' in line:
                m2 = re.search(r'Rs\s*([\d,]+\.?\d*)', line)
                if m2: current_opp['stop_loss_lkr'] = float(m2.group(1).replace(',', ''))
            elif 'Target:' in line:
                m2 = re.search(r'Rs\s*([\d,]+\.?\d*)', line)
                if m2: current_opp['target_lkr'] = float(m2.group(1).replace(',', ''))
            elif 'Time Horizon:' in line:
                current_opp['time_horizon'] = line.split(':', 1)[-1].strip()
            elif 'Explanation:' in line:
                current_opp['explanation'] = line.split(':', 1)[-1].strip()

        m3 = re.match(r'^-\s+([\w.]+)(\s+\[EMPIRE\])?:\s+(\w+)\s+\(Conf:\s*([\d.]+)%.*Return:\s*\+([\d.]+)%', line)
        if m3 and current_priority in ('MEDIUM', 'LOW'):
            rows.append({
                'run_date': run_date, 'symbol': m3.group(1), 'is_empire': bool(m3.group(2)),
                'pattern_type': m3.group(3).upper(), 'priority': current_priority,
                'confidence_pct': float(m3.group(4)), 'expected_return_pct': float(m3.group(5)),
                'win_rate_pct': None, 'action': 'BUY',
                'price_lkr': None, 'stop_loss_lkr': None, 'target_lkr': None,
                'time_horizon': None, 'explanation': None,
            })

    if current_opp:
        rows.append(current_opp)

    print(f"[upload_manipulation] Parsed {len(rows)} opportunities from TXT")
    return rows


# ─── Supabase upsert via common.database ─────────────────────────────────────

def upload_to_supabase(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        print("[upload_manipulation] No rows to upload")
        return True

    try:
        sb = get_supabase_client()   # ← singleton from common.database

        batch_size = 100
        total = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            sb.table('cse_manipulation_signals') \
              .upsert(batch, on_conflict='run_date,symbol') \
              .execute()
            total += len(batch)
            print(f"[upload_manipulation] Upserted batch {i//batch_size + 1}: {len(batch)} rows")

        print(f"[upload_manipulation] ✅ Upload complete: {total} rows → cse_manipulation_signals")
        return True

    except Exception as e:
        print(f"[upload_manipulation] ❌ ERROR: {e}")
        return False


# ─── Entry points ─────────────────────────────────────────────────────────────

def upload_signals_from_json(json_path: str, run_date: str | None = None) -> bool:
    run_date = run_date or date.today().isoformat()
    rows = parse_v5_json(json_path, run_date)
    return upload_to_supabase(rows)


def upload_signals_from_txt(txt_path: str, run_date: str | None = None) -> bool:
    run_date = run_date or date.today().isoformat()
    rows = parse_v5_txt(txt_path, run_date)
    return upload_to_supabase(rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload v5 manipulation signals to Supabase')
    parser.add_argument('--report', required=True, help='Path to v5_report_YYYY-MM-DD.json (or .txt)')
    parser.add_argument('--date', default=date.today().isoformat(), help='Run date YYYY-MM-DD')
    args = parser.parse_args()

    if args.report.endswith('.json'):
        success = upload_signals_from_json(args.report, args.date)
    else:
        success = upload_signals_from_txt(args.report, args.date)

    sys.exit(0 if success else 1)
