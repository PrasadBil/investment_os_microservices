#!/usr/bin/env python3
"""
REMITTANCES FEATURE EXTRACTOR — SUPABASE-NATIVE
Generates 14 external-sector features focused on remittances + FX reserves

FILE: remittances_feature_extractor.py
CREATED: 2026-02-21
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-02-21  Initial creation — adapted from old remittances_feature_extractor.py
                         Data source: cbsl_weekly_external_sector
                         (was: cbsl_external_workers_remittances — single-table monthly)
                         Key upgrade: trade balance, reserves, FX rate all co-located in
                         same weekly table → external pressure composite is now far richer

WHY THIS DATA SOURCE IS BETTER:
    Old: cbsl_external_workers_remittances — monthly table, single metric
    New: cbsl_weekly_external_sector — weekly, includes:
         remittances + reserves + trade balance + current account + USD/LKR indicative
         → Full external sector picture in one query

THE SIGNAL THESIS:
    Remittances → consumer disposable income → CSE retail flows
    High remittances + rising reserves + improving trade = external tailwind
    Remittances acceleration → Consumer staples / finance sector outperformance
    Remittances shock (YoY negative) → consumer discretionary headwind

OUTPUT FEATURES (14):
    Remittances Core (5):
        remittances_usd_mn          — raw Workers' Remittances (USD mn)
        remittances_wow_change_pct  — WoW % change
        remittances_4w_avg          — 4-week moving average (smooths noise)
        remittances_13w_avg         — 13-week (quarterly) moving average
        remittances_yoy_approx      — 52-week lookback YoY % change (approx)
    Consumer Signal (3):
        consumer_capacity_index     — normalised 0-100 (high = high consumer income)
        remittances_accelerating    — +1 if WoW momentum is increasing, else 0
        remittances_regime          — MA-crossover regime (+1/-1/0)
    External Sector (4):
        gross_reserves_usd_bn       — Official forex reserves (USD bn)
        reserves_wow_change         — WoW absolute change in reserves
        trade_balance_usd_mn        — Exports - Imports (negative = deficit)
        current_account_usd_mn      — Current account balance
    Composite Score (2):
        external_pressure_score     — 0-100 stress score (high = external pressure)
                                      inverse of: remittances + reserves + trade balance
        external_tailwind_score     — 0-100 tailwind (inverse of stress score)
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from cbsl_feature_base import CBSLFeatureBase


class RemittancesFeatureExtractor(CBSLFeatureBase):
    """
    14 external-sector features from cbsl_weekly_external_sector.
    Core signal: remittances as consumer income proxy + external balance health.
    """

    FEATURE_NAMES = [
        # Remittances core
        'remittances_usd_mn', 'remittances_wow_change_pct',
        'remittances_4w_avg', 'remittances_13w_avg', 'remittances_yoy_approx',
        # Consumer signal
        'consumer_capacity_index', 'remittances_accelerating', 'remittances_regime',
        # External sector
        'gross_reserves_usd_bn', 'reserves_wow_change',
        'trade_balance_usd_mn', 'current_account_usd_mn',
        # Composite
        'external_pressure_score', 'external_tailwind_score',
    ]

    def generate_features(self) -> pd.DataFrame:
        self.log("=" * 70)
        self.log("REMITTANCES FEATURE EXTRACTOR  v1.0.0")
        self.log(f"Window: {self.weeks} weeks")
        self.log("=" * 70)

        # ── Load ──────────────────────────────────────────────────────────
        self.log("\n[1/5] Loading cbsl_weekly_external_sector ...")
        # Need 52 extra weeks for the YoY approximation
        since_extended = None
        if self.weeks < 60:
            since_extended = None  # use default
        raw = self.query_weekly(self.TABLE_WEEKLY_EXTERNAL)

        if raw.empty:
            self.log("ERROR: No external sector data.", 'error')
            return pd.DataFrame()

        df = raw.copy()

        # ── Remittances core ───────────────────────────────────────────────
        self.log("\n[2/5] Computing remittances features ...")
        r = df['workers_remittances_usd_mn']

        df['remittances_usd_mn']         = r
        df['remittances_wow_change_pct'] = self.pct_change(r, 1)
        df['remittances_4w_avg']         = self.ma(r, 4)
        df['remittances_13w_avg']        = self.ma(r, 13)
        # YoY approximation: % change vs 52 weeks ago
        df['remittances_yoy_approx']     = self.pct_change(r, 52)

        # ── Consumer signal ────────────────────────────────────────────────
        self.log("\n[3/5] Computing consumer capacity signals ...")
        df['consumer_capacity_index'] = self.normalise_0_100(r)
        # Accelerating = WoW momentum is improving (positive derivative of WoW change)
        wow = df['remittances_wow_change_pct']
        df['remittances_accelerating'] = (wow.diff() > 0).astype(int)
        df['remittances_regime']       = self.regime(r, fast=4, slow=13)

        # ── External sector context ────────────────────────────────────────
        self.log("\n[4/5] Adding external sector context ...")
        res = df['gross_official_reserves_usd_bn']
        df['gross_reserves_usd_bn'] = res
        df['reserves_wow_change']   = self.momentum(res, 1)

        tb = df.get('trade_balance_usd_mn', pd.Series(np.nan, index=df.index))
        ca = df.get('current_account_usd_mn', pd.Series(np.nan, index=df.index))

        # trade_balance may need computing if not stored
        if tb.isna().all() and 'exports_usd_mn' in df.columns and 'imports_usd_mn' in df.columns:
            tb = df['exports_usd_mn'] - df['imports_usd_mn']

        df['trade_balance_usd_mn']  = tb
        df['current_account_usd_mn']= ca

        # ── External pressure composite ────────────────────────────────────
        # Three components driving LKR pressure:
        #   1. Low remittances → less FX inflow → pressure ↑
        #   2. Low reserves → buffer depleted → pressure ↑
        #   3. Negative trade balance → structural outflow → pressure ↑
        c1_stress = 100 - self.normalise_0_100(r)        # low remit = high stress
        c2_stress = 100 - self.normalise_0_100(res)      # low reserves = high stress
        trade_norm = tb.fillna(0)
        c3_stress = 100 - self.normalise_0_100(trade_norm)  # negative TB = high stress

        df['external_pressure_score']  = ((c1_stress + c2_stress + c3_stress) / 3).round(1)
        df['external_tailwind_score']  = (100 - df['external_pressure_score']).round(1)

        # ── Validate & save ───────────────────────────────────────────────
        self.log("\n[5/5] Validating & saving ...")
        out_df = df[['week_ending'] + self.FEATURE_NAMES].rename(
            columns={'week_ending': 'date'}
        )
        self.validate_features(out_df, self.FEATURE_NAMES)
        self.save_features(out_df, 'cbsl_features_remittances.csv')
        self.feature_names = self.FEATURE_NAMES

        self.log(f"\n✅ REMITTANCES COMPLETE  |  {len(self.FEATURE_NAMES)} features  |  {len(out_df)} rows")
        return out_df


def main():
    parser = argparse.ArgumentParser(description='Generate remittances features from cbsl_weekly_external_sector')
    parser.add_argument('--weeks', type=int, default=104)
    parser.add_argument('--output-dir', default=None)
    args = parser.parse_args()

    gen = RemittancesFeatureExtractor(weeks=args.weeks)
    if args.output_dir:
        gen.output_dir = Path(args.output_dir)

    df = gen.generate_features()
    if df.empty:
        print("❌ No features generated")
        sys.exit(1)

    print(f"\n✅ SUCCESS  |  {len(gen.FEATURE_NAMES)} features  |  {len(df)} rows")


if __name__ == '__main__':
    main()
