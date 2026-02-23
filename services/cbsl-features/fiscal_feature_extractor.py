#!/usr/bin/env python3
"""
FISCAL FEATURE EXTRACTOR — SUPABASE-NATIVE
Generates 15 fiscal/monetary-market features from live CBSL weekly data

FILE: fiscal_feature_extractor.py
CREATED: 2026-02-21
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-02-21  Initial creation — adapted from old fiscal_feature_extractor.py
                         Data source: cbsl_weekly_fiscal_sector (was: cbsl_fiscal_budget_deficit)
                         Key upgrade: T-bill market microstructure signals added (subscription
                         ratio, foreign flows, yield curve slope) — far richer than deficit alone

WHY THIS DATA SOURCE IS BETTER:
    Old: cbsl_fiscal_budget_deficit → monthly deficit (single number, lagged)
    New: cbsl_weekly_fiscal_sector → weekly T-bill/T-bond auction results, yields,
         foreign holdings, subscription demand — leading indicators of fiscal stress

SIGNALS THIS UNLOCKS:
    • Yield curve inversion → credit tightening pre-signal
    • T-bill subscription < 1.0 → government funding stress
    • Foreign holdings decline → capital flight early warning
    • 364d yield spike → market pricing sovereign risk premium
    • Secondary market volume → institutional positioning in rates

OUTPUT FEATURES (15):
    Yield Levels (3):
        tbill_91d_yield        — 91-day T-bill yield (%)
        tbill_182d_yield       — 182-day T-bill yield (%)
        tbill_364d_yield       — 364-day T-bill yield (%)
    Yield Dynamics (5):
        yield_curve_slope      — 364d - 91d spread (bps converted to %)
        tbill_91d_wow_change   — WoW absolute change in 91d yield
        tbill_364d_wow_change  — WoW absolute change in 364d yield
        tbill_364d_ma4w        — 4-week MA of 364d yield
        tbill_364d_zscore_13w  — 13-week z-score (regime detector)
    Auction Demand (3):
        tbill_subscription_ratio  — Accepted / Offered (demand signal)
        tbond_subscription_ratio  — T-bond demand
        tbill_demand_deficit      — 1 - subscription ratio (stress signal; >0 = under-subscribed)
    Foreign Flows (2):
        foreign_holdings_bn         — Total foreign T-bill/T-bond holdings (LKR bn)
        foreign_holdings_wow_change — WoW absolute change (capital flow signal)
    Composite Score (2):
        fiscal_stress_score    — 0-100 composite (high yields + low subscription + falling foreign)
        yield_regime           — MA-crossover regime on 364d yield (+1 rising / -1 falling / 0 flat)
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from cbsl_feature_base import CBSLFeatureBase


class FiscalFeatureExtractor(CBSLFeatureBase):
    """
    15 fiscal market features from cbsl_weekly_fiscal_sector.
    Focuses on T-bill market microstructure as leading fiscal signal.
    """

    FEATURE_NAMES = [
        # Yield levels
        'tbill_91d_yield', 'tbill_182d_yield', 'tbill_364d_yield',
        # Yield dynamics
        'yield_curve_slope', 'tbill_91d_wow_change', 'tbill_364d_wow_change',
        'tbill_364d_ma4w', 'tbill_364d_zscore_13w',
        # Auction demand
        'tbill_subscription_ratio', 'tbond_subscription_ratio', 'tbill_demand_deficit',
        # Foreign flows
        'foreign_holdings_bn', 'foreign_holdings_wow_change',
        # Composite
        'fiscal_stress_score', 'yield_regime',
    ]

    def generate_features(self) -> pd.DataFrame:
        self.log("=" * 70)
        self.log("FISCAL FEATURE EXTRACTOR  v1.0.0")
        self.log(f"Window: {self.weeks} weeks")
        self.log("=" * 70)

        # ── Load ──────────────────────────────────────────────────────────
        self.log("\n[1/5] Loading cbsl_weekly_fiscal_sector ...")
        raw = self.query_weekly(self.TABLE_WEEKLY_FISCAL)

        if raw.empty:
            self.log("ERROR: No fiscal data. Has the weekly CBSL parser run?", 'error')
            return pd.DataFrame()

        df = raw.copy()

        # ── Yield levels ──────────────────────────────────────────────────
        self.log("\n[2/5] Extracting yield levels ...")
        df['tbill_91d_yield']  = df['tbill_91d_yield']
        df['tbill_182d_yield'] = df['tbill_182d_yield']
        df['tbill_364d_yield'] = df['tbill_364d_yield']

        # ── Yield dynamics ────────────────────────────────────────────────
        self.log("\n[3/5] Computing yield dynamics ...")
        df['yield_curve_slope']    = df['tbill_364d_yield'] - df['tbill_91d_yield']
        df['tbill_91d_wow_change'] = self.momentum(df['tbill_91d_yield'], 1)
        df['tbill_364d_wow_change']= self.momentum(df['tbill_364d_yield'], 1)
        df['tbill_364d_ma4w']      = self.ma(df['tbill_364d_yield'], 4)
        df['tbill_364d_zscore_13w']= self.zscore(df['tbill_364d_yield'], 13)

        # ── Auction demand ────────────────────────────────────────────────
        self.log("\n[4/5] Computing auction demand signals ...")
        df['tbill_subscription_ratio'] = df['tbill_subscription_ratio']
        df['tbond_subscription_ratio'] = df['tbond_subscription_ratio']
        # Demand deficit: >0 means under-subscribed (fiscal stress)
        df['tbill_demand_deficit'] = (1 - df['tbill_subscription_ratio']).clip(lower=0)

        # ── Foreign flows ─────────────────────────────────────────────────
        df['foreign_holdings_bn']         = df['total_foreign_holdings_bn']
        df['foreign_holdings_wow_change']  = self.momentum(df['total_foreign_holdings_bn'], 1)

        # ── Composite stress score ────────────────────────────────────────
        # Three components (equally weighted):
        #   1. 364d yield normalised → high yield = stress
        #   2. 1 - subscription ratio normalised → under-subscription = stress
        #   3. Declining foreign holdings normalised → outflow = stress
        c1 = self.normalise_0_100(df['tbill_364d_yield'])
        c2 = self.normalise_0_100(df['tbill_demand_deficit'])
        # Declining foreign = stress → invert after normalise
        c3 = 100 - self.normalise_0_100(df['foreign_holdings_bn'])

        df['fiscal_stress_score'] = ((c1 + c2 + c3) / 3).round(1)

        # ── Regime ───────────────────────────────────────────────────────
        df['yield_regime'] = self.regime(df['tbill_364d_yield'], fast=4, slow=13)

        # ── Validate & save ───────────────────────────────────────────────
        self.log("\n[5/5] Validating & saving ...")
        out_df = df[['week_ending'] + self.FEATURE_NAMES].rename(
            columns={'week_ending': 'date'}
        )
        self.validate_features(out_df, self.FEATURE_NAMES)
        self.save_features(out_df, 'cbsl_features_fiscal.csv')
        self.feature_names = self.FEATURE_NAMES

        self.log(f"\n✅ FISCAL COMPLETE  |  {len(self.FEATURE_NAMES)} features  |  {len(out_df)} rows")
        return out_df


def main():
    parser = argparse.ArgumentParser(description='Generate fiscal features from cbsl_weekly_fiscal_sector')
    parser.add_argument('--weeks', type=int, default=104,
                        help='History window in weeks (default: 104)')
    parser.add_argument('--output-dir', default=None)
    args = parser.parse_args()

    gen = FiscalFeatureExtractor(weeks=args.weeks)
    if args.output_dir:
        gen.output_dir = Path(args.output_dir)

    df = gen.generate_features()
    if df.empty:
        print("❌ No features generated")
        sys.exit(1)

    print(f"\n✅ SUCCESS  |  {len(gen.FEATURE_NAMES)} features  |  {len(df)} rows")


if __name__ == '__main__':
    main()
