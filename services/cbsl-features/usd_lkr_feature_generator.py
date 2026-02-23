#!/usr/bin/env python3
"""
USD/LKR FEATURE GENERATOR — SUPABASE-NATIVE
Generates 22 FX features from live CBSL data

FILE: usd_lkr_feature_generator.py
CREATED: 2026-02-21
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-02-21  Initial creation — adapted from old usd_lkr_feature_generator.py
                         Data source: cbsl_daily_indicators (was: cbsl_external_32_usd_lkr_exchange_rate)
                         Key upgrade: daily buy/sell spread + EUR/GBP cross rates now included

WHY THIS DATA SOURCE IS BETTER:
    Old: cbsl_external_32_usd_lkr_exchange_rate (monthly CBSL report data)
         → Coarse, lagged, required JSON parsing of data_json
    New: cbsl_daily_indicators (live daily PDF collected each weekday at 5:30 PM SLK)
         → Daily resolution, typed columns, buy/sell spread, 3 currency pairs

OUTPUT FEATURES (22):
    FX Levels (5):
        usd_lkr_mid            — (buy+sell)/2 midpoint rate
        usd_lkr_spread         — sell - buy (liquidity/stress proxy)
        eur_lkr_mid            — EUR/LKR midpoint
        gbp_lkr_mid            — GBP/LKR midpoint
        usd_lkr_eur_cross      — USD/EUR implied rate (usd_lkr / eur_lkr)
    Returns / Momentum (7):
        usd_lkr_change_1d      — 1-day % change
        usd_lkr_change_5d      — 1-week % change
        usd_lkr_change_20d     — 1-month % change
        usd_lkr_momentum_5d    — absolute momentum (current - 5d ago)
        usd_lkr_momentum_20d   — absolute momentum (current - 20d ago)
        usd_lkr_rsi_14d        — RSI(14)
        usd_lkr_regime         — MA crossover regime (+1/-1/0)
    Moving Averages (3):
        usd_lkr_ma7
        usd_lkr_ma20
        usd_lkr_ma60
    Volatility (3):
        usd_lkr_volatility_7d
        usd_lkr_volatility_20d
        usd_lkr_volatility_60d
    Normalised / Lags (4):
        usd_lkr_zscore_60d     — 60-day z-score (distance from recent mean)
        usd_lkr_lag_1d
        usd_lkr_lag_5d
        usd_lkr_lag_20d
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from cbsl_feature_base import CBSLFeatureBase


class USDLKRFeatureGenerator(CBSLFeatureBase):
    """
    22 FX features from cbsl_daily_indicators.
    Primary signal: USD/LKR exchange rate dynamics.
    Secondary signals: EUR/LKR, GBP/LKR, spread.
    """

    FEATURE_NAMES = [
        # Levels
        'usd_lkr_mid', 'usd_lkr_spread', 'eur_lkr_mid', 'gbp_lkr_mid', 'usd_lkr_eur_cross',
        # Returns / momentum
        'usd_lkr_change_1d', 'usd_lkr_change_5d', 'usd_lkr_change_20d',
        'usd_lkr_momentum_5d', 'usd_lkr_momentum_20d',
        'usd_lkr_rsi_14d', 'usd_lkr_regime',
        # MAs
        'usd_lkr_ma7', 'usd_lkr_ma20', 'usd_lkr_ma60',
        # Volatility
        'usd_lkr_volatility_7d', 'usd_lkr_volatility_20d', 'usd_lkr_volatility_60d',
        # Normalised / lags
        'usd_lkr_zscore_60d',
        'usd_lkr_lag_1d', 'usd_lkr_lag_5d', 'usd_lkr_lag_20d',
    ]

    def generate_features(self) -> pd.DataFrame:
        self.log("=" * 70)
        self.log("USD/LKR FEATURE GENERATOR  v1.0.0")
        self.log(f"Window: {self.weeks} weeks")
        self.log("=" * 70)

        # ── Load ──────────────────────────────────────────────────────────
        self.log("\n[1/5] Loading cbsl_daily_indicators ...")
        raw = self.query_daily()

        if raw.empty:
            self.log("ERROR: No daily indicator data. Is cron running?", 'error')
            return pd.DataFrame()

        df = raw[['date', 'usd_tt_buy', 'usd_tt_sell',
                  'eur_tt_buy', 'eur_tt_sell',
                  'gbp_tt_buy', 'gbp_tt_sell']].copy()

        # ── Derived base series ───────────────────────────────────────────
        self.log("\n[2/5] Computing FX midpoints & spread ...")
        df['usd_lkr_mid']       = (df['usd_tt_buy'] + df['usd_tt_sell']) / 2
        df['usd_lkr_spread']    = df['usd_tt_sell'] - df['usd_tt_buy']
        df['eur_lkr_mid']       = (df['eur_tt_buy'] + df['eur_tt_sell']) / 2
        df['gbp_lkr_mid']       = (df['gbp_tt_buy'] + df['gbp_tt_sell']) / 2
        df['usd_lkr_eur_cross'] = (df['usd_lkr_mid'] / df['eur_lkr_mid']).round(4)

        s = df['usd_lkr_mid']   # primary series shorthand

        # ── Returns & momentum ───────────────────────────────────────────
        self.log("\n[3/5] Computing returns, momentum, RSI, regime ...")
        df['usd_lkr_change_1d']   = self.pct_change(s, 1)
        df['usd_lkr_change_5d']   = self.pct_change(s, 5)
        df['usd_lkr_change_20d']  = self.pct_change(s, 20)
        df['usd_lkr_momentum_5d'] = self.momentum(s, 5)
        df['usd_lkr_momentum_20d']= self.momentum(s, 20)
        df['usd_lkr_rsi_14d']     = self.rsi(s, 14)
        df['usd_lkr_regime']      = self.regime(s, fast=7, slow=20)

        # ── Moving averages ───────────────────────────────────────────────
        self.log("\n[4/5] Computing moving averages & volatility ...")
        df['usd_lkr_ma7']  = self.ma(s, 7)
        df['usd_lkr_ma20'] = self.ma(s, 20)
        df['usd_lkr_ma60'] = self.ma(s, 60)

        df['usd_lkr_volatility_7d']  = self.volatility(s, 7)
        df['usd_lkr_volatility_20d'] = self.volatility(s, 20)
        df['usd_lkr_volatility_60d'] = self.volatility(s, 60)

        df['usd_lkr_zscore_60d'] = self.zscore(s, 60)

        df['usd_lkr_lag_1d']  = self.lag(s, 1)
        df['usd_lkr_lag_5d']  = self.lag(s, 5)
        df['usd_lkr_lag_20d'] = self.lag(s, 20)

        # ── Validate & save ───────────────────────────────────────────────
        self.log("\n[5/5] Validating & saving ...")
        out_df = df[['date'] + self.FEATURE_NAMES]
        self.validate_features(out_df, self.FEATURE_NAMES)
        self.save_features(out_df, 'cbsl_features_usd_lkr.csv')
        self.feature_names = self.FEATURE_NAMES

        self.log(f"\n✅ USD/LKR COMPLETE  |  {len(self.FEATURE_NAMES)} features  |  {len(out_df)} rows")
        return out_df


def main():
    parser = argparse.ArgumentParser(description='Generate USD/LKR features from cbsl_daily_indicators')
    parser.add_argument('--weeks', type=int, default=104,
                        help='History window in weeks (default: 104 = 2 years)')
    parser.add_argument('--output-dir', default=None,
                        help='Output directory override')
    args = parser.parse_args()

    gen = USDLKRFeatureGenerator(weeks=args.weeks)
    if args.output_dir:
        gen.output_dir = Path(args.output_dir)

    df = gen.generate_features()

    if df.empty:
        print("❌ No features generated — check Supabase connection and data")
        sys.exit(1)

    print(f"\n✅ SUCCESS  |  {len(gen.FEATURE_NAMES)} features  |  {len(df)} rows")
    print(f"   Date range: {df['date'].min().date()} – {df['date'].max().date()}")


if __name__ == '__main__':
    main()
