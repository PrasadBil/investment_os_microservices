#!/usr/bin/env python3
"""
TEA SECTORAL FEATURE GENERATOR — SUPABASE-NATIVE
Generates 16 agricultural + inflation features from live CBSL weekly real sector data

FILE: tea_sectoral_feature_generator.py
CREATED: 2026-02-21
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-02-21  Initial creation — adapted from old tea_sectoral_credit_feature_generator.py
                         Data source: cbsl_weekly_real_sector
                         (was: cbsl_monetary_68_sectoral_credit + cbsl_real_02_tea_production
                               + cbsl_external_33_tea_exports — three old monolithic tables)
                         Key upgrade: inflation (NCPI/CCPI), IIP, PMI co-located in real
                         sector table → sector scoring with macro context in one pass

WHY THIS DATA SOURCE IS BETTER:
    Old: 3 separate old CBSL tables (credit, production, exports) → complex merge, JSON parsing
    New: cbsl_weekly_real_sector → tea_production_mn_kg + inflation + IIP + PMI + oil
         → Real sector co-movement in one table, weekly cadence

THE SECRET WEAPON (preserved from old generator):
    Tea production → Tea company revenue (LIPTON, KELANI TEA, HAPUGASTENNA)
    Production lag 4-13 weeks → revenue recognition → stock price
    Tea credit expansion → production capacity investment → forward earnings
    Tea = biggest Sri Lanka export earner → strong LKR impact channel

    BUT we now add the inflation-to-consumer signal:
    NCPI YoY rising → real purchasing power erosion → defensive rotation
    CCPI rising sharply → CBSL tightening risk → bond yields up → bank margins

OUTPUT FEATURES (16):
    Tea Production (6):
        tea_production_mn_kg        — raw tea production (millions kg)
        tea_production_wow_change   — WoW % change
        tea_production_ma4w         — 4-week MA (seasonal noise filter)
        tea_production_ma13w        — 13-week (quarterly) MA
        tea_production_yoy_approx   — 52-week YoY % change approximation
        tea_production_seasonal_idx — current / 13w MA (>1.0 = above seasonal norm)
    Agriculture Sector (2):
        rubber_production_mn_kg     — Rubber (secondary crop + industrial use)
        agri_sector_momentum        — Equal-weight MA of tea + rubber production change
    Inflation (4):
        ncpi_yoy_pct                — NCPI headline YoY % (national)
        ccpi_yoy_pct                — CCPI YoY % (Colombo = more timely)
        inflation_wow_change        — WoW change in CCPI YoY (acceleration proxy)
        real_rate_proxy             — tbill_91d_yield - ccpi_yoy (requires fiscal data or set to NaN)
    Economic Activity (2):
        pmi_manufacturing           — PMI Manufacturing (>50 = expansion)
        iip_yoy_pct                 — Index of Industrial Production YoY %
    Composite Score (2):
        tea_sector_health           — 0-100 (production momentum + seasonal position)
        real_sector_macro_score     — 0-100 (production + low inflation + PMI expansion)
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from cbsl_feature_base import CBSLFeatureBase


class TeaSectoralFeatureGenerator(CBSLFeatureBase):
    """
    16 real-sector features from cbsl_weekly_real_sector.
    Primary: tea production dynamics.
    Secondary: inflation, PMI, IIP for macro scoring context.
    """

    FEATURE_NAMES = [
        # Tea production
        'tea_production_mn_kg', 'tea_production_wow_change',
        'tea_production_ma4w', 'tea_production_ma13w',
        'tea_production_yoy_approx', 'tea_production_seasonal_idx',
        # Agriculture sector
        'rubber_production_mn_kg', 'agri_sector_momentum',
        # Inflation
        'ncpi_yoy_pct', 'ccpi_yoy_pct',
        'inflation_wow_change', 'real_rate_proxy',
        # Economic activity
        'pmi_manufacturing', 'iip_yoy_pct',
        # Composite
        'tea_sector_health', 'real_sector_macro_score',
    ]

    def generate_features(self) -> pd.DataFrame:
        self.log("=" * 70)
        self.log("TEA SECTORAL FEATURE GENERATOR  v1.0.0")
        self.log(f"Window: {self.weeks} weeks")
        self.log("=" * 70)

        # ── Load ──────────────────────────────────────────────────────────
        self.log("\n[1/5] Loading cbsl_weekly_real_sector ...")
        raw = self.query_weekly(self.TABLE_WEEKLY_REAL)

        if raw.empty:
            self.log("ERROR: No real sector data.", 'error')
            return pd.DataFrame()

        df = raw.copy()

        # ── Tea production ─────────────────────────────────────────────────
        self.log("\n[2/5] Computing tea production features ...")
        tea = df['tea_production_mn_kg']

        df['tea_production_mn_kg']      = tea
        df['tea_production_wow_change'] = self.pct_change(tea, 1)
        df['tea_production_ma4w']       = self.ma(tea, 4)
        df['tea_production_ma13w']      = self.ma(tea, 13)
        # YoY approximation via 52-week lookback
        df['tea_production_yoy_approx'] = self.pct_change(tea, 52)
        # Seasonal index: current / 13w MA — >1.0 = above seasonal norm
        ma13 = self.ma(tea, 13)
        df['tea_production_seasonal_idx'] = (tea / ma13.replace(0, np.nan)).round(3)

        # ── Agriculture sector ─────────────────────────────────────────────
        self.log("\n[3/5] Computing agriculture sector signals ...")
        rubber = df.get('rubber_production_mn_kg', pd.Series(np.nan, index=df.index))
        df['rubber_production_mn_kg'] = rubber

        # Equal-weight momentum: average of tea + rubber WoW changes
        tea_wow   = df['tea_production_wow_change']
        rubber_wow= self.pct_change(rubber, 1)
        agri_components = [s for s in [tea_wow, rubber_wow] if s.notna().sum() > 0]
        if agri_components:
            df['agri_sector_momentum'] = pd.concat(agri_components, axis=1).mean(axis=1)
        else:
            df['agri_sector_momentum'] = np.nan

        # ── Inflation ──────────────────────────────────────────────────────
        self.log("\n[4/5] Computing inflation & activity features ...")
        ncpi = df.get('ncpi_yoy_pct', pd.Series(np.nan, index=df.index))
        ccpi = df.get('ccpi_yoy_pct', pd.Series(np.nan, index=df.index))

        df['ncpi_yoy_pct']        = ncpi
        df['ccpi_yoy_pct']        = ccpi
        df['inflation_wow_change'] = self.momentum(ccpi, 1)

        # Real rate proxy: if we have CCPI we can approximate
        # Full real rate requires fiscal tbill data — set NaN here; master builder fills it
        df['real_rate_proxy'] = np.nan   # filled by master_feature_builder

        # ── Economic activity ──────────────────────────────────────────────
        pmi   = df.get('pmi_manufacturing', pd.Series(np.nan, index=df.index))
        iip   = df.get('iip_yoy_pct', pd.Series(np.nan, index=df.index))
        df['pmi_manufacturing'] = pmi
        df['iip_yoy_pct']       = iip

        # ── Tea sector health score ────────────────────────────────────────
        # Components: production momentum + seasonal position
        prod_score = self.normalise_0_100(tea)                         # high prod = good
        seas_score = self.normalise_0_100(df['tea_production_seasonal_idx'])  # above seasonal = good
        df['tea_sector_health'] = ((prod_score + seas_score) / 2).round(1)

        # ── Real sector macro score ────────────────────────────────────────
        # High production + low inflation + PMI >50 = real economy tailwind
        prod_component = self.normalise_0_100(tea)
        # Low inflation = good → invert normalised inflation
        infl_component = 100 - self.normalise_0_100(ccpi.fillna(ccpi.median()))
        # PMI > 50 = expansion → normalise around 50
        pmi_clean = pmi.fillna(50)
        pmi_component = self.normalise_0_100(pmi_clean)

        macro_components = [c for c in [prod_component, infl_component, pmi_component]
                            if c.notna().sum() > 0]
        if macro_components:
            df['real_sector_macro_score'] = (
                pd.concat(macro_components, axis=1).mean(axis=1).round(1)
            )
        else:
            df['real_sector_macro_score'] = 50.0

        # ── Validate & save ───────────────────────────────────────────────
        self.log("\n[5/5] Validating & saving ...")
        out_df = df[['week_ending'] + self.FEATURE_NAMES].rename(
            columns={'week_ending': 'date'}
        )
        self.validate_features(out_df, self.FEATURE_NAMES)
        self.save_features(out_df, 'cbsl_features_tea_sectoral.csv')
        self.feature_names = self.FEATURE_NAMES

        self.log(f"\n✅ TEA SECTORAL COMPLETE  |  {len(self.FEATURE_NAMES)} features  |  {len(out_df)} rows")
        return out_df


def main():
    parser = argparse.ArgumentParser(
        description='Generate tea sectoral features from cbsl_weekly_real_sector'
    )
    parser.add_argument('--weeks', type=int, default=104)
    parser.add_argument('--output-dir', default=None)
    args = parser.parse_args()

    gen = TeaSectoralFeatureGenerator(weeks=args.weeks)
    if args.output_dir:
        gen.output_dir = Path(args.output_dir)

    df = gen.generate_features()
    if df.empty:
        print("❌ No features generated")
        sys.exit(1)

    print(f"\n✅ SUCCESS  |  {len(gen.FEATURE_NAMES)} features  |  {len(df)} rows")


if __name__ == '__main__':
    main()
