
#!/usr/bin/env python3
"""
CBSL MACRO FEATURE MASTER BUILDER
Orchestrates all 4 CBSL feature generators, merges output, writes to Supabase
+ dated CSV archive

FILE: cbsl_master_feature_builder.py
CREATED: 2026-02-21
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-02-21  Initial creation — replaces old composite_feature_builder.py
                         + master_feature_combiner.py (Excel-based, /opt/selenium_automation)
                         Generators: USD/LKR (22) + Fiscal (15) + Remittances (14) + Tea (16) = 67 features
                         Output: daily-aligned DataFrame → cbsl_macro_features Supabase table + CSV

ARCHITECTURE:
    Run generators in sequence (shared Supabase client):
        1. USDLKRFeatureGenerator     → daily   → 22 features
        2. FiscalFeatureExtractor     → weekly  → 15 features (aligned to daily)
        3. RemittancesFeatureExtractor→ weekly  → 14 features (aligned to daily)
        4. TeaSectoralFeatureGenerator→ weekly  → 16 features (aligned to daily)

    Cross-generator feature (requires data from 2 generators):
        real_rate_proxy = tbill_91d_yield (fiscal) - ccpi_yoy_pct (tea/real)

    Merge strategy: daily USD/LKR is the spine.
        Weekly features are backward-filled to daily via align_to_daily().
        Final join key: 'date' (trading date).

    Output:
        a) CSV: output/cbsl_features/MASTER_CBSL_FEATURES_YYYYMMDD.csv  (archive)
        b) CSV: output/cbsl_features/MASTER_CBSL_FEATURES_LATEST.csv    (symlink-style)
        c) Supabase: cbsl_macro_features table (upsert on date)

SUPABASE TABLE SCHEMA (auto-created if missing):
    CREATE TABLE IF NOT EXISTS cbsl_macro_features (
        date            DATE        PRIMARY KEY,
        built_at        TIMESTAMPTZ DEFAULT NOW(),
        -- USD/LKR features (22 columns)
        usd_lkr_mid     DECIMAL, ... etc
        -- Fiscal features (15 columns)
        tbill_91d_yield DECIMAL, ... etc
        -- Remittances features (14 columns)
        remittances_usd_mn DECIMAL, ... etc
        -- Tea features (16 columns)
        tea_production_mn_kg DECIMAL, ... etc
        -- Cross-generator
        real_rate_proxy DECIMAL
    );

USAGE:
    python3 cbsl_master_feature_builder.py                     # build last 104 weeks
    python3 cbsl_master_feature_builder.py --weeks 260         # 5 years
    python3 cbsl_master_feature_builder.py --no-upload         # skip Supabase upload
    python3 cbsl_master_feature_builder.py --dry-run           # validate only, no writes
"""

import argparse
import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from cbsl_feature_base import CBSLFeatureBase, _get_client
from usd_lkr_feature_generator import USDLKRFeatureGenerator
from fiscal_feature_extractor import FiscalFeatureExtractor
from remittances_feature_extractor import RemittancesFeatureExtractor
from tea_sectoral_feature_generator import TeaSectoralFeatureGenerator


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

SUPABASE_TABLE = 'cbsl_macro_features'

# Total expected features (22 + 15 + 14 + 16 = 67)
# Note: real_rate_proxy is already counted inside Tea's 16 features;
# the master builder fills it via cross-generator logic, but it is not a +1 extra.
EXPECTED_FEATURE_COUNT = 67

# Metadata columns excluded from feature count
META_COLS = {'date', 'built_at'}


# ─────────────────────────────────────────────────────────────────────────────
# MASTER BUILDER
# ─────────────────────────────────────────────────────────────────────────────

class CBSLMasterFeatureBuilder(CBSLFeatureBase):
    """
    Orchestrates all CBSL feature generators and produces a single
    merged daily-aligned feature DataFrame.
    """

    def __init__(self, weeks: int = 104, verbose: bool = True,
                 upload: bool = True, dry_run: bool = False):
        super().__init__(weeks=weeks, verbose=verbose)
        self.upload   = upload
        self.dry_run  = dry_run
        self.run_date = datetime.now().strftime('%Y-%m-%d')
        self.run_ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ─────────────────────────────────────────────────────────────────────
    # RUN GENERATORS
    # ─────────────────────────────────────────────────────────────────────

    def _run_usd_lkr(self) -> pd.DataFrame:
        self.log("\n" + "─" * 60)
        self.log("GENERATOR 1/4: USD/LKR  (22 features, daily)")
        self.log("─" * 60)
        gen = USDLKRFeatureGenerator(weeks=self.weeks, verbose=self.verbose)
        gen.output_dir = self.output_dir
        df = gen.generate_features()
        if df.empty:
            self.log("WARNING: USD/LKR generator returned empty DataFrame", 'warning')
        return df

    def _run_fiscal(self) -> pd.DataFrame:
        self.log("\n" + "─" * 60)
        self.log("GENERATOR 2/4: FISCAL  (15 features, weekly → daily)")
        self.log("─" * 60)
        gen = FiscalFeatureExtractor(weeks=self.weeks, verbose=self.verbose)
        gen.output_dir = self.output_dir
        df = gen.generate_features()
        if df.empty:
            self.log("WARNING: Fiscal generator returned empty DataFrame", 'warning')
            return df
        return self.align_to_daily(df, date_col='date')

    def _run_remittances(self) -> pd.DataFrame:
        self.log("\n" + "─" * 60)
        self.log("GENERATOR 3/4: REMITTANCES  (14 features, weekly → daily)")
        self.log("─" * 60)
        gen = RemittancesFeatureExtractor(weeks=self.weeks, verbose=self.verbose)
        gen.output_dir = self.output_dir
        df = gen.generate_features()
        if df.empty:
            self.log("WARNING: Remittances generator returned empty DataFrame", 'warning')
            return df
        return self.align_to_daily(df, date_col='date')

    def _run_tea(self) -> pd.DataFrame:
        self.log("\n" + "─" * 60)
        self.log("GENERATOR 4/4: TEA SECTORAL  (16 features, weekly → daily)")
        self.log("─" * 60)
        gen = TeaSectoralFeatureGenerator(weeks=self.weeks, verbose=self.verbose)
        gen.output_dir = self.output_dir
        df = gen.generate_features()
        if df.empty:
            self.log("WARNING: Tea sectoral generator returned empty DataFrame", 'warning')
            return df
        return self.align_to_daily(df, date_col='date')

    # ─────────────────────────────────────────────────────────────────────
    # MERGE
    # ─────────────────────────────────────────────────────────────────────

    def _merge_all(self, usd_df, fiscal_df, remit_df, tea_df) -> pd.DataFrame:
        """
        Merge all feature DataFrames on 'date'.
        Spine: usd_df (daily). All others backward-joined.
        """
        self.log("\n" + "─" * 60)
        self.log("MERGE: Joining all generators on daily date spine")
        self.log("─" * 60)

        if usd_df.empty:
            self.log("ERROR: USD/LKR is the spine — cannot merge without it", 'error')
            return pd.DataFrame()

        merged = usd_df.copy()
        merged['date'] = pd.to_datetime(merged['date'])

        def _asof_join(base: pd.DataFrame, other: pd.DataFrame, label: str) -> pd.DataFrame:
            if other.empty:
                self.log(f"  SKIP: {label} is empty", 'warning')
                return base
            other = other.copy()
            other['date'] = pd.to_datetime(other['date'])
            # Drop any columns already in base (except 'date') to avoid conflicts
            overlap = [c for c in other.columns if c in base.columns and c != 'date']
            if overlap:
                self.log(f"  Dropping overlapping columns from {label}: {overlap}")
                other = other.drop(columns=overlap)
            result = pd.merge_asof(
                base.sort_values('date'),
                other.sort_values('date'),
                on='date',
                direction='backward'
            )
            self.log(f"  ✓ Joined {label}: +{len(other.columns)-1} features")
            return result

        merged = _asof_join(merged, fiscal_df, 'Fiscal')
        merged = _asof_join(merged, remit_df, 'Remittances')
        merged = _asof_join(merged, tea_df, 'Tea Sectoral')

        merged = merged.sort_values('date').reset_index(drop=True)
        self.log(f"\n  Total merged: {len(merged)} daily rows  |  {len(merged.columns)-1} feature columns")
        return merged

    # ─────────────────────────────────────────────────────────────────────
    # CROSS-GENERATOR FEATURES
    # ─────────────────────────────────────────────────────────────────────

    def _build_cross_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Features that require data from 2+ generators.
        Currently: real_rate_proxy = tbill_91d_yield - ccpi_yoy_pct
        """
        self.log("\n" + "─" * 60)
        self.log("CROSS-GENERATOR FEATURES")
        self.log("─" * 60)

        if 'tbill_91d_yield' in df.columns and 'ccpi_yoy_pct' in df.columns:
            df['real_rate_proxy'] = (df['tbill_91d_yield'] - df['ccpi_yoy_pct']).round(2)
            pos = (df['real_rate_proxy'] > 0).sum()
            neg = (df['real_rate_proxy'] < 0).sum()
            self.log(f"  ✓ real_rate_proxy: {pos} positive days (positive real rate)  "
                     f"| {neg} negative days (financial repression)")
        else:
            missing = [c for c in ['tbill_91d_yield', 'ccpi_yoy_pct'] if c not in df.columns]
            self.log(f"  SKIP real_rate_proxy: missing {missing}", 'warning')

        return df

    # ─────────────────────────────────────────────────────────────────────
    # SUPABASE UPLOAD
    # ─────────────────────────────────────────────────────────────────────

    def _upload_to_supabase(self, df: pd.DataFrame) -> int:
        """
        Upsert merged features to cbsl_macro_features table.
        Returns number of rows upserted.
        """
        self.log("\n" + "─" * 60)
        self.log(f"UPLOAD: Upserting to Supabase table '{SUPABASE_TABLE}'")
        self.log("─" * 60)

        df = df.copy()
        df['built_at'] = self.run_ts

        # Replace NaN/Inf with None for JSON serialisation
        df = df.replace([np.inf, -np.inf], np.nan)
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        records = df.to_dict(orient='records')

        # Convert NaN to None for Supabase
        clean_records = []
        for row in records:
            clean_row = {k: (None if (isinstance(v, float) and np.isnan(v)) else v)
                         for k, v in row.items()}
            clean_records.append(clean_row)

        try:
            client = _get_client()
            # Upsert in batches of 500
            batch_size = 500
            upserted = 0
            for i in range(0, len(clean_records), batch_size):
                batch = clean_records[i: i + batch_size]
                client.table(SUPABASE_TABLE).upsert(batch, on_conflict='date').execute()
                upserted += len(batch)
                self.log(f"  Upserted batch {i // batch_size + 1}: {upserted}/{len(clean_records)} rows")

            self.log(f"\n  ✅ Upload complete: {upserted} rows → {SUPABASE_TABLE}")
            return upserted

        except Exception as e:
            self.log(f"  ERROR uploading to Supabase: {e}", 'error')
            self.log("  CSV archive still saved — Supabase upload failed", 'warning')
            return 0

    # ─────────────────────────────────────────────────────────────────────
    # BUILD SUMMARY
    # ─────────────────────────────────────────────────────────────────────

    def _print_summary(self, df: pd.DataFrame, upserted: int):
        feature_cols = [c for c in df.columns if c not in META_COLS]
        self.log("\n" + "=" * 70)
        self.log("CBSL MACRO FEATURE BUILD — SUMMARY")
        self.log("=" * 70)
        self.log(f"  Run date:        {self.run_ts}")
        self.log(f"  History window:  {self.weeks} weeks")
        self.log(f"  Rows (daily):    {len(df):,}")
        self.log(f"  Date range:      {df['date'].min().date()} – {df['date'].max().date()}")
        self.log(f"  Feature columns: {len(feature_cols)} / {EXPECTED_FEATURE_COUNT} expected")

        if upserted > 0:
            self.log(f"  Supabase rows:   {upserted} upserted → {SUPABASE_TABLE}")
        else:
            self.log(f"  Supabase:        SKIPPED (--no-upload or error)")

        # Coverage check
        missing_features = EXPECTED_FEATURE_COUNT - len(feature_cols)
        if missing_features > 0:
            self.log(f"\n  ⚠️  {missing_features} expected features missing — check generator logs", 'warning')
        else:
            self.log(f"\n  ✅ All {EXPECTED_FEATURE_COUNT} features present")

        # Data quality summary
        null_pct = df[feature_cols].isna().mean().mean() * 100
        self.log(f"  Overall null %:  {null_pct:.1f}%")
        self.log("=" * 70)

    # ─────────────────────────────────────────────────────────────────────
    # MAIN ENTRY POINT
    # ─────────────────────────────────────────────────────────────────────

    def build(self) -> pd.DataFrame:
        self.log("=" * 70)
        self.log("CBSL MACRO FEATURE MASTER BUILDER  v1.0.0")
        self.log(f"Run: {self.run_ts}  |  Window: {self.weeks}w  |  "
                 f"Upload: {'YES' if self.upload else 'NO'}  |  "
                 f"Dry-run: {'YES' if self.dry_run else 'NO'}")
        self.log("=" * 70)

        # 1. Run all generators
        usd_df   = self._run_usd_lkr()
        fiscal_df = self._run_fiscal()
        remit_df  = self._run_remittances()
        tea_df    = self._run_tea()

        # 2. Merge
        merged = self._merge_all(usd_df, fiscal_df, remit_df, tea_df)
        if merged.empty:
            self.log("ERROR: Merge produced empty DataFrame — aborting", 'error')
            return pd.DataFrame()

        # 3. Cross-generator features
        merged = self._build_cross_features(merged)

        # 4. Save CSVs
        if not self.dry_run:
            dated_name   = f"MASTER_CBSL_FEATURES_{self.run_date}.csv"
            latest_name  = "MASTER_CBSL_FEATURES_LATEST.csv"
            self.save_features(merged, dated_name)
            self.save_features(merged, latest_name)

        # 5. Upload to Supabase
        upserted = 0
        if self.upload and not self.dry_run:
            upserted = self._upload_to_supabase(merged)

        # 6. Summary
        self._print_summary(merged, upserted)

        return merged

    # alias
    def generate_features(self) -> pd.DataFrame:
        return self.build()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Build CBSL macro feature set (USD/LKR + Fiscal + Remittances + Tea)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 cbsl_master_feature_builder.py                   # default 104-week window
  python3 cbsl_master_feature_builder.py --weeks 260       # 5-year history
  python3 cbsl_master_feature_builder.py --no-upload       # CSV only, skip Supabase
  python3 cbsl_master_feature_builder.py --dry-run         # validate only, no writes
        """
    )
    parser.add_argument('--weeks', type=int, default=104,
                        help='History window in weeks (default: 104 = 2 years)')
    parser.add_argument('--no-upload', action='store_true',
                        help='Skip Supabase upload (CSV output only)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run generators and validate but do not write anything')
    parser.add_argument('--output-dir', default=None,
                        help='Override output directory for CSV files')
    args = parser.parse_args()

    builder = CBSLMasterFeatureBuilder(
        weeks=args.weeks,
        verbose=True,
        upload=not args.no_upload,
        dry_run=args.dry_run
    )
    if args.output_dir:
        builder.output_dir = Path(args.output_dir)

    df = builder.build()

    if df.empty:
        print("\n❌ BUILD FAILED — check logs above")
        sys.exit(1)

    feature_cols = [c for c in df.columns if c not in META_COLS]
    print(f"\n✅ BUILD COMPLETE  |  {len(feature_cols)} features  |  {len(df)} rows")
    print(f"   Date range: {df['date'].min().date()} – {df['date'].max().date()}")


if __name__ == '__main__':
    main()
