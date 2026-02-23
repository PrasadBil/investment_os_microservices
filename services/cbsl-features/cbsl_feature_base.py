#!/usr/bin/env python3
"""
CBSL FEATURE EXTRACTION BASE — SUPABASE-FIRST
Replaces the old Excel/file-based CBSLFeatureExtractor

FILE: cbsl_feature_base.py
CREATED: 2026-02-21
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-02-21  Initial creation — Supabase-native base replacing
                         cbsl_feature_extractor_base.py (Excel/JSON parsing)
                         Data source: cbsl_weekly_* + cbsl_daily_indicators (live)
                         Architecture: uses packages/common/database.py singleton

KEY DIFFERENCES FROM OLD BASE:
    OLD: load_table(name) → raw rows with period + data_json (needed JSON parsing)
    NEW: query_weekly(table) / query_daily() → clean DataFrames, typed columns, no parsing
    OLD: parse_period_to_date() → string parsing of "2020-Q1", "March 2020"
    NEW: week_ending (DATE) / date (DATE) — already clean from parsers
    OLD: hardcoded output_dir = "/opt/selenium_automation/cbsl_features"
    NEW: output via save_features(df, filename) → configurable path from env

TABLES SERVED:
    Weekly (week_ending PK):
        cbsl_weekly_external_sector   → FX rates, remittances, reserves, trade
        cbsl_weekly_fiscal_sector     → T-bill yields, auctions, foreign holdings
        cbsl_weekly_real_sector       → Tea/rubber/coconut, inflation, GDP, PMI
        cbsl_weekly_monetary_sector   → Policy rates, money supply, credit
    Daily (date PK):
        cbsl_daily_indicators         → FX buy/sell, T-bills, ASPI, energy, oil
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# IMPORT: use common.database if on platform, fall back to direct create_client
# ─────────────────────────────────────────────────────────────────────────────
try:
    sys.path.insert(0, str(Path(__file__).parents[2] / 'packages'))
    from common.database import get_supabase_client
    from common.logging_config import get_logger
    _USE_COMMON = True
except ImportError:
    from supabase import create_client
    _USE_COMMON = False


def _get_client():
    if _USE_COMMON:
        return get_supabase_client()
    return create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_KEY')
    )


def _get_logger(name: str) -> logging.Logger:
    if _USE_COMMON:
        try:
            return get_logger(name)
        except Exception:
            pass
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(name)s: %(message)s')
    return logging.getLogger(name)


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT PATH
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_OUTPUT_DIR = os.getenv(
    'CBSL_FEATURES_OUTPUT_DIR',
    str(Path(__file__).parents[3] / 'output' / 'cbsl_features')
)


class CBSLFeatureBase:
    """
    Supabase-native base for all CBSL macro feature generators.

    Subclasses implement generate_features() → pd.DataFrame.

    Design principles:
        - Query by date range (default: last 104 weeks / 2 years)
        - No JSON parsing — new tables have clean typed columns
        - align_to_daily() for Granger/scoring compatibility
        - All calc utilities (change, MA, volatility, momentum, RSI, regime, zscore)
        - Standardised save + validation
    """

    # Weekly table names (canonical)
    TABLE_WEEKLY_EXTERNAL = 'cbsl_weekly_external_sector'
    TABLE_WEEKLY_FISCAL   = 'cbsl_weekly_fiscal_sector'
    TABLE_WEEKLY_REAL     = 'cbsl_weekly_real_sector'
    TABLE_WEEKLY_MONETARY = 'cbsl_weekly_monetary_sector'
    TABLE_DAILY           = 'cbsl_daily_indicators'

    def __init__(self, weeks: int = 104, verbose: bool = True):
        """
        Args:
            weeks:   How many weeks of history to load (default 104 = 2 years)
            verbose: Print progress logs
        """
        self.weeks = weeks
        self.verbose = verbose
        self.logger = _get_logger(self.__class__.__name__)
        self._supabase = None          # lazy-init
        self._cache: dict = {}
        self.feature_names: List[str] = []
        self.output_dir = Path(DEFAULT_OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ─────────────────────────────────────────────────────────────────────
    # LOGGING
    # ─────────────────────────────────────────────────────────────────────

    def log(self, msg: str, level: str = 'info'):
        if self.verbose:
            getattr(self.logger, level)(msg)

    # ─────────────────────────────────────────────────────────────────────
    # SUPABASE CLIENT (lazy singleton)
    # ─────────────────────────────────────────────────────────────────────

    @property
    def supabase(self):
        if self._supabase is None:
            self._supabase = _get_client()
        return self._supabase

    # ─────────────────────────────────────────────────────────────────────
    # QUERY HELPERS
    # ─────────────────────────────────────────────────────────────────────

    def _since_date(self) -> str:
        """ISO date string for start of window."""
        return (datetime.now() - timedelta(weeks=self.weeks)).strftime('%Y-%m-%d')

    def query_weekly(self, table: str, since: Optional[str] = None) -> pd.DataFrame:
        """
        Query a cbsl_weekly_* table and return a clean DataFrame.

        Args:
            table: One of TABLE_WEEKLY_* constants
            since: ISO date string (default: self._since_date())

        Returns:
            DataFrame sorted by week_ending, clean numeric types
        """
        cache_key = f"{table}:{since or self._since_date()}"
        if cache_key in self._cache:
            return self._cache[cache_key].copy()

        since = since or self._since_date()
        self.log(f"Querying {table} since {since} ...")

        try:
            resp = (
                self.supabase
                .table(table)
                .select('*')
                .gte('week_ending', since)
                .order('week_ending')
                .execute()
            )
        except Exception as e:
            self.log(f"ERROR querying {table}: {e}", 'error')
            return pd.DataFrame()

        if not resp.data:
            self.log(f"WARNING: no rows returned from {table}", 'warning')
            return pd.DataFrame()

        df = pd.DataFrame(resp.data)
        df['week_ending'] = pd.to_datetime(df['week_ending'])
        df = df.sort_values('week_ending').reset_index(drop=True)

        # Coerce all non-metadata columns to numeric
        meta_cols = {'week_ending', 'collected_at', 'source_file', 'parse_notes',
                     'remittances_data_month', 'trade_data_month'}
        for col in df.columns:
            if col not in meta_cols:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception:
                    pass

        self.log(f"  → {len(df)} weekly rows  |  "
                 f"{df['week_ending'].min().date()} – {df['week_ending'].max().date()}")
        self._cache[cache_key] = df
        return df.copy()

    def query_daily(self, since: Optional[str] = None) -> pd.DataFrame:
        """
        Query cbsl_daily_indicators table.

        Returns:
            DataFrame sorted by date, clean numeric types
        """
        since = since or self._since_date()
        cache_key = f"daily:{since}"
        if cache_key in self._cache:
            return self._cache[cache_key].copy()

        self.log(f"Querying {self.TABLE_DAILY} since {since} ...")

        try:
            resp = (
                self.supabase
                .table(self.TABLE_DAILY)
                .select('*')
                .gte('date', since)
                .order('date')
                .execute()
            )
        except Exception as e:
            self.log(f"ERROR querying {self.TABLE_DAILY}: {e}", 'error')
            return pd.DataFrame()

        if not resp.data:
            self.log(f"WARNING: no rows returned from {self.TABLE_DAILY}", 'warning')
            return pd.DataFrame()

        df = pd.DataFrame(resp.data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        meta_cols = {'date', 'source_file', 'parse_notes'}
        for col in df.columns:
            if col not in meta_cols:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception:
                    pass

        self.log(f"  → {len(df)} daily rows  |  "
                 f"{df['date'].min().date()} – {df['date'].max().date()}")
        self._cache[cache_key] = df
        return df.copy()

    # ─────────────────────────────────────────────────────────────────────
    # ALIGNMENT UTILITIES
    # ─────────────────────────────────────────────────────────────────────

    def align_to_daily(self, df: pd.DataFrame, date_col: str = 'week_ending') -> pd.DataFrame:
        """
        Upsample weekly/quarterly data to daily frequency via forward-fill.
        Enables join with daily OHLCV or scoring data.

        Returns DataFrame with 'date' column (daily).
        """
        df = df.copy()
        df = df.rename(columns={date_col: 'date'})
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').set_index('date')

        daily_idx = pd.date_range(df.index.min(), df.index.max(), freq='D')
        df = df.reindex(daily_idx, method='ffill')
        df.index.name = 'date'
        return df.reset_index()

    def align_weekly_to_daily_range(self, weekly_df: pd.DataFrame,
                                     daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        Align weekly features to the exact date range of daily_df.
        Uses backward-fill merge (weekly value persists until next week).
        """
        weekly_daily = self.align_to_daily(weekly_df)
        return pd.merge_asof(
            daily_df[['date']].sort_values('date'),
            weekly_daily.sort_values('date'),
            on='date',
            direction='backward'
        )

    # ─────────────────────────────────────────────────────────────────────
    # FEATURE CALCULATION UTILITIES
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def pct_change(s: pd.Series, periods: int = 1) -> pd.Series:
        """Percentage change over N periods."""
        return s.pct_change(periods=periods) * 100

    @staticmethod
    def ma(s: pd.Series, window: int, min_periods: int = 1) -> pd.Series:
        """Simple moving average."""
        return s.rolling(window=window, min_periods=min_periods).mean()

    @staticmethod
    def volatility(s: pd.Series, window: int, min_periods: int = 2) -> pd.Series:
        """Rolling standard deviation (volatility)."""
        return s.rolling(window=window, min_periods=min_periods).std()

    @staticmethod
    def momentum(s: pd.Series, window: int) -> pd.Series:
        """Momentum: current - N periods ago (absolute)."""
        return s - s.shift(window)

    @staticmethod
    def lag(s: pd.Series, n: int) -> pd.Series:
        """Lag by N periods."""
        return s.shift(n)

    @staticmethod
    def zscore(s: pd.Series, window: int) -> pd.Series:
        """Rolling z-score over window."""
        mu = s.rolling(window=window, min_periods=max(2, window // 4)).mean()
        sigma = s.rolling(window=window, min_periods=max(2, window // 4)).std()
        return (s - mu) / sigma.replace(0, np.nan)

    @staticmethod
    def rsi(s: pd.Series, window: int = 14) -> pd.Series:
        """Wilder RSI."""
        delta = s.diff()
        gain = delta.clip(lower=0).rolling(window=window, min_periods=window).mean()
        loss = (-delta.clip(upper=0)).rolling(window=window, min_periods=window).mean()
        rs = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def regime(s: pd.Series, fast: int = 4, slow: int = 13) -> pd.Series:
        """
        MA-crossover regime: +1 rising, -1 falling, 0 neutral.
        Defaults: fast=4w, slow=13w (quarterly) for weekly data.
        """
        ma_f = s.rolling(fast, min_periods=1).mean()
        ma_s = s.rolling(slow, min_periods=1).mean()
        r = pd.Series(0, index=s.index)
        r[ma_f > ma_s] = 1
        r[ma_f < ma_s] = -1
        return r

    @staticmethod
    def normalise_0_100(s: pd.Series) -> pd.Series:
        """Min-max normalisation to [0, 100]."""
        lo, hi = s.min(), s.max()
        if hi == lo:
            return pd.Series(50.0, index=s.index)
        return 100 * (s - lo) / (hi - lo)

    # ─────────────────────────────────────────────────────────────────────
    # VALIDATION
    # ─────────────────────────────────────────────────────────────────────

    def validate_features(self, df: pd.DataFrame, feature_cols: List[str]) -> None:
        """Print feature quality summary."""
        self.log("\n" + "=" * 70)
        self.log("FEATURE VALIDATION")
        self.log("=" * 70)
        self.log(f"  Rows:       {len(df)}")
        date_col = 'date' if 'date' in df.columns else 'week_ending'
        if date_col in df.columns:
            self.log(f"  Date range: {df[date_col].min().date()} – {df[date_col].max().date()}")
        self.log(f"  Features:   {len(feature_cols)}")
        self.log(f"\n  {'Feature':<45} {'Non-null':>8} {'Missing%':>9} {'Mean':>9}")
        self.log("  " + "─" * 75)
        for col in feature_cols:
            if col not in df.columns:
                self.log(f"  {col:<45} {'MISSING':>8}")
                continue
            s = df[col]
            non_null = s.notna().sum()
            miss_pct = s.isna().mean() * 100
            mean_val = s.mean() if pd.api.types.is_numeric_dtype(s) else 'N/A'
            mean_str = f"{mean_val:>9.2f}" if isinstance(mean_val, float) else f"{mean_val:>9}"
            self.log(f"  {col:<45} {non_null:>8} {miss_pct:>8.1f}% {mean_str}")
        self.log("=" * 70)

    # ─────────────────────────────────────────────────────────────────────
    # SAVE
    # ─────────────────────────────────────────────────────────────────────

    def save_features(self, df: pd.DataFrame, filename: str,
                      output_dir: Optional[Path] = None) -> Path:
        """Save feature DataFrame to CSV."""
        out = (output_dir or self.output_dir) / filename
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        self.log(f"  💾 Saved {len(df)} rows → {out}")
        return out

    # ─────────────────────────────────────────────────────────────────────
    # INTERFACE
    # ─────────────────────────────────────────────────────────────────────

    def generate_features(self) -> pd.DataFrame:
        """Override in subclasses. Returns DataFrame with feature columns."""
        raise NotImplementedError
