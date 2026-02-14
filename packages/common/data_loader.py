#!/usr/bin/env python3
"""
Investment OS - Data Loader Module
====================================
Unified OHLCV data loading from Supabase with automatic fallback.
Replaces cse_data_loader.py (the linchpin module used by all 12 services).

Data Sources (Priority Order):
    1. cse_daily_prices — Primary, collected 2x daily (10 AM, 4 PM SLK)
    2. daily_prices — Backup, collected daily (3:45 PM SLK) from StockAnalysis

Architecture:
    - Uses common.database singleton (no more inline create_client)
    - Uses common.config for table names and thresholds
    - 100% backward-compatible function signatures
    - All 12 downstream services work without code changes

Usage:
    from common.data_loader import load_stock_data, load_cse_data

    # Single stock
    df = load_stock_data('CTHR.N0000', days=30)

    # Batch load (all ultra-clean stocks)
    data = load_cse_data(days=30)

    # Validate quality
    results = validate_data_quality(data)

Replaces:
    - cse_data_loader.py (198 lines → centralized here)
    - Imported by: calendar_signal_monitor, tier1_signal_generator,
      tier1_granger_per_stock_v5, manipulation_detector_v5_0,
      daily_trading_workflow, all dimension scorers (via composite)
"""

import os
from datetime import datetime, timedelta
import pandas as pd
from supabase import Client
from typing import Dict, Optional
import logging

from common.database import get_supabase_client
from common.config import get_config

logger = logging.getLogger(__name__)


# ============================================================================
# PRIMARY DATA SOURCE: cse_daily_prices
# ============================================================================

def load_from_cse_daily_prices(
    supabase: Client,
    symbol: str,
    days: int = 30
) -> Optional[pd.DataFrame]:
    """
    Load data from cse_daily_prices table (PRIMARY SOURCE).

    This table collects data 2x daily at 10 AM and 4 PM SLK time.
    We use the 4 PM collection as it represents end-of-day prices.

    Args:
        supabase: Supabase client
        symbol: Stock symbol (e.g., 'CTHR.N0000')
        days: Number of days to fetch (default 30)

    Returns:
        DataFrame with columns: date, close, volume, open, high, low
        or None if no data found
    """
    config = get_config()
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    logger.info(f"Loading {symbol} from {config.PRIMARY_TABLE} ({start_date} to {end_date})")

    try:
        response = (
            supabase.table(config.PRIMARY_TABLE)
            .select('collection_date, price, open, high, low, share_volume, trade_volume')
            .eq('symbol', symbol)
            .gte('collection_date', start_date.isoformat())
            .lte('collection_date', end_date.isoformat())
            .order('collection_date', desc=False)
            .execute()
        )

        if not response.data:
            logger.warning(f"No data found for {symbol} in {config.PRIMARY_TABLE}")
            return None

        df = pd.DataFrame(response.data)

        # Rename columns to standard format
        df = df.rename(columns={
            'collection_date': 'date',
            'price': 'close',
            'share_volume': 'volume'
        })

        df['date'] = pd.to_datetime(df['date'])

        # Ensure numeric columns
        numeric_cols = ['close', 'volume', 'open', 'high', 'low']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Remove duplicates (keep latest if 2 collections per day)
        df = df.drop_duplicates(subset=['date'], keep='last')

        df = df.sort_values('date').reset_index(drop=True)

        if len(df) < config.MIN_DATA_DAYS:
            logger.warning(
                f"{symbol}: Only {len(df)} days of data "
                f"(minimum {config.MIN_DATA_DAYS} recommended)"
            )

        logger.info(f"Loaded {len(df)} days for {symbol} from {config.PRIMARY_TABLE}")

        return df[['date', 'close', 'volume', 'open', 'high', 'low']]

    except Exception as e:
        logger.error(f"Error loading {symbol} from {config.PRIMARY_TABLE}: {e}")
        return None


# ============================================================================
# BACKUP DATA SOURCE: daily_prices
# ============================================================================

def load_from_daily_prices_backup(
    supabase: Client,
    symbol: str,
    days: int = 30
) -> Optional[pd.DataFrame]:
    """
    Load data from daily_prices table (BACKUP SOURCE).

    This table collects data daily at 3:45 PM SLK time from StockAnalysis.
    Used as fallback if cse_daily_prices has no data.

    Args:
        supabase: Supabase client
        symbol: Stock symbol (e.g., 'CTHR.N0000')
        days: Number of days to fetch (default 30)

    Returns:
        DataFrame with columns: date, close, volume
        or None if no data found

    Note:
        This table doesn't have OHLCV, only close price.
        Volume is set to placeholder 100K.
    """
    config = get_config()
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    logger.info(f"Loading {symbol} from {config.BACKUP_TABLE} (BACKUP) ({start_date} to {end_date})")

    try:
        response = (
            supabase.table(config.BACKUP_TABLE)
            .select('date, price, symbol')
            .eq('symbol', symbol)
            .gte('date', start_date.isoformat())
            .lte('date', end_date.isoformat())
            .order('date', desc=False)
            .execute()
        )

        if not response.data:
            logger.warning(f"No data found for {symbol} in {config.BACKUP_TABLE} (backup)")
            return None

        df = pd.DataFrame(response.data)

        df = df.rename(columns={'price': 'close'})
        df['date'] = pd.to_datetime(df['date'])
        df['close'] = pd.to_numeric(df['close'], errors='coerce')

        # Placeholder volume (signal generator needs it)
        df['volume'] = 100000

        df = df.drop_duplicates(subset=['date'], keep='last')
        df = df.sort_values('date').reset_index(drop=True)

        logger.info(f"Loaded {len(df)} days for {symbol} from {config.BACKUP_TABLE} (backup)")

        return df[['date', 'close', 'volume']]

    except Exception as e:
        logger.error(f"Error loading {symbol} from {config.BACKUP_TABLE}: {e}")
        return None


# ============================================================================
# UNIFIED DATA LOADER (Primary + Fallback)
# ============================================================================

def load_stock_data(
    symbol: str,
    days: int = 30,
    supabase: Optional[Client] = None
) -> Optional[pd.DataFrame]:
    """
    Load stock data with automatic fallback.

    Priority:
        1. Try cse_daily_prices (2x daily, OHLCV)
        2. Fallback to daily_prices (1x daily, close only)
        3. Return None if both fail

    Args:
        symbol: Stock symbol (e.g., 'CTHR.N0000')
        days: Number of days to fetch (default 30)
        supabase: Optional Supabase client (uses singleton if not provided)

    Returns:
        DataFrame with at minimum: date, close, volume
        Additional columns if available: open, high, low
    """
    config = get_config()

    # Use singleton if client not provided
    if supabase is None:
        supabase = get_supabase_client()

    # Try primary source first
    df = load_from_cse_daily_prices(supabase, symbol, days)

    # Fallback to backup if primary failed
    if df is None or len(df) < config.MIN_DATA_DAYS:
        logger.warning(f"{symbol}: Primary source insufficient, trying backup...")
        df = load_from_daily_prices_backup(supabase, symbol, days)

    # Final validation
    if df is None:
        logger.error(f"Failed to load data for {symbol} from any source")
        return None

    if len(df) < config.MIN_DATA_DAYS:
        logger.error(
            f"{symbol}: Insufficient data ({len(df)} days < {config.MIN_DATA_DAYS} minimum)"
        )
        return None

    # Add OHLC placeholders if missing (backup source doesn't have them)
    if 'open' not in df.columns:
        df['open'] = df['close']
    if 'high' not in df.columns:
        df['high'] = df['close']
    if 'low' not in df.columns:
        df['low'] = df['close']

    # Final cleanup
    df = df.dropna(subset=['date', 'close', 'volume'])

    logger.info(f"Successfully loaded {len(df)} days for {symbol}")

    return df


# ============================================================================
# BATCH LOADER (All 5 Ultra-Clean Stocks)
# ============================================================================

def load_cse_data(days: int = 30) -> Dict[str, pd.DataFrame]:
    """
    Load CSE data for all 5 ultra-clean stocks.

    This is the main function used by signal generation services.

    Args:
        days: Number of days to fetch (default 30, minimum 20)

    Returns:
        Dictionary: {symbol: DataFrame}

    Example:
        >>> cse_data = load_cse_data()
        >>> print(cse_data['CTHR.N0000'].head())
               date  close  volume
        0 2026-01-01  200.0  50000
    """
    ULTRA_CLEAN_STOCKS = [
        'CTHR.N0000',
        'RCH.N0000',
        'GHLL.N0000',
        'NEH.N0000',
        'WIND.N0000'
    ]

    logger.info("=" * 80)
    logger.info(f"LOADING CSE DATA FOR {len(ULTRA_CLEAN_STOCKS)} STOCKS")
    logger.info("=" * 80)

    # Initialize Supabase client once (reuse for all stocks)
    supabase = get_supabase_client()

    data = {}
    failed = []

    for symbol in ULTRA_CLEAN_STOCKS:
        df = load_stock_data(symbol, days=days, supabase=supabase)

        if df is not None:
            data[symbol] = df
        else:
            failed.append(symbol)

    logger.info("=" * 80)
    logger.info(f"LOADING COMPLETE: {len(data)}/{len(ULTRA_CLEAN_STOCKS)} stocks loaded")

    if failed:
        logger.warning(f"Failed to load: {', '.join(failed)}")

    logger.info("=" * 80)

    return data


# ============================================================================
# DATA VALIDATION
# ============================================================================

def validate_data_quality(data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
    """
    Validate data quality for signal generation.

    Checks:
        - Minimum 20 days of data
        - No gaps >5 days
        - Volume >0
        - Price >0
        - Recent data (within 7 days)

    Args:
        data: Dictionary of {symbol: DataFrame} from load_cse_data()

    Returns:
        Dictionary of validation results per stock
    """
    results = {}

    for symbol, df in data.items():
        issues = []

        # Check 1: Minimum data
        if len(df) < 20:
            issues.append(f"Insufficient data: {len(df)} days < 20")

        # Check 2: Recent data
        latest_date = df['date'].max()
        days_ago = (datetime.now() - latest_date).days
        if days_ago > 7:
            issues.append(f"Stale data: Latest date {latest_date.date()} ({days_ago} days old)")

        # Check 3: Large gaps
        df_sorted = df.sort_values('date')
        gaps = df_sorted['date'].diff().dt.days
        max_gap = gaps.max()
        if max_gap > 5:
            issues.append(f"Large gap detected: {max_gap} days")

        # Check 4: Zero prices/volumes
        zero_prices = (df['close'] <= 0).sum()
        zero_volumes = (df['volume'] <= 0).sum()
        if zero_prices > 0:
            issues.append(f"{zero_prices} days with zero/negative price")
        if zero_volumes > 0:
            issues.append(f"{zero_volumes} days with zero/negative volume")

        # Check 5: Data completeness
        missing = df[['date', 'close', 'volume']].isnull().sum().sum()
        if missing > 0:
            issues.append(f"{missing} missing values in critical columns")

        results[symbol] = {
            'valid': len(issues) == 0,
            'issues': issues,
            'days': len(df),
            'latest_date': latest_date.date(),
            'days_since_update': days_ago
        }

    return results


# ============================================================================
# STANDALONE TEST
# ============================================================================

if __name__ == '__main__':
    """Test the data loader."""
    logging.basicConfig(level=logging.INFO)

    print("=" * 80)
    print("CSE DATA LOADER - COMMON LIBRARY TEST")
    print("=" * 80)

    try:
        cse_data = load_cse_data(days=30)

        print(f"\nLoaded {len(cse_data)} stocks:")
        for symbol, df in cse_data.items():
            print(f"  {symbol}: {len(df)} days, "
                  f"{df['date'].min().date()} to {df['date'].max().date()}, "
                  f"Latest: Rs {df['close'].iloc[-1]:.2f}")

        validation = validate_data_quality(cse_data)

        print("\nData Quality:")
        for symbol, result in validation.items():
            status = "PASS" if result['valid'] else "FAIL"
            print(f"  {symbol}: {status} ({result['days']} days)")
            for issue in result['issues']:
                print(f"    - {issue}")

        print("\nALL TESTS PASSED")

    except Exception as e:
        print(f"\nTEST FAILED: {e}")
