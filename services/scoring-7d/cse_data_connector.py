
#!/usr/bin/env python3
"""
CSE DATA CONNECTOR FOR DIMENSION 7 v2.0
Investment OS - Market Sentiment Enhancement
Phase 1, Day 1: Data Integration Module

PURPOSE:
Connects to Supabase CSE official data and calculates 6 new metrics:
1. Trade count (daily trades)
2. Share volume (daily shares traded)
3. Intraday volatility (high-low range)
4. Volume trends (5D vs 20D average)
5. Trade density (share_volume / trade_count)
6. Multi-timeframe momentum (5D, 20D, 60D returns)

INPUTS:
- Supabase CSE price data (cse_daily_prices table)
  * collection_date (date)
  * symbol (varchar)
  * price (numeric) - close price
  * open, high, low (numeric)
  * trade_volume (int) - number of trades
  * share_volume (bigint) - number of shares
- Date range for historical calculations

OUTPUTS:
- DataFrame with all CSE metrics per stock
- Ready for D7 v2.0 scoring engine

METHODOLOGY:
- Uses actual CSE exchange data (2x daily collection)
- Calculates rolling averages for trends
- Handles missing data gracefully
- Validates all calculations

VERSION: 1.1 (Updated for cse_daily_prices schema)
DATE: January 7, 2026
AUTHOR: Investment OS Team

Migration: Phase 2 (Feb 2026)
- Replaced: dotenv/load_dotenv + supabase.create_client -> common.database.get_supabase_client()
- Removed: manual SUPABASE_URL/KEY env vars + get_supabase_client() function
- Original: /opt/selenium_automation/cse_data_connector.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

# === Investment OS Common Library (Phase 2 Migration) ===
from common.database import get_supabase_client
from supabase import Client  # Keep type hint only

# =============================================================================
# CONFIGURATION
# =============================================================================

# CSE data table
CSE_TABLE = 'cse_daily_prices'

# Column mappings (actual schema)
DATE_COL = 'collection_date'
CLOSE_COL = 'price'
TRADE_VOL_COL = 'trade_volume'
SHARE_VOL_COL = 'share_volume'

# Lookback periods (trading days)
SHORT_TERM = 5   # 1 week
MEDIUM_TERM = 20 # 1 month
LONG_TERM = 60   # 3 months

# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_cse_data(
    supabase: Client,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    symbol: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch CSE price data from Supabase.

    Args:
        supabase: Supabase client
        start_date: Start date (YYYY-MM-DD), defaults to 90 days ago
        end_date: End date (YYYY-MM-DD), defaults to today
        symbol: Specific stock symbol (optional, fetches all if None)

    Returns:
        DataFrame with CSE price data
    """
    # Default date range: last 90 days (covers 60-day lookback + buffer)
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    if start_date is None:
        start = datetime.now() - timedelta(days=90)
        start_date = start.strftime('%Y-%m-%d')

    print(f"Fetching CSE data from {start_date} to {end_date}...")

    try:
        # Build query
        query = supabase.table(CSE_TABLE).select('*')

        # Filter by date range
        query = query.gte(DATE_COL, start_date).lte(DATE_COL, end_date)

        # Filter by symbol if provided
        if symbol:
            query = query.eq('symbol', symbol)

        # Execute query
        response = query.execute()

        # Convert to DataFrame
        if response.data:
            df = pd.DataFrame(response.data)
            print(f"Fetched {len(df):,} records for {df['symbol'].nunique()} stocks")

            # Rename columns to standard names
            df = df.rename(columns={
                DATE_COL: 'trade_date',
                CLOSE_COL: 'close',
                TRADE_VOL_COL: 'trade_volume',
                SHARE_VOL_COL: 'share_volume'
            })

            # Convert date column
            df['trade_date'] = pd.to_datetime(df['trade_date'])

            # Ensure numeric types for calculations
            numeric_cols = ['close', 'open', 'high', 'low', 'trade_volume', 'share_volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Sort by symbol and date
            df = df.sort_values(['symbol', 'trade_date']).reset_index(drop=True)

            return df
        else:
            print("No data found for specified criteria")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching CSE data: {e}")
        raise

# =============================================================================
# METRIC CALCULATIONS
# =============================================================================

def calculate_intraday_volatility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate intraday volatility as (high - low) / close percentage.

    Metric 3: Intraday Volatility
    - Measures daily price range relative to close
    - Higher volatility = more trading activity
    - Used in volatility adjustment
    """
    df = df.copy()
    df['intraday_volatility'] = ((df['high'] - df['low']) / df['close'] * 100)
    df['intraday_volatility'] = df['intraday_volatility'].fillna(0).clip(0, 100)
    return df

def calculate_volume_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate volume trends (5D vs 20D average).

    Metric 4: Volume Trends
    - 5-day average share volume
    - 20-day average share volume
    - Ratio: current vs average (5D/20D)
    """
    df = df.copy()

    df['share_volume_5d'] = df.groupby('symbol')['share_volume'].transform(
        lambda x: x.rolling(window=SHORT_TERM, min_periods=1).mean()
    )

    df['share_volume_20d'] = df.groupby('symbol')['share_volume'].transform(
        lambda x: x.rolling(window=MEDIUM_TERM, min_periods=1).mean()
    )

    df['volume_ratio_5d_20d'] = (df['share_volume_5d'] / df['share_volume_20d'])
    df['volume_ratio_5d_20d'] = df['volume_ratio_5d_20d'].fillna(1.0).clip(0, 10)

    return df

def calculate_trade_density(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate trade density (share_volume / trade_count).

    Metric 5: Trade Density
    - Average shares per trade
    - Higher = institutional activity (block trades)
    - Lower = retail activity (small lots)
    """
    df = df.copy()
    df['trade_density'] = df['share_volume'] / df['trade_volume'].replace(0, np.nan)
    df['trade_density'] = df['trade_density'].fillna(0).clip(0, 1_000_000)
    return df

def calculate_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate multi-timeframe price momentum.

    Metric 6: Multi-Timeframe Momentum
    - 5-day return (short-term)
    - 20-day return (medium-term)
    - 60-day return (long-term)
    """
    df = df.copy()

    for period, label in [(SHORT_TERM, '5d'), (MEDIUM_TERM, '20d'), (LONG_TERM, '60d')]:
        df[f'close_{period}d_ago'] = df.groupby('symbol')['close'].shift(period)
        df[f'momentum_{label}'] = ((df['close'] - df[f'close_{period}d_ago']) /
                                   df[f'close_{period}d_ago'] * 100)
        df.drop(f'close_{period}d_ago', axis=1, inplace=True)

    for col in ['momentum_5d', 'momentum_20d', 'momentum_60d']:
        df[col] = df[col].fillna(0).clip(-100, 1000)

    return df

def calculate_all_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate all 6 CSE metrics for D7 v2.0.
    """
    print("Calculating all CSE metrics...")

    df = calculate_intraday_volatility(df)
    df = calculate_volume_trends(df)
    df = calculate_trade_density(df)
    df = calculate_momentum(df)

    print("All metrics calculated")
    return df

# =============================================================================
# LATEST DATA EXTRACTION
# =============================================================================

def get_latest_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract latest metrics for each stock (most recent trading day).
    """
    latest = df.sort_values('trade_date').groupby('symbol', group_keys=False).tail(1).reset_index(drop=True)

    output_cols = [
        'symbol',
        'trade_date',
        'close',
        'trade_volume',           # Metric 1: Daily trade count
        'share_volume',           # Metric 2: Daily share volume
        'intraday_volatility',    # Metric 3: Intraday range %
        'share_volume_5d',        # Metric 4a: 5D avg volume
        'share_volume_20d',       # Metric 4b: 20D avg volume
        'volume_ratio_5d_20d',    # Metric 4c: Volume trend
        'trade_density',          # Metric 5: Shares per trade
        'momentum_5d',            # Metric 6a: 5D return
        'momentum_20d',           # Metric 6b: 20D return
        'momentum_60d'            # Metric 6c: 60D return
    ]

    available_cols = [col for col in output_cols if col in latest.columns]
    latest = latest[available_cols]

    return latest

# =============================================================================
# MAIN FUNCTION
# =============================================================================

def fetch_cse_metrics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    symbol: Optional[str] = None,
    output_file: Optional[str] = None
) -> pd.DataFrame:
    """
    Main function: Fetch CSE data and calculate all metrics.
    """
    print("=" * 80)
    print("CSE DATA CONNECTOR - Dimension 7 v2.0")
    print("=" * 80)

    # Connect to Supabase via common library
    supabase = get_supabase_client()

    # Fetch raw data
    df = fetch_cse_data(supabase, start_date, end_date, symbol)

    if df.empty:
        print("No data available")
        return pd.DataFrame()

    # Calculate all metrics
    df = calculate_all_metrics(df)

    # Extract latest metrics
    latest = get_latest_metrics(df)

    print(f"\nLatest metrics calculated for {len(latest)} stocks")

    # Save to file if requested
    if output_file:
        latest.to_csv(output_file, index=False)
        print(f"Saved to: {output_file}")

    # Display sample
    print("\nSample metrics (first 5 stocks):")
    print(latest.head().to_string())

    print("\nCSE metrics ready for D7 v2.0 scoring!")

    return latest

# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch CSE metrics for Dimension 7 v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch latest metrics for all stocks
  python3 cse_data_connector.py --output cse_metrics.csv

  # Fetch specific date range
  python3 cse_data_connector.py --start 2025-12-01 --end 2026-01-06

  # Fetch single stock
  python3 cse_data_connector.py --symbol CTC.N0000 --output ctc_metrics.csv
        """
    )

    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD), defaults to 90 days ago')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--symbol', type=str, help='Specific stock symbol (optional)')
    parser.add_argument('--output', type=str, default='cse_metrics.csv', help='Output CSV file (default: cse_metrics.csv)')

    args = parser.parse_args()

    fetch_cse_metrics(
        start_date=args.start,
        end_date=args.end,
        symbol=args.symbol,
        output_file=args.output
    )
