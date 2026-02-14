#!/usr/bin/env python3
"""
Tier 1 Granger Library Functions
Investment OS v6.0 - Institutional-grade causality validation

Reusable utilities for Granger causality testing across 296 CSE stocks.
Validates whether v5.0 manipulation patterns have statistical causality.

Author: Investment OS Team
Date: February 4, 2026
"""

import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import grangercausalitytests
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def load_stock_data(symbol: str, supabase, days: int = 365) -> pd.DataFrame:
    """
    Load stock price/volume data from Supabase.
    
    Args:
        symbol: Stock symbol (e.g., 'LOLC.N0000')
        supabase: Supabase client
        days: Historical days to load (default: 365)
    
    Returns:
        DataFrame with columns: date, close, volume, returns, next_day_return
    
    Raises:
        ValueError: If no data found or insufficient data
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    logger.debug(f"  Loading {symbol} data: {start_date.date()} to {end_date.date()}")
    
    # Query cse_daily_prices (primary source)
    result = supabase.table('cse_daily_prices')\
        .select('collection_date, price, share_volume')\
        .eq('symbol', symbol)\
        .gte('collection_date', start_date.strftime('%Y-%m-%d'))\
        .order('collection_date', desc=False)\
        .execute()
    
    if not result.data or len(result.data) < 100:
        # Fallback to daily_prices (backup source)
        logger.debug(f"  Trying backup source (daily_prices) for {symbol}")
        result = supabase.table('daily_prices')\
            .select('date, price, volume')\
            .eq('symbol', symbol)\
            .gte('date', start_date.strftime('%Y-%m-%d'))\
            .order('date', desc=False)\
            .execute()
        
        if not result.data:
            raise ValueError(f"No data found for {symbol} in either table")
        
        # Convert backup format
        df = pd.DataFrame(result.data)
        df.rename(columns={'volume': 'share_volume'}, inplace=True)
        df['collection_date'] = df['date']
    else:
        df = pd.DataFrame(result.data)
    
    # Standardize column names
    df.rename(columns={
        'collection_date': 'date',
        'price': 'close',
        'share_volume': 'volume'
    }, inplace=True)
    
    # Ensure proper types
    df['date'] = pd.to_datetime(df['date'])
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    
    # Sort by date
    df = df.sort_values('date').reset_index(drop=True)
    
    # Calculate returns
    df['returns'] = df['close'].pct_change()
    df['next_day_return'] = df['returns'].shift(-1)
    
    # Calculate volatility (20-day rolling std)
    df['volatility'] = df['returns'].rolling(window=20).std()
    
    # Calculate volume change
    df['volume_change'] = df['volume'].pct_change()
    
    # Drop NaN rows
    df = df.dropna()
    
    if len(df) < 100:
        raise ValueError(f"Insufficient data for {symbol}: {len(df)} days (need 100+)")
    
    # Calculate data quality score
    data_quality = (len(df) / days) * 100
    logger.debug(f"  Loaded {len(df)} days ({data_quality:.1f}% completeness)")
    
    return df


def load_alternative_signals(symbol: str, supabase, signal_type: str, days: int = 365) -> Optional[pd.DataFrame]:
    """
    Load alternative data signals (weather, macro, etc.)
    
    Args:
        symbol: Stock symbol
        supabase: Supabase client
        signal_type: 'weather', 'macro', 'sector'
        days: Historical days
    
    Returns:
        DataFrame with date + signal columns, or None if not available
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    try:
        if signal_type == 'weather':
            # Get stock location based on sector
            location = get_stock_location(symbol)
            if not location:
                return None
            
            # Query weather signals
            result = supabase.table('weather_signals')\
                .select('date, rainfall_mm, temperature_avg, signal_strength')\
                .eq('location', location)\
                .gte('date', start_date.strftime('%Y-%m-%d'))\
                .order('date', desc=False)\
                .execute()
            
            if not result.data:
                return None
            
            df = pd.DataFrame(result.data)
            df['date'] = pd.to_datetime(df['date'])
            df['weather_signal'] = pd.to_numeric(df['signal_strength'], errors='coerce')
            
            return df[['date', 'weather_signal']]
        
        elif signal_type == 'macro':
            # Load CBSL macro indicators
            result = supabase.table('cbsl_macro_data')\
                .select('date, usd_lkr, awpr, m2_money_supply, ccpi_inflation')\
                .gte('date', start_date.strftime('%Y-%m-%d'))\
                .order('date', desc=False)\
                .execute()
            
            if not result.data:
                logger.warning("  No macro data available")
                return None
            
            df = pd.DataFrame(result.data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Convert to numeric
            for col in ['usd_lkr', 'awpr', 'm2_money_supply', 'ccpi_inflation']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate changes (for causality testing)
            df['usd_lkr_change'] = df['usd_lkr'].pct_change()
            df['awpr_change'] = df['awpr'].diff()
            df['m2_change'] = df['m2_money_supply'].pct_change()
            
            return df
        
        else:
            logger.warning(f"  Unknown signal type: {signal_type}")
            return None
    
    except Exception as e:
        logger.error(f"  Failed to load {signal_type} signals: {str(e)}")
        return None


def get_stock_location(symbol: str) -> Optional[str]:
    """
    Determine geographic location for a stock (for weather signals).
    
    Args:
        symbol: Stock symbol
    
    Returns:
        Location string or None
    """
    # Tea plantations mapping
    tea_stocks = {
        'CTC.N0000': 'Colombo',  # Ceylon Tea Company
        'HAYA.N0000': 'Hatton',  # Haycarb (plantation division)
        'KEL.N0000': 'Kegalle',  # Kelani Tyres (plantation)
        'WAT.N0000': 'Nuwara Eliya',  # Watawala Plantations
        'AGAL.N0000': 'Nuwara Eliya',  # Agalawatte Plantations
    }
    
    return tea_stocks.get(symbol, None)


def get_stock_sector(symbol: str) -> str:
    """
    Determine sector classification for a stock.
    
    Args:
        symbol: Stock symbol
    
    Returns:
        Sector string: 'banking', 'tea', 'exports', 'other'
    """
    # Banking stocks
    banking = [
        'LOLC.N0000', 'COMB.N0000', 'HNB.N0000', 'SAMPATH.N0000',
        'NDB.N0000', 'DFCC.N0000', 'LOFC.N0000', 'DIPD.N0000'
    ]
    
    # Tea/plantation stocks
    tea = [
        'CTC.N0000', 'HAYA.N0000', 'KEL.N0000', 'WAT.N0000', 'AGAL.N0000'
    ]
    
    # Export-focused stocks
    exports = [
        'EXPO.N0000', 'TYRE.N0000', 'LOLC.N0000'  # LOLC has export finance
    ]
    
    if symbol in banking:
        return 'banking'
    elif symbol in tea:
        return 'tea'
    elif symbol in exports:
        return 'exports'
    else:
        return 'other'


def run_granger_test(data: pd.DataFrame, 
                     predictor_col: str, 
                     target_col: str, 
                     max_lag: int = 10,
                     min_sample: int = 100) -> List[Dict]:
    """
    Run Granger causality test for predictor → target relationship.
    
    Args:
        data: DataFrame with predictor and target columns
        predictor_col: Name of predictor variable
        target_col: Name of target variable
        max_lag: Maximum lag to test (default: 10)
        min_sample: Minimum sample size required (default: 100)
    
    Returns:
        List of dicts with results for each lag:
        [
            {
                'predictor_variable': str,
                'target_variable': str,
                'lag_period': int,
                'p_value': float,
                'f_statistic': float,
                'is_significant': bool,
                'sample_size': int
            },
            ...
        ]
    """
    results = []
    
    try:
        # Prepare data (drop NaN, ensure numeric)
        test_data = data[[target_col, predictor_col]].copy()
        test_data = test_data.dropna()
        
        # Check sample size
        if len(test_data) < min_sample:
            logger.debug(f"    ⚠️  Insufficient data: {len(test_data)} obs (need {min_sample}+)")
            return results
        
        # Adjust max_lag if sample too small
        adjusted_max_lag = min(max_lag, len(test_data) // 10)  # Rule: max_lag ≤ N/10
        
        if adjusted_max_lag < 1:
            logger.debug(f"    ⚠️  Sample too small for any lag testing")
            return results
        
        # Run Granger causality test
        gc_results = grangercausalitytests(
            test_data[[target_col, predictor_col]], 
            maxlag=adjusted_max_lag,
            verbose=False
        )
        
        # Extract p-values for each lag
        for lag in range(1, adjusted_max_lag + 1):
            p_value = gc_results[lag][0]['ssr_ftest'][1]  # F-test p-value
            f_stat = gc_results[lag][0]['ssr_ftest'][0]   # F-statistic
            
            results.append({
                'predictor_variable': predictor_col,
                'target_variable': target_col,
                'lag_period': lag,
                'p_value': float(p_value),
                'f_statistic': float(f_stat),
                'is_significant': p_value < 0.05,
                'sample_size': len(test_data)
            })
        
        # Log if any significant relationships found
        significant_lags = [r['lag_period'] for r in results if r['is_significant']]
        if significant_lags:
            logger.debug(f"    ✅ {predictor_col} → {target_col}: Significant at lags {significant_lags}")
    
    except Exception as e:
        logger.error(f"    ❌ Granger test failed ({predictor_col} → {target_col}): {str(e)}")
    
    return results


def merge_signals_with_stock_data(stock_df: pd.DataFrame, 
                                   signal_df: pd.DataFrame, 
                                   how: str = 'inner') -> pd.DataFrame:
    """
    Merge alternative signals with stock data on date.
    
    Args:
        stock_df: Stock price/volume DataFrame
        signal_df: Alternative signal DataFrame
        how: Merge strategy ('inner', 'left', 'right')
    
    Returns:
        Merged DataFrame
    """
    if signal_df is None or signal_df.empty:
        return stock_df
    
    # Ensure both have 'date' column
    if 'date' not in stock_df.columns or 'date' not in signal_df.columns:
        logger.error("  Cannot merge: 'date' column missing")
        return stock_df
    
    # Merge on date
    merged = pd.merge(stock_df, signal_df, on='date', how=how)
    
    logger.debug(f"  Merged data: {len(merged)} rows (from {len(stock_df)} stock + {len(signal_df)} signal)")
    
    return merged


def store_results(results: List[Dict], symbol: str, supabase) -> bool:
    """
    Store Granger test results to Supabase.
    
    Args:
        results: List of result dicts
        symbol: Stock symbol
        supabase: Supabase client
    
    Returns:
        True if successful, False otherwise
    """
    if not results:
        logger.debug(f"  No results to store for {symbol}")
        return True
    
    try:
        # Add symbol and metadata to each result
        test_date = datetime.now().strftime('%Y-%m-%d')
        
        for result in results:
            result['symbol'] = symbol
            result['test_date'] = test_date
            result['test_period_start'] = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            result['test_period_end'] = test_date
        
        # Batch insert (upsert to handle duplicates)
        response = supabase.table('tier1_granger_results').upsert(
            results,
            on_conflict='symbol,predictor_variable,target_variable,lag_period,test_date'
        ).execute()
        
        logger.debug(f"  ✅ Stored {len(results)} results for {symbol}")
        return True
    
    except Exception as e:
        logger.error(f"  ❌ Failed to store results for {symbol}: {str(e)}")
        return False


def generate_stock_summary(symbol: str, results: List[Dict], supabase) -> Dict:
    """
    Generate summary statistics for a single stock's Granger results.
    
    Args:
        symbol: Stock symbol
        results: List of result dicts
        supabase: Supabase client
    
    Returns:
        Summary dict with key metrics
    """
    if not results:
        return {
            'symbol': symbol,
            'total_tests_conducted': 0,
            'significant_relationships': 0,
            'significance_rate': 0.0
        }
    
    # Count significant relationships
    significant = [r for r in results if r['is_significant']]
    sig_count = len(significant)
    sig_rate = (sig_count / len(results)) * 100 if results else 0.0
    
    # Find strongest predictors (top 3 by p-value)
    sorted_results = sorted(results, key=lambda x: x['p_value'])
    
    summary = {
        'symbol': symbol,
        'test_date': datetime.now().strftime('%Y-%m-%d'),
        'total_tests_conducted': len(results),
        'significant_relationships': sig_count,
        'significance_rate': round(sig_rate, 2),
        
        # Top 3 predictors
        'strongest_predictor_1': sorted_results[0]['predictor_variable'] if len(sorted_results) > 0 else None,
        'strongest_predictor_1_lag': sorted_results[0]['lag_period'] if len(sorted_results) > 0 else None,
        'strongest_predictor_1_pvalue': sorted_results[0]['p_value'] if len(sorted_results) > 0 else None,
        
        'strongest_predictor_2': sorted_results[1]['predictor_variable'] if len(sorted_results) > 1 else None,
        'strongest_predictor_2_lag': sorted_results[1]['lag_period'] if len(sorted_results) > 1 else None,
        'strongest_predictor_2_pvalue': sorted_results[1]['p_value'] if len(sorted_results) > 1 else None,
        
        'strongest_predictor_3': sorted_results[2]['predictor_variable'] if len(sorted_results) > 2 else None,
        'strongest_predictor_3_lag': sorted_results[2]['lag_period'] if len(sorted_results) > 2 else None,
        'strongest_predictor_3_pvalue': sorted_results[2]['p_value'] if len(sorted_results) > 2 else None,
        
        # v6.0 integration flags
        'has_volume_causality': any(r['predictor_variable'] == 'volume' and r['is_significant'] for r in results),
        'has_alternative_signal': any(r['predictor_variable'] in ['weather_signal', 'usd_lkr_change', 'awpr_change'] and r['is_significant'] for r in results),
        'has_macro_causality': any(r['predictor_variable'] in ['m2_change', 'usd_lkr_change'] and r['is_significant'] for r in results),
        
        # Data quality
        'overall_data_quality': 100.0  # Will be updated by caller
    }
    
    return summary


def store_summary(summary: Dict, supabase) -> bool:
    """
    Store summary statistics to Supabase.
    
    Args:
        summary: Summary dict
        supabase: Supabase client
    
    Returns:
        True if successful, False otherwise
    """
    try:
        response = supabase.table('tier1_granger_summary').upsert(
            [summary],
            on_conflict='symbol,test_date'
        ).execute()
        
        logger.debug(f"  ✅ Stored summary for {summary['symbol']}")
        return True
    
    except Exception as e:
        logger.error(f"  ❌ Failed to store summary for {summary['symbol']}: {str(e)}")
        return False


def generate_overall_summary(supabase) -> Dict:
    """
    Generate overall summary statistics from all Granger results.
    
    Args:
        supabase: Supabase client
    
    Returns:
        Dict with overall statistics
    """
    try:
        # Query all results from today
        today = datetime.now().strftime('%Y-%m-%d')
        
        result = supabase.table('tier1_granger_results')\
            .select('*')\
            .eq('test_date', today)\
            .execute()
        
        if not result.data:
            return {
                'total_tests': 0,
                'total_significant': 0,
                'significance_rate': 0.0,
                'stocks_processed': 0
            }
        
        df = pd.DataFrame(result.data)
        
        total = len(df)
        significant = df['is_significant'].sum()
        rate = (significant / total) * 100 if total > 0 else 0.0
        stocks = df['symbol'].nunique()
        
        return {
            'total_tests': total,
            'total_significant': significant,
            'significance_rate': round(rate, 2),
            'stocks_processed': stocks,
            'test_date': today
        }
    
    except Exception as e:
        logger.error(f"  Failed to generate overall summary: {str(e)}")
        return {
            'total_tests': 0,
            'total_significant': 0,
            'significance_rate': 0.0,
            'stocks_processed': 0,
            'error': str(e)
        }


def calculate_data_quality_score(df: pd.DataFrame, expected_days: int = 365) -> float:
    """
    Calculate data quality score (0-100) based on completeness.
    
    Args:
        df: Stock DataFrame
        expected_days: Expected number of days
    
    Returns:
        Quality score (0-100)
    """
    actual_days = len(df)
    completeness = (actual_days / expected_days) * 100
    
    # Cap at 100%
    return min(completeness, 100.0)


# Export all functions
__all__ = [
    'load_stock_data',
    'load_alternative_signals',
    'get_stock_location',
    'get_stock_sector',
    'run_granger_test',
    'merge_signals_with_stock_data',
    'store_results',
    'generate_stock_summary',
    'store_summary',
    'generate_overall_summary',
    'calculate_data_quality_score'
]
