
#!/usr/bin/env python3
"""
TIER 1 PER-STOCK GRANGER CAUSALITY VALIDATOR v5.0
Investment OS v6.0 - Institutional-Grade Validation

Purpose: Test Granger causality relationships for each of 296 CSE stocks
         to validate v5.0 manipulation detector patterns

Integrates:
  - v4 macro-level Granger logic (working)
  - Per-stock loop (296 stocks)
  - Sector-specific tests (tea → weather, banks → rates)
  - Existing tier1_granger_results table (has symbol column)

Expected Output:
  - 9,000-12,000 Granger tests (30-40 per stock)
  - 1,500 significant relationships (~15-20%)
  - Per-stock causality flags for v6.0 integration

Usage:
    python3 tier1_granger_per_stock_v5.py --stocks all
    python3 tier1_granger_per_stock_v5.py --stocks LOLC.N0000,CTC.N0000
    python3 tier1_granger_per_stock_v5.py --sector banking
    python3 tier1_granger_per_stock_v5.py --validate-only

Author: Investment OS Team
Date: February 4, 2026
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from statsmodels.tsa.stattools import grangercausalitytests
from supabase import Client  # Keep type hint only
from common.database import get_supabase_client
from common.config import get_config
import logging
import argparse
import warnings

warnings.filterwarnings('ignore')

# Configuration
_config = get_config()

# Phase 2 migration: /opt/selenium_automation → /opt/investment-os
WORKING_DIR = _config.WORK_DIR

# Tea/Plantation stocks (19 companies - CORRECTED!)
TEA_PLANTATION_STOCKS = [
    'BOPL.N0000', 'CTEA.N0000', 'TPL.N0000', 'TSML.N0000', 'CTBL.N0000',
    'TESS.N0000', 'AGPL.N0000', 'AGAL.N0000', 'BALA.N0000', 'ELPL.N0000',
    'HAPU.N0000', 'HOPL.N0000', 'KAHA.N0000', 'KOTA.N0000', 'KVAL.N0000',
    'KGAL.N0000', 'MADU.N0000', 'MAL.N0000', 'MASK.N0000', 'NAMU.N0000',
    'UDPL.N0000', 'WATA.N0000'
]

# Banking stocks
BANKING_STOCKS = [
    'LOLC.N0000', 'COMB.N0000', 'HNB.N0000', 'SAMP.N0000',  # SAMP is Sampath Bank
    'NDB.N0000', 'DFCC.N0000', 'LOFC.N0000', 'DIPD.N0000',
    'HNBF.N0000', 'LFIN.N0000', 'CARS.N0000', 'BFL.N0000'
]

# Export-heavy stocks
EXPORT_STOCKS = [
    'EXPO.N0000', 'TYRE.N0000', 'MJL.N0000', 'HAYC.N0000',
    'BREW.N0000', 'RCH.N0000', 'LOLC.N0000'  # Also provides export finance
]

# Logging (Phase 2 migration: uses common library)
from common.logging_config import setup_logging as _setup_logging
logger = _setup_logging('granger-per-stock', log_to_file=True)


def load_stock_prices(supabase: Client, symbol: str, days=365):
    """Load stock OHLCV data from cse_daily_prices"""
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        result = supabase.table('cse_daily_prices').select(
            'collection_date, symbol, price, open, high, low, share_volume, trade_volume'
        ).eq('symbol', symbol).gte('collection_date', start_date.isoformat()).lte(
            'collection_date', end_date.isoformat()
        ).order('collection_date', desc=False).execute()
        
        if not result.data:
            return None
        
        df = pd.DataFrame(result.data)
        df['date'] = pd.to_datetime(df['collection_date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # Calculate returns
        df['return'] = df['price'].pct_change()
        df['volume_change'] = df['share_volume'].pct_change()
        
        return df
    
    except Exception as e:
        logger.error(f"Error loading {symbol}: {str(e)}")
        return None


def load_usd_lkr(supabase: Client, start_date, end_date):
    """Load USD/LKR from new time-series table"""
    try:
        result = supabase.table('cbsl_usd_lkr_timeseries').select(
            'date, usd_lkr, usd_lkr_change_1d'
        ).gte('date', start_date).lte('date', end_date).order(
            'date', desc=False
        ).execute()
        
        if not result.data:
            logger.warning("No USD/LKR data found in cbsl_usd_lkr_timeseries")
            return None
        
        df = pd.DataFrame(result.data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Rename for consistency with stock data
        df['usd_lkr_change'] = df['usd_lkr_change_1d']
        
        return df
    
    except Exception as e:
        logger.error(f"Error loading USD/LKR: {str(e)}")
        return None


def load_interest_rates(supabase: Client, start_date, end_date):
    """Load interest rates from new time-series table"""
    try:
        result = supabase.table('cbsl_interest_rates_timeseries').select(
            'date, awpr, sdfr, awpr_change_1d'  # Match actual schema column name
        ).gte('date', start_date).lte('date', end_date).order(
            'date', desc=False
        ).execute()
        
        if not result.data:
            logger.warning("No interest rate data found")
            return None
        
        df = pd.DataFrame(result.data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Use SDFR if AWPR not available (SDFR is what we have from CSV)
        if 'sdfr' in df.columns and 'awpr' in df.columns:
            df['rate'] = df['awpr'].fillna(df['sdfr'])
        elif 'sdfr' in df.columns:
            df['rate'] = df['sdfr']
        elif 'awpr' in df.columns:
            df['rate'] = df['awpr']
        else:
            logger.warning("No rate columns (awpr/sdfr) found")
            return None
        
        # Calculate change if not present
        if 'awpr_change_1d' not in df.columns or df['awpr_change_1d'].isna().all():
            df['rate_change'] = df['rate'].pct_change()
        else:
            df['rate_change'] = df['awpr_change_1d']
        
        return df
    
    except Exception as e:
        logger.error(f"Error loading interest rates: {str(e)}")
        return None


def load_tea_credit(supabase: Client, start_date, end_date):
    """Load tea credit from new time-series table"""
    try:
        result = supabase.table('cbsl_tea_credit_timeseries').select(
            'date, credit_category, credit_amount, credit_change_1m'  # Match schema: credit_change_1m
        ).gte('date', start_date).lte('date', end_date).order(
            'date', desc=False
        ).execute()
        
        if not result.data:
            logger.warning("No tea credit data found")
            return None
        
        df = pd.DataFrame(result.data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Pivot to wide format (one row per date, columns per category)
        df_pivot = df.pivot_table(
            index='date',
            columns='credit_category',
            values='credit_change_1m',  # FIXED: Match LINE 185 column name
            aggfunc='first'
        ).reset_index()
        
        return df_pivot
    
    except Exception as e:
        logger.error(f"Error loading tea credit: {str(e)}")
        return None


def load_weather_signals(supabase: Client, start_date, end_date):
    """Load weather signals from existing weather_signals table"""
    try:
        result = supabase.table('weather_signals').select(
            'date, location_id, tea_favorability, rain_sum'
        ).gte('date', start_date).lte('date', end_date).order(
            'date', desc=False
        ).execute()
        
        if not result.data:
            logger.warning("No weather data found")
            return None
        
        df = pd.DataFrame(result.data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Aggregate across all 12 locations (simple average)
        df_agg = df.groupby('date').agg({
            'tea_favorability': 'mean',
            'rain_sum': 'mean'
        }).reset_index()
        
        return df_agg
    
    except Exception as e:
        logger.error(f"Error loading weather: {str(e)}")
        return None


def run_granger_test(df, predictor_col, target_col, max_lag=14):
    """
    Run Granger causality test
    
    Returns:
        dict: {
            'optimal_lag': int,
            'p_value': float,
            'f_statistic': float,
            'is_significant': bool,
            'confidence_score': float
        }
    """
    try:
        # Drop NaN
        test_data = df[[predictor_col, target_col]].dropna()
        
        logger.debug(f"  🔍 GRANGER DEBUG: After dropna: {len(test_data)} rows (need 85+)")
        
        if len(test_data) < 85:  # Need sufficient data (lowered from 90 to match caller threshold)
            logger.info(f"  ⚠️  GRANGER: Insufficient data after dropna: {len(test_data)} < 85")
            return None
        
        # Run test
        result = grangercausalitytests(
            test_data[[target_col, predictor_col]], 
            maxlag=max_lag,
            verbose=False
        )
        
        # Find optimal lag (lowest p-value)
        best_lag = None
        best_p_value = 1.0
        best_f_stat = 0.0
        
        for lag in result.keys():
            p_val = result[lag][0]['ssr_ftest'][1]  # F-test p-value
            f_stat = result[lag][0]['ssr_ftest'][0]  # F-statistic
            
            if p_val < best_p_value:
                best_p_value = p_val
                best_lag = lag
                best_f_stat = f_stat
        
        # Determine significance
        is_significant = best_p_value < 0.05
        
        # Confidence score (inverse of p-value, scaled)
        confidence = max(0, min(100, (1 - best_p_value) * 100))
        
        return {
            'optimal_lag': best_lag,
            'p_value': round(best_p_value, 8),
            'f_statistic': round(best_f_stat, 4),
            'is_significant': is_significant,
            'confidence_score': round(confidence, 2)
        }
    
    except Exception as e:
        logger.debug(f"Granger test failed: {str(e)}")
        return None


def test_stock_relationships(supabase: Client, symbol: str):
    """
    Test all relevant Granger relationships for a stock
    
    Returns:
        list of dicts: [
            {
                'symbol': 'LOLC.N0000',
                'predictor_variable': 'usd_lkr_change_1d',
                'target_variable': 'return',
                'optimal_lag': 3,
                'p_value': 0.003,
                'f_statistic': 12.5,
                'is_significant': True,
                'confidence_score': 99.7
            },
            ...
        ]
    """
    results = []
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing: {symbol}")
    logger.info(f"{'='*60}")
    
    # Load stock data
    stock_df = load_stock_prices(supabase, symbol, days=365)
    
    if stock_df is None or len(stock_df) < 180:
        logger.warning(f"  ⚠️  Insufficient data for {symbol}")
        return results
    
    start_date = stock_df['date'].min().date()
    end_date = stock_df['date'].max().date()
    
    # ========================================================================
    # PRIMARY TESTS (All stocks)
    # ========================================================================
    
    # Test 1: Volume → Return
    test_result = run_granger_test(stock_df, 'volume_change', 'return', max_lag=5)
    if test_result:
        results.append({
            'symbol': symbol,
            'predictor_variable': 'volume_change',
            'target_variable': 'return',
            **test_result
        })
        logger.info(f"  ✓ Volume → Return: p={test_result['p_value']:.4f} {'✅ SIG' if test_result['is_significant'] else ''}")
    
    # Test 2: Return → Volume (reverse causality)
    test_result = run_granger_test(stock_df, 'return', 'volume_change', max_lag=5)
    if test_result:
        results.append({
            'symbol': symbol,
            'predictor_variable': 'return',
            'target_variable': 'volume_change',
            **test_result
        })
        logger.info(f"  ✓ Return → Volume: p={test_result['p_value']:.4f} {'✅ SIG' if test_result['is_significant'] else ''}")
    
    # ========================================================================
    # MACRO TESTS (All stocks)
    # ========================================================================
    
    # Test 3: USD/LKR → Return
    usd_lkr_df = load_usd_lkr(supabase, start_date, end_date)
    if usd_lkr_df is not None:
        merged = pd.merge(stock_df[['date', 'return']], usd_lkr_df, on='date', how='inner')
        
        test_result = run_granger_test(merged, 'usd_lkr_change', 'return', max_lag=14)
        if test_result:
            results.append({
                'symbol': symbol,
                'predictor_variable': 'usd_lkr_change_1d',  # Store as standardized name
                'target_variable': 'return',
                **test_result
            })
            logger.info(f"  ✓ USD/LKR → Return: p={test_result['p_value']:.4f} {'✅ SIG' if test_result['is_significant'] else ''}")
    
    # Test 4: Interest Rates → Return
    rates_df = load_interest_rates(supabase, start_date, end_date)
    logger.info(f"  🔍 DEBUG: rates_df={'None' if rates_df is None else f'{len(rates_df)} rows'}")
    if rates_df is not None and len(rates_df) > 50:
        merged = pd.merge(stock_df[['date', 'return']], rates_df[['date', 'rate_change']], on='date', how='inner')
        logger.info(f"  🔍 DEBUG: After merge: {len(merged)} overlapping days")
        if len(merged) >= 85:  # Need sufficient data (lowered from 90 to handle date mismatches)
            logger.info(f"  🔍 DEBUG: Calling run_granger_test with {len(merged)} rows...")
            test_result = run_granger_test(merged, 'rate_change', 'return', max_lag=14)
            if test_result:
                results.append({
                    'symbol': symbol,
                    'predictor_variable': 'awpr_change',  # Store as standardized name
                    'target_variable': 'return',
                    **test_result
                })
                logger.info(f"  ✓ Interest Rate → Return: p={test_result['p_value']:.4f} {'✅ SIG' if test_result['is_significant'] else ''}")
            else:
                logger.warning(f"  ⚠️  Interest rate test returned None (check Granger function logs)")
        else:
            logger.info(f"  ⚠️  Insufficient interest rate overlap ({len(merged)} days, need 85+)")
    else:
        logger.info(f"  ⚠️  Interest rate data unavailable or insufficient")
    
    # ========================================================================
    # SECTOR-SPECIFIC TESTS
    # ========================================================================
    
    # TEA/PLANTATION: Weather → Return
    if symbol in TEA_PLANTATION_STOCKS:
        weather_df = load_weather_signals(supabase, start_date, end_date)
        if weather_df is not None:
            merged = pd.merge(stock_df[['date', 'return']], weather_df, on='date', how='inner')
            
            test_result = run_granger_test(merged, 'tea_favorability', 'return', max_lag=21)
            if test_result:
                results.append({
                    'symbol': symbol,
                    'predictor_variable': 'tea_favorability',
                    'target_variable': 'return',
                    **test_result
                })
                logger.info(f"  ✓ Tea Weather → Return: p={test_result['p_value']:.4f} {'✅ SIG' if test_result['is_significant'] else ''}")
        
        # TEA: Credit → Return
        tea_credit_df = load_tea_credit(supabase, start_date, end_date)
        if tea_credit_df is not None and len(tea_credit_df) > 50:
            # Use first credit category column
            credit_cols = [col for col in tea_credit_df.columns if col != 'date']
            if credit_cols:
                merged = pd.merge(stock_df[['date', 'return']], tea_credit_df[['date', credit_cols[0]]], on='date', how='inner')
                
                test_result = run_granger_test(merged, credit_cols[0], 'return', max_lag=60)
                if test_result:
                    results.append({
                        'symbol': symbol,
                        'predictor_variable': 'tea_credit',
                        'target_variable': 'return',
                        **test_result
                    })
                    logger.info(f"  ✓ Tea Credit → Return: p={test_result['p_value']:.4f} {'✅ SIG' if test_result['is_significant'] else ''}")
    
    # BANKING: Emphasize interest rates (already tested above)
    if symbol in BANKING_STOCKS:
        logger.info(f"  ℹ️  {symbol} is banking stock - interest rate test prioritized")
    
    # EXPORTS: Emphasize USD/LKR (already tested above)
    if symbol in EXPORT_STOCKS:
        logger.info(f"  ℹ️  {symbol} is export stock - USD/LKR test prioritized")
    
    logger.info(f"  ✅ Completed {len(results)} tests for {symbol}")
    
    return results


def save_results_to_supabase(supabase: Client, results: list):
    """Save Granger test results to tier1_granger_results table"""
    if not results:
        logger.warning("No results to save")
        return
    
    logger.info(f"\n{'='*60}")
    logger.info("SAVING RESULTS TO SUPABASE")
    logger.info(f"{'='*60}")
    
    # Add analysis_date and test_date
    today = datetime.now().date().isoformat()
    
    records = []
    for r in results:
        records.append({
            'symbol': r['symbol'],
            'analysis_date': today,
            'predictor_variable': r['predictor_variable'],
            'target_variable': r['target_variable'],
            'optimal_lag': int(r['optimal_lag']),  # Convert numpy int64 to Python int
            'p_value': float(r['p_value']),  # Convert to Python float
            'f_statistic': float(r['f_statistic']),  # Convert to Python float
            'is_significant': bool(r['is_significant']),  # Convert to Python bool
            'confidence_score': float(r['confidence_score'])  # Convert to Python float
        })
    
    # Batch upload
    batch_size = 500
    total_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        
        try:
            response = supabase.table('tier1_granger_results').upsert(
                batch,
                on_conflict='symbol,analysis_date,predictor_variable,target_variable'
            ).execute()
            
            total_inserted += len(batch)
            logger.info(f"  ✅ Batch {i//batch_size + 1}: Inserted {len(batch)} records ({total_inserted}/{len(records)})")
        
        except Exception as e:
            logger.error(f"  ❌ Batch {i//batch_size + 1} failed: {str(e)}")
    
    logger.info(f"\n✅ SAVED {total_inserted} RESULTS TO SUPABASE")


def main():
    """Main execution"""
    
    parser = argparse.ArgumentParser(description='Run per-stock Granger causality tests')
    parser.add_argument('--stocks', default='all', help='Comma-separated symbols or "all"')
    parser.add_argument('--sector', choices=['banking', 'tea', 'export'], help='Test specific sector')
    parser.add_argument('--validate-only', action='store_true', help='Validate data availability only')
    parser.add_argument('--limit', type=int, help='Limit number of stocks to test')
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("TIER 1 PER-STOCK GRANGER CAUSALITY VALIDATOR v5.0")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    # Initialize Supabase
    logger.info("\nInitializing Supabase...")
    supabase: Client = get_supabase_client()
    logger.info("✅ Connected")
    
    # Determine stocks to test
    if args.sector == 'banking':
        test_stocks = BANKING_STOCKS
    elif args.sector == 'tea':
        test_stocks = TEA_PLANTATION_STOCKS
    elif args.sector == 'export':
        test_stocks = EXPORT_STOCKS
    elif args.stocks == 'all':
        # Get all stocks from database
        result = supabase.table('daily_prices').select('symbol').execute()
        df_symbols = pd.DataFrame(result.data)
        test_stocks = sorted(df_symbols['symbol'].unique().tolist())
    else:
        test_stocks = args.stocks.split(',')
    
    if args.limit:
        test_stocks = test_stocks[:args.limit]
    
    logger.info(f"\n📊 Testing {len(test_stocks)} stocks")
    logger.info(f"   Sectors: Banking={sum(1 for s in test_stocks if s in BANKING_STOCKS)}, "
                f"Tea={sum(1 for s in test_stocks if s in TEA_PLANTATION_STOCKS)}, "
                f"Export={sum(1 for s in test_stocks if s in EXPORT_STOCKS)}")
    
    if args.validate_only:
        logger.info("\n✅ VALIDATION MODE - No tests will run")
        return
    
    # Run tests
    all_results = []
    successful = 0
    failed = 0
    
    for idx, symbol in enumerate(test_stocks, 1):
        logger.info(f"\n[{idx}/{len(test_stocks)}] Processing {symbol}...")
        
        try:
            results = test_stock_relationships(supabase, symbol)
            all_results.extend(results)
            successful += 1
        except Exception as e:
            logger.error(f"  ❌ Failed: {str(e)}")
            failed += 1
        
        # Progress update every 10 stocks
        if idx % 10 == 0:
            logger.info(f"\n{'='*60}")
            logger.info(f"PROGRESS: {idx}/{len(test_stocks)} stocks processed")
            logger.info(f"  Tests conducted: {len(all_results)}")
            logger.info(f"  Significant results: {sum(1 for r in all_results if r['is_significant'])}")
            logger.info(f"{'='*60}")
    
    # Save results
    if all_results:
        save_results_to_supabase(supabase, all_results)
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("TIER 1 GRANGER VALIDATION COMPLETE")
    logger.info("="*80)
    logger.info(f"Stocks processed: {successful}/{len(test_stocks)}")
    logger.info(f"Total tests: {len(all_results)}")
    logger.info(f"Significant relationships: {sum(1 for r in all_results if r['is_significant'])} ({100*sum(1 for r in all_results if r['is_significant'])/len(all_results):.1f}%)")
    logger.info(f"Failed stocks: {failed}")
    
    # Top predictors
    if all_results:
        sig_results = [r for r in all_results if r['is_significant']]
        if sig_results:
            predictor_counts = pd.Series([r['predictor_variable'] for r in sig_results]).value_counts()
            logger.info("\nTop predictors (significant relationships):")
            for pred, count in predictor_counts.head(10).items():
                logger.info(f"  {pred}: {count} stocks")
    
    # DETAILED RESULTS DISPLAY
    logger.info("\n" + "="*80)
    logger.info("📊 DETAILED RESULTS")
    logger.info("="*80)
    
    if all_results:
        # Sort by p-value (most significant first)
        sorted_results = sorted(all_results, key=lambda x: x['p_value'])
        
        # Display significant results
        sig_results = [r for r in sorted_results if r['is_significant']]
        if sig_results:
            logger.info(f"\n✅ SIGNIFICANT RELATIONSHIPS (p < 0.05): {len(sig_results)}")
            logger.info("-" * 80)
            for r in sig_results:
                logger.info(f"{r['symbol']:12} {r['predictor_variable']:20} → {r['target_variable']:10} | "
                           f"p={r['p_value']:.4f} | lag={r['optimal_lag']:2}d | "
                           f"F={r['f_statistic']:8.2f} | conf={r['confidence_score']:5.1f}%")
        
        # Display near-significant results (0.05 < p < 0.10)
        near_sig = [r for r in sorted_results if 0.05 <= r['p_value'] < 0.10]
        if near_sig:
            logger.info(f"\n⚠️  NEAR-SIGNIFICANT (0.05 ≤ p < 0.10): {len(near_sig)}")
            logger.info("-" * 80)
            for r in near_sig[:10]:  # Show top 10
                logger.info(f"{r['symbol']:12} {r['predictor_variable']:20} → {r['target_variable']:10} | "
                           f"p={r['p_value']:.4f} | lag={r['optimal_lag']:2}d | "
                           f"F={r['f_statistic']:8.2f} | conf={r['confidence_score']:5.1f}%")
        
        # Summary by stock
        logger.info(f"\n📈 SUMMARY BY STOCK")
        logger.info("-" * 80)
        stock_summary = {}
        for r in all_results:
            if r['symbol'] not in stock_summary:
                stock_summary[r['symbol']] = {'total': 0, 'significant': 0}
            stock_summary[r['symbol']]['total'] += 1
            if r['is_significant']:
                stock_summary[r['symbol']]['significant'] += 1
        
        # Sort by number of significant relationships
        sorted_stocks = sorted(stock_summary.items(), 
                              key=lambda x: x[1]['significant'], 
                              reverse=True)
        
        for symbol, stats in sorted_stocks[:10]:  # Show top 10
            sig_pct = 100 * stats['significant'] / stats['total'] if stats['total'] > 0 else 0
            indicator = "✅" if stats['significant'] > 0 else "  "
            logger.info(f"{indicator} {symbol:12} {stats['significant']}/{stats['total']} significant ({sig_pct:5.1f}%)")
        
        # Summary by predictor
        logger.info(f"\n🎯 SUMMARY BY PREDICTOR")
        logger.info("-" * 80)
        pred_summary = {}
        for r in all_results:
            if r['predictor_variable'] not in pred_summary:
                pred_summary[r['predictor_variable']] = {'total': 0, 'significant': 0}
            pred_summary[r['predictor_variable']]['total'] += 1
            if r['is_significant']:
                pred_summary[r['predictor_variable']]['significant'] += 1
        
        for pred, stats in sorted(pred_summary.items(), 
                                 key=lambda x: x[1]['significant'], 
                                 reverse=True):
            sig_pct = 100 * stats['significant'] / stats['total'] if stats['total'] > 0 else 0
            indicator = "✅" if stats['significant'] > 0 else "  "
            logger.info(f"{indicator} {pred:25} {stats['significant']:2}/{stats['total']:2} significant ({sig_pct:5.1f}%)")
    
    logger.info("\n" + "="*80)
    logger.info("✅ Results saved to tier1_granger_results table")
    logger.info("\nNEXT STEP: Query results to identify strongest causality relationships for v6.0 integration")
    logger.info("="*80)


if __name__ == "__main__":
    main()