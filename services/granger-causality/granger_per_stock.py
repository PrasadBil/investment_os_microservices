#!/usr/bin/env python3
"""
Tier 1 Granger Causality Per-Stock Validator
Investment OS v6.0 - Institutional-grade causality validation

Validates whether v5.0 manipulation patterns have statistical causality
by testing Granger relationships across 296 CSE stocks.

Usage:
    python3 granger_per_stock_2026_02_04.py --symbols all
    python3 granger_per_stock_2026_02_04.py --symbols LOLC.N0000,CTC.N0000
    python3 granger_per_stock_2026_02_04.py --test-only  # 5 test stocks only

Author: Investment OS Team
Date: February 4, 2026
"""

import os
import sys
import logging
import argparse
import json
from datetime import datetime, timedelta
import pandas as pd
from supabase import Client  # Keep type hint only
from common.database import get_supabase_client
from common.config import get_config
from typing import List, Dict, Optional

# Import our library functions
from granger_lib import (
    load_stock_data,
    load_alternative_signals,
    get_stock_sector,
    run_granger_test,
    merge_signals_with_stock_data,
    store_results,
    generate_stock_summary,
    store_summary,
    generate_overall_summary,
    calculate_data_quality_score
)

# Import configuration
from granger_config import (
    PRIMARY_TESTS,
    SECTOR_SPECIFIC_TESTS,
    MACRO_TESTS,
    TEST_STOCKS,
    STATISTICAL_CONFIG,
    EXECUTION_CONFIG,
    get_tests_for_stock,
    is_empire_stock,
    get_empire_name
)

# ============================================================================
# CONFIGURATION
# ============================================================================

_config = get_config()

# Working directory (Phase 2 migration: /opt/selenium_automation → /opt/investment-os)
WORKING_DIR = _config.WORK_DIR
LOG_DIR = _config.LOG_DIR
OUTPUT_DIR = os.path.join(WORKING_DIR, 'outputs')
REPORT_DIR = os.path.join(WORKING_DIR, 'reports')

# Create directories if they don't exist
for directory in [LOG_DIR, OUTPUT_DIR, REPORT_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging setup
timestamp = datetime.now().strftime('%Y-%m-%d')
log_file = os.path.join(LOG_DIR, f'granger_per_stock_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy loggers
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)


# ============================================================================
# MAIN FUNCTIONS
# ============================================================================

def get_stock_universe(supabase: Client, test_only: bool = False) -> List[str]:
    """
    Get list of stocks to process.
    
    Args:
        supabase: Supabase client
        test_only: If True, return only test stocks
    
    Returns:
        List of stock symbols
    """
    if test_only:
        logger.info("Using TEST STOCKS ONLY (5 stocks)")
        return TEST_STOCKS
    
    # Query all unique symbols from cse_daily_prices
    result = supabase.table('cse_daily_prices')\
        .select('symbol')\
        .execute()
    
    if not result.data:
        logger.error("No stocks found in cse_daily_prices table")
        return []
    
    # Get unique symbols
    symbols = sorted(list(set([row['symbol'] for row in result.data])))
    
    return symbols


def process_stock(symbol: str, supabase: Client) -> List[Dict]:
    """
    Run all Granger tests for a single stock.
    
    Args:
        symbol: Stock symbol
        supabase: Supabase client
    
    Returns:
        List of result dicts for all tests
    """
    all_results = []
    
    try:
        # Step 1: Load stock data
        logger.info(f"  Loading data for {symbol}...")
        stock_df = load_stock_data(symbol, supabase, days=365)
        data_quality = calculate_data_quality_score(stock_df, 365)
        
        logger.info(f"  Loaded {len(stock_df)} days (quality: {data_quality:.1f}%)")
        
        # Step 2: Run primary tests (all stocks)
        logger.info(f"  Running primary tests...")
        for test_config in PRIMARY_TESTS:
            predictor = test_config['predictor']
            target = test_config['target']
            lags = test_config['lags']
            
            # Check if columns exist
            if predictor not in stock_df.columns or target not in stock_df.columns:
                logger.debug(f"    Skipping {predictor} → {target} (columns missing)")
                continue
            
            # Run test with specified lags
            max_lag = max(lags)
            results = run_granger_test(
                stock_df, 
                predictor, 
                target, 
                max_lag=max_lag,
                min_sample=STATISTICAL_CONFIG['min_sample_size']
            )
            
            # Filter to only requested lags
            results = [r for r in results if r['lag_period'] in lags]
            all_results.extend(results)
        
        # Step 3: Run sector-specific tests
        sector = get_stock_sector(symbol)
        if sector in SECTOR_SPECIFIC_TESTS:
            logger.info(f"  Running {sector} sector tests...")
            
            for test_config in SECTOR_SPECIFIC_TESTS[sector]['tests']:
                if symbol not in SECTOR_SPECIFIC_TESTS[sector]['symbols']:
                    continue
                
                # Load alternative signals
                signal_type = test_config.get('signal_type', 'weather')
                signal_df = load_alternative_signals(symbol, supabase, signal_type, days=365)
                
                if signal_df is None or signal_df.empty:
                    logger.debug(f"    No {signal_type} signals available")
                    continue
                
                # Merge with stock data
                merged_df = merge_signals_with_stock_data(stock_df, signal_df, how='inner')
                
                if len(merged_df) < STATISTICAL_CONFIG['min_sample_size']:
                    logger.debug(f"    Insufficient merged data ({len(merged_df)} days)")
                    continue
                
                # Run test
                predictor = test_config['predictor']
                target = test_config['target']
                lags = test_config['lags']
                
                if predictor not in merged_df.columns or target not in merged_df.columns:
                    logger.debug(f"    Skipping {predictor} → {target} (columns missing)")
                    continue
                
                max_lag = max(lags)
                results = run_granger_test(
                    merged_df,
                    predictor,
                    target,
                    max_lag=max_lag,
                    min_sample=STATISTICAL_CONFIG['min_sample_size']
                )
                
                results = [r for r in results if r['lag_period'] in lags]
                all_results.extend(results)
        
        # Step 4: Run macro tests (all stocks)
        logger.info(f"  Running macro tests...")
        macro_df = load_alternative_signals(symbol, supabase, 'macro', days=365)
        
        if macro_df is not None and not macro_df.empty:
            merged_df = merge_signals_with_stock_data(stock_df, macro_df, how='inner')
            
            if len(merged_df) >= STATISTICAL_CONFIG['min_sample_size']:
                for test_config in MACRO_TESTS:
                    predictor = test_config['predictor']
                    target = test_config['target']
                    lags = test_config['lags']
                    
                    if predictor not in merged_df.columns or target not in merged_df.columns:
                        logger.debug(f"    Skipping {predictor} → {target} (columns missing)")
                        continue
                    
                    max_lag = max(lags)
                    results = run_granger_test(
                        merged_df,
                        predictor,
                        target,
                        max_lag=max_lag,
                        min_sample=STATISTICAL_CONFIG['min_sample_size']
                    )
                    
                    results = [r for r in results if r['lag_period'] in lags]
                    all_results.extend(results)
        
        # Step 5: Log summary
        significant_count = sum(1 for r in all_results if r['is_significant'])
        logger.info(f"  ✅ {symbol}: {significant_count}/{len(all_results)} significant "
                   f"({100*significant_count/len(all_results):.1f}%)" if all_results else f"  ⚠️  {symbol}: No tests completed")
        
        return all_results
    
    except Exception as e:
        logger.error(f"  ❌ {symbol} failed: {str(e)}", exc_info=True)
        return []


def save_json_backup(results: Dict, timestamp: str):
    """Save JSON backup of all results."""
    output_file = os.path.join(OUTPUT_DIR, f'granger_results_{timestamp}.json')
    
    try:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"✅ JSON backup saved: {output_file}")
    except Exception as e:
        logger.error(f"Failed to save JSON backup: {str(e)}")


def generate_txt_report(summary: Dict, timestamp: str):
    """Generate human-readable text report."""
    report_file = os.path.join(REPORT_DIR, f'granger_summary_{timestamp}.txt')
    
    try:
        with open(report_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("TIER 1 GRANGER CAUSALITY VALIDATION - SUMMARY REPORT\n")
            f.write(f"Date: {timestamp}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write("OVERALL STATISTICS:\n")
            f.write(f"  Stocks Processed: {summary.get('stocks_processed', 0)}\n")
            f.write(f"  Total Tests: {summary.get('total_tests', 0)}\n")
            f.write(f"  Significant Relationships: {summary.get('total_significant', 0)}\n")
            f.write(f"  Significance Rate: {summary.get('significance_rate', 0):.2f}%\n")
            f.write("\n")
            
            f.write("EXPECTED PERFORMANCE:\n")
            f.write(f"  Target Significance Rate: 15-20%\n")
            f.write(f"  Actual vs Target: {'✅ PASS' if 15 <= summary.get('significance_rate', 0) <= 25 else '⚠️  REVIEW'}\n")
            f.write("\n")
            
            f.write("=" * 80 + "\n")
        
        logger.info(f"✅ Text report saved: {report_file}")
    except Exception as e:
        logger.error(f"Failed to save text report: {str(e)}")


def main():
    """Main execution flow."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Tier 1 Granger Causality Validator')
    parser.add_argument('--symbols', type=str, default='all', 
                       help='Comma-separated symbols or "all" (default: all)')
    parser.add_argument('--test-only', action='store_true',
                       help='Run on 5 test stocks only')
    args = parser.parse_args()
    
    # Print header
    logger.info("=" * 80)
    logger.info("TIER 1 GRANGER CAUSALITY VALIDATOR - START")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Working directory: {WORKING_DIR}")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 80)
    
    try:
        # Initialize Supabase
        logger.info("\nInitializing Supabase connection...")
        supabase: Client = get_supabase_client()
        logger.info("✅ Supabase connected")
        
        # Get stock universe
        logger.info("\nLoading stock universe...")
        
        if args.test_only:
            symbols = TEST_STOCKS
        elif args.symbols == 'all':
            symbols = get_stock_universe(supabase, test_only=False)
        else:
            symbols = [s.strip() for s in args.symbols.split(',')]
        
        logger.info(f"📊 Stock universe: {len(symbols)} symbols")
        if args.test_only:
            logger.info(f"   TEST MODE: {', '.join(symbols)}")
        
        # Main processing loop
        logger.info("\n" + "=" * 80)
        logger.info("PROCESSING STOCKS")
        logger.info("=" * 80)
        
        results_count = 0
        stocks_processed = 0
        stocks_failed = 0
        
        for i, symbol in enumerate(symbols, 1):
            # Progress indicator
            if i % EXECUTION_CONFIG['progress_interval'] == 0:
                logger.info(f"\n--- Progress: {i}/{len(symbols)} ({100*i/len(symbols):.1f}%) ---")
            
            logger.info(f"\n[{i}/{len(symbols)}] Processing {symbol}...")
            
            # Flag if empire stock
            if is_empire_stock(symbol):
                empire_name = get_empire_name(symbol)
                logger.info(f"  🏛️  EMPIRE STOCK: {empire_name}")
            
            try:
                # Run all Granger tests for this stock
                stock_results = process_stock(symbol, supabase)
                
                if not stock_results:
                    logger.warning(f"  ⚠️  {symbol}: No valid results")
                    stocks_failed += 1
                    continue
                
                results_count += len(stock_results)
                stocks_processed += 1
                
                # Store results to Supabase
                store_results(stock_results, symbol, supabase)
                
                # Generate and store summary for this stock
                summary = generate_stock_summary(symbol, stock_results, supabase)
                summary['overall_data_quality'] = 100.0  # Will be calculated properly
                store_summary(summary, supabase)
                
            except Exception as e:
                logger.error(f"  ❌ {symbol} failed: {str(e)}", exc_info=True)
                stocks_failed += 1
                continue
        
        # Generate overall summary
        logger.info("\n" + "=" * 80)
        logger.info("GENERATING OVERALL SUMMARY")
        logger.info("=" * 80)
        
        overall_summary = generate_overall_summary(supabase)
        overall_summary['stocks_processed'] = stocks_processed
        overall_summary['stocks_failed'] = stocks_failed
        
        logger.info(f"\n✅ VALIDATION COMPLETE")
        logger.info(f"   Stocks Processed: {stocks_processed}/{len(symbols)}")
        logger.info(f"   Stocks Failed: {stocks_failed}")
        logger.info(f"   Total Tests: {overall_summary['total_tests']}")
        logger.info(f"   Significant Relationships: {overall_summary['total_significant']}")
        logger.info(f"   Significance Rate: {overall_summary['significance_rate']:.2f}%")
        
        # Check if within expected range
        sig_rate = overall_summary['significance_rate']
        if 15 <= sig_rate <= 25:
            logger.info(f"   ✅ Significance rate within expected range (15-20%)")
        else:
            logger.warning(f"   ⚠️  Significance rate outside expected range (15-20%)")
        
        # Save outputs
        logger.info("\n" + "=" * 80)
        logger.info("SAVING OUTPUTS")
        logger.info("=" * 80)
        
        timestamp = datetime.now().strftime('%Y-%m-%d')
        
        if EXECUTION_CONFIG['save_json_backup']:
            save_json_backup(overall_summary, timestamp)
        
        if EXECUTION_CONFIG['generate_summary_txt']:
            generate_txt_report(overall_summary, timestamp)
        
        logger.info("\n" + "=" * 80)
        logger.info("TIER 1 GRANGER VALIDATION COMPLETE")
        logger.info("=" * 80)
        
        return 0
    
    except Exception as e:
        logger.error(f"\n❌ FATAL ERROR: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
