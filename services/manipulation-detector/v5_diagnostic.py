
#!/usr/bin/env python3
"""
v5.0 Manipulation Detector - Full Diagnostic
Analyzes why no patterns are being detected

Migration: Phase 2 (Feb 2026)
- Replaced: sys.path.insert hack → PYTHONPATH (configured in Phase 1)
- Replaced: cse_data_loader imports → common library
- Original: /opt/selenium_automation/v5_diagnostic.py
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# === Investment OS Common Library (Phase 2 Migration) ===
from common.database import get_supabase_client
from common.data_loader import load_stock_data

print("=" * 80)
print("v5.0 DETECTOR FULL DIAGNOSTIC")
print(f"Date: {datetime.now()}")
print("=" * 80)

# =============================================================================
# STEP 1: DATA AVAILABILITY CHECK
# =============================================================================

print("\nSTEP 1: Data Availability Check")
print("-" * 80)

sb = get_supabase_client()

# Check daily_prices table
result = sb.table('daily_prices').select('symbol').execute()
if result.data:
    unique_symbols = list(set([r['symbol'] for r in result.data]))
    print(f"Unique stocks in daily_prices: {len(unique_symbols)}")
else:
    print("No data in daily_prices table!")
    sys.exit(1)

# Check latest date
result = sb.table('daily_prices').select('date').order('date', desc=True).limit(1).execute()
latest_date = result.data[0]['date'] if result.data else None
print(f"Latest data date: {latest_date}")

if latest_date:
    latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
    days_old = (datetime.now() - latest_dt).days
    print(f"   Data age: {days_old} days")

# =============================================================================
# STEP 2: TEST STOCK DATA LOADING
# =============================================================================

print("\nSTEP 2: Test Stock Data Loading")
print("-" * 80)

test_stocks = ['LOLC.N0000', 'COMB.N0000', 'DIMO.N0000', 'ASPH.N0000']

loaded_stocks = []
for symbol in test_stocks:
    print(f"\nTesting {symbol}...")
    df = load_stock_data(symbol, days=300, supabase=sb)

    if df is not None and len(df) > 0:
        print(f"  Loaded {len(df)} days")
        print(f"     Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"     Price range: Rs {df['close'].min():.2f} to Rs {df['close'].max():.2f}")
        loaded_stocks.append((symbol, df))
    else:
        print(f"  Failed to load")

if not loaded_stocks:
    print("\nCould not load any test stocks!")
    sys.exit(1)

# =============================================================================
# STEP 3: MANUAL PATTERN DETECTION ON ONE STOCK
# =============================================================================

print("\nSTEP 3: Manual Pattern Detection")
print("-" * 80)

test_symbol, test_df = loaded_stocks[0]
print(f"\nAnalyzing {test_symbol} manually...")

if len(test_df) >= 90:
    # Calculate support and resistance
    support = test_df['close'].rolling(90).min().iloc[-1]
    resistance = test_df['close'].rolling(90).max().iloc[-1]
    current = test_df['close'].iloc[-1]

    print(f"\n   90-day Support: Rs {support:.2f}")
    print(f"   90-day Resistance: Rs {resistance:.2f}")
    print(f"   Current Price: Rs {current:.2f}")

    # Calculate metrics
    distance_to_support = abs(current - support) / support
    expected_return = (resistance - current) / current

    print(f"\n   Distance to support: {distance_to_support * 100:.1f}%")
    print(f"   Expected return: {expected_return * 100:.1f}%")

    # Check thresholds
    print("\n   Threshold checks:")

    PRICE_PROXIMITY = 0.05  # 5%
    MIN_RETURN = 0.05  # 5%
    MAX_RETURN = 2.00  # 200%

    if distance_to_support <= PRICE_PROXIMITY:
        print(f"   Price proximity OK ({distance_to_support*100:.1f}% <= 5%)")
    else:
        print(f"   Price too far from support ({distance_to_support*100:.1f}% > 5%)")
        print(f"      SOLUTION: Increase threshold to {distance_to_support*100:.0f}%+")

    if MIN_RETURN <= expected_return <= MAX_RETURN:
        print(f"   Return in valid range ({expected_return*100:.1f}%)")
    elif expected_return < MIN_RETURN:
        print(f"   Return too low ({expected_return*100:.1f}% < 5%)")
        print(f"      SOLUTION: Lower MIN_RETURN to {expected_return*100:.0f}%")
    else:
        print(f"   Return too high ({expected_return*100:.1f}% > 200%)")

    # Count support tests
    support_threshold = support * 1.02  # Within 2% of support
    tests = (test_df['close'] <= support_threshold).sum()
    print(f"\n   Support tests: {tests}x (price within 2% of support)")

    if tests >= 3:
        print(f"   Sufficient tests ({tests} >= 3)")
    else:
        print(f"   Too few tests ({tests} < 3)")
else:
    print(f"Not enough data ({len(test_df)} days < 90 required)")

# =============================================================================
# STEP 4: SCAN SAMPLE STOCKS
# =============================================================================

print("\nSTEP 4: Simulate Full Universe Scan")
print("-" * 80)

print("\nScanning sample of 10 stocks...")

# Get 10 random stocks
import random
sample_stocks = random.sample(unique_symbols[:50], min(10, len(unique_symbols)))

passing_count = 0
failing_reasons = {
    'too_far_from_support': 0,
    'return_too_low': 0,
    'return_too_high': 0,
    'not_enough_data': 0,
    'too_few_tests': 0
}

for symbol in sample_stocks:
    df = load_stock_data(symbol, days=300, supabase=sb)

    if df is None or len(df) < 90:
        failing_reasons['not_enough_data'] += 1
        continue

    # Calculate metrics
    support = df['close'].rolling(90).min().iloc[-1]
    resistance = df['close'].rolling(90).max().iloc[-1]
    current = df['close'].iloc[-1]

    distance = abs(current - support) / support
    ret = (resistance - current) / current
    tests = (df['close'] <= support * 1.02).sum()

    # Check thresholds
    if distance > 0.05:
        failing_reasons['too_far_from_support'] += 1
    elif ret < 0.05:
        failing_reasons['return_too_low'] += 1
    elif ret > 2.00:
        failing_reasons['return_too_high'] += 1
    elif tests < 3:
        failing_reasons['too_few_tests'] += 1
    else:
        passing_count += 1
        print(f"   {symbol}: Would pass (+{ret*100:.1f}%)")

print(f"\nResults from {len(sample_stocks)} stock sample:")
print(f"   Passing: {passing_count}")
print(f"   Failing: {sum(failing_reasons.values())}")
print(f"\n   Failure reasons:")
for reason, count in failing_reasons.items():
    if count > 0:
        print(f"   - {reason.replace('_', ' ').title()}: {count}")

# =============================================================================
# RECOMMENDATIONS
# =============================================================================

print("\n" + "=" * 80)
print("RECOMMENDATIONS")
print("=" * 80)

if failing_reasons['too_far_from_support'] > 0:
    print(f"\n1. INCREASE PRICE PROXIMITY THRESHOLD")
    print(f"   Current: 5%")
    print(f"   Suggested: 10% (to catch {failing_reasons['too_far_from_support']} more stocks)")

if failing_reasons['return_too_low'] > 0:
    print(f"\n2. LOWER MINIMUM RETURN THRESHOLD")
    print(f"   Current: 5%")
    print(f"   Suggested: 3% (to catch {failing_reasons['return_too_low']} more stocks)")

if failing_reasons['return_too_high'] > 0:
    print(f"\n3. INCREASE MAXIMUM RETURN THRESHOLD")
    print(f"   Current: 200%")
    print(f"   Suggested: 300% (to catch {failing_reasons['return_too_high']} more stocks)")

if passing_count == 0:
    print(f"\n   CRITICAL: No stocks passing current thresholds!")
    print(f"   Action: Relax ALL thresholds or detector will find nothing")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
