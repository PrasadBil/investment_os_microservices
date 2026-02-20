
#!/usr/bin/env python3
"""
CSE PERCENTILE CALCULATOR - DIMENSION 7 V2.0 (Phase 1B-D)
Investment OS - Market Sentiment Enhancement

FILE: cse_percentile_calculator.py
CREATED: 2026-01-07
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-01-07  Initial creation — 4-metric percentile calculator (in Dimension7/)
    v2.0.0  2026-02-15  Migrated to services/scoring-7d; expanded to 8 metrics (Phase 1B-D)
                         Added: volume_ratio_5d_20d, momentum_5d, momentum_20d, momentum_60d
    v2.0.1  2026-02-16  Added version history header (new project standard)

PURPOSE:
Calculates CSE-specific percentile thresholds for sentiment scoring.
Uses actual market distribution (empirical percentiles) rather than arbitrary cutoffs.

INPUTS:
- cse_metrics.csv (output from cse_data_connector.py)
- Contains ~292 stocks with 13 metrics

OUTPUTS:
- cse_percentiles.json (percentile thresholds per metric)
- cse_percentile_report.txt (human-readable analysis)
- Ready for D7 v2.0 Phase 1B-D scoring

METHODOLOGY:
- Empirical percentiles (90th, 75th, 50th, 25th, 10th)
- Based on actual CSE distribution
- 100% data-driven (no arbitrary thresholds)
- Defensible for regulatory scrutiny

AVAILABLE METRICS (Phase 1B-D — all 8):
✅ Metric 1: Trade Volume (trade count)
✅ Metric 2: Share Volume (shares traded)
✅ Metric 3: Intraday Volatility (price range %)
✅ Metric 4: Volume Ratio 5D/20D (volume trend)
✅ Metric 5: Trade Density (shares per trade)
✅ Metric 6a: Momentum 5D (short-term return %)
✅ Metric 6b: Momentum 20D (medium-term return %)
✅ Metric 6c: Momentum 60D (long-term return %)

VERSION: 2.0 (Phase 1B-D — all metrics enabled)
DATE: February 15, 2026
AUTHOR: Investment OS Team

Migration: Phase 2 (Feb 2026)
- Original: /opt/selenium_automation/cse_percentile_calculator.py (Dimension7/)
- Changes: Added 4 new metrics (volume_ratio, momentum_5d/20d/60d)
- Updated report/JSON to reflect Phase 1B-D
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
from typing import Dict, List, Tuple
import sys

# =============================================================================
# CONFIGURATION
# =============================================================================

# Percentiles to calculate (for scoring bands)
PERCENTILES = [10, 25, 50, 75, 90, 95]

# All metrics for percentile calculation (Phase 1B-D: all 8 active)
AVAILABLE_METRICS = {
    # --- Phase 1A metrics (existing) ---
    'trade_volume': {
        'name': 'Trade Volume',
        'description': 'Number of trades executed',
        'direction': 'higher_better',
        'unit': 'trades'
    },
    'share_volume': {
        'name': 'Share Volume',
        'description': 'Total shares traded',
        'direction': 'higher_better',
        'unit': 'shares'
    },
    'intraday_volatility': {
        'name': 'Intraday Volatility',
        'description': 'Daily price range percentage',
        'direction': 'neutral',
        'unit': 'percent'
    },
    'trade_density': {
        'name': 'Trade Density',
        'description': 'Average shares per trade (institutional indicator)',
        'direction': 'contextual',
        'unit': 'shares/trade'
    },
    # --- Phase 1B-D metrics (NEW) ---
    'volume_ratio_5d_20d': {
        'name': 'Volume Ratio 5D/20D',
        'description': 'Recent volume vs average volume (accumulation signal)',
        'direction': 'higher_better',
        'unit': 'ratio'
    },
    'momentum_5d': {
        'name': 'Momentum 5D',
        'description': '5-day price return percentage',
        'direction': 'higher_better',
        'unit': 'percent'
    },
    'momentum_20d': {
        'name': 'Momentum 20D',
        'description': '20-day price return percentage',
        'direction': 'higher_better',
        'unit': 'percent'
    },
    'momentum_60d': {
        'name': 'Momentum 60D',
        'description': '60-day price return percentage',
        'direction': 'higher_better',
        'unit': 'percent'
    }
}

# =============================================================================
# PERCENTILE CALCULATION
# =============================================================================

def calculate_percentiles(df: pd.DataFrame, metric: str) -> Dict:
    """
    Calculate percentiles for a given metric.

    Args:
        df: DataFrame with CSE metrics
        metric: Metric column name

    Returns:
        Dictionary with percentile values
    """
    # Remove NaN/inf values
    data = df[metric].replace([np.inf, -np.inf], np.nan).dropna()

    if len(data) == 0:
        return None

    # Calculate percentiles
    percentile_values = {}
    for p in PERCENTILES:
        percentile_values[f'p{p}'] = np.percentile(data, p)

    # Add statistics
    percentile_values['mean'] = data.mean()
    percentile_values['median'] = data.median()
    percentile_values['std'] = data.std()
    percentile_values['min'] = data.min()
    percentile_values['max'] = data.max()
    percentile_values['count'] = len(data)

    return percentile_values

def calculate_all_percentiles(csv_file: str) -> Dict:
    """
    Calculate percentiles for all available metrics.

    Args:
        csv_file: Path to cse_metrics.csv

    Returns:
        Dictionary with percentiles per metric
    """
    print("=" * 80)
    print("CSE PERCENTILE CALCULATOR - Dimension 7 v2.0 Phase 1B-D")
    print("=" * 80)

    # Load data
    print(f"\n📊 Loading: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"   Stocks: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Available: {', '.join(df.columns.tolist())}")

    # Calculate percentiles for each metric
    results = {}

    print("\n🔢 Calculating percentiles for all metrics...")

    for metric, info in AVAILABLE_METRICS.items():
        if metric not in df.columns:
            print(f"   ⚠️  Skipping {info['name']}: Column '{metric}' not found in CSV")
            continue

        print(f"   → {info['name']}...")
        percentiles = calculate_percentiles(df, metric)

        if percentiles:
            results[metric] = {
                'info': info,
                'percentiles': percentiles
            }
            print(f"      ✅ Calculated ({percentiles['count']} stocks, "
                  f"median={percentiles['median']:.2f} {info['unit']})")
        else:
            print(f"      ❌ Failed (no valid data)")

    print(f"\n✅ Percentiles calculated for {len(results)}/{len(AVAILABLE_METRICS)} metrics")

    return results

# =============================================================================
# SCORING BANDS
# =============================================================================

def create_scoring_bands(percentiles: Dict, metric: str, info: Dict) -> Dict:
    """
    Create scoring bands based on percentiles.

    Bands (0-100 scale):
    - 90-100: Exceptional (> 90th percentile)
    - 75-89:  Strong (75th-90th percentile)
    - 50-74:  Moderate (50th-75th percentile)
    - 25-49:  Weak (25th-50th percentile)
    - 0-24:   Poor (< 25th percentile)

    Args:
        percentiles: Percentile values
        metric: Metric name
        info: Metric info dict

    Returns:
        Dictionary with scoring bands
    """
    direction = info['direction']

    # Extract key percentiles
    p90 = percentiles['p90']
    p75 = percentiles['p75']
    p50 = percentiles['p50']
    p25 = percentiles['p25']
    p10 = percentiles['p10']

    if direction == 'higher_better':
        # Higher values = better scores
        bands = {
            'exceptional': {'min': p90, 'max': float('inf'), 'score_range': (90, 100)},
            'strong': {'min': p75, 'max': p90, 'score_range': (75, 89)},
            'moderate': {'min': p50, 'max': p75, 'score_range': (50, 74)},
            'weak': {'min': p25, 'max': p50, 'score_range': (25, 49)},
            'poor': {'min': 0, 'max': p25, 'score_range': (0, 24)}
        }
    elif direction == 'lower_better':
        # Lower values = better scores
        bands = {
            'exceptional': {'min': 0, 'max': p10, 'score_range': (90, 100)},
            'strong': {'min': p10, 'max': p25, 'score_range': (75, 89)},
            'moderate': {'min': p25, 'max': p50, 'score_range': (50, 74)},
            'weak': {'min': p50, 'max': p75, 'score_range': (25, 49)},
            'poor': {'min': p75, 'max': float('inf'), 'score_range': (0, 24)}
        }
    else:
        # Neutral or contextual - provide bands without scores
        bands = {
            'very_high': {'min': p90, 'max': float('inf'), 'score_range': None},
            'high': {'min': p75, 'max': p90, 'score_range': None},
            'moderate': {'min': p50, 'max': p75, 'score_range': None},
            'low': {'min': p25, 'max': p50, 'score_range': None},
            'very_low': {'min': 0, 'max': p25, 'score_range': None}
        }

    return bands

# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_report(results: Dict, output_file: str):
    """
    Generate human-readable percentile report.

    Args:
        results: Percentile results
        output_file: Path to output text file
    """
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    stock_count = next(iter(results.values()))['percentiles']['count'] if results else 0

    report = f"""
{'=' * 80}
CSE PERCENTILE ANALYSIS REPORT
Dimension 7 v2.0 - Phase 1B-D (All Metrics)
{'=' * 80}

Generated: {report_date}
Metrics Analyzed: {len(results)}/8
Stock Universe: {stock_count} stocks
Data Source: CSE Daily Prices (cse_daily_prices table)

{'=' * 80}
PERCENTILE THRESHOLDS
{'=' * 80}
"""

    for metric, data in results.items():
        info = data['info']
        perc = data['percentiles']

        report += f"""
{'-' * 80}
METRIC: {info['name']}
{'-' * 80}
Description: {info['description']}
Direction: {info['direction']}
Unit: {info['unit']}

Sample Size: {perc['count']} stocks

DISTRIBUTION:
  Minimum:     {perc['min']:>12,.2f} {info['unit']}
  10th %%ile:   {perc['p10']:>12,.2f} {info['unit']}
  25th %%ile:   {perc['p25']:>12,.2f} {info['unit']}
  Median (50): {perc['median']:>12,.2f} {info['unit']}
  Mean:        {perc['mean']:>12,.2f} {info['unit']}
  75th %%ile:   {perc['p75']:>12,.2f} {info['unit']}
  90th %%ile:   {perc['p90']:>12,.2f} {info['unit']}
  95th %%ile:   {perc['p95']:>12,.2f} {info['unit']}
  Maximum:     {perc['max']:>12,.2f} {info['unit']}
  Std Dev:     {perc['std']:>12,.2f} {info['unit']}

"""

        # Add scoring bands if applicable
        if info['direction'] in ['higher_better', 'lower_better']:
            bands = create_scoring_bands(perc, metric, info)

            report += "SCORING BANDS (0-100 scale):\n"
            for band_name, band_info in bands.items():
                min_val = band_info['min']
                max_val = band_info['max'] if band_info['max'] != float('inf') else '∞'
                score_range = band_info['score_range']

                report += f"  {band_name.upper():12s}: "
                report += f"{min_val:>10,.2f} to {str(max_val):>10s} {info['unit']} "
                report += f"→ Score {score_range[0]}-{score_range[1]}\n"
            report += "\n"

    report += f"""
{'=' * 80}
PHASE 1B-D NEW METRICS INTERPRETATION
{'=' * 80}

VOLUME RATIO 5D/20D (Accumulation Signal):
  • Ratio > 2.0: Significant accumulation — recent volume 2x above average
  • Ratio 1.3-2.0: Above-average interest — building momentum
  • Ratio 0.8-1.3: Normal trading activity
  • Ratio 0.5-0.8: Declining interest — potential distribution
  • Ratio < 0.5: Severe decline — abandonment signal

MOMENTUM 5D (Short-Term Return):
  • Captures recent catalysts, earnings reactions, news impact
  • Positive = recent price appreciation
  • Most volatile of the three timeframes (highest noise)

MOMENTUM 20D (Medium-Term Return):
  • Most actionable timeframe — filters 5D noise
  • Captures trends lasting 1+ month
  • Key signal for scoring: sustained trend vs. one-day spike

MOMENTUM 60D (Long-Term Return):
  • Captures sustained trends over 3 months
  • Differentiates real trends from mean-reversion bounces
  • Low noise, high conviction signal

MULTI-TIMEFRAME CONFLUENCE:
  • All three positive = Strong trend confirmation (highest conviction)
  • 5D positive, 60D negative = Bounce in downtrend (low conviction)
  • 5D negative, 60D positive = Pullback in uptrend (buying opportunity signal)
  • All three negative = Sustained downtrend (lowest sentiment)

{'=' * 80}
METHODOLOGY NOTES
{'=' * 80}

1. EMPIRICAL PERCENTILES:
   All thresholds derived from actual CSE distribution ({stock_count} stocks)
   No arbitrary cutoffs - 100% data-driven
   Defensible for regulatory scrutiny

2. SCORING PHILOSOPHY:
   90-100 points: Top 10% of CSE (Exceptional)
   75-89 points:  Top 25% of CSE (Strong)
   50-74 points:  Top 50% of CSE (Moderate)
   25-49 points:  Bottom 50% of CSE (Weak)
   0-24 points:   Bottom 25% of CSE (Poor)

3. DATA REQUIREMENTS:
   Volume Ratio: Needs >= 20 trading days of OHLCV data
   Momentum 5D:  Needs >= 5 trading days
   Momentum 20D: Needs >= 20 trading days
   Momentum 60D: Needs >= 60 trading days
   → All satisfied: OHLCV data available since 2010-01-01

4. PERCENTILE REFRESH:
   Recalculate monthly or when data accumulation changes distribution
   Scoring bands adapt to evolving market conditions

{'=' * 80}
END OF REPORT
{'=' * 80}
"""

    # Write report
    with open(output_file, 'w') as f:
        f.write(report)

    print(f"\n📄 Report saved: {output_file}")

# =============================================================================
# JSON EXPORT
# =============================================================================

def export_json(results: Dict, output_file: str):
    """
    Export percentiles to JSON for programmatic use by scorer.

    Args:
        results: Percentile results
        output_file: Path to output JSON file
    """
    stock_count = next(iter(results.values()))['percentiles']['count'] if results else 0

    # Convert to JSON-serializable format
    output = {
        'generated_at': datetime.now().isoformat(),
        'phase': '1B-D',
        'data_source': 'cse_daily_prices',
        'stocks_analyzed': stock_count,
        'metrics_count': len(results),
        'metrics': {}
    }

    for metric, data in results.items():
        info = data['info']
        perc = data['percentiles']

        # Convert numpy types to Python types
        percentiles_clean = {}
        for k, v in perc.items():
            if isinstance(v, (np.integer, np.floating)):
                percentiles_clean[k] = float(v)
            else:
                percentiles_clean[k] = v

        # Create scoring bands
        bands = create_scoring_bands(perc, metric, info)

        # Convert bands to serializable format
        bands_clean = {}
        for band_name, band_info in bands.items():
            bands_clean[band_name] = {
                'min': float(band_info['min']) if band_info['min'] != float('inf') else None,
                'max': float(band_info['max']) if band_info['max'] != float('inf') else None,
                'score_range': band_info['score_range']
            }

        output['metrics'][metric] = {
            'name': info['name'],
            'description': info['description'],
            'direction': info['direction'],
            'unit': info['unit'],
            'percentiles': percentiles_clean,
            'scoring_bands': bands_clean
        }

    # Write JSON
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"💾 JSON saved: {output_file} ({len(results)} metrics)")

# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main(csv_file: str = 'cse_metrics.csv'):
    """
    Main function: Calculate percentiles and generate outputs.

    Args:
        csv_file: Path to CSE metrics CSV
    """
    print("\n🚀 Starting percentile calculation (Phase 1B-D)...")

    # Calculate percentiles
    results = calculate_all_percentiles(csv_file)

    if not results:
        print("\n❌ No percentiles calculated - check input data")
        return False

    # Generate outputs
    print("\n📊 Generating outputs...")
    generate_report(results, 'cse_percentile_report.txt')
    export_json(results, 'cse_percentiles.json')

    print("\n" + "=" * 80)
    print("✅ PERCENTILE CALCULATION COMPLETE (Phase 1B-D)")
    print("=" * 80)
    print(f"\nOutputs:")
    print(f"  📄 cse_percentile_report.txt  - Human-readable analysis")
    print(f"  💾 cse_percentiles.json       - Machine-readable thresholds ({len(results)} metrics)")
    print(f"\nReady for: dimension7_scorer_v2_0_phase1bcd.py")
    print("=" * 80)

    return True

# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'cse_metrics.csv'
    success = main(csv_file)
    sys.exit(0 if success else 1)
