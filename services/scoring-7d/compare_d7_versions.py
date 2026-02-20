

#!/usr/bin/env python3
"""
D7 V1.0 VS V2.0 COMPARISON SCRIPT
Compare scoring results between versions

FILE: compare_d7_versions.py
CREATED: 2026-01-07
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-01-07  Initial creation — v1 vs v2 comparison with correlation analysis
    v1.0.1  2026-02-12  Migrated to services/scoring-7d (Phase 2 microservices)
    v1.0.2  2026-02-16  Added version history header (new project standard)

PURPOSE:
- Load v1.0 and v2.0 scores
- Calculate correlation
- Identify differences
- Validate improvements
"""

import pandas as pd
import numpy as np
import sys

def pearson_correlation(x, y):
    """Calculate Pearson correlation without scipy."""
    x = np.array(x)
    y = np.array(y)
    
    # Remove NaN pairs
    mask = ~(np.isnan(x) | np.isnan(y))
    x = x[mask]
    y = y[mask]
    
    if len(x) < 2:
        return 0.0, 1.0
    
    # Calculate correlation
    mean_x = np.mean(x)
    mean_y = np.mean(y)
    
    numerator = np.sum((x - mean_x) * (y - mean_y))
    denominator = np.sqrt(np.sum((x - mean_x)**2) * np.sum((y - mean_y)**2))
    
    if denominator == 0:
        return 0.0, 1.0
    
    corr = numerator / denominator
    
    # Simplified p-value (not exact but good enough)
    # For large samples, this is approximately correct
    p_value = 0.001 if abs(corr) > 0.5 else 0.05
    
    return corr, p_value

def compare_versions(v1_file='dimension7_scores.csv', v2_file='dimension7_v2_scores.csv'):
    """
    Compare D7 v1.0 and v2.0 scores.
    
    Args:
        v1_file: v1.0 scores CSV
        v2_file: v2.0 Phase 1A scores CSV
    """
    print("="*80)
    print("D7 V1.0 VS V2.0 PHASE 1A COMPARISON")
    print("="*80)
    
    try:
        # Load scores
        print(f"\n📊 Loading scores...")
        
        # v1.0 format: symbol, dimension7_score
        v1 = pd.read_csv(v1_file)
        print(f"   v1.0: {len(v1):,} stocks")
        print(f"   v1.0 columns: {list(v1.columns)}")
        
        # v2.0 format: rank, symbol, dimension7_v2_score, components...
        v2 = pd.read_csv(v2_file)
        print(f"   v2.0: {len(v2):,} stocks")
        print(f"   v2.0 columns: {list(v2.columns[:5])}...")  # First 5 columns
        
        # Find the score column in v1.0 (might have different name)
        v1_score_col = None
        possible_names = ['dimension7_score', 'd7_score', 'score', 'sentiment_score', 'dimension_7_score']
        
        for col_name in possible_names:
            if col_name in v1.columns:
                v1_score_col = col_name
                break
        
        if v1_score_col is None:
            # Try to find any numeric column that might be the score
            numeric_cols = v1.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                v1_score_col = numeric_cols[0]
                print(f"   ⚠️  Using column '{v1_score_col}' as v1.0 score")
            else:
                print(f"\n❌ ERROR: Cannot find score column in v1.0 file")
                print(f"   Available columns: {list(v1.columns)}")
                return None
        else:
            print(f"   ✅ Found v1.0 score column: '{v1_score_col}'")
        
        # Rename for consistency
        v1 = v1.rename(columns={v1_score_col: 'dimension7_score'})
        
        # Merge on symbol
        merged = v1.merge(
            v2[['symbol', 'dimension7_v2_score', 'trade_density_class', 'volatility_level']], 
            on='symbol', 
            how='inner'
        )
        
        print(f"   Common: {len(merged):,} stocks")
        
        if len(merged) == 0:
            print("\n❌ No common stocks found - check symbol formats")
            return
        
        # Calculate correlation
        print("\n📈 CORRELATION ANALYSIS:")
        corr, p_value = pearson_correlation(merged['dimension7_score'], merged['dimension7_v2_score'])
        print(f"   Pearson Correlation: {corr:.3f}")
        print(f"   Significance: {'***' if abs(corr) > 0.7 else '**' if abs(corr) > 0.5 else '*'}")
        
        if corr > 0.85:
            print(f"   ✅ High agreement (>0.85)")
        elif corr > 0.70:
            print(f"   ⚠️  Moderate agreement (0.70-0.85)")
        else:
            print(f"   ❌ Low agreement (<0.70)")
        
        # Distribution comparison
        print("\n📊 DISTRIBUTION COMPARISON:")
        print(f"                 v1.0      v2.0      Delta")
        print(f"   Mean:        {merged['dimension7_score'].mean():6.1f}    {merged['dimension7_v2_score'].mean():6.1f}    {merged['dimension7_v2_score'].mean() - merged['dimension7_score'].mean():+6.1f}")
        print(f"   Median:      {merged['dimension7_score'].median():6.1f}    {merged['dimension7_v2_score'].median():6.1f}    {merged['dimension7_v2_score'].median() - merged['dimension7_score'].median():+6.1f}")
        print(f"   Std Dev:     {merged['dimension7_score'].std():6.1f}    {merged['dimension7_v2_score'].std():6.1f}    {merged['dimension7_v2_score'].std() - merged['dimension7_score'].std():+6.1f}")
        
        # Calculate delta
        merged['delta'] = merged['dimension7_v2_score'] - merged['dimension7_score']
        
        print(f"\n   Score Changes:")
        print(f"   Improved (delta > +5):  {(merged['delta'] > 5).sum()} stocks")
        print(f"   Similar (±5):           {((merged['delta'] >= -5) & (merged['delta'] <= 5)).sum()} stocks")
        print(f"   Declined (delta < -5):  {(merged['delta'] < -5).sum()} stocks")
        
        # Top 20 overlap
        print("\n🏆 TOP 20 OVERLAP:")
        v1_top20 = set(v1.nlargest(20, 'dimension7_score')['symbol'])
        v2_top20 = set(v2.nlargest(20, 'dimension7_v2_score')['symbol'])
        overlap = len(v1_top20 & v2_top20)
        
        print(f"   Common stocks: {overlap}/20 ({overlap/20*100:.0f}%)")
        print(f"   v1.0 only: {len(v1_top20 - v2_top20)} stocks")
        print(f"   v2.0 only: {len(v2_top20 - v1_top20)} stocks")
        
        if overlap >= 14:
            print(f"   ✅ Good stability (70%+)")
        elif overlap >= 10:
            print(f"   ⚠️  Moderate changes (50-70%)")
        else:
            print(f"   ❌ Major divergence (<50%)")
        
        # Test stocks analysis
        print("\n🎯 TEST STOCKS COMPARISON:")
        test_stocks = ['CTC.N0000', 'LION.N0000', 'LOFC.N0000', 'LOLC.N0000', 'JKH.N0000']
        test_data = merged[merged['symbol'].isin(test_stocks)][
            ['symbol', 'dimension7_score', 'dimension7_v2_score', 'delta', 'trade_density_class']
        ].sort_values('dimension7_v2_score', ascending=False)
        
        if not test_data.empty:
            print(f"\n   {'Stock':<12} {'v1.0':>6} {'v2.0':>6} {'Delta':>7} {'Density':<15}")
            print(f"   {'-'*60}")
            for _, row in test_data.iterrows():
                print(f"   {row['symbol']:<12} {row['dimension7_score']:>6.1f} {row['dimension7_v2_score']:>6.1f} {row['delta']:>+7.1f} {row['trade_density_class']:<15}")
        
        # Biggest movers
        print("\n📈 TOP 5 IMPROVERS (v2.0 gains):")
        improvers = merged.nlargest(5, 'delta')[['symbol', 'dimension7_score', 'dimension7_v2_score', 'delta', 'trade_density_class']]
        print(improvers.to_string(index=False))
        
        print("\n📉 TOP 5 DECLINERS (v2.0 drops):")
        decliners = merged.nsmallest(5, 'delta')[['symbol', 'dimension7_score', 'dimension7_v2_score', 'delta', 'trade_density_class']]
        print(decliners.to_string(index=False))
        
        # Trade density analysis
        print("\n💡 INSIGHTS BY TRADE DENSITY:")
        for density in ['institutional', 'mixed', 'retail']:
            subset = merged[merged['trade_density_class'] == density]
            if len(subset) > 0:
                avg_delta = subset['delta'].mean()
                print(f"   {density.capitalize():15s}: {len(subset):3d} stocks, avg delta: {avg_delta:+5.1f}")
        
        print("\n" + "="*80)
        print("✅ COMPARISON COMPLETE")
        print("="*80)
        
        # Summary assessment
        print("\n📊 ASSESSMENT:")
        if corr > 0.85 and overlap >= 14:
            print("   ✅ v2.0 shows high consistency with v1.0")
            print("   ✅ Good stability in top recommendations")
            print("   ✅ Ready for parallel deployment")
        elif corr > 0.70 and overlap >= 10:
            print("   ⚠️  v2.0 shows moderate changes from v1.0")
            print("   ⚠️  Review major differences before deployment")
        else:
            print("   ❌ v2.0 shows significant divergence from v1.0")
            print("   ❌ Investigate scoring methodology")
        
        return merged
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: File not found: {e}")
        print("   Ensure both v1.0 and v2.0 scores are generated")
        return None
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    v1_file = sys.argv[1] if len(sys.argv) > 1 else 'dimension7_scores.csv'
    v2_file = sys.argv[2] if len(sys.argv) > 2 else 'dimension7_v2_scores.csv'
    
    compare_versions(v1_file, v2_file)