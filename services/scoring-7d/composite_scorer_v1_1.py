
"""
INVESTMENT OS - UNIFIED COMPOSITE SCORER v1.1
7-Dimensional Scoring Framework + Watch List Integration
Date: January 3, 2026
Version: 1.1 (Production - With Regulatory Compliance)

CHANGES FROM v1.0:
- Added CSE Watch List integration
- Watch list stocks forced to "DO NOT BUY"
- Regulatory warnings for non-compliant companies
- Enhanced investor protection
"""

import sys
import os
import importlib.util
import argparse

# Import v1.0 as a module
v1_0_path = os.path.join(os.path.dirname(__file__), 'composite_scorer_v1_0.py')

if not os.path.exists(v1_0_path):
    print(f"❌ ERROR: composite_scorer_v1_0.py not found at {v1_0_path}")
    sys.exit(1)

# Load v1.0 module
spec = importlib.util.spec_from_file_location("composite_v1_0", v1_0_path)
composite_v1_0 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(composite_v1_0)

# Import necessary components from v1.0
load_dimension_scores = composite_v1_0.load_dimension_scores
calculate_composite_scores = composite_v1_0.calculate_composite_scores
generate_summary_report = composite_v1_0.generate_summary_report
pd = composite_v1_0.pd

# Import watch list checker
try:
    from watchlist_utils import WatchListChecker
    WATCHLIST_AVAILABLE = True
except ImportError:
    WATCHLIST_AVAILABLE = False
    print("⚠️  Warning: watchlist_utils not available. Using fallback list.")


def apply_watch_list_override(df):
    """
    Apply watch list overrides to composite scores
    
    Changes:
    1. Add watch_list flag
    2. Override recommendations (force "DO NOT BUY")
    3. Add regulatory warnings
    """
    print("\n" + "=" * 80)
    print("APPLYING WATCH LIST OVERRIDES")
    print("=" * 80)
    
    # Initialize watch list columns
    df['watch_list'] = False
    df['watch_list_warning'] = ''
    
    # Get watch list stocks
    if not WATCHLIST_AVAILABLE:
        print("⚠️  Watch list checking disabled (watchlist_utils not available)")
        print("   Using fallback hardcoded list...")
        watch_list_stocks = [
            'ACAP.N0000', 'ACME.N0000', 'ALHP.N0000', 'BBH.N0000', 'BLI.N0000',
            'BLUE.N0000', 'BLUE.X0000', 'CHOU.N0000', 'CSF.N0000', 'DOCK.N0000',
            'DOCK.R0000', 'HELA.N0000', 'KDL.N0000', 'MHDL.N0000', 'ODEL.N0000',
            'SHL.N0000', 'SHL.W0000', 'SING.N0000'
        ]
    else:
        try:
            checker = WatchListChecker()
            watch_list_stocks = checker.get_all_watch_list_stocks()
            print(f"✅ Loaded {len(watch_list_stocks)} watch list stocks from Supabase")
        except Exception as e:
            print(f"⚠️  Error loading watch list: {e}")
            print("   Using fallback list...")
            watch_list_stocks = [
                'ACAP.N0000', 'ACME.N0000', 'ALHP.N0000', 'BBH.N0000', 'BLI.N0000',
                'BLUE.N0000', 'BLUE.X0000', 'CHOU.N0000', 'CSF.N0000', 'DOCK.N0000',
                'DOCK.R0000', 'HELA.N0000', 'KDL.N0000', 'MHDL.N0000', 'ODEL.N0000',
                'SHL.N0000', 'SHL.W0000', 'SING.N0000'
            ]
    
    # Apply overrides
    override_count = 0
    
    for idx, row in df.iterrows():
        symbol = row['symbol']
        
        if symbol in watch_list_stocks:
            # Mark as watch list
            df.at[idx, 'watch_list'] = True
            df.at[idx, 'watch_list_warning'] = 'NON-COMPLIANT WITH CSE LISTING RULES'
            
            # Override recommendation if it's a BUY
            original_rec = row['recommendation']
            
            if original_rec in ['STRONG BUY', 'BUY', 'SPECULATIVE BUY']:
                df.at[idx, 'recommendation'] = 'DO NOT BUY - WATCH LIST'
                df.at[idx, 'interpretation'] = 'Non-compliant with CSE Listing Rules - Regulatory Risk'
                
                print(f"⚠️  OVERRIDE: {symbol}")
                print(f"   Original: {original_rec}")
                print(f"   New: DO NOT BUY - WATCH LIST")
                
                override_count += 1
    
    # Summary
    watch_list_count = df['watch_list'].sum()
    print(f"\n✅ Watch List Summary:")
    print(f"   Total watch list stocks: {watch_list_count}")
    print(f"   Recommendations overridden: {override_count}")
    
    if override_count > 0:
        print(f"\n   ⚠️  {override_count} stocks changed from BUY to DO NOT BUY")
    else:
        print(f"\n   ✅ No BUY recommendations needed override (system working correctly!)")
    
    return df


def add_watch_list_to_report(scores_df, output_file):
    """Add watch list section to report"""
    watch_list_stocks = scores_df[scores_df['watch_list'] == True]
    
    if len(watch_list_stocks) > 0:
        additional_report = []
        additional_report.append("\n" + "=" * 80)
        additional_report.append("WATCH LIST STOCKS (NON-COMPLIANT)")
        additional_report.append("=" * 80)
        additional_report.append(f"\nTotal: {len(watch_list_stocks)} stocks")
        additional_report.append("\n⚠️  These stocks are non-compliant with CSE Listing Rules")
        additional_report.append("   Investment OS does NOT recommend purchase\n")
        
        for _, row in watch_list_stocks.sort_values('rank').iterrows():
            additional_report.append(f"{row['symbol']:12s} - Rank #{row['rank']:<3} - Score: {row['composite_score']:5.1f} - {row['recommendation']}")
        
        additional_report.append("\n" + "=" * 80)
        
        # Append to file
        with open(output_file, 'a') as f:
            f.write("\n".join(additional_report))
        
        print("\n✅ Watch list section added to report")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("INVESTMENT OS - COMPOSITE SCORER v1.1 (With Watch List)")
    print("=" * 80)
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description='Investment OS - Composite Scorer v1.1 (With Watch List)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--d1', type=str, required=True, help='Dimension 1 scores CSV')
    parser.add_argument('--d2', type=str, required=True, help='Dimension 2 scores CSV')
    parser.add_argument('--d3', type=str, required=True, help='Dimension 3 scores CSV')
    parser.add_argument('--d4', type=str, required=True, help='Dimension 4 scores CSV')
    parser.add_argument('--d5', type=str, required=True, help='Dimension 5 scores CSV')
    parser.add_argument('--d6', type=str, required=True, help='Dimension 6 scores CSV')
    parser.add_argument('--d7', type=str, required=True, help='Dimension 7 scores CSV')
    parser.add_argument('--output', type=str, default='composite_scores.csv', help='Output CSV')
    parser.add_argument('--report', type=str, default='composite_report.txt', help='Output report')
    
    args = parser.parse_args()
    
    print(f"\n📂 Input files:")
    for i in range(1, 8):
        dim_file = getattr(args, f'd{i}')
        print(f"  Dimension {i}: {dim_file}")
    
    print(f"\n📊 Output files:")
    print(f"  Scores CSV: {args.output}")
    print(f"  Report TXT: {args.report}")
    print("")
    
    # Build dimension files dict for v1.0
    dimension_files = {
        'dimension1': args.d1,
        'dimension2': args.d2,
        'dimension3': args.d3,
        'dimension4': args.d4,
        'dimension5': args.d5,
        'dimension6': args.d6,
        'dimension7': args.d7,
    }
    
    # Load dimension scores (v1.0 function)
    print("=" * 80)
    print("LOADING DIMENSION SCORES")
    print("=" * 80)
    dimension_data = load_dimension_scores(dimension_files)
    
    # Calculate composite scores (v1.0 function)
    print("\n" + "=" * 80)
    print("CALCULATING COMPOSITE SCORES")
    print("=" * 80)
    composite_df = calculate_composite_scores(dimension_data)
    
    # Apply watch list overrides (v1.1 addition)
    composite_df = apply_watch_list_override(composite_df)
    
    # Save scores
    composite_df.to_csv(args.output, index=False)
    print(f"\n✅ Scores saved: {args.output}")
    
    # Generate report (v1.0 function)
    generate_summary_report(composite_df, args.report)
    
    # Add watch list section to report (v1.1 addition)
    add_watch_list_to_report(composite_df, args.report)
    
    print("\n🎯 Composite scoring v1.1 complete!")
    print(f"\nFiles created:")
    print(f"  • {args.output}")
    print(f"  • {args.report}")
    print(f"\n✅ Watch list integration active")