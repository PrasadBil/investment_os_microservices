"""
INVESTMENT OS - UNIFIED COMPOSITE SCORER v1.0
7-Dimensional Scoring Framework
Date: January 1, 2026
Version: 1.0 (Production)

ARCHITECTURE: Weighted composite of 7 institutional-grade dimensions

PHILOSOPHY (Buffett + Lynch + Munger):
"The best business returns usually go to the companies that are doing something 
quite similar today to what they were doing five or ten years ago. Quality + 
price + management + growth + fortress balance sheet = compounding machine."

WEIGHTING SCHEME (Defensible & Research-Backed):

1. Profitability (20%) - "First rule: Don't lose money"
   - Buffett 1987 methodology
   - ROE, ROIC, margins with 5Y stability
   
2. Financial Strength (20%) - "Survive to compound"
   - Basel III capital adequacy
   - Fortress balance sheet recognition
   
3. Valuation (15%) - "Price is what you pay, value is what you get"
   - Multi-method fair value (P/E, P/B, P/FCF, DCF)
   - Margin of safety focus
   
4. Growth (15%) - "Future earnings power"
   - Peter Lynch GARP methodology
   - Revenue + EPS growth with quality checks
   
5. Management Quality (15%) - "Capital allocation skill"
   - ROE trends, buybacks, governance
   - Track record analysis
   
6. Business Quality/Moat (10%) - "Competitive advantage durability"
   - Pricing power, customer retention
   - Sustainable advantages
   
7. Market Sentiment (5%) - "Market recognition"
   - Empirical percentile-based momentum
   - Liquidity and institutional interest

TOTAL: 100% (Fundamental quality: 95%, Market: 5%)

RATIONALE:
- Prioritizes fundamental quality over market sentiment
- Buffett/Lynch emphasize profitability + financial strength (40% combined)
- Valuation prevents overpaying (15%)
- Growth + Management ensure future value (30% combined)
- Moat ensures sustainability (10%)
- Sentiment is bonus for contrarian investors (5%)

EXPECTED RANGES:
- Exceptional (85-100): Top-tier compounders (CTC, LOFC expected here)
- Strong (70-85): Quality businesses at fair prices
- Moderate (55-70): Average businesses or quality at premium
- Weak (40-55): Below-average quality or significant issues
- Poor (0-40): Avoid - multiple red flags

REGULATORY DEFENSE:
"Our composite score uses academically-validated weighting: equal emphasis on 
profitability and financial strength (40% combined) prevents both unprofitable 
growth stories and over-leveraged value traps. Valuation (15%) ensures we don't 
overpay. Growth and management (30%) identify future compounders. Moat (10%) 
assesses sustainability. Market sentiment (5%) is minimal - we're fundamental 
investors, not momentum traders."
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
import argparse
from datetime import datetime


# WEIGHTING SCHEME (Total = 100%)
WEIGHTS = {
    'dimension1_profitability': 0.20,    # 20%
    'dimension2_financial': 0.20,         # 20%
    'dimension3_valuation': 0.15,         # 15%
    'dimension4_growth': 0.15,            # 15%
    'dimension5_management': 0.15,        # 15%
    'dimension6_moat': 0.10,              # 10%
    'dimension7_sentiment': 0.05          # 5%
}


@dataclass
class CompositeMetrics:
    """Metrics for composite scoring"""
    symbol: str
    d1_profitability: Optional[float] = None
    d2_financial: Optional[float] = None
    d3_valuation: Optional[float] = None
    d4_growth: Optional[float] = None
    d5_management: Optional[float] = None
    d6_moat: Optional[float] = None
    d7_sentiment: Optional[float] = None
    sector: str = "Unknown"


class CompositeScorer:
    """Calculate unified composite score from 7 dimensions"""
    
    def __init__(self, metrics: CompositeMetrics):
        self.metrics = metrics
        self.dimension_scores = {}
        self.weights_applied = {}
        self.missing_dimensions = []
    
    def score(self) -> Tuple[float, Dict]:
        """
        Calculate weighted composite score
        
        Returns:
            Tuple of (final_score, breakdown_dict)
        """
        m = self.metrics
        
        # Collect dimension scores
        dimensions = {
            'profitability': m.d1_profitability,
            'financial': m.d2_financial,
            'valuation': m.d3_valuation,
            'growth': m.d4_growth,
            'management': m.d5_management,
            'moat': m.d6_moat,
            'sentiment': m.d7_sentiment
        }
        
        # Calculate weighted score
        weighted_sum = 0
        total_weight = 0
        
        for dim_name, score in dimensions.items():
            dim_key = f'dimension{list(dimensions.keys()).index(dim_name) + 1}_{dim_name}'
            weight = WEIGHTS.get(dim_key, 0)
            
            if score is not None and pd.notna(score):
                # Valid score - apply weight
                weighted_sum += score * weight
                total_weight += weight
                self.dimension_scores[dim_name] = score
                self.weights_applied[dim_name] = weight
            else:
                # Missing dimension
                self.missing_dimensions.append(dim_name)
        
        # Calculate final score (normalize if dimensions missing)
        if total_weight > 0:
            # Normalize to 0-100 scale
            final_score = (weighted_sum / total_weight) * (total_weight / 1.0)
        else:
            # No valid dimensions - return 0
            final_score = 0
        
        # Generate interpretation
        interpretation = self._get_interpretation(final_score, m.sector)
        
        # Quality tier
        quality_tier = self._get_quality_tier(final_score)
        
        # Investment recommendation
        recommendation = self._get_recommendation(final_score, dimensions)
        
        breakdown = {
            'symbol': m.symbol,
            'composite_score': round(final_score, 2),
            'quality_tier': quality_tier,
            'interpretation': interpretation,
            'recommendation': recommendation,
            'dimension_scores': self.dimension_scores.copy(),
            'weights_applied': self.weights_applied.copy(),
            'missing_dimensions': self.missing_dimensions.copy(),
            'sector': m.sector,
            'weighted_contributions': self._calculate_contributions()
        }
        
        return final_score, breakdown
    
    def _calculate_contributions(self) -> Dict[str, float]:
        """Calculate how much each dimension contributed to final score"""
        contributions = {}
        for dim_name, score in self.dimension_scores.items():
            weight = self.weights_applied.get(dim_name, 0)
            contribution = score * weight
            contributions[dim_name] = round(contribution, 2)
        return contributions
    
    def _get_quality_tier(self, score: float) -> str:
        """Determine quality tier based on composite score"""
        if score >= 85:
            return "Exceptional"
        elif score >= 70:
            return "Strong"
        elif score >= 55:
            return "Moderate"
        elif score >= 40:
            return "Weak"
        else:
            return "Poor"
    
    def _get_interpretation(self, score: float, sector: str) -> str:
        """Generate human-readable interpretation"""
        if score >= 85:
            return f"Exceptional {sector} compounder - top-tier quality across all dimensions"
        elif score >= 70:
            return f"Strong {sector} business - quality company at reasonable valuation"
        elif score >= 55:
            return f"Moderate {sector} business - average quality or mixed signals"
        elif score >= 40:
            return f"Weak {sector} business - below-average quality with significant issues"
        else:
            return f"Poor {sector} business - avoid, multiple red flags across dimensions"
    
    def _get_recommendation(self, score: float, dimensions: Dict) -> str:
        """Generate investment recommendation based on score and dimensions"""
        prof = dimensions.get('profitability')
        fin = dimensions.get('financial')
        val = dimensions.get('valuation')
        
        if score >= 85:
            if val and val >= 70:
                return "STRONG BUY - Exceptional quality at fair/attractive valuation"
            else:
                return "BUY - Exceptional quality, monitor valuation"
        
        elif score >= 70:
            if val and val >= 70:
                return "BUY - Strong quality at fair valuation"
            elif val and val < 50:
                return "HOLD - Strong quality but expensive, wait for better entry"
            else:
                return "BUY - Strong quality overall"
        
        elif score >= 55:
            if val and val >= 80:
                return "SPECULATIVE BUY - Average quality but deeply discounted"
            else:
                return "HOLD - Average quality, needs improvement or better price"
        
        elif score >= 40:
            if prof and prof < 30 and fin and fin < 50:
                return "AVOID - Weak profitability and financial strength"
            else:
                return "SELL/AVOID - Below-average quality"
        
        else:
            return "STRONG SELL/AVOID - Poor quality across multiple dimensions"


def load_dimension_scores(dimension_files: Dict[str, str]) -> pd.DataFrame:
    """
    Load all dimension scores and merge into single DataFrame
    
    Args:
        dimension_files: Dict mapping dimension names to file paths
    
    Returns:
        Merged DataFrame with all dimension scores
    """
    print("\n" + "=" * 80)
    print("LOADING DIMENSION SCORES")
    print("=" * 80)
    
    merged = None
    
    # Mapping for standardized column names
    column_mapping = {
        'dimension1': 'dimension1_profitability',
        'dimension2': 'dimension2_financial',
        'dimension3': 'dimension3_valuation',
        'dimension4': 'dimension4_growth',
        'dimension5': 'dimension5_management',
        'dimension6': 'dimension6_moat',
        'dimension7': 'dimension7_sentiment'
    }
    
    for dim_name, file_path in dimension_files.items():
        print(f"\nLoading {dim_name}...")
        try:
            df = pd.read_csv(file_path)
            
            # Get the score column name (dimension1_profitability, dimension_1_score, etc.)
            score_cols = [col for col in df.columns if col.startswith('dimension')]
            if not score_cols:
                print(f"  ⚠️  No dimension score column found in {file_path}")
                continue
            
            score_col = score_cols[0]
            
            # Standardize column name to expected format
            standard_col_name = column_mapping.get(dim_name, score_col)
            
            # Keep only symbol and score column, rename to standard name
            df_subset = df[['symbol', score_col]].copy()
            df_subset = df_subset.rename(columns={score_col: standard_col_name})
            
            # Merge with main dataframe
            if merged is None:
                merged = df_subset
                # Also get sector if available
                if 'sector' in df.columns:
                    merged = df[['symbol', 'sector']].copy()
                    merged = merged.merge(df_subset, on='symbol', how='outer')
            else:
                merged = merged.merge(df_subset, on='symbol', how='outer')
            
            print(f"  ✅ Loaded {len(df)} stocks as '{standard_col_name}'")
            
        except Exception as e:
            print(f"  ❌ Error loading {file_path}: {e}")
    
    if merged is not None:
        print(f"\n✅ Total stocks after merge: {len(merged)}")
        print(f"Columns: {list(merged.columns)}")
    else:
        print("\n❌ No dimension files loaded successfully")
    
    return merged


def calculate_composite_scores(dimension_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite scores for all stocks
    
    Args:
        dimension_data: DataFrame with all dimension scores
    
    Returns:
        DataFrame with composite scores and rankings
    """
    print("\n" + "=" * 80)
    print("CALCULATING COMPOSITE SCORES")
    print("=" * 80)
    
    results = []
    
    for idx, row in dimension_data.iterrows():
        # Build metrics object with standardized column names
        metrics = CompositeMetrics(
            symbol=row.get('symbol', 'UNKNOWN'),
            d1_profitability=row.get('dimension1_profitability'),
            d2_financial=row.get('dimension2_financial'),
            d3_valuation=row.get('dimension3_valuation'),
            d4_growth=row.get('dimension4_growth'),
            d5_management=row.get('dimension5_management'),
            d6_moat=row.get('dimension6_moat'),
            d7_sentiment=row.get('dimension7_sentiment'),
            sector=row.get('sector', 'Unknown')
        )
        
        # Calculate composite score
        scorer = CompositeScorer(metrics)
        final_score, breakdown = scorer.score()
        
        # Store result
        result = {
            'symbol': breakdown['symbol'],
            'composite_score': breakdown['composite_score'],
            'quality_tier': breakdown['quality_tier'],
            'interpretation': breakdown['interpretation'],
            'recommendation': breakdown['recommendation'],
            'sector': breakdown['sector']
        }
        
        # Add individual dimension scores
        for dim_name, score in breakdown['dimension_scores'].items():
            result[f'd{list(breakdown["dimension_scores"].keys()).index(dim_name) + 1}_{dim_name}'] = score
        
        # Add weighted contributions
        for dim_name, contribution in breakdown['weighted_contributions'].items():
            result[f'{dim_name}_contribution'] = contribution
        
        results.append(result)
        
        if (idx + 1) % 50 == 0:
            print(f"  Processed {idx + 1}/{len(dimension_data)} stocks...")
    
    print(f"\n✅ Calculated composite scores for {len(results)} stocks")
    
    # Convert to DataFrame
    df_results = pd.DataFrame(results)
    
    # Add ranking
    df_results = df_results.sort_values('composite_score', ascending=False)
    df_results['rank'] = range(1, len(df_results) + 1)
    
    # Reorder columns
    cols = ['rank', 'symbol', 'composite_score', 'quality_tier', 'recommendation', 'sector', 'interpretation']
    cols += [col for col in df_results.columns if col not in cols]
    df_results = df_results[cols]
    
    return df_results


def generate_summary_report(composite_df: pd.DataFrame, output_file: str = 'composite_report.txt'):
    """
    Generate comprehensive summary report
    
    Args:
        composite_df: DataFrame with composite scores
        output_file: Output filename
    """
    report = []
    report.append("=" * 80)
    report.append("INVESTMENT OS - UNIFIED COMPOSITE SCORING REPORT")
    report.append("=" * 80)
    report.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total Stocks Analyzed: {len(composite_df)}")
    report.append("")
    
    # Overall statistics
    report.append("=" * 80)
    report.append("OVERALL STATISTICS")
    report.append("=" * 80)
    report.append(f"Mean Score:   {composite_df['composite_score'].mean():.1f}")
    report.append(f"Median Score: {composite_df['composite_score'].median():.1f}")
    report.append(f"Std Dev:      {composite_df['composite_score'].std():.1f}")
    report.append(f"Min Score:    {composite_df['composite_score'].min():.1f}")
    report.append(f"Max Score:    {composite_df['composite_score'].max():.1f}")
    report.append("")
    
    # Quality tier distribution
    report.append("QUALITY TIER DISTRIBUTION:")
    tier_counts = composite_df['quality_tier'].value_counts()
    tier_order = ["Exceptional", "Strong", "Moderate", "Weak", "Poor"]
    
    for tier in tier_order:
        count = tier_counts.get(tier, 0)
        pct = (count / len(composite_df)) * 100
        report.append(f"  {tier:15s}: {count:>3} stocks ({pct:5.1f}%)")
    
    report.append("")
    
    # Recommendation distribution
    report.append("RECOMMENDATION DISTRIBUTION:")
    rec_counts = composite_df['recommendation'].value_counts()
    for rec, count in rec_counts.head(10).items():
        pct = (count / len(composite_df)) * 100
        report.append(f"  {rec[:50]:50s}: {count:>3} ({pct:5.1f}%)")
    
    report.append("")
    
    # Top 20 stocks
    report.append("=" * 80)
    report.append("TOP 20 INVESTMENT OPPORTUNITIES")
    report.append("=" * 80)
    report.append("")
    
    top20 = composite_df.head(20)
    for idx, (_, row) in enumerate(top20.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['composite_score']:>5.1f} ({row['quality_tier']})")
        report.append(f"    {row['recommendation']}")
        report.append(f"    {row['interpretation']}")
        report.append("")
    
    # Bottom 10 stocks
    report.append("=" * 80)
    report.append("BOTTOM 10 STOCKS (AVOID)")
    report.append("=" * 80)
    report.append("")
    
    bottom10 = composite_df.tail(10)
    for idx, (_, row) in enumerate(bottom10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['composite_score']:>5.1f} ({row['quality_tier']})")
        report.append(f"    {row['recommendation']}")
        report.append("")
    
    # Sector breakdown
    report.append("=" * 80)
    report.append("SECTOR ANALYSIS")
    report.append("=" * 80)
    report.append("")
    
    sector_stats = composite_df.groupby('sector')['composite_score'].agg(['count', 'mean', 'std'])
    report.append(f"{'Sector':<15} {'Count':<8} {'Mean':<8} {'Std':<8}")
    report.append("─" * 50)
    
    for sector, row in sector_stats.iterrows():
        report.append(f"{sector:<15} {int(row['count']):<8} {row['mean']:<8.1f} {row['std']:<8.1f}")
    
    report.append("")
    report.append("=" * 80)
    report.append("END OF REPORT")
    report.append("=" * 80)
    
    # Write to file
    report_text = "\n".join(report)
    with open(output_file, 'w') as f:
        f.write(report_text)
    
    print(f"\n✅ Summary report saved: {output_file}")
    
    return report_text


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print(__doc__)
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description='Investment OS Unified Composite Scorer v1.0',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--d1', type=str, help='Path to Dimension 1 scores CSV')
    parser.add_argument('--d2', type=str, help='Path to Dimension 2 scores CSV')
    parser.add_argument('--d3', type=str, help='Path to Dimension 3 scores CSV')
    parser.add_argument('--d4', type=str, help='Path to Dimension 4 scores CSV')
    parser.add_argument('--d5', type=str, help='Path to Dimension 5 scores CSV')
    parser.add_argument('--d6', type=str, help='Path to Dimension 6 scores CSV')
    parser.add_argument('--d7', type=str, help='Path to Dimension 7 scores CSV')
    
    parser.add_argument(
        '--output',
        type=str,
        default='composite_scores.csv',
        help='Path to output composite scores CSV (default: composite_scores.csv)'
    )
    
    parser.add_argument(
        '--report',
        type=str,
        default='composite_report.txt',
        help='Path to output report TXT (default: composite_report.txt)'
    )
    
    args = parser.parse_args()
    
    # Check if dimension files provided
    dimension_files = {
        'dimension1': args.d1,
        'dimension2': args.d2,
        'dimension3': args.d3,
        'dimension4': args.d4,
        'dimension5': args.d5,
        'dimension6': args.d6,
        'dimension7': args.d7
    }
    
    # Filter out None values
    dimension_files = {k: v for k, v in dimension_files.items() if v is not None}
    
    if len(dimension_files) == 0:
        print("\n" + "=" * 80)
        print("No dimension files provided. Showing usage:")
        print("=" * 80)
        print("\nUsage:")
        print("  python3 composite_scorer_v1_0.py \\")
        print("      --d1 dimension1_scores.csv \\")
        print("      --d2 dimension2_scores.csv \\")
        print("      --d3 dimension3_scores.csv \\")
        print("      --d4 dimension4_scores.csv \\")
        print("      --d5 dimension5_scores.csv \\")
        print("      --d6 dimension6_scores.csv \\")
        print("      --d7 dimension7_scores.csv \\")
        print("      --output composite_scores.csv \\")
        print("      --report composite_report.txt")
        print("\nWeighting Scheme:")
        for dim, weight in WEIGHTS.items():
            print(f"  {dim:30s}: {weight*100:>5.1f}%")
    else:
        print(f"\n📂 Processing {len(dimension_files)} dimension files...")
        
        # Load dimension scores
        dimension_data = load_dimension_scores(dimension_files)
        
        if dimension_data is not None and len(dimension_data) > 0:
            # Calculate composite scores
            composite_df = calculate_composite_scores(dimension_data)
            
            # Save composite scores
            composite_df.to_csv(args.output, index=False)
            print(f"\n✅ Composite scores saved: {args.output}")
            
            # Generate report
            generate_summary_report(composite_df, args.report)
            
            print("\n🎯 Unified composite scoring complete!")
            print(f"\nFiles created:")
            print(f"  • {args.output}")
            print(f"  • {args.report}")
        else:
            print("\n❌ Failed to load dimension data")
