
"""
DIMENSION 7: MARKET SENTIMENT SCORER v1.0
Investment OS - 7D Scoring Framework
Date: January 1, 2026
Version: 1.0 (Production - Percentile-Based)

FRAMEWORK: Trading Activity + Price Momentum + Market Recognition

PHILOSOPHY (Lynch):
"The stock market is filled with individuals who know the price of everything, 
but the value of nothing. However, when the market recognizes quality, it can 
create powerful momentum. The key is to find quality companies before the market 
does, or ride the wave when validation occurs."

METHODOLOGY: EMPIRICAL PERCENTILE-BASED SCORING
This dimension uses data-driven thresholds derived from actual CSE distribution,
following industry best practices for quantitative analysis.

ARCHITECTURE:
Component 1 (40%): Trading Activity & Liquidity
  - Relative volume (percentile-based: 90th, 75th, 50th, 25th)
  - Uses actual CSE distribution, not arbitrary thresholds
  
Component 2 (35%): Price Momentum
  - Total return performance (TR5Y, TR3Y, TR1Y)
  - Percentile-based thresholds from CSE data
  - Trend analysis (accelerating vs decelerating)
  
Component 3 (25%): Market Recognition
  - Market cap size (percentile-based)
  - Beta volatility (adjusted for CSE mean=0.37)

CSE EMPIRICAL PERCENTILES (Data-Driven):
TR5Y: 90th=546%, 75th=340%, 50th=180%, 25th=72%
TR1Y: 90th=167%, 75th=102%, 50th=49%
Market Cap: 90th=75B, 75th=26B, 50th=9B LKR
RelVol: 90th=231, 75th=102, 50th=41
Beta: 75th=0.60, 50th=0.34, 25th=0.12

EXPECTED SCORES:
CTC:  85-95 (mega cap, strong momentum, high liquidity)
LION: 75-85 (strong momentum, good liquidity)
LOLC: 60-75 (large cap but declining momentum)
LOFC: 80-90 (strong performance, institutional favorite)
JKH:  50-65 (large cap but declining sentiment)

REGULATORY DEFENSE:
"We evaluate market sentiment using empirically-derived percentile thresholds from 
the Colombo Stock Exchange. Rather than arbitrary cutoffs, we use actual CSE 
distribution (50th, 75th, 90th percentiles) to classify trading activity, price 
momentum, and market capitalization. This data-driven approach aligns with 
quantitative finance best practices and eliminates subjective bias."
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class SentimentMetrics:
    """Metrics for market sentiment evaluation"""
    # Trading activity
    volume: Optional[float] = None
    avg_volume_10d: Optional[float] = None
    volume_to_shares: Optional[float] = None  # Volume/shares outstanding
    
    # Price momentum
    tr_1y: Optional[float] = None
    tr_3y: Optional[float] = None
    tr_5y: Optional[float] = None
    
    # Market recognition
    market_cap: Optional[float] = None
    beta: Optional[float] = None
    
    # Additional context
    shares_outstanding: Optional[float] = None
    price: Optional[float] = None
    
    # Sector
    sector: str = "Unknown"


class MarketSentimentScorer:
    """Score market sentiment based on trading and momentum"""
    
    def __init__(self, metrics: SentimentMetrics):
        self.metrics = metrics
        self.modifiers = {}
        self.penalties = {}
        self.component_scores = {}
    
    def score(self) -> Tuple[float, Dict]:
        """
        Calculate market sentiment score (0-100)
        
        Returns:
            Tuple of (final_score, breakdown_dict)
        """
        m = self.metrics
        
        # Sanity checks
        max_score_cap = 100
        sanity_flags = []
        
        # SANITY CHECK 1: Severe negative returns (TR5Y < -50%) = cap at 30
        if m.tr_5y is not None and m.tr_5y < -50:
            max_score_cap = min(max_score_cap, 30)
            sanity_flags.append('severe_underperformance')
        
        # SANITY CHECK 2: All negative returns = cap at 40
        if (m.tr_1y is not None and m.tr_3y is not None and m.tr_5y is not None and
            m.tr_1y < 0 and m.tr_3y < 0 and m.tr_5y < 0):
            max_score_cap = min(max_score_cap, 40)
            sanity_flags.append('consistent_negative_returns')
        
        # Component 1: Trading Activity (40%)
        activity_score = self._score_trading_activity()
        
        # Component 2: Price Momentum (35%)
        momentum_score = self._score_price_momentum()
        
        # Component 3: Market Recognition (25%)
        recognition_score = self._score_market_recognition()
        
        # Calculate base score
        base_score = (
            activity_score * 0.40 +
            momentum_score * 0.35 +
            recognition_score * 0.25
        )
        
        # Apply modifiers and penalties
        modifier_total = sum(self.modifiers.values())
        penalty_total = max(sum(self.penalties.values()), -40)  # Cap penalties
        
        # Calculate score before sanity cap
        uncapped_score = base_score + modifier_total + penalty_total
        
        # Apply sanity check cap
        final_score = max(0, min(max_score_cap, uncapped_score))
        
        breakdown = {
            'base_score': round(base_score, 2),
            'component_1_activity': round(activity_score, 2),
            'component_2_momentum': round(momentum_score, 2),
            'component_3_recognition': round(recognition_score, 2),
            'modifiers': self.modifiers.copy(),
            'penalties': self.penalties.copy(),
            'modifier_total': round(modifier_total, 2),
            'penalty_total': round(penalty_total, 2),
            'sanity_cap': max_score_cap,
            'sanity_flags': sanity_flags,
            'final_score': round(final_score, 2),
            'sector': m.sector
        }
        
        return final_score, breakdown
    
    def _score_trading_activity(self) -> float:
        """
        Component 1: Trading Activity & Liquidity (40% weight)
        
        Uses CSE EMPIRICAL PERCENTILES (data-driven approach)
        """
        m = self.metrics
        score = 40  # Start at moderate baseline
        
        # CSE Relative Volume Percentiles (empirically derived):
        # 90th: 231, 75th: 102, 50th: 41, 25th: 19
        if m.volume_to_shares is not None and pd.notna(m.volume_to_shares):
            rel_vol = m.volume_to_shares
            
            # Percentile-based scoring (empirically derived from CSE)
            if rel_vol > 231:
                # >90th percentile - exceptional activity
                score += 30
                self.modifiers['exceptional_activity'] = 15
            elif rel_vol > 102:
                # >75th percentile - very high activity
                score += 25
                self.modifiers['high_activity'] = 10
            elif rel_vol > 41:
                # >50th percentile - above average activity
                score += 20
            elif rel_vol > 19:
                # >25th percentile - moderate activity
                score += 15
            elif rel_vol > 6:
                # >10th percentile - low activity
                score += 10
            else:
                # <10th percentile - very low activity
                score += 5
                self.penalties['very_low_activity'] = -5
        else:
            # No data = assume average
            score += 15
        
        self.component_scores['trading_activity'] = score
        return max(0, min(100, score))
    
    def _score_price_momentum(self) -> float:
        """
        Component 2: Price Momentum (35% weight)
        
        Uses CSE EMPIRICAL PERCENTILES (data-driven approach)
        """
        m = self.metrics
        score = 35  # Start at moderate baseline
        
        # Sub-component 1: Long-term Performance (50% of component)
        # CSE TR5Y Percentiles: 90th: 546%, 75th: 340%, 50th: 180%, 25th: 72%
        if m.tr_5y is not None and pd.notna(m.tr_5y):
            if m.tr_5y > 546:
                # >90th percentile - exceptional
                score += 28
                self.modifiers['exceptional_returns'] = 15
            elif m.tr_5y > 340:
                # >75th percentile - very strong
                score += 24
                self.modifiers['strong_returns'] = 12
            elif m.tr_5y > 180:
                # >50th percentile - good
                score += 20
            elif m.tr_5y > 72:
                # >25th percentile - moderate
                score += 16
            elif m.tr_5y > 0:
                # Positive but below average
                score += 10
            elif m.tr_5y > -20:
                # Slightly negative
                score -= 5
            elif m.tr_5y > -50:
                # Negative
                score -= 12
                self.penalties['negative_returns'] = -12
            else:
                # Severe underperformance
                score -= 20
                self.penalties['severe_underperformance'] = -20
        
        # Sub-component 2: Momentum Trend (30% of component)
        # CSE TR3Y Percentiles: 75th: 304%, 50th: 140%, 25th: 40%
        if (m.tr_1y is not None and m.tr_3y is not None and 
            pd.notna(m.tr_1y) and pd.notna(m.tr_3y)):
            
            # Annualize TR3Y for comparison
            tr3y_annualized = m.tr_3y / 3
            
            if m.tr_1y > tr3y_annualized * 2.0:
                # Strongly accelerating
                score += 12
                self.modifiers['accelerating_momentum'] = 8
            elif m.tr_1y > tr3y_annualized * 1.5:
                # Accelerating
                score += 8
            elif m.tr_1y > tr3y_annualized:
                # Slightly accelerating
                score += 4
            elif m.tr_1y > 0 and m.tr_3y > 0:
                # Positive but decelerating
                score += 2
            elif m.tr_1y < 0 and m.tr_3y < 0:
                # Consistently negative
                score -= 10
                self.penalties['declining_momentum'] = -10
        
        # Sub-component 3: Recent Performance (20% of component)
        # CSE TR1Y Percentiles: 90th: 167%, 75th: 102%, 50th: 49%
        if m.tr_1y is not None and pd.notna(m.tr_1y):
            if m.tr_1y > 167:
                # >90th percentile
                score += 10
            elif m.tr_1y > 102:
                # >75th percentile
                score += 8
            elif m.tr_1y > 49:
                # >50th percentile
                score += 6
            elif m.tr_1y > 0:
                # Positive
                score += 4
            elif m.tr_1y > -20:
                # Moderate decline
                score -= 4
            else:
                # Sharp decline
                score -= 8
        
        self.component_scores['price_momentum'] = score
        return max(0, min(100, score))
    
    def _score_market_recognition(self) -> float:
        """
        Component 3: Market Recognition (25% weight)
        
        Uses CSE EMPIRICAL PERCENTILES (data-driven approach)
        """
        m = self.metrics
        score = 30  # Start at moderate baseline
        
        # Sub-component 1: Market Cap Size (70% of component)
        # CSE Market Cap Percentiles: 90th: 75B, 75th: 26B, 50th: 9B, 25th: 3B
        if m.market_cap is not None and pd.notna(m.market_cap):
            mcap_b = m.market_cap / 1_000_000_000
            
            if mcap_b > 75:
                # >90th percentile - mega cap
                score += 30
                self.modifiers['mega_cap'] = 15
            elif mcap_b > 26:
                # >75th percentile - large cap
                score += 26
                self.modifiers['large_cap'] = 12
            elif mcap_b > 9:
                # >50th percentile - mid cap
                score += 22
            elif mcap_b > 3:
                # >25th percentile - small-mid cap
                score += 18
            elif mcap_b > 1:
                # >10th percentile - small cap
                score += 14
            else:
                # <10th percentile - micro cap
                score += 10
        
        # Sub-component 2: Beta Stability (30% of component)
        # CSE Beta Percentiles: 25th: 0.12, 50th: 0.34, 75th: 0.60
        # Note: CSE mean beta (0.37) < 1.0, suggesting less volatile market
        if m.beta is not None and pd.notna(m.beta):
            if m.beta < 0.12:
                # <25th percentile - very stable
                score += 12
            elif m.beta < 0.34:
                # <50th percentile - stable
                score += 10
            elif m.beta < 0.60:
                # <75th percentile - average volatility
                score += 7
            elif m.beta < 1.0:
                # Above CSE average but still reasonable
                score += 4
            else:
                # Very high volatility for CSE
                score -= 3
                self.penalties['high_volatility'] = -3
        
        self.component_scores['market_recognition'] = score
        return max(0, min(100, score))
    
    def get_interpretation(self, score: float, sector: str) -> str:
        """Generate human-readable interpretation of the score"""
        if score >= 85:
            return f"Exceptional {sector} sentiment - strong momentum, high liquidity"
        elif score >= 70:
            return f"Strong {sector} sentiment - positive momentum, good liquidity"
        elif score >= 55:
            return f"Moderate {sector} sentiment - mixed signals"
        elif score >= 40:
            return f"Weak {sector} sentiment - limited interest"
        else:
            return f"Poor {sector} sentiment - negative momentum, low liquidity"


def score_stock_sentiment(stock_data: Dict) -> Tuple[float, Dict, str]:
    """
    Score a single stock's market sentiment
    
    Args:
        stock_data: Dictionary containing stock data
        
    Returns:
        Tuple of (score, breakdown, interpretation)
    """
    # Determine sector (use same classifier as other dimensions)
    industry = stock_data.get('industry')
    if industry is None:
        sector = "Industrial"
    else:
        industry_lower = str(industry).lower()
        if 'tobacco' in industry_lower or 'utilities' in industry_lower:
            sector = "Mature"
        elif any(kw in industry_lower for kw in ['bank', 'finance', 'insurance', 'credit']):
            sector = "Financial"
        else:
            sector = "Industrial"
    
    # Build metrics object
    metrics = SentimentMetrics(
        # Trading activity
        volume=stock_data.get('volume'),
        avg_volume_10d=stock_data.get('averagevolume'),  # Correct column name
        volume_to_shares=stock_data.get('relativevolume'),  # Use relative volume as proxy
        
        # Price momentum
        tr_1y=stock_data.get('tr1y'),
        tr_3y=stock_data.get('tr3y'),
        tr_5y=stock_data.get('tr5y'),
        
        # Market recognition
        market_cap=stock_data.get('marketcap'),
        beta=stock_data.get('beta'),
        
        # Additional
        shares_outstanding=None,  # Not available
        price=stock_data.get('price'),
        
        sector=sector
    )
    
    # Score the stock
    scorer = MarketSentimentScorer(metrics)
    final_score, breakdown = scorer.score()
    interpretation = scorer.get_interpretation(final_score, sector)
    
    return final_score, breakdown, interpretation


def batch_score_from_csv(csv_path: str) -> pd.DataFrame:
    """
    Score all stocks from CSV file (for VPS production use)
    
    Args:
        csv_path: Path to CSV file with stock data
    
    Returns: DataFrame with scores
    """
    print("=" * 80)
    print("DIMENSION 7: MARKET SENTIMENT SCORER v1.0")
    print("=" * 80)
    print(f"\nLoading data from: {csv_path}")
    
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} stocks")
    
    results = []
    errors = []
    
    print("\nScoring stocks...")
    for idx, row in df.iterrows():
        symbol = row.get('symbol', 'UNKNOWN')
        
        try:
            stock_data = row.to_dict()
            score, breakdown, interpretation = score_stock_sentiment(stock_data)
            
            results.append({
                'symbol': symbol,
                'dimension7_sentiment': score,
                'activity_score': breakdown['component_1_activity'],
                'momentum_score': breakdown['component_2_momentum'],
                'recognition_score': breakdown['component_3_recognition'],
                'modifiers': breakdown['modifier_total'],
                'penalties': breakdown['penalty_total'],
                'sector': breakdown['sector'],
                'interpretation': interpretation
            })
            
            if (idx + 1) % 50 == 0:
                print(f"  Processed {idx + 1}/{len(df)} stocks...")
                
        except Exception as e:
            errors.append({'symbol': symbol, 'error': str(e)})
            results.append({
                'symbol': symbol,
                'dimension7_sentiment': None,
                'activity_score': None,
                'momentum_score': None,
                'recognition_score': None,
                'modifiers': None,
                'penalties': None,
                'sector': 'Unknown',
                'interpretation': f'Error: {e}'
            })
    
    print(f"\n✅ Scoring complete!")
    print(f"  Success: {len(results) - len(errors)}/{len(df)}")
    print(f"  Errors: {len(errors)}/{len(df)}")
    
    if errors:
        print("\nFirst 5 errors:")
        for err in errors[:5]:
            print(f"  {err['symbol']}: {err['error']}")
    
    return pd.DataFrame(results)


def generate_validation_report(scores_df: pd.DataFrame, output_file: str = 'dimension7_report.txt'):
    """
    Generate human-readable validation report
    
    Args:
        scores_df: DataFrame with scores
        output_file: Output filename for report
    """
    
    # Validation stocks
    validation_stocks = {
        'CTC.N0000': {'expected': (85, 95), 'description': 'Stable blue chip - high liquidity, steady returns'},
        'LION.N0000': {'expected': (75, 85), 'description': 'Strong momentum - good liquidity'},
        'LOLC.N0000': {'expected': (60, 75), 'description': 'Declining momentum but liquid'},
        'LOFC.N0000': {'expected': (80, 90), 'description': 'Strong performance - institutional favorite'},
        'JKH.N0000': {'expected': (50, 65), 'description': 'Large cap but declining sentiment'}
    }
    
    report = []
    report.append("=" * 80)
    report.append("DIMENSION 7: MARKET SENTIMENT SCORER v1.0 - VALIDATION REPORT")
    report.append("=" * 80)
    report.append(f"Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total Stocks Scored: {len(scores_df)}")
    report.append("")
    
    # Overall statistics
    report.append("=" * 80)
    report.append("OVERALL STATISTICS")
    report.append("=" * 80)
    report.append(f"Mean:   {scores_df['dimension7_sentiment'].mean():.1f}")
    report.append(f"Median: {scores_df['dimension7_sentiment'].median():.1f}")
    report.append(f"Std:    {scores_df['dimension7_sentiment'].std():.1f}")
    report.append(f"Min:    {scores_df['dimension7_sentiment'].min():.1f}")
    report.append(f"Max:    {scores_df['dimension7_sentiment'].max():.1f}")
    report.append("")
    
    # Score distribution
    report.append("SCORE DISTRIBUTION:")
    bands = [
        (85, 100, "Exceptional"),
        (70, 85, "Strong"),
        (55, 70, "Moderate"),
        (40, 55, "Weak"),
        (0, 40, "Poor")
    ]
    
    for low, high, label in bands:
        count = len(scores_df[(scores_df['dimension7_sentiment'] >= low) & 
                              (scores_df['dimension7_sentiment'] < high)])
        pct = (count / len(scores_df)) * 100
        report.append(f"  {label:15s} ({low:>3}-{high:<3}): {count:>3} stocks ({pct:5.1f}%)")
    
    report.append("")
    
    # Sector breakdown
    report.append("SECTOR BREAKDOWN:")
    sector_stats = scores_df.groupby('sector')['dimension7_sentiment'].agg(['count', 'mean'])
    for sector, row in sector_stats.iterrows():
        report.append(f"  {sector:12s}: {int(row['count']):>3} stocks, mean = {row['mean']:5.1f}")
    report.append("")
    
    # Validation results
    report.append("=" * 80)
    report.append("VALIDATION RESULTS")
    report.append("=" * 80)
    report.append("")
    
    for symbol, info in validation_stocks.items():
        stock = scores_df[scores_df['symbol'] == symbol]
        
        if stock.empty:
            report.append(f"❌ {symbol} - NOT FOUND")
            report.append("")
            continue
        
        score = stock['dimension7_sentiment'].values[0]
        expected_min, expected_max = info['expected']
        passed = expected_min <= score <= expected_max
        
        status = "✅ PASS" if passed else "❌ FAIL"
        
        report.append(f"{status} {symbol} - {stock.iloc[0]['interpretation']}")
        report.append(f"─" * 60)
        report.append(f"Expected: {expected_min}-{expected_max}")
        report.append(f"Actual:   {score:.1f}")
        report.append(f"Sector:   {stock['sector'].values[0]}")
        report.append(f"Description: {info['description']}")
        report.append("")
        report.append("Component Breakdown:")
        report.append(f"  Trading Activity:    {stock['activity_score'].values[0]:>6.1f} (40% weight)")
        report.append(f"  Price Momentum:      {stock['momentum_score'].values[0]:>6.1f} (35% weight)")
        report.append(f"  Market Recognition:  {stock['recognition_score'].values[0]:>6.1f} (25% weight)")
        report.append(f"  Modifiers:           {stock['modifiers'].values[0]:>+6.1f}")
        report.append(f"  Penalties:           {stock['penalties'].values[0]:>+6.1f}")
        report.append("")
    
    # Top performers
    report.append("=" * 80)
    report.append("TOP 10 MARKET SENTIMENT")
    report.append("=" * 80)
    report.append("")
    
    top10 = scores_df.nlargest(10, 'dimension7_sentiment')
    for idx, (_, row) in enumerate(top10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension7_sentiment']:5.1f} ({row['sector']})")
    
    report.append("")
    
    # Bottom performers
    report.append("=" * 80)
    report.append("BOTTOM 10 MARKET SENTIMENT (RED FLAGS)")
    report.append("=" * 80)
    report.append("")
    
    bottom10 = scores_df.nsmallest(10, 'dimension7_sentiment')
    for idx, (_, row) in enumerate(bottom10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension7_sentiment']:5.1f} ({row['sector']})")
    
    report.append("")
    report.append("=" * 80)
    report.append("END OF REPORT")
    report.append("=" * 80)
    
    # Write to file
    report_text = "\n".join(report)
    with open(output_file, 'w') as f:
        f.write(report_text)
    
    print(f"\n✅ Validation report saved: {output_file}")
    
    return report_text


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    print(__doc__)
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description='Dimension 7: Market Sentiment Scorer v1.0 - Investment OS 7D Framework',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--input',
        type=str,
        required=False,
        help='Path to input CSV file (e.g., output/2025-12-30/cleaned_data.csv)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='dimension7_scores.csv',
        help='Path to output scores CSV file (default: dimension7_scores.csv)'
    )
    
    parser.add_argument(
        '--report',
        type=str,
        default='dimension7_report.txt',
        help='Path to output validation report TXT file (default: dimension7_report.txt)'
    )
    
    args = parser.parse_args()
    
    # Check if input file provided
    if args.input:
        csv_path = args.input
        output_csv = args.output
        output_report = args.report
        
        print(f"\n📂 Input CSV: {csv_path}")
        print(f"📊 Output CSV: {output_csv}")
        print(f"📄 Output Report: {output_report}")
        
        # Score all stocks from CSV
        scores_df = batch_score_from_csv(csv_path)
        
        # Save scores CSV
        scores_df.to_csv(output_csv, index=False)
        print(f"\n✅ Scores saved: {output_csv}")
        
        # Generate validation report
        generate_validation_report(scores_df, output_report)
        
        print("\n🎯 Dimension 7 scoring complete!")
        print(f"\nFiles created:")
        print(f"  • {output_csv}")
        print(f"  • {output_report}")
        
    else:
        # Show usage
        print("\n" + "=" * 80)
        print("No input file provided. Showing usage:")
        print("=" * 80)
        print("\nUsage for production:")
        print("  python3 dimension7_scorer_v1_0.py \\")
        print("      --input output/2025-12-30/cleaned_data.csv \\")
        print("      --output dimension7_scores.csv \\")
        print("      --report dimension7_report.txt")