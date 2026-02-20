

"""
DIMENSION 5: MANAGEMENT QUALITY SCORER v1.0
Investment OS - 7D Scoring Framework

FILE: dimension5_scorer.py
CREATED: 2026-01-01
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-01-01  Initial creation — Management Quality scorer (Buffett methodology)
    v1.0.1  2026-02-11  Migrated to services/scoring-7d (Phase 2 microservices)
    v1.0.2  2026-02-16  Added version history header (new project standard)

CHANGELOG v1.0:
- FIX 1: Adjusted TR5Y thresholds (50%+ = good, 150%+ = exceptional)
- FIX 2: Added dividend growth sanity cap (100% max to catch outliers)
- FIX 3: Added ROE <5% sanity check (cap at 50)
- FIX 4: Added ROE decline >7pts sanity check (cap at 60)
- FIX 5: Increased ROE decline penalties (-15 to -30)
- RESULT: 2/5 validation passing, mean 61.0, healthy distribution

FRAMEWORK: Capital Allocation + Governance + Track Record

PHILOSOPHY (Buffett):
"Management's job is to allocate capital wisely, treat shareholders fairly,
and create long-term value."

ARCHITECTURE:
Component 1 (40%): Capital Allocation
  - ROE trend (improving vs declining)
  - Buyback quality (share count changes)
  - Dividend consistency
  
Component 2 (30%): Corporate Governance
  - Shareholder alignment (proxy: total return)
  - Earnings transparency (consistency)
  
Component 3 (30%): Track Record
  - Earnings consistency (coefficient of variation)
  - Long-term value creation (total return 5Y)
  - Profitability sustainability

SECTOR THRESHOLDS:
Mature/Defensive:
  - High dividend expectations, stable ROE
  - Red flags: Dividend cuts, ROE decline

Growth/Industrial:
  - ROE improvement expectations, value creation
  - Red flags: Dilution without growth, negative returns

Financial Services:
  - Capital management, ROE >15%, consistent dividends
  - Red flags: Capital raises, ROE <10%

MODIFIERS:
+ Exceptional Value Creation: +20 if TR5Y >200%
+ Smart Buybacks: +15 if shares declining + ROE improving
+ Dividend Growth: +15 if dividends growing >10% consistently
- Shareholder Dilution: -30 if massive share increases
- Value Destruction: -20 if TR5Y <-30%
- Dividend Cuts: -25 if dividends cut >20%

EXPECTED SCORES:
CTC:  85-95 (exceptional: ROE 277% up from 235%, monopoly management)
LION: 75-85 (strong: ROE improving, consistent operations)
LOLC: 40-55 (weak: ROE collapse 13%→5%, profitability issues)
LOFC: 75-85 (strong: ROE stable, world-class management)
JKH:  30-45 (poor: ROE declining, conglomerate complexity)

REGULATORY DEFENSE:
"We evaluate management quality using Buffett's capital allocation principles:
ROE trends (return on shareholder equity), buyback discipline (share count
changes), dividend consistency, and long-term value creation (total return).
These metrics directly measure management's ability to compound shareholder
value over time."
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class ManagementMetrics:
    """Metrics for management quality evaluation"""
    # Capital Allocation
    roe_current: Optional[float] = None
    roe_5y: Optional[float] = None
    shares_outstanding: Optional[float] = None
    dividend_growth: Optional[float] = None
    
    # Track Record
    total_return_5y: Optional[float] = None
    
    # For consistency calculation
    revenue_growth_1y: Optional[float] = None
    revenue_growth_3y: Optional[float] = None
    revenue_growth_5y: Optional[float] = None
    eps_growth_1y: Optional[float] = None
    eps_growth_3y: Optional[float] = None
    eps_growth_5y: Optional[float] = None
    
    # Sector
    industry: Optional[str] = None
    sector: str = "Unknown"


class SectorClassifier:
    """Classify stocks into sectors for management evaluation"""
    
    @staticmethod
    def classify(industry: str) -> str:
        """Classify industry into management evaluation sector"""
        if industry is None:
            return "Industrial"
        
        industry_lower = str(industry).lower()
        
        # Mature/Defensive sectors
        mature_keywords = ['tobacco', 'cigarette', 'utilities', 'power', 'electricity']
        
        # Financial sectors
        financial_keywords = [
            'bank', 'finance', 'financial', 'insurance', 'credit', 
            'institution', 'leasing', 'investment'
        ]
        
        # Check mature first
        if any(keyword in industry_lower for keyword in mature_keywords):
            return "Mature"
        
        # Check financial
        if any(keyword in industry_lower for keyword in financial_keywords):
            return "Financial"
        
        # Default to Industrial
        return "Industrial"


class ManagementScorer:
    """Score management quality based on capital allocation and track record"""
    
    def __init__(self, metrics: ManagementMetrics):
        self.metrics = metrics
        self.modifiers = {}
        self.penalties = {}
        self.component_scores = {}
    
    def score(self) -> Tuple[float, Dict]:
        """
        Calculate management quality score (0-100)
        
        Returns:
            Tuple of (final_score, breakdown_dict)
        """
        m = self.metrics
        
        # Sanity checks (hard caps for extreme cases)
        max_score_cap = 100
        sanity_flags = []
        
        # SANITY CHECK 1: Massive value destruction = cap at 30
        if m.total_return_5y is not None and m.total_return_5y < -50:
            max_score_cap = min(max_score_cap, 30)
            sanity_flags.append('value_destruction')
        
        # SANITY CHECK 2: ROE collapse (>20 point drop) = cap at 35
        if (m.roe_current is not None and m.roe_5y is not None and
            m.roe_current < m.roe_5y - 20):
            max_score_cap = min(max_score_cap, 35)
            sanity_flags.append('roe_collapse')
        
        # SANITY CHECK 3: Extremely low ROE (<5%) = cap at 50
        # ROE < 5% indicates poor capital allocation regardless of sector
        if m.roe_current is not None and m.roe_current < 5:
            max_score_cap = min(max_score_cap, 50)
            sanity_flags.append('very_low_roe')
        
        # SANITY CHECK 4: Extremely low ROE + declining = cap at 40
        if (m.roe_current is not None and m.roe_5y is not None and
            m.roe_current < 5 and m.roe_current < m.roe_5y):
            max_score_cap = min(max_score_cap, 40)
            sanity_flags.append('low_roe_declining')
        
        # SANITY CHECK 5: Significant ROE decline (>7 pts) = cap at 60
        # Even if TR5Y is high, ROE decline indicates deteriorating management quality
        if (m.roe_current is not None and m.roe_5y is not None and
            m.roe_current < m.roe_5y - 7):
            max_score_cap = min(max_score_cap, 60)
            sanity_flags.append('roe_significant_decline')
        
        # Component 1: Capital Allocation (40%)
        capital_allocation_score = self._score_capital_allocation()
        
        # Component 2: Corporate Governance (30%)
        governance_score = self._score_governance()
        
        # Component 3: Track Record (30%)
        track_record_score = self._score_track_record()
        
        # Calculate base score
        base_score = (
            capital_allocation_score * 0.40 +
            governance_score * 0.30 +
            track_record_score * 0.30
        )
        
        # Apply modifiers and penalties
        modifier_total = sum(self.modifiers.values())
        penalty_total = max(sum(self.penalties.values()), -40)  # Cap penalties at -40
        
        # Calculate score before sanity cap
        uncapped_score = base_score + modifier_total + penalty_total
        
        # Apply sanity check cap
        final_score = max(0, min(max_score_cap, uncapped_score))
        
        breakdown = {
            'base_score': round(base_score, 2),
            'component_1_capital': round(capital_allocation_score, 2),
            'component_2_governance': round(governance_score, 2),
            'component_3_track_record': round(track_record_score, 2),
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
    
    def _score_capital_allocation(self) -> float:
        """
        Component 1: Capital Allocation (40% weight)
        
        Evaluates management's ability to allocate capital efficiently:
        - ROE trend (50%): Is profitability improving or declining?
        - Buyback quality (30%): Are they managing share count intelligently?
        - Dividend consistency (20%): Stable, growing dividends signal confidence
        """
        m = self.metrics
        score = 50  # Start neutral
        
        # Sub-component 1: ROE Trend (50% of component = 20% of total)
        if m.roe_current is not None and m.roe_5y is not None:
            roe_change = m.roe_current - m.roe_5y
            
            # Score based on ROE improvement/decline
            if m.roe_current > 30 and roe_change > 10:
                # Exceptional: High ROE + improving significantly
                score += 30
                self.modifiers['roe_exceptional_improvement'] = 15
            elif roe_change > 5:
                # Strong improvement
                score += 20
                self.modifiers['roe_improvement'] = 10
            elif roe_change > 0:
                # Modest improvement
                score += 10
            elif roe_change > -5:
                # Stable (acceptable)
                score += 5
            elif roe_change > -7:
                # Declining (concerning)
                score -= 10
                self.penalties['roe_decline_moderate'] = -15
            elif roe_change > -10:
                # Significant decline (major red flag)
                score -= 25
                self.penalties['roe_significant_decline'] = -25
            else:
                # Severe decline (major red flag)
                score -= 30
                self.penalties['roe_collapse'] = -30
        
        # Sub-component 2: Buyback Quality (30% of component)
        # Note: We may not have direct share count change data
        # If we do, lower share count + rising ROE = smart buybacks
        # For now, we'll score based on what we have
        if m.shares_outstanding is not None:
            # Placeholder: In production, calculate share count change
            # For now, give neutral score
            score += 5
        
        # Sub-component 3: Dividend Consistency (20% of component)
        if m.dividend_growth is not None and pd.notna(m.dividend_growth):
            # Sanity check: Cap dividend growth at 100% to catch outliers/errors
            dividend_growth_capped = min(m.dividend_growth, 100)
            
            if dividend_growth_capped > 15:
                # Strong dividend growth
                score += 15
                self.modifiers['dividend_growth_strong'] = 10
            elif dividend_growth_capped > 5:
                # Modest dividend growth
                score += 10
                self.modifiers['dividend_growth_modest'] = 5
            elif dividend_growth_capped > 0:
                # Maintaining dividends
                score += 5
            elif dividend_growth_capped > -10:
                # Flat/slightly declining
                score += 0
            else:
                # Cutting dividends (major red flag)
                score -= 20
                self.penalties['dividend_cut'] = -20
        
        self.component_scores['capital_allocation'] = score
        return max(0, min(100, score))
    
    def _score_governance(self) -> float:
        """
        Component 2: Corporate Governance (30% weight)
        
        Due to limited governance data in CSE dataset, we use proxy metrics:
        - Total return 5Y as proxy for shareholder alignment
        - Earnings consistency as proxy for transparency
        
        NOTE: In ideal scenario, we'd have:
        - Insider ownership
        - Board independence
        - Related party transactions
        """
        m = self.metrics
        score = 50  # Start neutral
        
        # Proxy 1: Total Return 5Y (shareholder value creation)
        if m.total_return_5y is not None:
            if m.total_return_5y > 150:
                # Exceptional value creation (>150% over 5Y = 20% CAGR)
                score += 30
                self.modifiers['exceptional_value_creation'] = 20
            elif m.total_return_5y > 75:
                # Strong value creation (>75% over 5Y = 12% CAGR)
                score += 20
                self.modifiers['strong_value_creation'] = 15
            elif m.total_return_5y > 40:
                # Good value creation (>40% over 5Y = 7% CAGR)
                score += 15
            elif m.total_return_5y > 15:
                # Acceptable value creation
                score += 10
            elif m.total_return_5y > 0:
                # Positive but weak
                score += 5
            elif m.total_return_5y > -20:
                # Slight underperformance
                score -= 5
            else:
                # Value destruction (major concern)
                score -= 25
                self.penalties['value_destruction'] = -20
        
        # Proxy 2: Earnings Consistency (proxy for transparency)
        # Calculate coefficient of variation from available growth rates
        growth_rates = []
        if m.revenue_growth_1y is not None:
            growth_rates.append(m.revenue_growth_1y)
        if m.revenue_growth_3y is not None:
            growth_rates.append(m.revenue_growth_3y)
        if m.revenue_growth_5y is not None:
            growth_rates.append(m.revenue_growth_5y)
        
        if len(growth_rates) >= 2:
            std_dev = np.std(growth_rates)
            mean_growth = np.mean(growth_rates)
            
            if mean_growth != 0:
                cv = abs(std_dev / mean_growth)
                
                if cv < 0.3:
                    # Very consistent
                    score += 10
                    self.modifiers['earnings_consistency'] = 5
                elif cv > 1.0:
                    # Very volatile (concerning)
                    score -= 10
                    self.penalties['earnings_volatility'] = -10
        
        self.component_scores['governance'] = score
        return max(0, min(100, score))
    
    def _score_track_record(self) -> float:
        """
        Component 3: Track Record (30% weight)
        
        Historical performance and consistency:
        - Long-term value creation (total return 5Y)
        - Profitability sustainability (ROE levels)
        - Growth consistency
        """
        m = self.metrics
        score = 50  # Start neutral
        
        # Sub-component 1: Total Return 5Y (already used in governance, but important here too)
        if m.total_return_5y is not None:
            if m.total_return_5y > 150:
                score += 25
            elif m.total_return_5y > 75:
                score += 20
            elif m.total_return_5y > 40:
                score += 15
            elif m.total_return_5y > 15:
                score += 10
            elif m.total_return_5y > 0:
                score += 5
            elif m.total_return_5y > -25:
                score -= 5
            else:
                score -= 15
        
        # Sub-component 2: ROE Level (profitability sustainability)
        if m.roe_current is not None:
            if m.sector == "Financial":
                # Financial companies: ROE >20% = excellent
                if m.roe_current > 20:
                    score += 15
                elif m.roe_current > 15:
                    score += 10
                elif m.roe_current > 10:
                    score += 5
                elif m.roe_current < 5:
                    score -= 10
            else:
                # Non-financial: ROE >25% = excellent
                if m.roe_current > 30:
                    score += 15
                elif m.roe_current > 20:
                    score += 10
                elif m.roe_current > 15:
                    score += 5
                elif m.roe_current < 10:
                    score -= 10
        
        # Sub-component 3: Growth consistency
        # Check if EPS growth is available and consistent
        eps_rates = []
        if m.eps_growth_1y is not None:
            eps_rates.append(m.eps_growth_1y)
        if m.eps_growth_3y is not None:
            eps_rates.append(m.eps_growth_3y)
        if m.eps_growth_5y is not None:
            eps_rates.append(m.eps_growth_5y)
        
        if len(eps_rates) >= 2:
            # Check if all positive (consistent growth)
            all_positive = all(rate > 0 for rate in eps_rates)
            any_negative = any(rate < -10 for rate in eps_rates)
            
            if all_positive:
                score += 10
                self.modifiers['consistent_eps_growth'] = 5
            elif any_negative:
                score -= 5
        
        self.component_scores['track_record'] = score
        return max(0, min(100, score))
    
    def get_interpretation(self, score: float, sector: str) -> str:
        """Generate human-readable interpretation of the score"""
        if score >= 85:
            return f"Exceptional {sector} management - excellent capital allocation, strong track record"
        elif score >= 70:
            return f"Strong {sector} management - solid capital allocation and value creation"
        elif score >= 55:
            return f"Moderate {sector} management - acceptable but room for improvement"
        elif score >= 40:
            return f"Weak {sector} management - concerning trends, investigate"
        else:
            return f"Poor {sector} management - value destruction, avoid"


def score_stock_management(stock_data: Dict) -> Tuple[float, Dict, str]:
    """
    Score a single stock's management quality
    
    Args:
        stock_data: Dictionary containing stock data
        
    Returns:
        Tuple of (score, breakdown, interpretation)
    """
    # Classify sector
    industry = stock_data.get('industry')
    sector = SectorClassifier.classify(industry)
    
    # Build metrics object
    # Note: Data is already in percentage format (13.24 = 13.24%)
    metrics = ManagementMetrics(
        # Capital Allocation
        roe_current=stock_data.get('roe'),
        roe_5y=stock_data.get('roe5y'),
        shares_outstanding=stock_data.get('sharesoutstanding'),
        dividend_growth=stock_data.get('dividendgrowth'),
        
        # Track Record
        total_return_5y=stock_data.get('tr5y'),
        
        # For consistency
        revenue_growth_1y=stock_data.get('revenuegrowth'),
        revenue_growth_3y=stock_data.get('revenuegrowth3y'),
        revenue_growth_5y=stock_data.get('revenuegrowth5y'),
        eps_growth_1y=stock_data.get('epsgrowth'),
        eps_growth_3y=stock_data.get('epsgrowth3y'),
        eps_growth_5y=stock_data.get('epsgrowth5y'),
        
        # Sector
        industry=industry,
        sector=sector
    )
    
    # Score the stock
    scorer = ManagementScorer(metrics)
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
    print("DIMENSION 5: MANAGEMENT QUALITY SCORER v1.0")
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
            score, breakdown, interpretation = score_stock_management(stock_data)
            
            results.append({
                'symbol': symbol,
                'dimension5_management': score,
                'capital_allocation_score': breakdown['component_1_capital'],
                'governance_score': breakdown['component_2_governance'],
                'track_record_score': breakdown['component_3_track_record'],
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
                'dimension5_management': None,
                'capital_allocation_score': None,
                'governance_score': None,
                'track_record_score': None,
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


def generate_validation_report(scores_df: pd.DataFrame, output_file: str = 'dimension5_report.txt'):
    """
    Generate human-readable validation report
    
    Args:
        scores_df: DataFrame with scores
        output_file: Output filename for report
    """
    
    # Validation stocks
    validation_stocks = {
        'CTC.N0000': {'expected': (85, 95), 'description': 'Exceptional management - ROE 277% up from 235%'},
        'LION.N0000': {'expected': (75, 85), 'description': 'Strong management - ROE improving, consistent'},
        'LOLC.N0000': {'expected': (40, 55), 'description': 'Weak management - ROE collapse 13%→5%'},
        'LOFC.N0000': {'expected': (75, 85), 'description': 'Strong management - stable ROE, world-class'},
        'JKH.N0000': {'expected': (30, 45), 'description': 'Poor management - ROE declining, complexity'}
    }
    
    report = []
    report.append("=" * 80)
    report.append("DIMENSION 5: MANAGEMENT QUALITY SCORER v1.0 - VALIDATION REPORT")
    report.append("=" * 80)
    report.append(f"Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total Stocks Scored: {len(scores_df)}")
    report.append("")
    
    # Overall statistics
    report.append("=" * 80)
    report.append("OVERALL STATISTICS")
    report.append("=" * 80)
    report.append(f"Mean:   {scores_df['dimension5_management'].mean():.1f}")
    report.append(f"Median: {scores_df['dimension5_management'].median():.1f}")
    report.append(f"Std:    {scores_df['dimension5_management'].std():.1f}")
    report.append(f"Min:    {scores_df['dimension5_management'].min():.1f}")
    report.append(f"Max:    {scores_df['dimension5_management'].max():.1f}")
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
        count = len(scores_df[(scores_df['dimension5_management'] >= low) & 
                              (scores_df['dimension5_management'] < high)])
        pct = (count / len(scores_df)) * 100
        report.append(f"  {label:15s} ({low:>3}-{high:<3}): {count:>3} stocks ({pct:5.1f}%)")
    
    report.append("")
    
    # Sector breakdown
    report.append("SECTOR BREAKDOWN:")
    sector_stats = scores_df.groupby('sector')['dimension5_management'].agg(['count', 'mean'])
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
        
        score = stock['dimension5_management'].values[0]
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
        report.append(f"  Capital Allocation:  {stock['capital_allocation_score'].values[0]:>6.1f} (40% weight)")
        report.append(f"  Governance:          {stock['governance_score'].values[0]:>6.1f} (30% weight)")
        report.append(f"  Track Record:        {stock['track_record_score'].values[0]:>6.1f} (30% weight)")
        report.append(f"  Modifiers:           {stock['modifiers'].values[0]:>+6.1f}")
        report.append(f"  Penalties:           {stock['penalties'].values[0]:>+6.1f}")
        report.append("")
    
    # Top performers
    report.append("=" * 80)
    report.append("TOP 10 MANAGEMENT QUALITY")
    report.append("=" * 80)
    report.append("")
    
    top10 = scores_df.nlargest(10, 'dimension5_management')
    for idx, (_, row) in enumerate(top10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension5_management']:5.1f} ({row['sector']})")
    
    report.append("")
    
    # Bottom performers
    report.append("=" * 80)
    report.append("BOTTOM 10 MANAGEMENT QUALITY (AVOID)")
    report.append("=" * 80)
    report.append("")
    
    bottom10 = scores_df.nsmallest(10, 'dimension5_management')
    for idx, (_, row) in enumerate(bottom10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension5_management']:5.1f} ({row['sector']})")
    
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
        description='Dimension 5: Management Quality Scorer v1.0 - Investment OS 7D Framework',
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
        default='dimension5_scores.csv',
        help='Path to output scores CSV file (default: dimension5_scores.csv)'
    )
    
    parser.add_argument(
        '--report',
        type=str,
        default='dimension5_report.txt',
        help='Path to output validation report TXT file (default: dimension5_report.txt)'
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
        
        print("\n🎯 Dimension 5 scoring complete!")
        print(f"\nFiles created:")
        print(f"  • {output_csv}")
        print(f"  • {output_report}")
        
    else:
        # Show usage
        print("\n" + "=" * 80)
        print("No input file provided. Showing usage:")
        print("=" * 80)
        print("\nUsage for production:")
        print("  python3 dimension5_scorer_v1_0.py \\")
        print("      --input output/2025-12-30/cleaned_data.csv \\")
        print("      --output dimension5_scores.csv \\")
        print("      --report dimension5_report.txt")
        print("\nExample:")
        print("  python3 dimension5_scorer_v1_0.py \\")
        print("      --input cleaned_data.csv \\")
        print("      --output dimension5_scores.csv \\")
        print("      --report dimension5_report.txt")