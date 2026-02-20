
"""
DIMENSION 6: BUSINESS QUALITY / MOAT SCORER v1.1
Investment OS - 7D Scoring Framework

FILE: dimension6_scorer.py
CREATED: 2026-01-01
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-01-01  Initial creation — Business Quality/Moat scorer
    v1.1.0  2026-01-01  Fix: Investment vehicle cap at 70 (Ceylon Guardian)
    v1.1.1  2026-02-11  Migrated to services/scoring-7d (Phase 2 microservices)
    v1.1.2  2026-02-16  Added version history header (new project standard)

CHANGELOG v1.1:
- FIX: Investment vehicles (industry='Investment Advice') capped at 70
- REASON: Investment trusts don't have traditional moats - compete on returns
- AFFECTED: GUAR.N0000 (100.0 → 70.0) - Ceylon Guardian Investment Trust
- METHODOLOGY: Should score on NAV discount, track record, fees (future v2.0)

CHANGELOG v1.0:
- FIX 1: Removed dependency on operatingmargin5y (data not available)
- FIX 2: Financial sector uses profit margin, not operating margin (negative op margin OK)
- FIX 3: Added financial sector sanity cap at 85 (non-monopoly businesses)
- FIX 4: Relaxed retention CV thresholds for companies with strong margins (>25%)
- FIX 5: Simplified pricing power to current margin only (no trend data)
- RESULT: 4/5 validation passing (80%), mean 63.4, healthy distribution

FRAMEWORK: Competitive Moat + Pricing Power + Customer Retention

PHILOSOPHY (Buffett):
"The key to investing is determining the competitive advantage of any given 
company and, above all, the durability of that advantage. The products or 
services that have wide, sustainable moats around them are the ones that 
deliver rewards to investors."

ARCHITECTURE:
Component 1 (40%): Competitive Moat Width
  - Gross margin (efficiency advantage)
  - Operating margin stability (sustainable advantage)
  - Market position indicators
  
Component 2 (30%): Pricing Power
  - Operating margin level (can charge premium)
  - Margin trends (pricing power sustainability)
  - Gross margin level (cost advantage)
  
Component 3 (30%): Customer Retention / Switching Costs
  - Revenue consistency (loyal customers)
  - Earnings stability (predictable business)
  - Growth consistency (customer base expansion)

MOAT TYPES:
1. Cost Advantage: Low operating costs, scale economies
2. Brand Power: Premium pricing, customer loyalty
3. Network Effects: Value increases with users
4. Switching Costs: Expensive to change providers
5. Regulatory/Legal: Licenses, patents, monopolies

EXPECTED SCORES:
CTC:  90-100 (monopoly: 78% margin, tobacco license, brand)
LION: 75-85 (strong moat: brand power, distribution, 28% margin)
LOLC: 60-75 (moderate moat: financial network, scale)
LOFC: 70-80 (good moat: financial relationships, ROA 5.2%)
JKH:  40-55 (weak moat: conglomerate, diverse, no focus)

REGULATORY DEFENSE:
"We evaluate business quality using Buffett's moat framework: competitive 
advantages that allow companies to maintain high returns on capital. We assess 
gross margins (cost advantage), operating margins (pricing power), and revenue 
consistency (customer retention). These metrics identify businesses with 
durable competitive advantages that can sustain profitability over decades."
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class BusinessMetrics:
    """Metrics for business quality / moat evaluation"""
    # Moat indicators
    gross_margin: Optional[float] = None
    operating_margin_current: Optional[float] = None
    
    # Pricing power
    revenue_per_employee: Optional[float] = None
    
    # Customer retention (consistency metrics)
    revenue_growth_1y: Optional[float] = None
    revenue_growth_3y: Optional[float] = None
    revenue_growth_5y: Optional[float] = None
    eps_growth_1y: Optional[float] = None
    eps_growth_3y: Optional[float] = None
    eps_growth_5y: Optional[float] = None
    
    # Profitability trends
    roe_current: Optional[float] = None
    roe_5y: Optional[float] = None
    
    # Sector
    industry: Optional[str] = None
    sector: str = "Unknown"


class SectorClassifier:
    """Classify stocks into sectors for business quality evaluation"""
    
    @staticmethod
    def classify(industry: str) -> str:
        """Classify industry into business quality sector"""
        if industry is None:
            return "Industrial"
        
        industry_lower = str(industry).lower()
        
        # Mature/Monopolistic sectors
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


class BusinessQualityScorer:
    """Score business quality / moat based on competitive advantages"""
    
    def __init__(self, metrics: BusinessMetrics):
        self.metrics = metrics
        self.modifiers = {}
        self.penalties = {}
        self.component_scores = {}
    
    def score(self) -> Tuple[float, Dict]:
        """
        Calculate business quality / moat score (0-100)
        
        Returns:
            Tuple of (final_score, breakdown_dict)
        """
        m = self.metrics
        
        # Sanity checks (hard caps for extreme cases)
        max_score_cap = 100
        sanity_flags = []
        
        # SANITY CHECK 1: Commodity business (gross margin <10%) = cap at 40
        if m.gross_margin is not None and m.gross_margin < 10:
            max_score_cap = min(max_score_cap, 40)
            sanity_flags.append('commodity_business')
        
        # SANITY CHECK 2: Negative operating margin FOR NON-FINANCIAL = cap at 30
        # Financial companies use profit margin, not operating margin
        if (m.operating_margin_current is not None and m.operating_margin_current < 0 and
            m.sector != "Financial"):
            max_score_cap = min(max_score_cap, 30)
            sanity_flags.append('negative_margin')
        
        # SANITY CHECK 3: Financial sector cap at 85 (unless monopoly-level margins >70%)
        # Financial companies rarely have true monopolies, cap at 85
        if (m.sector == "Financial" and m.operating_margin_current is not None and
            m.operating_margin_current < 70):
            max_score_cap = min(max_score_cap, 85)
            sanity_flags.append('financial_sector_cap')
        
        # Component 1: Competitive Moat Width (40%)
        moat_score = self._score_competitive_moat()
        
        # Component 2: Pricing Power (30%)
        pricing_score = self._score_pricing_power()
        
        # Component 3: Customer Retention (30%)
        retention_score = self._score_customer_retention()
        
        # Calculate base score
        base_score = (
            moat_score * 0.40 +
            pricing_score * 0.30 +
            retention_score * 0.30
        )
        
        # Apply modifiers and penalties
        modifier_total = sum(self.modifiers.values())
        penalty_total = max(sum(self.penalties.values()), -40)  # Cap penalties at -40
        
        # Calculate score before sanity cap
        uncapped_score = base_score + modifier_total + penalty_total
        
        # Apply sanity check cap
        final_score = max(0, min(max_score_cap, uncapped_score))
        
        # SANITY CHECK 4: Investment vehicles cap at 70 (v1.1 fix)
        # Investment trusts/funds don't have traditional "moats"
        # They compete on portfolio returns, not pricing power
        if m.industry and 'investment' in str(m.industry).lower():
            if final_score > 70:
                final_score = 70
                sanity_flags.append('investment_vehicle_cap')
        
        breakdown = {
            'base_score': round(base_score, 2),
            'component_1_moat': round(moat_score, 2),
            'component_2_pricing': round(pricing_score, 2),
            'component_3_retention': round(retention_score, 2),
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
    
    def _score_competitive_moat(self) -> float:
        """
        Component 1: Competitive Moat Width (40% weight)
        
        Evaluates the width and sustainability of competitive advantages:
        - Gross margin: Cost advantage or pricing power
        - Operating margin stability: Sustainable competitive position
        - ROE level: Returns on capital indicate moat
        
        High gross margin + stable operating margin = wide moat
        """
        m = self.metrics
        score = 50  # Start neutral
        
        # Sub-component 1: Gross Margin (70% of component)
        # Gross margin indicates pricing power or cost advantage
        if m.gross_margin is not None:
            if m.gross_margin > 70:
                # Exceptional moat (monopoly-like pricing power)
                score += 35
                self.modifiers['monopoly_pricing'] = 20
            elif m.gross_margin > 50:
                # Wide moat (strong pricing power)
                score += 28
                self.modifiers['wide_moat'] = 15
            elif m.gross_margin > 35:
                # Good moat (decent pricing power)
                score += 20
            elif m.gross_margin > 25:
                # Narrow moat
                score += 12
            elif m.gross_margin > 15:
                # Weak moat
                score += 6
            else:
                # Commodity business (no moat)
                score -= 15
                self.penalties['commodity'] = -15
        
        # Sub-component 2: ROE Level (30% of component)
        # High ROE = strong competitive advantage
        if m.roe_current is not None:
            if m.sector == "Financial":
                # Financial: ROE >20% = strong moat
                if m.roe_current > 25:
                    score += 15
                elif m.roe_current > 20:
                    score += 12
                elif m.roe_current > 15:
                    score += 8
                elif m.roe_current < 10:
                    score -= 5
            else:
                # Non-financial: ROE >25% = strong moat
                if m.roe_current > 35:
                    score += 15
                elif m.roe_current > 25:
                    score += 12
                elif m.roe_current > 20:
                    score += 8
                elif m.roe_current < 15:
                    score -= 5
        
        self.component_scores['competitive_moat'] = score
        return max(0, min(100, score))
    
    def _score_pricing_power(self) -> float:
        """
        Component 2: Pricing Power (30% weight)
        
        Ability to raise prices without losing customers:
        - Operating margin level: Higher = more pricing power
        - Margin trends: Improving = gaining pricing power
        - Gross margin: High = strong pricing power
        
        Companies with pricing power can pass costs to customers
        """
        m = self.metrics
        score = 50  # Start neutral
        
        # Sub-component 1: Operating Margin Level (100% of component)
        # Higher margin = more pricing power
        if m.operating_margin_current is not None:
            if m.operating_margin_current > 40:
                # Exceptional pricing power
                score += 30
                self.modifiers['exceptional_pricing_power'] = 15
            elif m.operating_margin_current > 30:
                # Strong pricing power
                score += 25
                self.modifiers['strong_pricing_power'] = 10
            elif m.operating_margin_current > 20:
                # Good pricing power
                score += 20
            elif m.operating_margin_current > 10:
                # Moderate pricing power
                score += 15
            elif m.operating_margin_current > 5:
                # Weak pricing power
                score += 10
            else:
                # No pricing power (if not financial)
                if m.sector != "Financial":
                    score -= 10
        
        self.component_scores['pricing_power'] = score
        return max(0, min(100, score))
    
    def _score_customer_retention(self) -> float:
        """
        Component 3: Customer Retention / Switching Costs (30% weight)
        
        Evaluates customer loyalty and switching costs:
        - Revenue consistency: Loyal customers = consistent revenue
        - Earnings stability: Predictable business = customer retention
        - Growth consistency: Expanding customer base
        
        High switching costs = customers stay even if prices rise
        """
        m = self.metrics
        score = 50  # Start neutral
        
        # Sub-component 1: Revenue Consistency (50% of component)
        # Calculate coefficient of variation for revenue growth
        revenue_rates = []
        if m.revenue_growth_1y is not None and pd.notna(m.revenue_growth_1y):
            revenue_rates.append(m.revenue_growth_1y)
        if m.revenue_growth_3y is not None and pd.notna(m.revenue_growth_3y):
            revenue_rates.append(m.revenue_growth_3y)
        if m.revenue_growth_5y is not None and pd.notna(m.revenue_growth_5y):
            revenue_rates.append(m.revenue_growth_5y)
        
        if len(revenue_rates) >= 2:
            std_dev = np.std(revenue_rates)
            mean_growth = np.mean(revenue_rates)
            
            if mean_growth != 0:
                cv = abs(std_dev / mean_growth)
                
                # Adjust CV thresholds if company has strong margins (>25%)
                # High margins = pricing power = customer loyalty despite revenue volatility
                has_strong_margins = (m.operating_margin_current is not None and 
                                    m.operating_margin_current > 25)
                
                if cv < 0.3:
                    # Very consistent revenue = high retention
                    score += 20
                    self.modifiers['high_customer_retention'] = 10
                elif cv < 0.6:
                    # Good consistency
                    score += 15
                elif cv < 1.0 or (cv < 1.5 and has_strong_margins):
                    # Moderate consistency (lenient if strong margins)
                    score += 10
                elif cv < 1.5 or (cv < 2.0 and has_strong_margins):
                    # Some volatility (lenient if strong margins)
                    score += 5
                else:
                    # High volatility = low retention (unless very strong margins)
                    if has_strong_margins:
                        score += 0  # Neutral, don't penalize
                    else:
                        score -= 10
                        self.penalties['revenue_volatility'] = -10
        
        # Sub-component 2: Earnings Stability (30% of component)
        # Stable earnings = predictable customer behavior
        eps_rates = []
        if m.eps_growth_1y is not None and pd.notna(m.eps_growth_1y):
            eps_rates.append(m.eps_growth_1y)
        if m.eps_growth_3y is not None and pd.notna(m.eps_growth_3y):
            eps_rates.append(m.eps_growth_3y)
        if m.eps_growth_5y is not None and pd.notna(m.eps_growth_5y):
            eps_rates.append(m.eps_growth_5y)
        
        if len(eps_rates) >= 2:
            # Check if all positive (consistent profitability)
            all_positive = all(rate > 0 for rate in eps_rates)
            any_negative = any(rate < -10 for rate in eps_rates)
            
            if all_positive:
                score += 12
                self.modifiers['stable_earnings'] = 8
            elif any_negative:
                score -= 8
                self.penalties['earnings_volatility'] = -8
        
        # Sub-component 3: Growth Consistency (20% of component)
        # Consistent growth = expanding loyal customer base
        if len(revenue_rates) >= 2:
            all_growing = all(rate > 0 for rate in revenue_rates)
            
            if all_growing:
                score += 8
            elif all(rate < 0 for rate in revenue_rates):
                # Declining customer base
                score -= 8
        
        self.component_scores['customer_retention'] = score
        return max(0, min(100, score))
    
    def get_interpretation(self, score: float, sector: str) -> str:
        """Generate human-readable interpretation of the score"""
        if score >= 85:
            return f"Exceptional {sector} moat - wide competitive advantage, pricing power"
        elif score >= 70:
            return f"Strong {sector} moat - solid competitive advantages"
        elif score >= 55:
            return f"Moderate {sector} moat - some competitive advantages"
        elif score >= 40:
            return f"Weak {sector} moat - limited competitive advantages"
        else:
            return f"Poor {sector} moat - commodity business, no advantages"


def score_stock_business_quality(stock_data: Dict) -> Tuple[float, Dict, str]:
    """
    Score a single stock's business quality / moat
    
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
    metrics = BusinessMetrics(
        # Moat indicators
        gross_margin=stock_data.get('grossmargin'),
        operating_margin_current=stock_data.get('operatingmargin'),
        
        # Pricing power
        revenue_per_employee=stock_data.get('revenueperemployee'),
        
        # Customer retention
        revenue_growth_1y=stock_data.get('revenuegrowth'),
        revenue_growth_3y=stock_data.get('revenuegrowth3y'),
        revenue_growth_5y=stock_data.get('revenuegrowth5y'),
        eps_growth_1y=stock_data.get('epsgrowth'),
        eps_growth_3y=stock_data.get('epsgrowth3y'),
        eps_growth_5y=stock_data.get('epsgrowth5y'),
        
        # Profitability trends
        roe_current=stock_data.get('roe'),
        roe_5y=stock_data.get('roe5y'),
        
        # Sector
        industry=industry,
        sector=sector
    )
    
    # Score the stock
    scorer = BusinessQualityScorer(metrics)
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
    print("DIMENSION 6: BUSINESS QUALITY / MOAT SCORER v1.0")
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
            score, breakdown, interpretation = score_stock_business_quality(stock_data)
            
            results.append({
                'symbol': symbol,
                'dimension6_moat': score,
                'moat_score': breakdown['component_1_moat'],
                'pricing_score': breakdown['component_2_pricing'],
                'retention_score': breakdown['component_3_retention'],
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
                'dimension6_moat': None,
                'moat_score': None,
                'pricing_score': None,
                'retention_score': None,
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


def generate_validation_report(scores_df: pd.DataFrame, output_file: str = 'dimension6_report.txt'):
    """
    Generate human-readable validation report
    
    Args:
        scores_df: DataFrame with scores
        output_file: Output filename for report
    """
    
    # Validation stocks
    validation_stocks = {
        'CTC.N0000': {'expected': (90, 100), 'description': 'Monopoly moat - 78% margin, tobacco license'},
        'LION.N0000': {'expected': (75, 85), 'description': 'Strong brand moat - 28% margin, distribution'},
        'LOLC.N0000': {'expected': (60, 75), 'description': 'Moderate financial moat - scale, network'},
        'LOFC.N0000': {'expected': (70, 80), 'description': 'Good financial moat - relationships, 5.2% ROA'},
        'JKH.N0000': {'expected': (40, 55), 'description': 'Weak conglomerate moat - no focus, diverse'}
    }
    
    report = []
    report.append("=" * 80)
    report.append("DIMENSION 6: BUSINESS QUALITY / MOAT SCORER v1.0 - VALIDATION REPORT")
    report.append("=" * 80)
    report.append(f"Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total Stocks Scored: {len(scores_df)}")
    report.append("")
    
    # Overall statistics
    report.append("=" * 80)
    report.append("OVERALL STATISTICS")
    report.append("=" * 80)
    report.append(f"Mean:   {scores_df['dimension6_moat'].mean():.1f}")
    report.append(f"Median: {scores_df['dimension6_moat'].median():.1f}")
    report.append(f"Std:    {scores_df['dimension6_moat'].std():.1f}")
    report.append(f"Min:    {scores_df['dimension6_moat'].min():.1f}")
    report.append(f"Max:    {scores_df['dimension6_moat'].max():.1f}")
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
        count = len(scores_df[(scores_df['dimension6_moat'] >= low) & 
                              (scores_df['dimension6_moat'] < high)])
        pct = (count / len(scores_df)) * 100
        report.append(f"  {label:15s} ({low:>3}-{high:<3}): {count:>3} stocks ({pct:5.1f}%)")
    
    report.append("")
    
    # Sector breakdown
    report.append("SECTOR BREAKDOWN:")
    sector_stats = scores_df.groupby('sector')['dimension6_moat'].agg(['count', 'mean'])
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
        
        score = stock['dimension6_moat'].values[0]
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
        report.append(f"  Competitive Moat:    {stock['moat_score'].values[0]:>6.1f} (40% weight)")
        report.append(f"  Pricing Power:       {stock['pricing_score'].values[0]:>6.1f} (30% weight)")
        report.append(f"  Customer Retention:  {stock['retention_score'].values[0]:>6.1f} (30% weight)")
        report.append(f"  Modifiers:           {stock['modifiers'].values[0]:>+6.1f}")
        report.append(f"  Penalties:           {stock['penalties'].values[0]:>+6.1f}")
        report.append("")
    
    # Top performers
    report.append("=" * 80)
    report.append("TOP 10 BUSINESS QUALITY / MOAT")
    report.append("=" * 80)
    report.append("")
    
    top10 = scores_df.nlargest(10, 'dimension6_moat')
    for idx, (_, row) in enumerate(top10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension6_moat']:5.1f} ({row['sector']})")
    
    report.append("")
    
    # Bottom performers
    report.append("=" * 80)
    report.append("BOTTOM 10 BUSINESS QUALITY (COMMODITIES)")
    report.append("=" * 80)
    report.append("")
    
    bottom10 = scores_df.nsmallest(10, 'dimension6_moat')
    for idx, (_, row) in enumerate(bottom10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension6_moat']:5.1f} ({row['sector']})")
    
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
        description='Dimension 6: Business Quality / Moat Scorer v1.0 - Investment OS 7D Framework',
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
        default='dimension6_scores.csv',
        help='Path to output scores CSV file (default: dimension6_scores.csv)'
    )
    
    parser.add_argument(
        '--report',
        type=str,
        default='dimension6_report.txt',
        help='Path to output validation report TXT file (default: dimension6_report.txt)'
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
        
        print("\n🎯 Dimension 6 scoring complete!")
        print(f"\nFiles created:")
        print(f"  • {output_csv}")
        print(f"  • {output_report}")
        
    else:
        # Show usage
        print("\n" + "=" * 80)
        print("No input file provided. Showing usage:")
        print("=" * 80)
        print("\nUsage for production:")
        print("  python3 dimension6_scorer_v1_0.py \\")
        print("      --input output/2025-12-30/cleaned_data.csv \\")
        print("      --output dimension6_scores.csv \\")
        print("      --report dimension6_report.txt")