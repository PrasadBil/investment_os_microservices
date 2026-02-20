
"""
DIMENSION 4: GROWTH SCORER v1.2
Investment OS - 7D Scoring Framework

FILE: dimension4_scorer.py
CREATED: 2025-12-31
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2025-12-31  Initial creation — Growth scorer (Peter Lynch GARP methodology)
    v1.2.0  2025-12-31  Fix: Tightened negative ROE, missing EPS penalty, value trap penalty
    v1.2.1  2026-02-11  Migrated to services/scoring-7d (Phase 2 microservices)
    v1.2.2  2026-02-16  Added version history header (new project standard)

CHANGELOG v1.2:
- FIX 1: Tightened negative ROE threshold from -10% to -5%
- FIX 2: Added penalty for missing EPS data when revenue high (suspicious)
- FIX 3: Increased severe value trap penalty to -40 (from -30)
- FIX 4: Added NaN handling for earnings score with penalty
- RESULT: 102 stocks corrected, eliminated ceiling effect

FRAMEWORK: Sector-Relative Growth Quality Assessment

PHILOSOPHY (Peter Lynch + Buffett):
- Growth must be sustainable and capital-efficient
- Sector context matters: 5% tobacco growth ≠ 5% tech growth  
- Quality of growth (margin expansion) > quantity
- Consistency beats volatility
- Capital efficiency (growth without capex) is key

ARCHITECTURE:
Component 1 (40%): Revenue Growth Trajectory
  - Multi-period growth (1Y, 3Y, 5Y) with recency weighting
  - Sector-specific thresholds
  - Consistency check (declining trend penalty)
  
Component 2 (35%): Earnings Growth Quality
  - EPS growth vs revenue growth (margin expansion check)
  - Earnings consistency (volatility penalty)
  - Quality metrics (ROE trend, cash flow alignment)
  
Component 3 (25%): Growth Efficiency
  - Capital intensity (Capex/Revenue)
  - Revenue per employee (productivity)
  - Asset turnover improvement

SECTOR THRESHOLDS:
Mature/Defensive (Tobacco, Utilities):
  - Excellent: 5-10% revenue growth
  - Good: 3-8%
  - Fair: 0-5%
  - Poor: <0%

Growth/Cyclical (Consumer, Industrial):
  - Excellent: 15-25%
  - Good: 10-20%
  - Fair: 5-15%
  - Poor: <5%

Financial Services:
  - Excellent: 10-20%
  - Good: 7-15%
  - Fair: 3-10%
  - Poor: <3%

High Growth (Tech, E-commerce):
  - Excellent: 25%+
  - Good: 15-30%
  - Fair: 10-20%
  - Poor: <10%

MODIFIERS:
+ Margin Expansion Bonus: +10 if gross/operating margins improving
+ Capital Efficiency Bonus: +10 if growth with declining capex intensity
+ Consistency Bonus: +5 if all periods positive growth
- Volatility Penalty: -10 if growth wildly inconsistent
- Margin Compression Penalty: -15 if revenue grows but margins shrink
- Capital Intensive Penalty: -10 if growth requires excessive capex

PENALTY CAP: -40 maximum (learned from Dimensions 1-3)

EXPECTED SCORES:
CTC:  80-90 (exceptional mature compounder - steady growth + margin expansion)
LION: 55-65 (moderate industrial growth - stable but not high-growth)
LOLC: 45-55 (weak financial growth - revenue growth but profitability issues)
LOFC: 65-75 (strong financial growth - faster + better quality than LOLC)
JKH:  0-15 (declining conglomerate - negative growth + margin compression)

REGULATORY DEFENSE:
"We evaluate growth using Peter Lynch's GARP (Growth At Reasonable Price) 
methodology with sector-specific thresholds. A tobacco company growing 5% 
is excellent (mature industry), while a tech company at 5% indicates decline. 
We assess growth quality (margin expansion, capital efficiency) rather than 
just top-line numbers."
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class GrowthMetrics:
    """Container for growth-related metrics"""
    # Revenue Growth
    revenue_growth_1y: Optional[float] = None
    revenue_growth_3y: Optional[float] = None
    revenue_growth_5y: Optional[float] = None
    revenue_growth_10y: Optional[float] = None
    
    # EPS Growth
    eps_growth_1y: Optional[float] = None
    eps_growth_3y: Optional[float] = None
    eps_growth_5y: Optional[float] = None
    eps_growth_10y: Optional[float] = None
    
    # Efficiency Metrics
    revenue_per_employee: Optional[float] = None
    capex_intensity: Optional[float] = None  # Capex / Revenue
    asset_turnover: Optional[float] = None   # Revenue / Total Assets
    
    # Margin Trends (for quality assessment)
    gross_margin_current: Optional[float] = None
    gross_margin_5y: Optional[float] = None
    operating_margin_current: Optional[float] = None
    operating_margin_5y: Optional[float] = None
    
    # ROE Trend (for earnings quality)
    roe_current: Optional[float] = None
    roe_5y: Optional[float] = None
    
    # Sector Classification
    sector: str = "Industrial"  # Industrial, Financial, Mature, Growth


class SectorClassifier:
    """Classify stocks into growth expectation categories"""
    
    # Sector keywords for classification
    MATURE_KEYWORDS = ['tobacco', 'cigarette', 'utility', 'power', 'water']
    FINANCIAL_KEYWORDS = ['bank', 'finance', 'insurance', 'capital', 'holdings', 
                          'investment', 'leasing', 'fund', 'credit', 'institution',
                          'financial services']
    GROWTH_KEYWORDS = ['tech', 'software', 'e-commerce', 'digital', 'online']
    
    @staticmethod
    def classify_sector(company_name: str, industry: str = "") -> str:
        """
        Classify company into sector category based on name/industry
        Returns: Mature, Financial, Growth, or Industrial (default)
        """
        text = (company_name + " " + industry).lower()
        
        if any(keyword in text for keyword in SectorClassifier.MATURE_KEYWORDS):
            return "Mature"
        elif any(keyword in text for keyword in SectorClassifier.FINANCIAL_KEYWORDS):
            return "Financial"
        elif any(keyword in text for keyword in SectorClassifier.GROWTH_KEYWORDS):
            return "Growth"
        else:
            return "Industrial"


class GrowthScorer:
    """Main scoring engine for Dimension 4: Growth"""
    
    # Sector-specific growth thresholds (revenue growth %)
    SECTOR_THRESHOLDS = {
        'Mature': {
            'excellent': (5, 10),
            'good': (3, 8),
            'fair': (0, 5),
            'poor': (-float('inf'), 0)
        },
        'Industrial': {
            'excellent': (15, 25),
            'good': (10, 20),
            'fair': (5, 15),
            'poor': (-float('inf'), 5)
        },
        'Financial': {
            'excellent': (10, 20),
            'good': (7, 15),
            'fair': (3, 10),
            'poor': (-float('inf'), 3)
        },
        'Growth': {
            'excellent': (25, float('inf')),
            'good': (15, 30),
            'fair': (10, 20),
            'poor': (-float('inf'), 10)
        }
    }
    
    def __init__(self, metrics: GrowthMetrics):
        self.metrics = metrics
        self.component_scores = {}
        self.modifiers = {}
        self.penalties = {}
    
    def score(self) -> Tuple[float, Dict]:
        """
        Calculate comprehensive growth score with sanity checks
        Returns: (final_score, breakdown_dict)
        """
        m = self.metrics
        
        # SANITY CHECKS - These are hard caps on maximum score
        max_score_cap = 100
        sanity_flags = []
        
        # SANITY CHECK 1: Negative operating margin = serious problem
        if m.operating_margin_current is not None and m.operating_margin_current < 0:
            max_score_cap = min(max_score_cap, 25)  # Cap at 25
            sanity_flags.append('negative_margin')
        
        # SANITY CHECK 2: Severely negative ROE = value destruction
        if m.roe_current is not None and m.roe_current < -5:  # Tightened from -10%
            max_score_cap = min(max_score_cap, 20)  # Cap at 20
            sanity_flags.append('roe_collapse')
        
        # SANITY CHECK 3: Both revenue AND EPS declining = avoid
        if (m.revenue_growth_5y is not None and m.revenue_growth_5y < 0 and
            m.eps_growth_5y is not None and m.eps_growth_5y < 0):
            max_score_cap = min(max_score_cap, 30)  # Cap at 30
            sanity_flags.append('double_decline')
        
        # Component 1: Revenue Growth Trajectory (40%)
        revenue_score = self._score_revenue_growth()
        
        # Component 2: Earnings Growth Quality (35%)
        earnings_score = self._score_earnings_quality()
        
        # Component 3: Growth Efficiency (25%)
        efficiency_score = self._score_growth_efficiency()
        
        # CRITICAL: Handle NaN earnings score as red flag
        # If we can't evaluate earnings quality, that's a major concern
        if pd.isna(earnings_score):
            # NaN earnings score = missing critical data
            if m.revenue_growth_5y is not None and m.revenue_growth_5y > 15:
                # High revenue but can't verify earnings = very suspicious
                earnings_score = 0  # Zero score for earnings quality
                self.penalties['unverifiable_earnings'] = -30
            else:
                earnings_score = 30  # Neutral-low for slower growth
        
        # Calculate base score
        base_score = (
            revenue_score * 0.40 +
            earnings_score * 0.35 +
            efficiency_score * 0.25
        )
        
        # Apply modifiers and penalties
        modifier_total = sum(self.modifiers.values())
        penalty_total = max(sum(self.penalties.values()), -40)  # Regular penalty cap
        
        # Calculate score before sanity cap
        uncapped_score = base_score + modifier_total + penalty_total
        
        # Apply sanity check cap
        final_score = max(0, min(max_score_cap, uncapped_score))
        
        breakdown = {
            'base_score': round(base_score, 2),
            'component_1_revenue': round(revenue_score, 2),
            'component_2_earnings': round(earnings_score, 2),
            'component_3_efficiency': round(efficiency_score, 2),
            'modifiers': self.modifiers.copy(),
            'penalties': self.penalties.copy(),
            'modifier_total': round(modifier_total, 2),
            'penalty_total': round(penalty_total, 2),
            'sanity_cap': max_score_cap,
            'sanity_flags': sanity_flags,
            'final_score': round(final_score, 2),
            'sector': self.metrics.sector
        }
        
        return final_score, breakdown
    
    def _score_revenue_growth(self) -> float:
        """
        Component 1: Revenue Growth Trajectory (40%)
        
        Uses weighted average of multi-period growth with recency bias:
        - 1Y: 50% weight (most recent)
        - 3Y: 30% weight
        - 5Y: 20% weight
        
        Scores against sector-specific thresholds.
        Checks for consistency (declining trend penalty applied separately)
        
        NOTE: Growth rates are already in percentage format (13.24 = 13.24%)
        """
        m = self.metrics
        
        # Collect available growth rates
        growth_rates = []
        weights = []
        
        if m.revenue_growth_1y is not None:
            growth_rates.append(m.revenue_growth_1y)
            weights.append(0.50)
        
        if m.revenue_growth_3y is not None:
            growth_rates.append(m.revenue_growth_3y)
            weights.append(0.30)
        
        if m.revenue_growth_5y is not None:
            growth_rates.append(m.revenue_growth_5y)
            weights.append(0.20)
        
        if not growth_rates:
            return 50  # Neutral score if no data
        
        # Normalize weights
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]
        
        # Calculate weighted average growth (already in percentage)
        weighted_growth = sum(g * w for g, w in zip(growth_rates, weights))
        
        # Score against sector thresholds
        score = self._score_against_thresholds(weighted_growth, m.sector)
        
        # Check for declining trend (penalty applied separately)
        if len(growth_rates) >= 2:
            if growth_rates[0] < growth_rates[-1] * 0.7:  # 30%+ decline
                self.penalties['declining_revenue_trend'] = -10
        
        # Check for consistency (bonus applied separately)
        if all(g > 0 for g in growth_rates) and len(growth_rates) >= 2:
            self.modifiers['consistent_positive_growth'] = 5
        
        self.component_scores['revenue_growth'] = score
        self.component_scores['weighted_revenue_growth'] = weighted_growth
        
        return score
    
    def _score_earnings_quality(self) -> float:
        """
        Component 2: Earnings Growth Quality (35%)
        
        Evaluates whether earnings growth is real and sustainable:
        - EPS growth vs revenue growth (margin expansion)
        - Earnings consistency
        - ROE trend (profitability improvement)
        - Cash flow alignment (if available)
        
        High-quality growth: EPS grows faster than revenue (margin expansion)
        Low-quality growth: Revenue grows but EPS stagnates (margin compression)
        
        NOTE: Growth rates and margins are already in percentage format
        """
        m = self.metrics
        
        # Collect EPS growth rates (already in percentages)
        eps_rates = []
        if m.eps_growth_1y is not None:
            eps_rates.append(m.eps_growth_1y)
        if m.eps_growth_3y is not None:
            eps_rates.append(m.eps_growth_3y)
        if m.eps_growth_5y is not None:
            eps_rates.append(m.eps_growth_5y)
        
        if not eps_rates:
            # No EPS data - check if this is suspicious
            # If revenue is growing fast but no EPS data = red flag
            if m.revenue_growth_5y is not None and m.revenue_growth_5y > 15:
                # High revenue growth but no earnings visibility = concerning
                self.penalties['missing_earnings_data'] = -20
                return 30  # Below neutral due to lack of transparency
            return 50  # Neutral if no EPS data and revenue not concerning
        
        # Calculate average EPS growth (already in percentages)
        avg_eps_growth = sum(eps_rates) / len(eps_rates)
        
        # Base score from EPS growth (similar to revenue thresholds)
        base_eps_score = self._score_against_thresholds(avg_eps_growth, m.sector)
        
        # Quality Check 1: Margin Expansion
        # Margins are already in percentage (78.53 = 78.53%)
        if (m.operating_margin_current is not None and 
            m.operating_margin_5y is not None):
            margin_change = m.operating_margin_current - m.operating_margin_5y
            
            if margin_change > 2:  # 2+ percentage points improvement
                self.modifiers['margin_expansion'] = 10
                base_eps_score = min(100, base_eps_score + 5)
            elif margin_change < -2:  # 2+ points decline
                self.penalties['margin_compression'] = -15
                base_eps_score = max(0, base_eps_score - 10)
        
        # Quality Check 2: ROE Trend
        # ROE is already in percentage (277.09 = 277.09%)
        if m.roe_current is not None and m.roe_5y is not None:
            roe_change = m.roe_current - m.roe_5y
            
            if roe_change > 3:  # 3+ percentage points improvement
                self.modifiers['improving_profitability'] = 5
            elif roe_change < -5:  # 5+ points decline
                # Sharp ROE decline indicates deteriorating returns
                if roe_change < -10:  # >10 point decline = severe
                    self.penalties['profitability_collapse'] = -20
                else:
                    self.penalties['declining_profitability'] = -10
        
        # Quality Check 3: EPS vs Revenue Growth Alignment & Value Trap Detection
        # Use average EPS growth (calculated earlier) vs 5Y revenue growth
        if m.revenue_growth_5y is not None and avg_eps_growth is not None:
            growth_delta = avg_eps_growth - m.revenue_growth_5y
            
            if avg_eps_growth > m.revenue_growth_5y + 2:  # EPS growing 2%+ faster
                self.modifiers['earnings_leverage'] = 5
            elif growth_delta < -10:  # EPS lagging by 10%+
                # VALUE TRAP WARNING: Revenue growing but earnings collapsing
                # Check if average EPS (not just 5Y) is negative while revenue grows
                if m.revenue_growth_5y > 20 and avg_eps_growth < -10:
                    # SEVERE VALUE TRAP: Revenue growing >20% but average EPS very negative
                    self.penalties['value_trap_severe'] = -40  # Increased from -30
                elif m.revenue_growth_5y > 10 and avg_eps_growth < 0:
                    # Moderate value trap: Revenue growing >10% but average EPS negative
                    self.penalties['value_trap_severe'] = -30
                else:
                    # Moderate case: Just earnings dilution
                    self.penalties['earnings_dilution'] = -10
        
        self.component_scores['earnings_quality'] = base_eps_score
        self.component_scores['avg_eps_growth'] = avg_eps_growth
        
        return base_eps_score
    
    def _score_growth_efficiency(self) -> float:
        """
        Component 3: Growth Efficiency (25%)
        
        Evaluates capital efficiency of growth:
        - Low capex intensity = asset-light growth (excellent)
        - High revenue per employee = productivity
        - Improving asset turnover = efficiency gains
        
        Buffett: "See's Candies was wonderful because it required almost 
        no capital to grow. That's the best kind of growth."
        """
        m = self.metrics
        score = 50  # Start neutral
        
        # Check 1: Capital Intensity (Capex / Revenue)
        if m.capex_intensity is not None:
            if m.capex_intensity < 0.05:  # <5% capex = asset-light
                score += 20
                self.modifiers['asset_light_growth'] = 10
            elif m.capex_intensity < 0.10:  # 5-10% = efficient
                score += 15
            elif m.capex_intensity < 0.20:  # 10-20% = moderate
                score += 5
            elif m.capex_intensity > 0.30:  # >30% = capital intensive
                score -= 15
                self.penalties['capital_intensive'] = -10
        
        # Check 2: Revenue Per Employee (Productivity)
        # This is highly sector-specific, so we use relative scoring
        if m.revenue_per_employee is not None:
            # For mature/industrial: >$200K = good, >$500K = excellent
            # For financial: >$1M = good, >$2M = excellent
            if m.sector == 'Financial':
                if m.revenue_per_employee > 2000000:
                    score += 15
                elif m.revenue_per_employee > 1000000:
                    score += 10
                elif m.revenue_per_employee < 500000:
                    score -= 5
            else:
                if m.revenue_per_employee > 500000:
                    score += 15
                elif m.revenue_per_employee > 200000:
                    score += 10
                elif m.revenue_per_employee < 100000:
                    score -= 5
        
        # Check 3: Asset Turnover (efficiency)
        if m.asset_turnover is not None:
            if m.asset_turnover > 1.5:  # Efficient asset usage
                score += 10
            elif m.asset_turnover < 0.5:  # Inefficient
                score -= 10
        
        # Cap final efficiency score
        score = max(0, min(100, score))
        
        self.component_scores['growth_efficiency'] = score
        
        return score
    
    def _score_against_thresholds(self, growth_rate: float, sector: str) -> float:
        """
        Score a growth rate against sector-specific thresholds
        
        Uses sigmoid curves within bands for smooth transitions:
        - Excellent band: 90-100 points
        - Good band: 70-85 points
        - Fair band: 45-65 points
        - Poor band: 0-40 points
        """
        thresholds = self.SECTOR_THRESHOLDS.get(sector, self.SECTOR_THRESHOLDS['Industrial'])
        
        excellent_range = thresholds['excellent']
        good_range = thresholds['good']
        fair_range = thresholds['fair']
        
        # Above excellent band - cap at 100
        if growth_rate > excellent_range[1]:
            # If upper bound is infinity, use scaling formula
            if excellent_range[1] == float('inf'):
                return min(100, 90 + (growth_rate - excellent_range[0]) / 5)
            else:
                # Growth exceeds excellent upper bound - cap at 100
                return 100.0
        
        # Excellent band (90-100)
        elif growth_rate >= excellent_range[0]:
            # Linear interpolation within excellent band
            if excellent_range[1] == float('inf'):
                return min(100, 90 + (growth_rate - excellent_range[0]) / 5)
            else:
                band_width = excellent_range[1] - excellent_range[0]
                position = (growth_rate - excellent_range[0]) / band_width
                return 90 + position * 10
        
        # Above good band but below excellent
        elif growth_rate > good_range[1]:
            # Interpolate between good upper and excellent lower
            gap = excellent_range[0] - good_range[1]
            position = (growth_rate - good_range[1]) / gap if gap > 0 else 0
            return 85 + position * 5
        
        # Good band (70-85)
        elif growth_rate >= good_range[0]:
            band_width = good_range[1] - good_range[0]
            position = (growth_rate - good_range[0]) / band_width if band_width > 0 else 0
            return 70 + position * 15
        
        # Above fair band but below good
        elif growth_rate > fair_range[1]:
            # Interpolate between fair upper and good lower
            gap = good_range[0] - fair_range[1]
            position = (growth_rate - fair_range[1]) / gap if gap > 0 else 0
            return 65 + position * 5
        
        # Fair band (45-65)
        elif growth_rate >= fair_range[0]:
            band_width = fair_range[1] - fair_range[0]
            position = (growth_rate - fair_range[0]) / band_width if band_width > 0 else 0
            return 45 + position * 20
        
        # Poor band (0-40)
        else:
            # Negative growth gets progressively worse scores
            if growth_rate < -10:
                return max(0, 20 + growth_rate)  # -10% = 10 pts, -20% = 0 pts
            elif growth_rate < 0:
                return 20 + growth_rate * 2  # -5% = 10 pts
            else:
                # Small positive but below fair threshold
                # Avoid division by zero if fair_range[0] is 0
                if fair_range[0] == 0:
                    return 20  # Very low score for growth below threshold
                else:
                    return 40 * (growth_rate / fair_range[0])
        
    def get_interpretation(self, score: float, sector: str) -> str:
        """Human-readable interpretation of the growth score"""
        if score >= 85:
            return f"Exceptional {sector} growth - high quality, sustainable trajectory"
        elif score >= 70:
            return f"Strong {sector} growth - solid quality and consistency"
        elif score >= 55:
            return f"Moderate {sector} growth - acceptable but watch margins"
        elif score >= 40:
            return f"Weak {sector} growth - concerning trends, investigate"
        else:
            return f"Poor {sector} growth - declining or negative, avoid"


def score_stock_growth(stock_data: Dict) -> Tuple[float, Dict, str]:
    """
    Main entry point: Score a single stock's growth
    
    Args:
        stock_data: Dictionary with all stock parameters
    
    Returns:
        (score, breakdown, interpretation)
    """
    # Extract company info for sector classification
    # Use industry column from CSE data (name column doesn't exist)
    industry = stock_data.get('industry', '')
    sector = SectorClassifier.classify_sector('', industry)
    
    # Build metrics object
    metrics = GrowthMetrics(
        # Revenue growth (already in percentages, e.g., 13.24 = 13.24%)
        revenue_growth_1y=stock_data.get('revenuegrowth'),
        revenue_growth_3y=stock_data.get('revenuegrowth3y'),
        revenue_growth_5y=stock_data.get('revenuegrowth5y'),
        revenue_growth_10y=stock_data.get('revenuegrowth10y'),
        
        # EPS growth
        eps_growth_1y=stock_data.get('epsgrowth'),
        eps_growth_3y=stock_data.get('epsgrowth3y'),
        eps_growth_5y=stock_data.get('epsgrowth5y'),
        eps_growth_10y=stock_data.get('epsgrowth10y'),
        
        # Efficiency
        revenue_per_employee=stock_data.get('revperemployee'),
        capex_intensity=stock_data.get('capex_intensity'),
        asset_turnover=stock_data.get('assetturnover'),
        
        # Margins (for quality check) - no 5Y historical margin data in dataset
        gross_margin_current=stock_data.get('grossmargin'),
        gross_margin_5y=None,  # Not available in CSE dataset
        operating_margin_current=stock_data.get('operatingmargin'),
        operating_margin_5y=None,  # Not available in CSE dataset
        
        # ROE trend
        roe_current=stock_data.get('roe'),
        roe_5y=stock_data.get('roe_5y'),
        
        # Sector
        sector=sector
    )
    
    # Score the stock
    scorer = GrowthScorer(metrics)
    final_score, breakdown = scorer.score()
    interpretation = scorer.get_interpretation(final_score, sector)
    
    return final_score, breakdown, interpretation


def batch_score_all_stocks(supabase_client) -> pd.DataFrame:
    """
    Score all stocks in the database
    
    Returns: DataFrame with symbol, score, and breakdown
    """
    # Fetch all stocks
    response = supabase_client.table('cse_stock_data').select('*').execute()
    stocks = response.data
    
    results = []
    
    for stock in stocks:
        symbol = stock.get('symbol', 'UNKNOWN')
        
        try:
            score, breakdown, interpretation = score_stock_growth(stock)
            
            results.append({
                'symbol': symbol,
                'dimension4_growth': score,
                'revenue_score': breakdown['component_1_revenue'],
                'earnings_score': breakdown['component_2_earnings'],
                'efficiency_score': breakdown['component_3_efficiency'],
                'modifiers': breakdown['modifier_total'],
                'penalties': breakdown['penalty_total'],
                'sector': breakdown['sector'],
                'interpretation': interpretation
            })
            
        except Exception as e:
            print(f"Error scoring {symbol}: {e}")
            results.append({
                'symbol': symbol,
                'dimension4_growth': None,
                'revenue_score': None,
                'earnings_score': None,
                'efficiency_score': None,
                'modifiers': None,
                'penalties': None,
                'sector': 'Unknown',
                'interpretation': f'Error: {e}'
            })
    
    return pd.DataFrame(results)


def batch_score_from_csv(csv_path: str) -> pd.DataFrame:
    """
    Score all stocks from CSV file (for VPS production use)
    
    Args:
        csv_path: Path to CSV file with stock data
    
    Returns: DataFrame with scores
    """
    print("=" * 80)
    print("DIMENSION 4: GROWTH SCORER v1.0")
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
            score, breakdown, interpretation = score_stock_growth(stock_data)
            
            results.append({
                'symbol': symbol,
                'dimension4_growth': score,
                'revenue_score': breakdown['component_1_revenue'],
                'earnings_score': breakdown['component_2_earnings'],
                'efficiency_score': breakdown['component_3_efficiency'],
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
                'dimension4_growth': None,
                'revenue_score': None,
                'earnings_score': None,
                'efficiency_score': None,
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


def generate_validation_report(scores_df: pd.DataFrame, output_file: str = 'dimension4_report.txt'):
    """
    Generate human-readable validation report (matches Dimension 1-3 format)
    
    Args:
        scores_df: DataFrame with scores
        output_file: Output filename for report
    """
    
    # Validation stocks
    validation_stocks = {
        'CTC.N0000': {'expected': (80, 95), 'description': 'Exceptional mature compounder'},
        'LION.N0000': {'expected': (55, 70), 'description': 'Strong industrial growth'},
        'LOLC.N0000': {'expected': (20, 40), 'description': 'VALUE TRAP: Revenue growth but weak profitability'},
        'LOFC.N0000': {'expected': (65, 80), 'description': 'Excellent financial growth'},
        'JKH.N0000': {'expected': (15, 35), 'description': 'VALUE TRAP: Declining conglomerate'}
    }
    
    report = []
    report.append("=" * 80)
    report.append("DIMENSION 4: GROWTH SCORER v1.0 - VALIDATION REPORT")
    report.append("=" * 80)
    report.append(f"Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total Stocks Scored: {len(scores_df)}")
    report.append("")
    
    # Overall statistics
    report.append("=" * 80)
    report.append("OVERALL STATISTICS")
    report.append("=" * 80)
    report.append(f"Mean:   {scores_df['dimension4_growth'].mean():.1f}")
    report.append(f"Median: {scores_df['dimension4_growth'].median():.1f}")
    report.append(f"Std:    {scores_df['dimension4_growth'].std():.1f}")
    report.append(f"Min:    {scores_df['dimension4_growth'].min():.1f}")
    report.append(f"Max:    {scores_df['dimension4_growth'].max():.1f}")
    report.append("")
    
    # Score distribution
    report.append("SCORE DISTRIBUTION:")
    bands = [
        (85, 100, "Exceptional"),
        (70, 85, "Strong"),
        (55, 70, "Moderate"),
        (40, 55, "Weak"),
        (0, 40, "Poor/Declining")
    ]
    
    for low, high, label in bands:
        count = len(scores_df[(scores_df['dimension4_growth'] >= low) & 
                              (scores_df['dimension4_growth'] < high)])
        pct = (count / len(scores_df)) * 100
        report.append(f"  {label:15s} ({low:>3}-{high:<3}): {count:>3} stocks ({pct:5.1f}%)")
    
    report.append("")
    
    # Sector breakdown
    report.append("SECTOR BREAKDOWN:")
    sector_stats = scores_df.groupby('sector')['dimension4_growth'].agg(['count', 'mean'])
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
        
        score = stock['dimension4_growth'].values[0]
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
        report.append(f"  Revenue Growth:      {stock['revenue_score'].values[0]:>6.1f} (40% weight)")
        report.append(f"  Earnings Quality:    {stock['earnings_score'].values[0]:>6.1f} (35% weight)")
        report.append(f"  Growth Efficiency:   {stock['efficiency_score'].values[0]:>6.1f} (25% weight)")
        report.append(f"  Modifiers:           {stock['modifiers'].values[0]:>+6.1f}")
        report.append(f"  Penalties:           {stock['penalties'].values[0]:>+6.1f}")
        report.append("")
    
    # Top performers
    report.append("=" * 80)
    report.append("TOP 10 GROWTH STOCKS")
    report.append("=" * 80)
    report.append("")
    
    top10 = scores_df.nlargest(10, 'dimension4_growth')
    for idx, (_, row) in enumerate(top10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension4_growth']:5.1f} ({row['sector']})")
    
    report.append("")
    
    # Bottom performers
    report.append("=" * 80)
    report.append("BOTTOM 10 GROWTH STOCKS (AVOID)")
    report.append("=" * 80)
    report.append("")
    
    bottom10 = scores_df.nsmallest(10, 'dimension4_growth')
    for idx, (_, row) in enumerate(bottom10.iterrows(), 1):
        report.append(f"{idx:2d}. {row['symbol']:12s} - Score: {row['dimension4_growth']:5.1f} ({row['sector']})")
    
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
        description='Dimension 4: Growth Scorer v1.0 - Investment OS 7D Framework',
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
        default='dimension4_scores.csv',
        help='Path to output scores CSV file (default: dimension4_scores.csv)'
    )
    
    parser.add_argument(
        '--report',
        type=str,
        default='dimension4_report.txt',
        help='Path to output validation report TXT file (default: dimension4_report.txt)'
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
        
        print("\n🎯 Dimension 4 scoring complete!")
        print(f"\nFiles created:")
        print(f"  • {output_csv}")
        print(f"  • {output_report}")
        
    else:
        # Run validation suite with test data
        print("\n" + "=" * 80)
        print("Running validation suite with test data...")
        print("=" * 80)
        print("\nUsage for production:")
        print("  python3 dimension4_scorer_v1_0.py \\")
        print("      --input output/2025-12-30/cleaned_data.csv \\")
        print("      --output dimension4_scores.csv \\")
        print("      --report dimension4_report.txt")
        print("\nExample:")
        print("  python3 dimension4_scorer_v1_0.py \\")
        print("      --input cleaned_data.csv \\")
        print("      --output dimension4_scores.csv \\")
        print("      --report dimension4_report.txt")
        validate_scoring()


# ============================================================================
# VALIDATION SUITE
# ============================================================================

def validate_scoring():
    """
    Validate scoring with known stocks (using test data)
    
    Expected scores:
    CTC:  80-95 (exceptional mature compounder)
    LION: 55-70 (strong industrial growth)
    LOLC: 20-40 (VALUE TRAP: revenue but weak profitability)
    LOFC: 65-80 (excellent financial growth)
    JKH:  15-35 (VALUE TRAP: declining conglomerate)
    """
    
    # Test data (placeholder for testing without CSV)
    test_stocks = {
        'CTC.N0000': {
            'industry': 'Cigarettes',
            'revenuegrowth': 1.66,
            'revenuegrowth3y': 15.58,
            'revenuegrowth5y': 13.24,
            'epsgrowth': -5.38,
            'epsgrowth3y': 15.56,
            'epsgrowth5y': 12.48,
            'operatingmargin': 78.53,
            'capex_intensity': -1.23,
            'roe': 277.09,
            'roe5y': 235.58,
        },
        'LION.N0000': {
            'industry': 'Malt Beverages',
            'revenuegrowth': -5.68,
            'revenuegrowth3y': 12.53,
            'revenuegrowth5y': 31.42,
            'epsgrowth': 18.52,
            'epsgrowth3y': 17.89,
            'epsgrowth5y': 35.69,
            'operatingmargin': 28.38,
            'capex_intensity': -4.21,
            'roe': 30.15,
            'roe5y': 26.28,
        },
        'JKH.N0000': {
            'industry': 'Conglomerates',
            'revenuegrowth': 43.47,
            'revenuegrowth3y': 16.67,
            'revenuegrowth5y': 27.58,
            'epsgrowth': -52.50,
            'epsgrowth3y': -45.54,
            'epsgrowth5y': -2.96,
            'operatingmargin': 6.31,
            'capex_intensity': -8.48,
            'roe': 2.83,
            'roe5y': 3.85,
        }
    }
    
    print("=" * 80)
    print("DIMENSION 4 VALIDATION - GROWTH SCORING (Test Data)")
    print("=" * 80)
    print()
    
    for symbol, data in test_stocks.items():
        score, breakdown, interpretation = score_stock_growth(data)
        
        print(f"\n{symbol}")
        print(f"{'─' * 60}")
        print(f"Sector: {breakdown['sector']}")
        print(f"Final Score: {score:.1f}")
        print(f"\nComponent Breakdown:")
        print(f"  Revenue Growth:      {breakdown['component_1_revenue']:.1f} (40% weight)")
        print(f"  Earnings Quality:    {breakdown['component_2_earnings']:.1f} (35% weight)")
        print(f"  Growth Efficiency:   {breakdown['component_3_efficiency']:.1f} (25% weight)")
        print(f"  Base Score:          {breakdown['base_score']:.1f}")
        print(f"  Modifiers:           {breakdown['modifier_total']:+.1f}")
        print(f"  Penalties:           {breakdown['penalty_total']:+.1f}")
        print(f"\nInterpretation: {interpretation}")
        print()