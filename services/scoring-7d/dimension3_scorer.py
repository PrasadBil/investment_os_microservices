#!/usr/bin/env python3
"""
Dimension 3: Valuation Scorer v1.1 (COLUMN NAME FIX)
Based on Graham/Buffett/Damodaran Methodologies

FIXES IN v1.1:
- Column name mapping corrected to match StockAnalysis.com format
  - 'pe' → 'peratio'
  - 'pb' → 'pbratio'
  - 'pfcf' → 'pfcfratio'
  - 'avgpe3y' → 'peratio3y'
  - 'avgpe5y' → 'peratio5y'
  - '52weeklow' → 'low52'
  - '52weekhigh' → 'high52'
  - 'freecashflow' → 'fcf'

THREE-PILLAR FRAMEWORK:
1. Relative Valuation (40%): P/E, P/FCF, EV/EBITDA, Earnings Yield
2. Intrinsic Value Gap (35%): DCF-based fair value, Graham Net-Net, ROE-based P/B
3. Historical Context (25%): Mean reversion, 52W range, ATH drawdown

Usage:
    python dimension3_scorer.py --input cleaned_data.csv --output dimension3_scores.csv

Author: Investment OS
Date: December 30, 2025
Version: 1.0
"""

import pandas as pd
import numpy as np
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Dimension3Scorer:
    """
    Calculate Dimension 3 (Valuation) scores.
    
    Three-Pillar Institutional Framework:
    - Pillar 1: Relative Valuation (compare to peers/market)
    - Pillar 2: Intrinsic Value (what is it worth?)
    - Pillar 3: Historical Context (mean reversion)
    
    Academic Foundations:
    - Benjamin Graham: Margin of safety, net-net valuation
    - Warren Buffett: DCF of owner earnings, quality at fair price
    - Aswath Damodaran: Multiple approaches, sector-specific
    """
    
    def __init__(self):
        self.stats = {
            'total_stocks': 0,
            'scored_stocks': 0,
            'industrial_companies': 0,
            'financial_companies': 0,
            'deep_value': 0,  # Score 80+
            'overvalued': 0,  # Score <40
            'missing_data_stocks': []
        }
        
        # Discount rates for DCF
        self.discount_rate_quality = 0.10  # 10% for quality businesses
        self.discount_rate_emerging = 0.12  # 12% for emerging markets default
        self.max_growth_rate = 0.08  # Cap growth at 8% (conservative)
    
    def classify_industry(self, sector: str, industry: str) -> str:
        """Classify company type."""
        if pd.isna(sector):
            sector = ''
        if pd.isna(industry):
            industry = ''
            
        sector = str(sector).lower()
        industry = str(industry).lower()
        
        financial_keywords = ['bank', 'finance', 'insurance', 'investment', 
                            'capital', 'fund', 'trust', 'credit', 'leasing']
        if any(kw in sector or kw in industry for kw in financial_keywords):
            return 'financial'
        
        growth_keywords = ['technology', 'software', 'internet', 'biotech', 'pharmaceutical']
        if any(kw in sector or kw in industry for kw in growth_keywords):
            return 'growth'
        
        return 'value'
    
    # ========== PILLAR 1: RELATIVE VALUATION (40%) ==========
    
    def score_pe_ratio(self, pe: float) -> Tuple[float, str]:
        """
        Score P/E ratio (Graham/Buffett standard).
        
        Graham: P/E < 15 for value stocks
        Buffett: Reasonable P/E for quality (10-20 range)
        """
        if pd.isna(pe) or pe <= 0:
            return 50, "No Data"
        
        # Handle extreme P/E (loss-making or near-zero earnings)
        if pe > 100:
            return 20, "Very Expensive"
        
        if pe < 10:
            score = 100
            label = "Deep Value"
        elif pe < 15:
            score = 80 + (15 - pe) * 4  # 80-100 range
            label = "Value"
        elif pe < 20:
            score = 60 + (20 - pe) * 4  # 60-80 range
            label = "Fair"
        elif pe < 30:
            score = 40 + (30 - pe) * 2  # 40-60 range
            label = "Premium"
        else:
            score = max(20, 40 - (pe - 30))
            label = "Expensive"
        
        return score, label
    
    def score_pfcf_ratio(self, pfcf: float) -> float:
        """
        Score P/FCF ratio (Buffett's preferred metric).
        
        Buffett focuses on owner earnings ≈ Free Cash Flow
        Target: P/FCF < 15 for quality companies
        """
        if pd.isna(pfcf) or pfcf <= 0:
            return 50
        
        if pfcf > 50:  # Extreme values
            return 20
        
        if pfcf < 10:
            return 100
        elif pfcf < 15:
            return 80 + (15 - pfcf) * 4
        elif pfcf < 20:
            return 60 + (20 - pfcf) * 4
        elif pfcf < 30:
            return 40 + (30 - pfcf) * 2
        else:
            return max(20, 40 - (pfcf - 30))
    
    def score_ev_ebitda(self, ev_ebitda: float) -> float:
        """
        Score EV/EBITDA (Most common institutional metric).
        
        Standard: EV/EBITDA 8-12 = fair value for most industries
        """
        if pd.isna(ev_ebitda) or ev_ebitda <= 0:
            return 50
        
        if ev_ebitda > 20:
            return 20
        
        if ev_ebitda < 6:
            return 100
        elif ev_ebitda < 8:
            return 80 + (8 - ev_ebitda) * 10
        elif ev_ebitda < 12:
            return 60 + (12 - ev_ebitda) * 5
        elif ev_ebitda < 15:
            return 40 + (15 - ev_ebitda) * 6.7
        else:
            return max(20, 40 - (ev_ebitda - 15))
    
    def score_earnings_yield(self, earnings_yield: float) -> float:
        """
        Score Earnings Yield (Graham's metric).
        
        Earnings Yield = E/P = 1/P/E
        Compare to bond yields (investment alternatives)
        """
        if pd.isna(earnings_yield) or earnings_yield <= 0:
            return 50
        
        # Earnings yield as percentage
        if earnings_yield > 10:
            return 100  # Better than most bonds
        elif earnings_yield > 7:
            return 80 + (earnings_yield - 7) * 6.7
        elif earnings_yield > 5:
            return 60 + (earnings_yield - 5) * 10
        elif earnings_yield > 3:
            return 40 + (earnings_yield - 3) * 10
        else:
            return max(20, earnings_yield * 6.7)
    
    def score_pb_ratio_bank(self, pb: float, roe: float) -> Tuple[float, str]:
        """
        Score P/B ratio for banks (sector-specific).
        
        Industry Standard: Book value = capital base for banks
        Fair P/B ≈ ROE / Required Return
        """
        if pd.isna(pb) or pb <= 0:
            return 50, "No Data"
        
        # Calculate fair P/B if we have ROE
        if pd.notna(roe) and roe > 0:
            fair_pb = roe / self.discount_rate_emerging
            ratio = pb / fair_pb
            
            if ratio < 0.6:
                score = 100
                label = "Deep Value"
            elif ratio < 0.8:
                score = 85 + (0.8 - ratio) * 75
                label = "Value"
            elif ratio < 1.0:
                score = 70 + (1.0 - ratio) * 75
                label = "Fair"
            elif ratio < 1.2:
                score = 50 + (1.2 - ratio) * 100
                label = "Slight Premium"
            else:
                score = max(20, 50 - (ratio - 1.2) * 50)
                label = "Overvalued"
        else:
            # Fallback: Absolute P/B scoring
            if pb < 0.7:
                score = 100
                label = "Below Book"
            elif pb < 1.0:
                score = 85 + (1.0 - pb) * 50
                label = "At Book"
            elif pb < 1.5:
                score = 70 + (1.5 - pb) * 30
                label = "Moderate Premium"
            elif pb < 2.0:
                score = 50 + (2.0 - pb) * 40
                label = "High Premium"
            elif pb < 3.0:
                score = 30 + (3.0 - pb) * 20
                label = "Very High"
            else:
                score = max(20, 30 - (pb - 3.0) * 5)
                label = "Expensive"
        
        return score, label
    
    def calculate_pillar1_industrial(self, row: pd.Series) -> Dict:
        """Pillar 1: Relative Valuation for industrial companies."""
        # Component 1: P/E Ratio (35%)
        pe = row.get('peratio', None)  # FIXED: 'pe' → 'peratio'
        pe_score, pe_label = self.score_pe_ratio(pe)
        
        # Component 2: P/FCF Ratio (30%)
        pfcf = row.get('pfcfratio', None)  # FIXED: 'pfcf' → 'pfcfratio'
        pfcf_score = self.score_pfcf_ratio(pfcf)
        
        # Component 3: EV/EBITDA (25%)
        ev_ebitda = row.get('evebitda', None)  # Already correct ✓
        ev_ebitda_score = self.score_ev_ebitda(ev_ebitda)
        
        # Component 4: Earnings Yield (10%)
        earnings_yield = row.get('earningsyield', None)  # Already correct ✓
        ey_score = self.score_earnings_yield(earnings_yield)
        
        # Weighted combination
        pillar1_score = (pe_score * 0.35) + (pfcf_score * 0.30) + \
                       (ev_ebitda_score * 0.25) + (ey_score * 0.10)
        
        return {
            'pillar1_score': pillar1_score,
            'pe': pe if pd.notna(pe) else 0,
            'pe_label': pe_label,
            'pfcf': pfcf if pd.notna(pfcf) else 0,
            'ev_ebitda': ev_ebitda if pd.notna(ev_ebitda) else 0,
            'earnings_yield': earnings_yield if pd.notna(earnings_yield) else 0
        }
    
    def calculate_pillar1_financial(self, row: pd.Series) -> Dict:
        """Pillar 1: Relative Valuation for financial companies."""
        # Component 1: P/B Ratio (50%) - Primary for banks
        pb = row.get('pbratio', None)  # FIXED: 'pb' → 'pbratio'
        roe = row.get('roe5y', None) or row.get('roe', None)
        pb_score, pb_label = self.score_pb_ratio_bank(pb, roe)
        
        # Component 2: P/E Ratio (30%)
        pe = row.get('peratio', None)  # FIXED: 'pe' → 'peratio'
        pe_score, pe_label = self.score_pe_ratio(pe)
        
        # Component 3: P/Tangible Book (20%)
        # If not available, use P/B as proxy
        ptbv = row.get('pricetotangiblebookvalue', None) or pb
        ptbv_score, _ = self.score_pb_ratio_bank(ptbv, roe)
        
        # Weighted combination
        pillar1_score = (pb_score * 0.50) + (pe_score * 0.30) + (ptbv_score * 0.20)
        
        return {
            'pillar1_score': pillar1_score,
            'pb': pb if pd.notna(pb) else 0,
            'pb_label': pb_label,
            'pe': pe if pd.notna(pe) else 0,
            'pe_label': pe_label
        }
    
    # ========== PILLAR 2: INTRINSIC VALUE GAP (35%) ==========
    
    def calculate_dcf_intrinsic_value(self, fcf: float, growth: float, 
                                     discount_rate: float) -> float:
        """
        Calculate intrinsic value using simplified DCF.
        
        IV = FCF × (1 + g) / (r - g)
        
        Buffett's approach: Owner earnings discounted to present
        """
        if pd.isna(fcf) or fcf <= 0:
            return 0
        
        if pd.isna(growth):
            growth = 0.03  # Default 3% growth
        
        # Cap growth at max_growth_rate (conservative)
        growth = min(growth, self.max_growth_rate)
        
        # Ensure discount rate > growth rate
        if discount_rate <= growth:
            discount_rate = growth + 0.02  # Add 2% margin
        
        try:
            intrinsic_value = (fcf * (1 + growth)) / (discount_rate - growth)
            return intrinsic_value
        except:
            return 0
    
    def calculate_graham_ncav(self, current_assets: float, total_liabilities: float,
                             shares_outstanding: float) -> float:
        """
        Calculate Graham's Net Current Asset Value per share.
        
        NCAV = (Current Assets - Total Liabilities) / Shares
        Graham: Buy at < 0.67 × NCAV for extreme margin of safety
        """
        if pd.isna(current_assets) or pd.isna(total_liabilities) or pd.isna(shares_outstanding):
            return 0
        
        if shares_outstanding <= 0:
            return 0
        
        ncav = (current_assets - total_liabilities) / shares_outstanding
        return max(0, ncav)
    
    def score_price_vs_intrinsic(self, price: float, intrinsic_value: float) -> Tuple[float, str]:
        """
        Score based on Price vs Intrinsic Value ratio.
        
        Buffett: Want 25%+ margin of safety (price < 0.75 × IV)
        """
        if price <= 0 or intrinsic_value <= 0:
            return 50, "Unable to Calculate"
        
        ratio = price / intrinsic_value
        
        if ratio < 0.5:
            score = 100
            label = "Extreme Value (50%+ MOS)"
        elif ratio < 0.7:
            score = 85 + (0.7 - ratio) * 75
            label = "Deep Value (30-50% MOS)"
        elif ratio < 0.9:
            score = 70 + (0.9 - ratio) * 75
            label = "Value (10-30% MOS)"
        elif ratio < 1.1:
            score = 50 + (1.1 - ratio) * 100
            label = "Fair Value"
        elif ratio < 1.3:
            score = 35 + (1.3 - ratio) * 75
            label = "Slight Premium"
        else:
            score = max(20, 35 - (ratio - 1.3) * 25)
            label = "Overvalued"
        
        return score, label
    
    def calculate_pillar2_industrial(self, row: pd.Series) -> Dict:
        """Pillar 2: Intrinsic Value for industrial companies."""
        # Get FCF and growth
        fcf = row.get('fcf', None)  # FIXED: 'freecashflow' → 'fcf'
        
        # Use 3Y FCF growth (most recent, balanced)
        fcf_growth_3y = row.get('fcfgrowth3y', None)  # Already correct ✓
        if pd.isna(fcf_growth_3y):
            fcf_growth_3y = 0.03  # Default 3%
        else:
            fcf_growth_3y = fcf_growth_3y / 100  # Convert from percentage
        
        # Calculate intrinsic value
        intrinsic_value = self.calculate_dcf_intrinsic_value(
            fcf, fcf_growth_3y, self.discount_rate_emerging
        )
        
        # Get market cap
        market_cap = row.get('marketcap', None)
        
        # Score price vs intrinsic
        if intrinsic_value > 0 and pd.notna(market_cap):
            iv_score, iv_label = self.score_price_vs_intrinsic(market_cap, intrinsic_value)
            iv_ratio = market_cap / intrinsic_value
        else:
            iv_score = 50
            iv_label = "Unable to Calculate"
            iv_ratio = 0
        
        # Graham Net-Net (for asset-heavy companies)
        current_assets = row.get('totalcurrentassets', None)
        total_liabilities = row.get('total_liabilities', None)
        shares = row.get('sharesoutstanding', None)
        price = row.get('price', None)
        
        ncav = self.calculate_graham_ncav(current_assets, total_liabilities, shares)
        
        if ncav > 0 and pd.notna(price) and price > 0:
            ncav_ratio = price / ncav
            
            if ncav_ratio < 0.5:
                ncav_score = 100
            elif ncav_ratio < 0.67:
                ncav_score = 90 + (0.67 - ncav_ratio) * 59
            elif ncav_ratio < 1.0:
                ncav_score = 70 + (1.0 - ncav_ratio) * 60
            elif ncav_ratio < 1.5:
                ncav_score = 50 + (1.5 - ncav_ratio) * 40
            else:
                ncav_score = max(30, 50 - (ncav_ratio - 1.5) * 20)
        else:
            ncav_score = 50
            ncav_ratio = 0
        
        # Weighted combination (70% DCF, 30% NCAV)
        pillar2_score = (iv_score * 0.70) + (ncav_score * 0.30)
        
        return {
            'pillar2_score': pillar2_score,
            'intrinsic_value': intrinsic_value,
            'price_iv_ratio': iv_ratio,
            'iv_label': iv_label,
            'ncav': ncav,
            'price_ncav_ratio': ncav_ratio
        }
    
    def calculate_pillar2_financial(self, row: pd.Series) -> Dict:
        """Pillar 2: Intrinsic Value for financial companies."""
        # For banks: Fair P/B = ROE / Required Return
        roe = row.get('roe5y', None) or row.get('roe', None)
        pb = row.get('pbratio', None)  # FIXED: 'pb' → 'pbratio'
        
        if pd.notna(roe) and roe > 0:
            fair_pb = roe / self.discount_rate_emerging
            
            if pd.notna(pb) and pb > 0:
                pb_ratio = pb / fair_pb
                score, label = self.score_price_vs_intrinsic(pb, fair_pb)
            else:
                score = 50
                label = "No P/B Data"
                pb_ratio = 0
        else:
            fair_pb = 0
            pb_ratio = 0
            score = 50
            label = "No ROE Data"
        
        return {
            'pillar2_score': score,
            'fair_pb': fair_pb,
            'pb_ratio': pb_ratio,
            'iv_label': label
        }
    
    # ========== PILLAR 3: HISTORICAL CONTEXT (25%) ==========
    
    def calculate_pe_percentile(self, current_pe: float, avg_pe_3y: float, 
                               avg_pe_5y: float) -> Tuple[float, str]:
        """
        Calculate where current P/E sits vs historical averages.
        
        Mean Reversion: Damodaran - "Multiples revert to mean over time"
        """
        if pd.isna(current_pe) or current_pe <= 0:
            return 50, "No Data"
        
        # Use 5Y average as baseline (more stable)
        if pd.notna(avg_pe_5y) and avg_pe_5y > 0:
            baseline = avg_pe_5y
        elif pd.notna(avg_pe_3y) and avg_pe_3y > 0:
            baseline = avg_pe_3y
        else:
            return 50, "No Historical Data"
        
        # Calculate ratio
        ratio = current_pe / baseline
        
        if ratio < 0.7:
            score = 100
            label = "Historically Cheap"
        elif ratio < 0.85:
            score = 80 + (0.85 - ratio) * 133
            label = "Below Average"
        elif ratio < 1.15:
            score = 60 + (1.15 - ratio) * 66
            label = "Average"
        elif ratio < 1.3:
            score = 40 + (1.3 - ratio) * 133
            label = "Above Average"
        else:
            score = max(20, 40 - (ratio - 1.3) * 33)
            label = "Historically Expensive"
        
        return score, label
    
    def calculate_52w_position(self, price: float, week52_low: float, 
                              week52_high: float) -> Tuple[float, str]:
        """
        Calculate where current price sits in 52-week range.
        
        Position near 52W low = potential value opportunity
        """
        if pd.isna(price) or pd.isna(week52_low) or pd.isna(week52_high):
            return 50, "No Data"
        
        if week52_high <= week52_low:
            return 50, "Invalid Range"
        
        # Calculate position (0 = at low, 1 = at high)
        position = (price - week52_low) / (week52_high - week52_low)
        position = max(0, min(1, position))
        
        if position < 0.2:
            score = 100
            label = "Near 52W Low"
        elif position < 0.4:
            score = 80 + (0.4 - position) * 100
            label = "Lower Third"
        elif position < 0.6:
            score = 60 + (0.6 - position) * 100
            label = "Middle Range"
        elif position < 0.8:
            score = 40 + (0.8 - position) * 100
            label = "Upper Third"
        else:
            score = max(20, 40 - (position - 0.8) * 100)
            label = "Near 52W High"
        
        return score, label
    
    def calculate_ath_drawdown(self, price: float, all_time_high: float) -> Tuple[float, str]:
        """
        Calculate drawdown from all-time high.
        
        Large drawdowns = potential opportunity if fundamentals strong
        """
        if pd.isna(price) or pd.isna(all_time_high) or all_time_high <= 0:
            return 50, "No Data"
        
        drawdown = (all_time_high - price) / all_time_high
        drawdown = max(0, drawdown)  # Can't be negative
        
        if drawdown > 0.5:
            score = 100
            label = ">50% Below ATH"
        elif drawdown > 0.3:
            score = 80 + (drawdown - 0.3) * 100
            label = "30-50% Below ATH"
        elif drawdown > 0.15:
            score = 60 + (drawdown - 0.15) * 133
            label = "15-30% Below ATH"
        elif drawdown > 0:
            score = 40 + (drawdown) * 133
            label = "0-15% Below ATH"
        else:
            score = 30
            label = "At/Near ATH"
        
        return score, label
    
    def calculate_pillar3(self, row: pd.Series) -> Dict:
        """Pillar 3: Historical Valuation Context."""
        # Component 1: P/E vs Historical Average (40%)
        current_pe = row.get('peratio', None)  # FIXED: 'pe' → 'peratio'
        avg_pe_3y = row.get('peratio3y', None)  # FIXED: 'avgpe3y' → 'peratio3y'
        avg_pe_5y = row.get('peratio5y', None)  # FIXED: 'avgpe5y' → 'peratio5y'
        
        pe_hist_score, pe_hist_label = self.calculate_pe_percentile(
            current_pe, avg_pe_3y, avg_pe_5y
        )
        
        # Component 2: 52-Week Position (30%)
        price = row.get('price', None)  # Already correct ✓
        week52_low = row.get('low52', None)  # FIXED: '52weeklow' → 'low52'
        week52_high = row.get('high52', None)  # FIXED: '52weekhigh' → 'high52'
        
        week52_score, week52_label = self.calculate_52w_position(
            price, week52_low, week52_high
        )
        
        # Component 3: ATH Drawdown (30%)
        all_time_high = row.get('alltimehigh', None)  # Already correct ✓
        
        ath_score, ath_label = self.calculate_ath_drawdown(price, all_time_high)
        
        # Weighted combination
        pillar3_score = (pe_hist_score * 0.40) + (week52_score * 0.30) + (ath_score * 0.30)
        
        return {
            'pillar3_score': pillar3_score,
            'pe_historical_label': pe_hist_label,
            'week52_position_label': week52_label,
            'ath_drawdown_label': ath_label,
            'current_vs_avg_pe': (current_pe / avg_pe_5y) if (pd.notna(current_pe) and pd.notna(avg_pe_5y) and avg_pe_5y > 0) else 0
        }
    
    # ========== MAIN SCORING LOGIC ==========
    
    def calculate_dimension3(self, row: pd.Series) -> Dict:
        """Calculate complete Dimension 3: Valuation score."""
        symbol = row.get('symbol', 'UNKNOWN')
        
        try:
            sector = row.get('sector', '')
            industry = row.get('industry', '')
            industry_type = self.classify_industry(sector, industry)
            
            is_financial = (industry_type == 'financial')
            
            # Calculate three pillars
            if is_financial:
                self.stats['financial_companies'] += 1
                pillar1 = self.calculate_pillar1_financial(row)
                pillar2 = self.calculate_pillar2_financial(row)
            else:
                self.stats['industrial_companies'] += 1
                pillar1 = self.calculate_pillar1_industrial(row)
                pillar2 = self.calculate_pillar2_industrial(row)
            
            pillar3 = self.calculate_pillar3(row)
            
            # Weighted combination
            pillar1_contribution = pillar1['pillar1_score'] * 0.40
            pillar2_contribution = pillar2['pillar2_score'] * 0.35
            pillar3_contribution = pillar3['pillar3_score'] * 0.25
            
            final_score = pillar1_contribution + pillar2_contribution + pillar3_contribution
            final_score = max(0, min(final_score, 100))
            
            # Quality Label
            if final_score >= 80:
                quality_label = "Deep Value"
                self.stats['deep_value'] += 1
            elif final_score >= 70:
                quality_label = "Value"
            elif final_score >= 60:
                quality_label = "Fair Value"
            elif final_score >= 50:
                quality_label = "Fairly Valued"
            elif final_score >= 40:
                quality_label = "Slight Premium"
            else:
                quality_label = "Overvalued"
                self.stats['overvalued'] += 1
            
            result = {
                'symbol': symbol,
                'dimension_3_score': round(final_score, 1),
                'quality_label': quality_label,
                'industry_type': industry_type,
                'pillar1_score': round(pillar1['pillar1_score'], 1),
                'pillar1_contribution': round(pillar1_contribution, 1),
                'pillar2_score': round(pillar2['pillar2_score'], 1),
                'pillar2_contribution': round(pillar2_contribution, 1),
                'pillar3_score': round(pillar3['pillar3_score'], 1),
                'pillar3_contribution': round(pillar3_contribution, 1),
                'sector': sector,
                'industry': industry
            }
            
            result.update(pillar1)
            result.update(pillar2)
            result.update(pillar3)
            
            return result
            
        except Exception as e:
            logger.error(f"Error scoring {symbol}: {e}")
            self.stats['missing_data_stocks'].append(symbol)
            return {
                'symbol': symbol,
                'dimension_3_score': 0,
                'quality_label': 'Data Insufficient',
                'error': str(e)
            }
    
    def score_all_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Score all stocks."""
        logger.info(f"Starting Dimension 3 scoring for {len(df)} stocks...")
        logger.info("Three-Pillar Framework: Relative, Intrinsic, Historical")
        
        self.stats['total_stocks'] = len(df)
        
        results = []
        for idx, row in df.iterrows():
            result = self.calculate_dimension3(row)
            results.append(result)
            
            if result['dimension_3_score'] > 0:
                self.stats['scored_stocks'] += 1
        
        results_df = pd.DataFrame(results)
        
        logger.info(f"Scoring complete!")
        logger.info(f"  Total: {self.stats['total_stocks']}")
        logger.info(f"  Scored: {self.stats['scored_stocks']}")
        logger.info(f"  Industrial: {self.stats['industrial_companies']}")
        logger.info(f"  Financial: {self.stats['financial_companies']}")
        logger.info(f"  Deep Value (80+): {self.stats['deep_value']}")
        logger.info(f"  Overvalued (<40): {self.stats['overvalued']}")
        
        return results_df
    
    def generate_report(self, results_df: pd.DataFrame) -> str:
        """Generate scoring summary report."""
        report = []
        report.append("=" * 80)
        report.append("DIMENSION 3: VALUATION SCORING REPORT v1.1 (COLUMN FIX)")
        report.append("Graham/Buffett/Damodaran Three-Pillar Framework")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("FIXES IN v1.1:")
        report.append("  ✓ Column name mapping corrected (peratio, pbratio, pfcfratio, etc.)")
        report.append("")
        report.append("THREE-PILLAR FRAMEWORK:")
        report.append("  Pillar 1 (40%): Relative Valuation (P/E, P/FCF, EV/EBITDA)")
        report.append("  Pillar 2 (35%): Intrinsic Value Gap (DCF, Graham Net-Net)")
        report.append("  Pillar 3 (25%): Historical Context (Mean Reversion)")
        report.append("=" * 80)
        report.append("")
        
        report.append("OVERALL STATISTICS:")
        report.append(f"  Total stocks: {len(results_df)}")
        report.append(f"  Successfully scored: {self.stats['scored_stocks']}")
        report.append(f"  Industrial companies: {self.stats['industrial_companies']}")
        report.append(f"  Financial companies: {self.stats['financial_companies']}")
        report.append(f"  Deep Value (80+): {self.stats['deep_value']}")
        report.append(f"  Overvalued (<40): {self.stats['overvalued']}")
        report.append("")
        
        valid_scores = results_df[results_df['dimension_3_score'] > 0]['dimension_3_score']
        if len(valid_scores) > 0:
            report.append("SCORE DISTRIBUTION:")
            report.append(f"  Mean: {valid_scores.mean():.1f}")
            report.append(f"  Median: {valid_scores.median():.1f}")
            report.append(f"  Std Dev: {valid_scores.std():.1f}")
            report.append(f"  Min: {valid_scores.min():.1f}")
            report.append(f"  Max: {valid_scores.max():.1f}")
            report.append("")
        
        report.append("QUALITY DISTRIBUTION:")
        quality_counts = results_df['quality_label'].value_counts()
        for quality, count in quality_counts.items():
            pct = (count / len(results_df)) * 100
            report.append(f"  {quality}: {count} ({pct:.1f}%)")
        report.append("")
        
        report.append("TOP 10 VALUE OPPORTUNITIES:")
        top10 = results_df.nlargest(10, 'dimension_3_score')[
            ['symbol', 'dimension_3_score', 'quality_label', 'industry_type']
        ]
        for idx, row in top10.iterrows():
            report.append(f"  {row['symbol']:10} Score: {row['dimension_3_score']:5.1f}  "
                        f"({row['quality_label']:15}) [{row['industry_type']}]")
        report.append("")
        
        report.append("VALIDATION STOCKS:")
        validation_symbols = ['CTC.N0000', 'JKH.N0000', 'LOLC.N0000', 'LOFC.N0000', 
                            'LION.N0000', 'ABAN.N0000']
        validation_stocks = results_df[results_df['symbol'].isin(validation_symbols)]
        
        if len(validation_stocks) > 0:
            for idx, row in validation_stocks.sort_values('dimension_3_score', ascending=False).iterrows():
                report.append(f"  {row['symbol']:10} Score: {row['dimension_3_score']:5.1f}  "
                            f"({row['quality_label']:15}) [{row['industry_type']}]")
            report.append("")
        
        report.append("=" * 80)
        report.append("END OF REPORT")
        report.append("=" * 80)
        
        return "\n".join(report)


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description='Dimension 3 Valuation Scorer v1.1 (Column Fix)'
    )
    parser.add_argument('--input', default='cleaned_data.csv')
    parser.add_argument('--output', default='dimension3_scores_v1.1.csv')
    parser.add_argument('--report', default='dimension3_report_v1.1.txt')
    
    args = parser.parse_args()
    
    logger.info(f"Loading data from {args.input}...")
    try:
        df = pd.read_csv(args.input)
        logger.info(f"Loaded {len(df)} stocks with {len(df.columns)} columns")
    except FileNotFoundError:
        logger.error(f"Input file not found: {args.input}")
        return
    
    scorer = Dimension3Scorer()
    results_df = scorer.score_all_stocks(df)
    
    logger.info(f"Saving scores to {args.output}...")
    results_df.to_csv(args.output, index=False)
    
    logger.info(f"Generating report...")
    report = scorer.generate_report(results_df)
    
    with open(args.report, 'w') as f:
        f.write(report)
    
    print("\n")
    print(report)
    
    logger.info("=" * 80)
    logger.info("DIMENSION 3 SCORING COMPLETE!")
    logger.info(f"Scores: {args.output}")
    logger.info(f"Report: {args.report}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()