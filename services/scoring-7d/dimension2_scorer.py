
#!/usr/bin/env python3
"""
Dimension 2: Financial Strength Scorer v1.1 (FORTRESS FIX)
Based on Buffett's Balance Sheet Principles + Basel III Standards

FILE: dimension2_scorer.py
CREATED: 2025-12-30
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2025-12-30  Initial creation — Financial Strength scorer (Basel III)
    v1.1.0  2025-12-30  Fix: Fortress balance sheet bonus; extended ROA range for banks
    v1.1.1  2026-02-11  Migrated to services/scoring-7d (Phase 2 microservices)
    v1.1.2  2026-02-16  Added version history header (new project standard)

FIXES IN v1.1:
1. Fortress Balance Sheet Bonus: Companies with zero/minimal debt get liquidity credit
2. Extended ROA Range: Differentiates exceptional banks (ROA 5%+) from good banks (ROA 3%+)

Usage:
    python dimension2_scorer.py --input cleaned_data.csv --output dimension2_scores.csv
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


class Dimension2ScorerV11:
    """
    Calculate Dimension 2 (Financial Strength) scores.
    
    Version 1.1 - Fortress Balance Sheet Recognition
    
    Buffett's Insight: "Companies with no debt don't need high liquidity ratios"
    """
    
    def __init__(self):
        self.stats = {
            'total_stocks': 0,
            'scored_stocks': 0,
            'industrial_companies': 0,
            'financial_companies': 0,
            'net_cash_companies': 0,
            'fortress_balance_sheets': 0,
            'fortress_bonus_applied': 0,
            'missing_data_stocks': []
        }
    
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
        
        light_keywords = ['technology', 'software', 'internet', 'media', 
                         'telecom', 'service', 'consulting', 'tobacco', 'beverage']
        if any(kw in sector or kw in industry for kw in light_keywords):
            return 'asset_light'
        
        return 'asset_heavy'
    
    # ========== INDUSTRIAL COMPANY SCORING ==========
    
    def score_debt_equity_ratio(self, debt_equity: float) -> Tuple[float, str]:
        """Score Debt/Equity ratio for industrial companies."""
        if pd.isna(debt_equity):
            return 50, "No Data"
        
        if debt_equity < 0:
            return 10, "Negative Equity"
        
        if debt_equity <= 0.3:
            score = 100
        elif debt_equity <= 0.5:
            score = 90 + (0.5 - debt_equity) * 50
        elif debt_equity <= 1.0:
            score = 70 + (1.0 - debt_equity) * 40
        elif debt_equity <= 2.0:
            score = 50 + (2.0 - debt_equity) * 20
        elif debt_equity <= 3.0:
            score = 30 + (3.0 - debt_equity) * 20
        else:
            score = max(20, 30 - (debt_equity - 3.0) * 5)
        
        quality = "Fortress" if debt_equity <= 0.3 else "Strong" if debt_equity <= 0.5 else \
                  "Good" if debt_equity <= 1.0 else "Moderate" if debt_equity <= 2.0 else \
                  "High" if debt_equity <= 3.0 else "Very High"
        
        return min(score, 100), quality
    
    def score_debt_ebitda_ratio(self, debt_ebitda: float) -> float:
        """Score Debt/EBITDA ratio."""
        if pd.isna(debt_ebitda) or debt_ebitda < 0:
            return 50
        
        if debt_ebitda <= 1.0:
            return 100
        elif debt_ebitda <= 2.0:
            return 85 + (2.0 - debt_ebitda) * 15
        elif debt_ebitda <= 3.0:
            return 65 + (3.0 - debt_ebitda) * 20
        elif debt_ebitda <= 4.0:
            return 45 + (4.0 - debt_ebitda) * 20
        elif debt_ebitda <= 5.0:
            return 30 + (5.0 - debt_ebitda) * 15
        else:
            return max(20, 30 - (debt_ebitda - 5.0) * 5)
    
    def calculate_industrial_component1(self, row: pd.Series) -> Dict:
        """Component 1: Debt Metrics (40% weight) for industrial companies."""
        debt_equity = row.get('debtequity', None)
        de_score, de_quality = self.score_debt_equity_ratio(debt_equity)
        
        debt_ebitda = row.get('debtebitda', None)
        de_ebitda_score = self.score_debt_ebitda_ratio(debt_ebitda)
        
        base_score = (de_score * 0.7) + (de_ebitda_score * 0.3)
        
        net_cash = row.get('net_cash', 0)
        has_net_cash = pd.notna(net_cash) and net_cash > 0
        if has_net_cash:
            base_score = min(100, base_score + 10)
            self.stats['net_cash_companies'] += 1
        
        return {
            'component1_score': base_score,
            'debt_equity': debt_equity if pd.notna(debt_equity) else 0,
            'debt_quality': de_quality,
            'debt_ebitda': debt_ebitda if pd.notna(debt_ebitda) else 0,
            'has_net_cash': has_net_cash
        }
    
    def score_current_ratio(self, current_ratio: float, industry_type: str) -> float:
        """Score Current Ratio (adjusted by industry)."""
        if pd.isna(current_ratio) or current_ratio < 0:
            return 50
        
        if industry_type == 'asset_light':
            if current_ratio >= 3.0:
                return 100
            elif current_ratio >= 2.0:
                return 85 + (current_ratio - 2.0) * 15
            elif current_ratio >= 1.5:
                return 70 + (current_ratio - 1.5) * 30
            elif current_ratio >= 1.0:
                return 50 + (current_ratio - 1.0) * 40
            else:
                return max(20, current_ratio * 50)
        else:
            if current_ratio >= 2.5:
                return 100
            elif current_ratio >= 1.5:
                return 85 + (current_ratio - 1.5) * 15
            elif current_ratio >= 1.2:
                return 65 + (current_ratio - 1.2) * 66
            elif current_ratio >= 1.0:
                return 45 + (current_ratio - 1.0) * 100
            else:
                return max(20, current_ratio * 45)
    
    def score_quick_ratio(self, quick_ratio: float) -> float:
        """Score Quick Ratio."""
        if pd.isna(quick_ratio) or quick_ratio < 0:
            return 50
        
        if quick_ratio >= 2.0:
            return 100
        elif quick_ratio >= 1.5:
            return 85 + (quick_ratio - 1.5) * 30
        elif quick_ratio >= 1.0:
            return 65 + (quick_ratio - 1.0) * 40
        elif quick_ratio >= 0.5:
            return 40 + (quick_ratio - 0.5) * 50
        else:
            return max(20, quick_ratio * 40)
    
    def calculate_industrial_component2(self, row: pd.Series, industry_type: str, 
                                       debt_equity: float, has_net_cash: bool, 
                                       interest_coverage: float) -> Dict:
        """
        Component 2: Liquidity (35% weight) for industrial companies.
        
        NEW in v1.1: Fortress Balance Sheet Bonus
        If company has minimal/zero debt, liquidity ratios less important.
        """
        current_ratio = row.get('currentratio', None)
        cr_score = self.score_current_ratio(current_ratio, industry_type)
        
        quick_ratio = row.get('quickratio', None)
        qr_score = self.score_quick_ratio(quick_ratio)
        
        liquidity_score = (cr_score * 0.7) + (qr_score * 0.3)
        
        # NEW in v1.1: Fortress Balance Sheet Bonus
        # Buffett: "Companies with no debt don't need high liquidity ratios"
        fortress_bonus_applied = False
        
        if pd.notna(debt_equity) and pd.notna(interest_coverage):
            # If minimal debt + net cash + high coverage → liquidity not a concern
            if debt_equity < 0.2 and has_net_cash and interest_coverage > 10:
                # Give minimum 85 points (Good liquidity assumed)
                liquidity_score = max(liquidity_score, 85)
                fortress_bonus_applied = True
                self.stats['fortress_bonus_applied'] += 1
        
        return {
            'component2_score': liquidity_score,
            'current_ratio': current_ratio if pd.notna(current_ratio) else 0,
            'quick_ratio': quick_ratio if pd.notna(quick_ratio) else 0,
            'fortress_bonus_applied': fortress_bonus_applied
        }
    
    def score_interest_coverage(self, interest_coverage: float) -> float:
        """Score Interest Coverage ratio."""
        if pd.isna(interest_coverage):
            return 50
        
        if interest_coverage > 100 or interest_coverage < 0:
            return 100
        
        if interest_coverage >= 10:
            return 100
        elif interest_coverage >= 5:
            return 85 + (interest_coverage - 5) * 3
        elif interest_coverage >= 3:
            return 65 + (interest_coverage - 3) * 10
        elif interest_coverage >= 2:
            return 45 + (interest_coverage - 2) * 20
        elif interest_coverage >= 1:
            return 25 + (interest_coverage - 1) * 20
        else:
            return max(10, interest_coverage * 15)
    
    def calculate_equity_assets_ratio(self, total_equity: float, total_assets: float) -> float:
        """Calculate Equity/Assets ratio."""
        if pd.isna(total_equity) or pd.isna(total_assets) or total_assets <= 0:
            return 0
        
        return (total_equity / total_assets) * 100
    
    def score_equity_assets_ratio(self, equity_assets: float) -> float:
        """Score Equity/Assets ratio."""
        if equity_assets <= 0:
            return 20
        
        if equity_assets >= 60:
            return 100
        elif equity_assets >= 50:
            return 90 + (equity_assets - 50) * 1
        elif equity_assets >= 40:
            return 75 + (equity_assets - 40) * 1.5
        elif equity_assets >= 30:
            return 55 + (equity_assets - 30) * 2
        elif equity_assets >= 20:
            return 35 + (equity_assets - 20) * 2
        else:
            return max(20, equity_assets * 1.5)
    
    def calculate_industrial_component3(self, row: pd.Series) -> Dict:
        """Component 3: Solvency (25% weight) for industrial companies."""
        interest_coverage = row.get('interestcoverage', None)
        ic_score = self.score_interest_coverage(interest_coverage)
        
        total_equity = row.get('total_equity', None)
        total_assets = row.get('total_assets', None)
        equity_assets = self.calculate_equity_assets_ratio(total_equity, total_assets)
        ea_score = self.score_equity_assets_ratio(equity_assets)
        
        solvency_score = (ic_score * 0.7) + (ea_score * 0.3)
        
        return {
            'component3_score': solvency_score,
            'interest_coverage': interest_coverage if pd.notna(interest_coverage) else 0,
            'equity_assets_ratio': equity_assets
        }
    
    # ========== FINANCIAL COMPANY SCORING ==========
    
    def score_bank_equity_ratio(self, equity_assets: float) -> Tuple[float, str]:
        """Score Equity/Assets ratio for banks (Basel III proxy)."""
        if equity_assets <= 0:
            return 20, "Undercapitalized"
        
        if equity_assets >= 12:
            score = 100
            quality = "Excellent"
        elif equity_assets >= 10:
            score = 90 + (equity_assets - 10) * 5
            quality = "Strong"
        elif equity_assets >= 8:
            score = 75 + (equity_assets - 8) * 7.5
            quality = "Adequate"
        elif equity_assets >= 6:
            score = 55 + (equity_assets - 6) * 10
            quality = "Moderate"
        elif equity_assets >= 4:
            score = 35 + (equity_assets - 4) * 10
            quality = "Weak"
        else:
            score = max(20, equity_assets * 5)
            quality = "Undercapitalized"
        
        return score, quality
    
    def calculate_leverage_ratio(self, total_assets: float, total_equity: float) -> float:
        """Calculate leverage ratio (Assets/Equity)."""
        if pd.isna(total_equity) or total_equity <= 0:
            return 0
        
        if pd.isna(total_assets) or total_assets <= 0:
            return 0
        
        return total_assets / total_equity
    
    def score_bank_leverage(self, leverage: float) -> float:
        """Score bank leverage ratio."""
        if leverage <= 0:
            return 50
        
        if leverage <= 8:
            return 100
        elif leverage <= 10:
            return 90 + (10 - leverage) * 5
        elif leverage <= 12:
            return 75 + (12 - leverage) * 7.5
        elif leverage <= 15:
            return 55 + (15 - leverage) * 6.7
        elif leverage <= 20:
            return 35 + (20 - leverage) * 4
        else:
            return max(20, 35 - (leverage - 20))
    
    def calculate_financial_component1(self, row: pd.Series) -> Dict:
        """Component 1: Capital Adequacy (40% weight) for financial companies."""
        total_equity = row.get('total_equity', None)
        total_assets = row.get('total_assets', None)
        equity_assets = self.calculate_equity_assets_ratio(total_equity, total_assets)
        
        ea_score, capital_quality = self.score_bank_equity_ratio(equity_assets)
        
        leverage = self.calculate_leverage_ratio(total_assets, total_equity)
        lev_score = self.score_bank_leverage(leverage)
        
        capital_score = (ea_score * 0.7) + (lev_score * 0.3)
        
        return {
            'component1_score': capital_score,
            'equity_assets_ratio': equity_assets,
            'capital_quality': capital_quality,
            'leverage_ratio': leverage
        }
    
    def score_bank_roa(self, roa_5y: float) -> Tuple[float, str]:
        """
        Score ROA for banks (Buffett's primary metric).
        
        NEW in v1.1: Extended range for exceptional banks (ROA 5%+)
        Differentiates LOFC (5.22%) from LOLC (3.44%)
        """
        if pd.isna(roa_5y) or roa_5y == 0:
            return 50, "No Data"
        
        # NEW: Extended scoring for exceptional banks
        if roa_5y >= 5:
            score = 100
            quality = "World-Class"
        elif roa_5y >= 4:
            score = 95 + (roa_5y - 4) * 5  # 95-100 range
            quality = "Exceptional"
        elif roa_5y >= 3.5:
            score = 90 + (roa_5y - 3.5) * 10  # 90-95 range
            quality = "Exceptional"
        elif roa_5y >= 3:
            score = 85 + (roa_5y - 3) * 10  # 85-90 range
            quality = "Excellent"
        elif roa_5y >= 2:
            score = 70 + (roa_5y - 2) * 15  # 70-85 range
            quality = "Good"
        elif roa_5y >= 1.5:
            score = 55 + (roa_5y - 1.5) * 30  # 55-70 range
            quality = "Average"
        elif roa_5y >= 1:
            score = 40 + (roa_5y - 1) * 30  # 40-55 range
            quality = "Below Average"
        elif roa_5y >= 0.5:
            score = 25 + (roa_5y - 0.5) * 30  # 25-40 range
            quality = "Weak"
        else:
            score = max(20, roa_5y * 25)
            quality = "Poor"
        
        return score, quality
    
    def calculate_financial_component2(self, row: pd.Series) -> Dict:
        """Component 2: Asset Efficiency (35% weight) for financial companies."""
        roa_5y = row.get('roa5y', None) or row.get('roa', 0)
        roa_score, asset_quality = self.score_bank_roa(roa_5y)
        
        working_capital = row.get('working_capital', 0)
        total_assets = row.get('total_assets', None)
        
        if pd.notna(working_capital) and pd.notna(total_assets) and total_assets > 0:
            wc_ratio = (working_capital / total_assets) * 100
            if wc_ratio >= 15:
                wc_score = 100
            elif wc_ratio >= 10:
                wc_score = 85 + (wc_ratio - 10) * 3
            elif wc_ratio >= 5:
                wc_score = 70 + (wc_ratio - 5) * 3
            elif wc_ratio >= 0:
                wc_score = 50 + (wc_ratio) * 4
            else:
                wc_score = max(30, 50 + wc_ratio * 2)
        else:
            wc_score = 70
        
        efficiency_score = (roa_score * 0.7) + (wc_score * 0.3)
        
        return {
            'component2_score': efficiency_score,
            'roa_5y': roa_5y if pd.notna(roa_5y) else 0,
            'asset_quality': asset_quality
        }
    
    def score_financial_current_ratio(self, current_ratio: float) -> float:
        """Score current ratio for financial companies."""
        if pd.isna(current_ratio) or current_ratio < 0:
            return 50
        
        if current_ratio >= 1.3:
            return 100
        elif current_ratio >= 1.2:
            return 90 + (current_ratio - 1.2) * 100
        elif current_ratio >= 1.1:
            return 75 + (current_ratio - 1.1) * 150
        elif current_ratio >= 1.0:
            return 60 + (current_ratio - 1.0) * 150
        elif current_ratio >= 0.9:
            return 40 + (current_ratio - 0.9) * 200
        else:
            return max(20, current_ratio * 40)
    
    def calculate_financial_component3(self, row: pd.Series) -> Dict:
        """Component 3: Liquidity (25% weight) for financial companies."""
        current_ratio = row.get('currentratio', None)
        cr_score = self.score_financial_current_ratio(current_ratio)
        
        liquidity_score = cr_score
        
        return {
            'component3_score': liquidity_score,
            'current_ratio': current_ratio if pd.notna(current_ratio) else 0
        }
    
    # ========== MAIN SCORING LOGIC ==========
    
    def calculate_dimension2(self, row: pd.Series) -> Dict:
        """Calculate complete Dimension 2: Financial Strength score."""
        symbol = row.get('symbol', 'UNKNOWN')
        
        try:
            sector = row.get('sector', '')
            industry = row.get('industry', '')
            industry_type = self.classify_industry(sector, industry)
            
            is_financial = (industry_type == 'financial')
            
            if is_financial:
                self.stats['financial_companies'] += 1
                
                comp1 = self.calculate_financial_component1(row)
                comp2 = self.calculate_financial_component2(row)
                comp3 = self.calculate_financial_component3(row)
                
                framework = "Financial (Basel III)"
                
            else:
                self.stats['industrial_companies'] += 1
                
                comp1 = self.calculate_industrial_component1(row)
                
                # Pass debt info to component 2 for fortress bonus
                comp2 = self.calculate_industrial_component2(
                    row, industry_type, 
                    comp1['debt_equity'], 
                    comp1['has_net_cash'],
                    row.get('interestcoverage', 0)
                )
                
                comp3 = self.calculate_industrial_component3(row)
                
                framework = "Industrial"
            
            component1_contribution = comp1['component1_score'] * 0.40
            component2_contribution = comp2['component2_score'] * 0.35
            component3_contribution = comp3['component3_score'] * 0.25
            
            final_score = component1_contribution + component2_contribution + component3_contribution
            final_score = max(0, min(final_score, 100))
            
            if final_score >= 90:
                quality_label = "Fortress"
                if final_score >= 90:
                    self.stats['fortress_balance_sheets'] += 1
            elif final_score >= 80:
                quality_label = "Strong"
            elif final_score >= 70:
                quality_label = "Good"
            elif final_score >= 60:
                quality_label = "Adequate"
            elif final_score >= 40:
                quality_label = "Moderate"
            else:
                quality_label = "Weak"
            
            result = {
                'symbol': symbol,
                'dimension_2_score': round(final_score, 1),
                'quality_label': quality_label,
                'framework_used': framework,
                'component1_score': round(comp1['component1_score'], 1),
                'component1_contribution': round(component1_contribution, 1),
                'component2_score': round(comp2['component2_score'], 1),
                'component2_contribution': round(component2_contribution, 1),
                'component3_score': round(comp3['component3_score'], 1),
                'component3_contribution': round(component3_contribution, 1),
                'industry_type': industry_type,
                'sector': sector,
                'industry': industry
            }
            
            result.update(comp1)
            result.update(comp2)
            result.update(comp3)
            
            return result
            
        except Exception as e:
            logger.error(f"Error scoring {symbol}: {e}")
            self.stats['missing_data_stocks'].append(symbol)
            return {
                'symbol': symbol,
                'dimension_2_score': 0,
                'quality_label': 'Data Insufficient',
                'error': str(e)
            }
    
    def score_all_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Score all stocks."""
        logger.info(f"Starting Dimension 2 scoring v1.1 for {len(df)} stocks...")
        logger.info("NEW: Fortress balance sheet bonus + Extended ROA scoring")
        
        self.stats['total_stocks'] = len(df)
        
        results = []
        for idx, row in df.iterrows():
            result = self.calculate_dimension2(row)
            results.append(result)
            
            if result['dimension_2_score'] > 0:
                self.stats['scored_stocks'] += 1
        
        results_df = pd.DataFrame(results)
        
        logger.info(f"Scoring complete!")
        logger.info(f"  Total: {self.stats['total_stocks']}")
        logger.info(f"  Scored: {self.stats['scored_stocks']}")
        logger.info(f"  Industrial: {self.stats['industrial_companies']}")
        logger.info(f"  Financial: {self.stats['financial_companies']}")
        logger.info(f"  Net cash: {self.stats['net_cash_companies']}")
        logger.info(f"  Fortress bonus applied: {self.stats['fortress_bonus_applied']}")
        logger.info(f"  Fortress balance sheets: {self.stats['fortress_balance_sheets']}")
        
        return results_df
    
    def generate_report(self, results_df: pd.DataFrame) -> str:
        """Generate scoring summary report."""
        report = []
        report.append("=" * 80)
        report.append("DIMENSION 2: FINANCIAL STRENGTH SCORING REPORT v1.1 (FORTRESS FIX)")
        report.append("Buffett's Balance Sheet Principles + Basel III Standards")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("IMPROVEMENTS IN v1.1:")
        report.append("  ✓ Fortress Balance Sheet Bonus (zero debt = liquidity not a concern)")
        report.append("  ✓ Extended ROA scoring (differentiates exceptional banks 5%+ from good 3%+)")
        report.append("=" * 80)
        report.append("")
        
        report.append("OVERALL STATISTICS:")
        report.append(f"  Total stocks: {len(results_df)}")
        report.append(f"  Successfully scored: {self.stats['scored_stocks']}")
        report.append(f"  Industrial companies: {self.stats['industrial_companies']}")
        report.append(f"  Financial companies: {self.stats['financial_companies']}")
        report.append(f"  Net cash positions: {self.stats['net_cash_companies']}")
        report.append(f"  Fortress bonus applied: {self.stats['fortress_bonus_applied']}")
        report.append(f"  Fortress balance sheets (90+): {self.stats['fortress_balance_sheets']}")
        report.append("")
        
        valid_scores = results_df[results_df['dimension_2_score'] > 0]['dimension_2_score']
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
        
        report.append("TOP 10 FINANCIAL STRENGTH SCORES:")
        top10 = results_df.nlargest(10, 'dimension_2_score')[
            ['symbol', 'dimension_2_score', 'quality_label', 'framework_used']
        ]
        for idx, row in top10.iterrows():
            report.append(f"  {row['symbol']:10} Score: {row['dimension_2_score']:5.1f}  "
                        f"({row['quality_label']:10}) [{row['framework_used']}]")
        report.append("")
        
        report.append("VALIDATION STOCKS:")
        report.append("  v1.0: CTC=80✗ (too low), LOLC=95✓, LOFC=95✗ (same as LOLC)")
        report.append("  v1.1 Expected: CTC=90+✓, LOLC=95✓, LOFC=97+✓ (better than LOLC)")
        report.append("")
        
        validation_symbols = ['CTC.N0000', 'JKH.N0000', 'LOLC.N0000', 'LOFC.N0000', 
                            'LION.N0000', 'ABAN.N0000', 'DOCK.N0000']
        validation_stocks = results_df[results_df['symbol'].isin(validation_symbols)]
        
        if len(validation_stocks) > 0:
            for idx, row in validation_stocks.sort_values('dimension_2_score', ascending=False).iterrows():
                report.append(f"  {row['symbol']:10} Score: {row['dimension_2_score']:5.1f}  "
                            f"({row['quality_label']:10}) [{row['framework_used']}]")
            report.append("")
        
        report.append("=" * 80)
        report.append("END OF REPORT")
        report.append("=" * 80)
        
        return "\n".join(report)


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description='Dimension 2 Financial Strength Scorer v1.1 (Fortress Fix)'
    )
    parser.add_argument('--input', default='cleaned_data.csv')
    parser.add_argument('--output', default='dimension2_scores_v1.1.csv')
    parser.add_argument('--report', default='dimension2_report_v1.1.txt')
    
    args = parser.parse_args()
    
    logger.info(f"Loading data from {args.input}...")
    try:
        df = pd.read_csv(args.input)
        logger.info(f"Loaded {len(df)} stocks with {len(df.columns)} columns")
    except FileNotFoundError:
        logger.error(f"Input file not found: {args.input}")
        return
    
    scorer = Dimension2ScorerV11()
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
    logger.info("DIMENSION 2 SCORING v1.1 COMPLETE!")
    logger.info(f"Scores: {args.output}")
    logger.info(f"Report: {args.report}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()