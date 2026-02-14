
#!/usr/bin/env python3
"""
MANIPULATION DETECTOR v5.0 - PRODUCTION CODE (CORRECTED)
===========================================================
Working Directory: /opt/investment-os
Date: January 31, 2026

INTEGRATION: Uses common library (Phase 2 migration)
DATA SOURCE: cse_daily_prices table (primary) + daily_prices (backup)

Migration: Phase 2 (Feb 2026)
- Replaced: dotenv/load_dotenv → removed (common.config handles .env)
- Replaced: cse_data_loader.get_supabase_client → common.database.get_supabase_client
- Replaced: cse_data_loader.load_stock_data → common.data_loader.load_stock_data
- Replaced: manual logging.basicConfig → common.logging_config.setup_logging
- Replaced: send_v5_email.py → common.email_sender.EmailSender
- Original: /opt/selenium_automation/manipulation_detector_v5_0.py
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional
import json
import os
import sys
from dataclasses import dataclass, asdict
import warnings
warnings.filterwarnings('ignore')

# === Investment OS Common Library (Phase 2 Migration) ===
from common.database import get_supabase_client
from common.data_loader import load_stock_data
from common.logging_config import setup_logging

# Initialize via common library (replaces manual basicConfig + dotenv)
logger = setup_logging('manipulation-detector', log_to_file=True)


# ============================================================================
# EMPIRE STOCKS & CONFIGURATION
# ============================================================================

# Empire stocks (from Week 7 research)
EMPIRE_STOCKS = {
    'empire_1_dhammika': [
        'SAMP.N0000', 'PABC.N0000', 'BLUE.N0000', 'YORK.N0000', 'KAPI.N0000',
        'PALM.N0000', 'VONE.N0000', 'DIPD.N0000', 'SINS.N0000', 'LGL.N0000',
        'LWL.N0000', 'LLUB.N0000', 'SLND.N0000', 'LFIN.N0000', 'TILE.N0000',
        'SFCL.N0000', 'LION.N0000', 'AGST.N0000', 'HAYL.N0000', 'CFVF.N0000',
        'KVAL.N0000', 'COLO.N0000', 'ODEL.N0000'
    ],
    'empire_2_lolc_browns': [
        'LOLC.N0000', 'LOFC.N0000', 'COMB.N0000', 'BFL.N0000', 'SEMB.N0000',
        'TYRE.N0000', 'PACK.N0000', 'CIFL.N0000', 'HNBF.N0000', 'COMD.N0000',
        'BPPL.N0000', 'CLPL.N0000', 'CZER.N0000', 'SELF.N0000', 'BRWN.N0000'
    ],
    'empire_3_jayawardena': [
        'AHPL.N0000', 'ASPH.N0000', 'GREG.N0000', 'TAFL.N0000', 'AEL.N0000',
        'COCO.N0000', 'EML.N0000', 'PLC.N0000', 'SLTL.N0000', 'SERV.N0000',
        'HDFC.N0000', 'CRES.N0000'
    ]
}

ALL_EMPIRE_STOCKS = (EMPIRE_STOCKS['empire_1_dhammika'] +
                     EMPIRE_STOCKS['empire_2_lolc_browns'] +
                     EMPIRE_STOCKS['empire_3_jayawardena'])

# Ultra-clean stocks (for testing)
ULTRA_CLEAN_STOCKS = ['CTHR.N0000', 'RCH.N0000', 'GHLL.N0000', 'NEH.N0000', 'WIND.N0000']


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class PatternSignal:
    """Individual pattern detection result"""
    pattern_name: str
    detected: bool
    confidence: float
    expected_return: float
    win_rate: float
    time_horizon: str
    action: str
    entry_price: float
    stop_loss: float
    target_price: float
    explanation: str
    metadata: Dict

    def to_dict(self):
        return asdict(self)


@dataclass
class Opportunity:
    """Combined opportunity from multiple patterns"""
    stock_symbol: str
    timestamp: datetime
    patterns: List[PatternSignal]
    best_pattern: PatternSignal
    total_score: float
    expected_value: float
    position_size: float
    priority: str
    is_empire_stock: bool

    def to_dict(self):
        return {
            'stock_symbol': self.stock_symbol,
            'timestamp': self.timestamp.isoformat(),
            'patterns': [p.to_dict() for p in self.patterns],
            'best_pattern': self.best_pattern.to_dict(),
            'total_score': self.total_score,
            'expected_value': self.expected_value,
            'position_size': self.position_size,
            'priority': self.priority,
            'is_empire_stock': self.is_empire_stock
        }


# ============================================================================
# DATA LOADING (Get all available stocks)
# ============================================================================

def get_all_stocks_to_scan() -> List[str]:
    """
    Get list of stocks to scan

    Priority:
    1. Try to get from Supabase cse_daily_prices table
    2. Fallback to daily_prices (backup table)
    3. Fallback to known stocks (ultra-clean + empire)
    """
    try:
        supabase = get_supabase_client()

        # Try cse_daily_prices first - use raw SQL query for DISTINCT
        try:
            # Use rpc or distinct query
            response = supabase.rpc('get_distinct_symbols_cse').execute()
            if response.data and len(response.data) > 10:
                symbols = [row['symbol'] for row in response.data]
                logger.info(f"Found {len(symbols)} stocks in cse_daily_prices")
                return symbols
        except:
            pass

        # Fallback: Try daily_prices table (has 296 stocks)
        try:
            response = supabase.table('daily_prices')\
                .select('symbol')\
                .limit(5000)\
                .execute()

            if response.data:
                symbols = list(set([row['symbol'] for row in response.data]))
                logger.info(f"Found {len(symbols)} stocks in daily_prices (backup table)")
                return symbols
        except Exception as e:
            pass

    except Exception as e:
        pass

    # Final fallback: Known stocks
    known_stocks = list(set(ULTRA_CLEAN_STOCKS + ALL_EMPIRE_STOCKS))
    logger.warning(f"Using known stocks list ({len(known_stocks)} stocks)")
    return known_stocks


# ============================================================================
# MODULE 1: ACCUMULATION DETECTOR
# ============================================================================

class AccumulationDetector:
    """Detect empire accumulation using Volume-Price divergence"""

    def __init__(self):
        self.name = "accumulation"

    def detect(self, symbol: str, df: pd.DataFrame) -> PatternSignal:
        """
        Detect accumulation pattern

        Args:
            symbol: Stock symbol
            df: DataFrame with columns [date, close, volume, open, high, low]
                (from common.data_loader.load_stock_data)
        """
        try:
            if len(df) < 252:
                return self._no_signal("Need 252 days minimum")

            # Calculate metrics
            df = df.copy()
            df['returns'] = df['close'].pct_change()
            df['volume_ma_long'] = df['volume'].rolling(252).mean()
            df['volume_ma_short'] = df['volume'].rolling(60).mean()
            df['price_ma'] = df['close'].rolling(60).mean()
            df['volume_surge'] = df['volume'] / df['volume_ma_long']
            df['price_momentum'] = (df['close'] - df['price_ma']) / df['price_ma']

            # Recent window (last 60 days)
            recent = df.tail(60)
            vol_surge = recent['volume_surge'].mean()
            price_mom = recent['price_momentum'].mean()
            corr = recent['volume_surge'].corr(recent['price_momentum'])

            # ACCUMULATION: High volume + suppressed price + decoupled
            if vol_surge > 1.8 and price_mom < 0.05 and corr < 0.3:
                price = df['close'].iloc[-1]
                return PatternSignal(
                    pattern_name='accumulation',
                    detected=True,
                    confidence=0.75,
                    expected_return=0.80,
                    win_rate=0.65,
                    time_horizon='6_months',
                    action='BUY',
                    entry_price=price,
                    stop_loss=price * 0.95,
                    target_price=price * 1.50,
                    explanation=f'ACCUMULATION! Volume {vol_surge:.1f}x normal but price suppressed ({price_mom:+.1%}). Empire likely accumulating - BUY before pump!',
                    metadata={
                        'stage': 'ACCUMULATION',
                        'volume_surge': float(vol_surge),
                        'price_momentum': float(price_mom),
                        'correlation': float(corr)
                    }
                )

            # PUMP: High volume + explosive price
            elif vol_surge > 2.0 and price_mom > 0.15 and corr > 0.5:
                price = df['close'].iloc[-1]
                return PatternSignal(
                    pattern_name='accumulation',
                    detected=True,
                    confidence=0.60,
                    expected_return=0.25,
                    win_rate=0.55,
                    time_horizon='1_month',
                    action='BUY',
                    entry_price=price,
                    stop_loss=price * 0.97,
                    target_price=price * 1.20,
                    explanation=f'PUMP PHASE active. Volume {vol_surge:.1f}x, price +{price_mom:.1%}. Late entry - small position + tight stop.',
                    metadata={
                        'stage': 'PUMP',
                        'volume_surge': float(vol_surge),
                        'price_momentum': float(price_mom)
                    }
                )

            return self._no_signal("No pattern detected")

        except Exception as e:
            return self._no_signal(f"Error: {str(e)}")

    def _no_signal(self, reason: str) -> PatternSignal:
        return PatternSignal(
            pattern_name='accumulation',
            detected=False,
            confidence=0.0,
            expected_return=0.0,
            win_rate=0.0,
            time_horizon='6_months',
            action='PASS',
            entry_price=0.0,
            stop_loss=0.0,
            target_price=0.0,
            explanation=reason,
            metadata={}
        )


# ============================================================================
# MODULE 2: STAIRSTEP DETECTOR
# ============================================================================

class StairstepDetector:
    """Detect repeated support level bounces"""

    def __init__(self):
        self.name = "stairstep"
        self.tolerance = 0.02

    def detect(self, symbol: str, df: pd.DataFrame) -> PatternSignal:
        """
        Detect stairstep accumulation

        Args:
            symbol: Stock symbol
            df: DataFrame with columns [date, close, volume, open, high, low]
        """
        try:
            if len(df) < 180:
                return self._no_signal("Need 180 days minimum")

            recent = df.tail(180).copy()
            support = self._find_support(recent)

            if support is None:
                return self._no_signal("No support level found")

            bounces = self._count_bounces(recent, support)

            if len(bounces) < 3:
                return self._no_signal(f"Only {len(bounces)} bounces (need 3+)")

            price = df['close'].iloc[-1]

            # CRITICAL FIX: Price must be NEAR support (within +/-5%)
            # Not way below it!
            dist_from_support = abs(price - support) / support

            # Price should be between 95% and 105% of support
            if price < support * 0.95:
                return self._no_signal(f"Price Rs {price:.2f} too far BELOW support Rs {support:.2f} ({((price/support - 1)*100):.1f}%)")

            if price > support * 1.05:
                return self._no_signal(f"Price Rs {price:.2f} too far ABOVE support Rs {support:.2f} ({((price/support - 1)*100):.1f}%)")

            # Price is near support - calculate realistic target
            resistance = max([b['high'] for b in bounces])

            # Sanity check: Resistance must be above current price
            if resistance <= price:
                return self._no_signal(f"Resistance Rs {resistance:.2f} not above price Rs {price:.2f}")

            # Calculate expected return
            target = resistance * 1.05
            expected_return = (target - price) / price

            # Sanity check: Expected return should be reasonable (5% to 200%)
            if expected_return < 0.05:
                return self._no_signal(f"Expected return too low: {expected_return:.1%}")

            if expected_return > 2.0:
                return self._no_signal(f"Expected return unrealistic: {expected_return:.1%} (likely data error)")

            # All checks passed - valid stairstep pattern
            confidence = min(0.70 + (len(bounces) - 3) * 0.05, 0.90)

            return PatternSignal(
                pattern_name='stairstep',
                detected=True,
                confidence=confidence,
                expected_return=expected_return,
                win_rate=0.75,
                time_horizon='3_months',
                action='BUY',
                entry_price=price,
                stop_loss=support * 0.97,
                target_price=target,
                explanation=f'STAIRSTEP! Support Rs {support:.2f} tested {len(bounces)}x. Price Rs {price:.2f} near support ({dist_from_support:.1%} away). Empire defending level - BUY!',
                metadata={
                    'support_level': float(support),
                    'resistance_level': float(resistance),
                    'num_bounces': len(bounces),
                    'distance_from_support': float(dist_from_support)
                }
            )

        except Exception as e:
            return self._no_signal(f"Error: {str(e)}")

    def _find_support(self, df: pd.DataFrame) -> Optional[float]:
        """Find most common low price (support level)"""
        # Use recent lows only (last 90 days) to avoid stale support
        recent_lows = df.tail(90)

        bin_size = recent_lows['low'].mean() * 0.02  # 2% bins
        recent_lows['low_bin'] = (recent_lows['low'] // bin_size) * bin_size

        bin_counts = recent_lows['low_bin'].value_counts()

        if len(bin_counts) == 0 or bin_counts.iloc[0] < 3:
            return None

        return float(bin_counts.index[0])

    def _count_bounces(self, df: pd.DataFrame, support: float) -> List[Dict]:
        """Count bounces off support"""
        bounces = []
        tolerance = support * self.tolerance

        for i in range(len(df)):
            if abs(df['low'].iloc[i] - support) < tolerance:
                # Check if price bounced (closed above low)
                if df['close'].iloc[i] > df['low'].iloc[i] * 1.01:
                    # Look ahead 5-10 days for high after bounce
                    future_window = df.iloc[i:min(i+10, len(df))]
                    high_after = future_window['high'].max()

                    # Only count as bounce if price went up after touching support
                    if high_after > df['low'].iloc[i] * 1.02:
                        bounces.append({
                            'date': df['date'].iloc[i],
                            'low': float(df['low'].iloc[i]),
                            'high': float(high_after)
                        })

        return bounces

    def _no_signal(self, reason: str) -> PatternSignal:
        return PatternSignal(
            pattern_name='stairstep',
            detected=False,
            confidence=0.0,
            expected_return=0.0,
            win_rate=0.0,
            time_horizon='3_months',
            action='PASS',
            entry_price=0.0,
            stop_loss=0.0,
            target_price=0.0,
            explanation=reason,
            metadata={}
        )


# ============================================================================
# MASTER ORCHESTRATOR
# ============================================================================

class ManipulationDetector_v5_0:
    """Master detector - orchestrates all pattern detectors"""

    def __init__(self):
        self.detectors = {
            'accumulation': AccumulationDetector(),
            'stairstep': StairstepDetector()
        }
        self.supabase = get_supabase_client()

    def scan_stock(self, symbol: str, days: int = 300) -> Optional[Opportunity]:
        """
        Scan single stock for manipulation patterns

        Uses common.data_loader infrastructure
        """
        try:
            # Load data using common library
            # Returns DataFrame with: date, close, volume, open, high, low
            df = load_stock_data(symbol, days=days, supabase=self.supabase)

            if df is None or len(df) < 180:
                return None

            signals = []

            # Run all detectors
            for detector in self.detectors.values():
                sig = detector.detect(symbol, df)
                if sig.detected:
                    signals.append(sig)

            if not signals:
                return None

            # Score and prioritize
            opportunity = self._score_opportunity(symbol, signals)
            return opportunity

        except Exception as e:
            # Suppress errors - they're usually just data issues
            return None

    def scan_universe(self, stocks: List[str] = None, days: int = 300) -> List[Opportunity]:
        """Scan all stocks for manipulation patterns"""
        if stocks is None:
            stocks = get_all_stocks_to_scan()

        logger.info(f"Scanning {len(stocks)} stocks for manipulation patterns")
        print(f"\nScanning {len(stocks)} stocks for manipulation patterns...")
        print(f"{'='*80}\n")

        opportunities = []

        for i, stock in enumerate(stocks, 1):
            # Progress update every 50 stocks
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(stocks)} stocks scanned ({len(opportunities)} patterns found)")
                print(f"Progress: {i}/{len(stocks)} stocks scanned... ({len(opportunities)} patterns found)")

            opp = self.scan_stock(stock, days=days)
            if opp:
                opportunities.append(opp)
                # Only print when pattern found
                empire_tag = " [EMPIRE]" if opp.is_empire_stock else ""
                print(f"  {stock}{empire_tag}: {opp.best_pattern.pattern_name.upper()} "
                      f"(Return: {opp.best_pattern.expected_return:+.0%}, Conf: {opp.best_pattern.confidence:.0%})")

        # Sort by expected value
        opportunities.sort(key=lambda x: x.expected_value, reverse=True)

        logger.info(f"Scan complete. Found {len(opportunities)} opportunities")
        print(f"\n{'='*80}")
        print(f"Scan complete! Found {len(opportunities)} opportunities.\n")

        return opportunities

    def _score_opportunity(self, symbol: str, signals: List[PatternSignal]) -> Opportunity:
        """Score and prioritize opportunity"""
        scores = []

        for signal in signals:
            # Expected Value = Return x Win Rate x Confidence
            ev = signal.expected_return * signal.win_rate * signal.confidence
            scores.append({'signal': signal, 'score': ev, 'ev': ev})

        # Best pattern
        best = max(scores, key=lambda x: x['score'])
        best_signal = best['signal']

        total_score = sum(s['score'] for s in scores)
        expected_value = best['ev']

        # Position size
        base_sizes = {
            'accumulation': 0.08,
            'stairstep': 0.10
        }

        size = base_sizes.get(best_signal.pattern_name, 0.05)

        # Adjust for confidence
        if best_signal.confidence > 0.8:
            size *= 1.2

        # Adjust for multiple patterns
        if len(signals) > 1:
            size *= 1.15

        # Caps
        size = min(size, 0.12)
        size = max(size, 0.02)

        # CORRECTED PRIORITY LOGIC:
        # High priority: Expected return > 40% OR Expected Value > 0.5
        # Medium: Expected return 15-40% OR EV 0.25-0.5
        # Low: Everything else

        if best_signal.expected_return > 0.40 or expected_value > 0.5:
            priority = 'HIGH'
        elif best_signal.expected_return > 0.15 or expected_value > 0.25:
            priority = 'MEDIUM'
        else:
            priority = 'LOW'

        # Check if empire stock
        is_empire = symbol in ALL_EMPIRE_STOCKS

        return Opportunity(
            stock_symbol=symbol,
            timestamp=datetime.now(),
            patterns=[s['signal'] for s in scores],
            best_pattern=best_signal,
            total_score=total_score,
            expected_value=expected_value,
            position_size=size,
            priority=priority,
            is_empire_stock=is_empire
        )

    def generate_report(self, opportunities: List[Opportunity]) -> str:
        """Generate text report"""
        lines = []
        lines.append("=" * 80)
        lines.append("MANIPULATION DETECTOR v5.0 - DAILY REPORT")
        lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 80)
        lines.append("")

        if not opportunities:
            lines.append("No manipulation patterns detected today.")
            return "\n".join(lines)

        # Group by priority
        high = [o for o in opportunities if o.priority == 'HIGH']
        medium = [o for o in opportunities if o.priority == 'MEDIUM']
        low = [o for o in opportunities if o.priority == 'LOW']

        # High priority
        if high:
            lines.append(f"HIGH PRIORITY OPPORTUNITIES ({len(high)}):")
            lines.append("-" * 80)
            for i, opp in enumerate(high, 1):
                empire_tag = " [EMPIRE]" if opp.is_empire_stock else ""

                lines.append(f"\n{i}. {opp.stock_symbol}{empire_tag} - "
                           f"{opp.best_pattern.pattern_name.upper()}")
                lines.append(f"   Confidence: {opp.best_pattern.confidence:.0%}")
                lines.append(f"   Expected Return: {opp.best_pattern.expected_return:+.0%}")
                lines.append(f"   Win Rate: {opp.best_pattern.win_rate:.0%}")
                lines.append(f"   Action: {opp.best_pattern.action} at "
                           f"Rs {opp.best_pattern.entry_price:.2f}")
                lines.append(f"   Position Size: {opp.position_size:.1%}")
                lines.append(f"   Stop Loss: Rs {opp.best_pattern.stop_loss:.2f} "
                           f"({((opp.best_pattern.stop_loss / opp.best_pattern.entry_price - 1) * 100):.1f}%)")
                lines.append(f"   Target: Rs {opp.best_pattern.target_price:.2f} "
                           f"({((opp.best_pattern.target_price / opp.best_pattern.entry_price - 1) * 100):.1f}%)")
                lines.append(f"   Time Horizon: {opp.best_pattern.time_horizon}")
                lines.append(f"   Explanation: {opp.best_pattern.explanation}")

                if len(opp.patterns) > 1:
                    other = [p.pattern_name for p in opp.patterns if p != opp.best_pattern]
                    lines.append(f"   Additional Patterns: {', '.join(other)}")

        # Medium priority
        if medium:
            lines.append(f"\n\nMEDIUM PRIORITY ({len(medium)}):")
            lines.append("-" * 80)
            for opp in medium:
                empire_tag = " [EMPIRE]" if opp.is_empire_stock else ""
                lines.append(f"- {opp.stock_symbol}{empire_tag}: "
                           f"{opp.best_pattern.pattern_name} "
                           f"(Conf: {opp.best_pattern.confidence:.0%}, "
                           f"Return: {opp.best_pattern.expected_return:+.0%})")

        # Summary
        lines.append("\n" + "=" * 80)
        lines.append(f"Total Opportunities: {len(opportunities)}")
        lines.append(f"Empire Stocks: {len([o for o in opportunities if o.is_empire_stock])}")
        lines.append(f"Expected Portfolio Alpha: "
                    f"{sum(o.expected_value * o.position_size for o in opportunities):.1%}")
        lines.append("=" * 80)

        return "\n".join(lines)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(description='v5.0 Manipulation Detector')
    parser.add_argument('--scan', action='store_true', help='Scan all stocks')
    parser.add_argument('--stock', type=str, help='Scan single stock')
    parser.add_argument('--output', type=str,
                       default=None,
                       help='Output file')

    args = parser.parse_args()

    # Generate default filenames with timestamp if not provided
    if args.output is None:
        timestamp = datetime.now().strftime('%Y-%m-%d')
        report_dir = '/opt/investment-os/v5_reports'
        os.makedirs(report_dir, exist_ok=True)
        args.output = f'{report_dir}/v5_report_{timestamp}.txt'

    logger.info("Manipulation Detector v5.0 starting")

    detector = ManipulationDetector_v5_0()

    if args.stock:
        logger.info(f"Scanning {args.stock}")
        opp = detector.scan_stock(args.stock)

        if opp:
            report = detector.generate_report([opp])
            print(report)

            if args.output:
                os.makedirs(os.path.dirname(args.output), exist_ok=True)
                with open(args.output, 'w') as f:
                    f.write(report)
                logger.info(f"Report saved: {args.output}")
        else:
            print("No patterns detected")

    elif args.scan:
        opportunities = detector.scan_universe()
        report = detector.generate_report(opportunities)

        print(report)

        if args.output:
            os.makedirs(os.path.dirname(args.output), exist_ok=True)
            with open(args.output, 'w') as f:
                f.write(report)
            logger.info(f"Report saved: {args.output}")

        # Save JSON
        json_file = args.output.replace('.txt', '.json')
        with open(json_file, 'w') as f:
            json.dump([o.to_dict() for o in opportunities], f, indent=2, default=str)
        logger.info(f"JSON saved: {json_file}")

        return opportunities

    else:
        parser.print_help()
        return None


if __name__ == '__main__':
    main()