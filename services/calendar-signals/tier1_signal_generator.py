
#!/usr/bin/env python3
"""
INVESTMENT OS - TIER 1 SIGNAL GENERATOR (PRODUCTION)
====================================================
Generates daily trading signals for 5 ultra-clean CSE stocks
Based on Granger causality analysis (16 years historical validation)

Stocks: CTHR, RCH, GHLL, NEH, WIND
Signals: Volume, Volatility, 5D Momentum, 20D Momentum
Contamination: 0% (triple-verified against manipulation empires)

Author: Investment OS Development Team
Date: January 26, 2026
Version: 1.0 (Production)

Migration: Phase 2 (Feb 2026)
- NO CHANGES: Pure computation module, zero database imports
- Receives data as parameter (dependency injection pattern)
- Original: /opt/selenium_automation/tier1_signal_generator.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import sys
from typing import Dict, List, Tuple

# ============================================================================
# SIGNAL CONFIGURATION (From Granger Causality Results)
# ============================================================================

SIGNAL_CONFIG = {
    'CTHR.N0000': {
        'volume': {
            'optimal_lag': 10,
            'p_value': 1.551e-06,
            'correlation': 0.129,
            'direction': 'BUY',
            'confidence': 99.84,
            'enabled': True
        },
        'volatility': {
            'optimal_lag': 2,
            'p_value': 2.411e-10,
            'correlation': -0.016,
            'direction': 'SELL',  # Negative correlation
            'confidence': 99.99,
            'enabled': True
        },
        'momentum_5d': {
            'optimal_lag': 12,
            'p_value': 3.910e-10,
            'correlation': -0.007,
            'direction': 'SELL',  # Reversal
            'confidence': 99.99,
            'enabled': True
        },
        'momentum_20d': {
            'optimal_lag': 25,
            'p_value': 0.000195,
            'correlation': 0.003,
            'direction': 'BUY',
            'confidence': 98.05,
            'enabled': True
        }
    },

    'RCH.N0000': {
        'volume': {
            'optimal_lag': 29,
            'p_value': 0.000812,
            'correlation': 0.114,
            'direction': 'BUY',
            'confidence': 91.88,
            'enabled': True
        },
        'volatility': {
            'optimal_lag': 11,
            'p_value': 0.038,
            'correlation': -0.003,
            'direction': 'SELL',
            'confidence': 62.03,
            'enabled': True
        },
        'momentum_5d': {
            'optimal_lag': 4,
            'p_value': 4.294e-05,
            'correlation': 0.010,
            'direction': 'BUY',
            'confidence': 95.71,
            'enabled': True
        },
        'momentum_20d': {
            'optimal_lag': 7,
            'p_value': 0.0435,
            'correlation': 0.053,
            'direction': 'BUY',
            'confidence': 56.46,
            'enabled': True
        }
    },

    'GHLL.N0000': {
        'volume': {
            'optimal_lag': 29,
            'p_value': 0.0053,
            'correlation': 0.093,
            'direction': 'BUY',
            'confidence': 46.71,
            'enabled': True
        },
        'volatility': {
            'optimal_lag': 6,
            'p_value': 0.00104,
            'correlation': 0.019,
            'direction': 'BUY',
            'confidence': 89.56,
            'enabled': True
        },
        'momentum_5d': {
            'optimal_lag': 30,
            'p_value': 1.287e-08,
            'correlation': -0.020,
            'direction': 'SELL',  # Reversal
            'confidence': 99.99,
            'enabled': True
        },
        'momentum_20d': {
            'optimal_lag': 26,
            'p_value': 1.749e-09,
            'correlation': -0.008,
            'direction': 'SELL',  # Reversal
            'confidence': 99.99,
            'enabled': True
        }
    },

    'NEH.N0000': {
        'volume': {
            'optimal_lag': 3,
            'p_value': 0.000465,
            'correlation': 0.091,
            'direction': 'BUY',
            'confidence': 95.35,
            'enabled': True
        },
        'volatility': {
            'optimal_lag': 6,
            'p_value': 0.0653,
            'correlation': -0.036,
            'direction': 'SELL',
            'confidence': 34.71,
            'enabled': False  # Below 50% confidence threshold
        },
        'momentum_5d': {
            'optimal_lag': 7,
            'p_value': 0.000120,
            'correlation': -0.033,
            'direction': 'SELL',  # Reversal
            'confidence': 98.80,
            'enabled': True
        },
        'momentum_20d': {
            'optimal_lag': 28,
            'p_value': 0.00134,
            'correlation': -0.003,
            'direction': 'SELL',
            'confidence': 86.65,
            'enabled': True
        }
    },

    'WIND.N0000': {
        'volume': {
            'optimal_lag': 2,
            'p_value': 0.646,
            'correlation': 0.032,
            'direction': 'BUY',
            'confidence': -64.60,  # Not significant
            'enabled': False
        },
        'volatility': {
            'optimal_lag': 10,
            'p_value': 0.000273,
            'correlation': 0.091,
            'direction': 'BUY',
            'confidence': 97.27,
            'enabled': True
        },
        'momentum_5d': {
            'optimal_lag': 28,
            'p_value': 2.218e-12,
            'correlation': 0.011,
            'direction': 'BUY',
            'confidence': 99.99,
            'enabled': True
        },
        'momentum_20d': {
            'optimal_lag': 30,
            'p_value': 7.567e-06,
            'correlation': -0.024,
            'direction': 'SELL',  # Reversal
            'confidence': 99.24,
            'enabled': True
        }
    }
}

# ============================================================================
# SIGNAL THRESHOLDS (Tunable)
# ============================================================================

THRESHOLDS = {
    'volume_spike': 2.0,  # 2x average volume = spike
    'volatility_high': 90,  # 90th percentile
    'momentum_5d_threshold': 0.02,  # +2% for continuation
    'momentum_20d_threshold': 0.05,  # +5% for continuation
    'min_confidence': 50,  # Minimum confidence to generate signal
    'composite_threshold': 2  # Minimum 2 signals must align for trade
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_indicators(df: pd.DataFrame, stock: str) -> Dict:
    """Calculate current market indicators for signal generation"""

    # Get latest data point
    latest = df.iloc[-1]

    # Calculate volume change (vs 20-day average)
    avg_volume_20d = df['volume'].tail(20).mean()
    volume_change = latest['volume'] / avg_volume_20d if avg_volume_20d > 0 else 1.0

    # Calculate 20-day volatility
    returns = df['close'].pct_change()
    volatility_20d = returns.tail(20).std() * np.sqrt(252) * 100  # Annualized %

    # Calculate 5-day and 20-day momentum
    momentum_5d = (latest['close'] / df['close'].iloc[-6] - 1) * 100 if len(df) > 5 else 0
    momentum_20d = (latest['close'] / df['close'].iloc[-21] - 1) * 100 if len(df) > 20 else 0

    # Convert date to string for JSON serialization
    date_str = latest['date'].strftime('%Y-%m-%d') if 'date' in df.columns and hasattr(latest['date'], 'strftime') else datetime.now().strftime('%Y-%m-%d')

    return {
        'price': float(latest['close']),
        'volume': float(latest['volume']),
        'volume_change': float(volume_change),
        'volatility_20d': float(volatility_20d),
        'momentum_5d': float(momentum_5d),
        'momentum_20d': float(momentum_20d),
        'date': date_str
    }

def check_signal_trigger(stock: str, indicators: Dict, signal_type: str) -> Tuple[bool, str, float]:
    """Check if a specific signal type triggers for a stock"""

    config = SIGNAL_CONFIG[stock][signal_type]

    # Skip if not enabled or confidence too low
    if not config['enabled'] or config['confidence'] < THRESHOLDS['min_confidence']:
        return False, 'HOLD', 0.0

    triggered = False
    expected_return = abs(config['correlation']) * 100  # Convert to %

    if signal_type == 'volume':
        if indicators['volume_change'] > THRESHOLDS['volume_spike']:
            triggered = True

    elif signal_type == 'volatility':
        # High volatility signal (mean reversion or risk premium)
        if indicators['volatility_20d'] > THRESHOLDS['volatility_high']:
            triggered = True

    elif signal_type == 'momentum_5d':
        if config['direction'] == 'BUY' and indicators['momentum_5d'] > THRESHOLDS['momentum_5d_threshold']:
            triggered = True
        elif config['direction'] == 'SELL' and indicators['momentum_5d'] > THRESHOLDS['momentum_5d_threshold']:
            triggered = True  # Reversal signal

    elif signal_type == 'momentum_20d':
        if config['direction'] == 'BUY' and indicators['momentum_20d'] > THRESHOLDS['momentum_20d_threshold']:
            triggered = True
        elif config['direction'] == 'SELL' and indicators['momentum_20d'] > THRESHOLDS['momentum_20d_threshold']:
            triggered = True  # Reversal signal

    if triggered:
        return True, config['direction'], expected_return
    else:
        return False, 'HOLD', 0.0

def generate_composite_signal(stock: str, indicators: Dict) -> Dict:
    """Generate composite trading signal from all indicators"""

    signals = []

    # Check each signal type
    for signal_type in ['volume', 'volatility', 'momentum_5d', 'momentum_20d']:
        triggered, direction, expected_return = check_signal_trigger(stock, indicators, signal_type)

        if triggered:
            signals.append({
                'type': signal_type,
                'direction': direction,
                'expected_return': expected_return,
                'confidence': SIGNAL_CONFIG[stock][signal_type]['confidence'],
                'lag': SIGNAL_CONFIG[stock][signal_type]['optimal_lag']
            })

    # Composite decision logic
    if len(signals) == 0:
        composite_direction = 'HOLD'
        composite_confidence = 0
        composite_return = 0

    elif len(signals) >= THRESHOLDS['composite_threshold']:
        # Multiple signals - check alignment
        buy_signals = [s for s in signals if s['direction'] == 'BUY']
        sell_signals = [s for s in signals if s['direction'] == 'SELL']

        if len(buy_signals) > len(sell_signals):
            composite_direction = 'BUY'
            composite_confidence = np.mean([s['confidence'] for s in buy_signals])
            composite_return = np.mean([s['expected_return'] for s in buy_signals])
        elif len(sell_signals) > len(buy_signals):
            composite_direction = 'SELL'
            composite_confidence = np.mean([s['confidence'] for s in sell_signals])
            composite_return = np.mean([s['expected_return'] for s in sell_signals])
        else:
            composite_direction = 'HOLD'  # Conflicting signals
            composite_confidence = 50
            composite_return = 0
    else:
        # Single signal - use it but with lower confidence
        composite_direction = signals[0]['direction']
        composite_confidence = signals[0]['confidence'] * 0.7  # Reduce confidence for single signal
        composite_return = signals[0]['expected_return']

    return {
        'stock': stock,
        'date': indicators['date'],
        'price': indicators['price'],
        'signal': composite_direction,
        'confidence': round(composite_confidence, 2),
        'expected_return': round(composite_return, 2),
        'hold_period_days': max([s['lag'] for s in signals]) if signals else 0,
        'num_signals': len(signals),
        'active_signals': signals,
        'indicators': indicators
    }

# ============================================================================
# MAIN SIGNAL GENERATION
# ============================================================================

def generate_daily_signals(data_dict: Dict[str, pd.DataFrame]) -> List[Dict]:
    """Generate signals for all 5 stocks"""

    all_signals = []

    for stock in ['CTHR.N0000', 'RCH.N0000', 'GHLL.N0000', 'NEH.N0000', 'WIND.N0000']:
        if stock not in data_dict:
            print(f"WARNING: No data for {stock}, skipping...")
            continue

        # Calculate indicators
        indicators = calculate_indicators(data_dict[stock], stock)

        # Generate composite signal
        signal = generate_composite_signal(stock, indicators)

        all_signals.append(signal)

    return all_signals

def print_signal_report(signals: List[Dict]):
    """Print human-readable signal report"""

    print("\n" + "="*80)
    print("INVESTMENT OS - TIER 1 DAILY SIGNALS")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Stocks Analyzed: 5 (Ultra-Clean, 0% Manipulation)")
    print("="*80 + "\n")

    for signal in signals:
        print(f"Stock: {signal['stock']}")
        print(f"  Price: Rs {signal['price']:.2f}")
        print(f"  Signal: {signal['signal']} (Confidence: {signal['confidence']:.1f}%)")

        if signal['signal'] != 'HOLD':
            print(f"  Expected Return: {signal['expected_return']:.2f}%")
            print(f"  Hold Period: {signal['hold_period_days']} days")
            print(f"  Active Signals: {signal['num_signals']} ({', '.join([s['type'] for s in signal['active_signals']])})")

        print()

    # Summary
    buy_count = sum(1 for s in signals if s['signal'] == 'BUY')
    sell_count = sum(1 for s in signals if s['signal'] == 'SELL')
    hold_count = sum(1 for s in signals if s['signal'] == 'HOLD')

    print("="*80)
    print(f"SUMMARY: {buy_count} BUY | {sell_count} SELL | {hold_count} HOLD")
    print("="*80 + "\n")

def save_signals_to_file(signals: List[Dict], output_file: str):
    """Save signals to JSON file"""

    output = {
        'generated_at': datetime.now().isoformat(),
        'stocks_analyzed': 5,
        'manipulation_contamination': '0%',
        'signals': signals
    }

    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"Signals saved to: {output_file}")

# ============================================================================
# DEMO / TESTING
# ============================================================================

if __name__ == '__main__':
    print("""
    ================================================================
         INVESTMENT OS - TIER 1 SIGNAL GENERATOR v1.0
                      (PRODUCTION READY)
    ================================================================

    This script generates daily trading signals for 5 ultra-clean CSE stocks:
    - CTHR.N0000, RCH.N0000, GHLL.N0000, NEH.N0000, WIND.N0000

    Based on 16 years of Granger causality validation (2010-2026)
    Contamination: 0% (verified against all 3 manipulation empires)

    Usage:
        python3 tier1_signal_generator.py [--demo]

    For production use, integrate with your data pipeline to feed live CSE data.
    """)

    # Demo mode - create synthetic data
    if '--demo' in sys.argv or len(sys.argv) == 1:
        print("\nRunning in DEMO mode (synthetic data)...\n")

        # Create demo data for testing
        demo_data = {}
        for stock in ['CTHR.N0000', 'RCH.N0000', 'GHLL.N0000', 'NEH.N0000', 'WIND.N0000']:
            dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
            demo_data[stock] = pd.DataFrame({
                'date': dates,
                'close': np.random.uniform(180, 220, 30),
                'volume': np.random.uniform(50000, 150000, 30)
            })

        # Generate signals
        signals = generate_daily_signals(demo_data)

        # Print report
        print_signal_report(signals)

        # Save to file
        save_signals_to_file(signals, '/opt/investment-os/signals/tier1_signals_demo.json')

    else:
        print("\nProduction mode requires live data integration.")
        print("Please connect to your CSE data source and call generate_daily_signals(data_dict)")
