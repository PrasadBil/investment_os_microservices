
#!/usr/bin/env python3
"""
INVESTMENT OS - COMPLETE INTEGRATION EXAMPLE
=============================================
Shows how to connect Supabase data -> Signal Generator -> Trading Decisions

This is your DAILY WORKFLOW script!

Author: Investment OS Development Team
Date: January 27, 2026
Version: 1.0

Migration: Phase 2 (Feb 2026)
- Replaced: cse_data_loader imports -> common.data_loader
- Replaced: dotenv/os.getenv -> common.config
- Added: Standardized logging via common.logging_config
- Original: /opt/selenium_automation/daily_trading_workflow.py
"""

from datetime import datetime

# === Investment OS Common Library (Phase 2 Migration) ===
from common.config import get_config
from common.data_loader import load_cse_data, validate_data_quality
from common.logging_config import setup_logging
from common.email_sender import EmailSender

# Local module (unchanged - pure computation, no DB imports)
from tier1_signal_generator import generate_daily_signals, print_signal_report, save_signals_to_file

# Initialize
config = get_config()
logger = setup_logging('daily-workflow', log_to_file=True)

# ============================================================================
# STEP 1: SET UP ENVIRONMENT
# ============================================================================

def setup_environment():
    """
    Verify Supabase credentials are available via common config.

    Replaces manual os.getenv() checks - config.validate() handles this.
    """
    try:
        if not config.SUPABASE_URL:
            logger.error("SUPABASE_URL not configured")
            return False
        if not config.SUPABASE_KEY:
            logger.error("SUPABASE_KEY not configured")
            return False

        logger.info("Supabase credentials verified via common.config")
        return True
    except Exception as e:
        logger.error(f"Environment setup failed: {e}")
        return False

# ============================================================================
# STEP 2: LOAD DATA FROM SUPABASE
# ============================================================================

def load_data():
    """Load CSE data from Supabase (last 30 days)"""

    print("\n" + "="*80)
    print("STEP 1: LOADING CSE DATA FROM SUPABASE")
    print("="*80)

    # Load data for all 5 stocks (uses common.data_loader)
    cse_data = load_cse_data(days=30)

    if not cse_data:
        logger.error("Failed to load any data")
        return None

    # Validate data quality
    print("\n" + "="*80)
    print("DATA QUALITY CHECK")
    print("="*80)

    validation = validate_data_quality(cse_data)

    all_valid = True
    for symbol, result in validation.items():
        if result['valid']:
            logger.info(f"{symbol}: {result['days']} days, updated {result['days_since_update']} days ago")
        else:
            logger.warning(f"{symbol}: Issues found:")
            for issue in result['issues']:
                logger.warning(f"   - {issue}")
            all_valid = False

    if not all_valid:
        logger.warning("Some data quality issues found. Proceed with caution")

    return cse_data

# ============================================================================
# STEP 3: GENERATE SIGNALS
# ============================================================================

def generate_signals(cse_data):
    """Generate trading signals from CSE data"""

    print("\n" + "="*80)
    print("STEP 2: GENERATING TRADING SIGNALS")
    print("="*80)

    # Generate signals using your Granger-validated signal generator
    signals = generate_daily_signals(cse_data)

    # Print human-readable report
    print_signal_report(signals)

    return signals

# ============================================================================
# STEP 4: FILTER & PRIORITIZE SIGNALS
# ============================================================================

def prioritize_signals(signals):
    """Filter and prioritize signals for manual trading"""

    print("\n" + "="*80)
    print("STEP 3: PRIORITIZING SIGNALS FOR TRADING")
    print("="*80)

    # Filter for actionable signals (not HOLD)
    actionable = [s for s in signals if s['signal'] != 'HOLD']

    if not actionable:
        logger.info("No actionable signals today. Market conditions unclear.")
        print("\nAction: HOLD all positions, no new trades.")
        return []

    # Filter for high confidence (>80%)
    high_confidence = [s for s in actionable if s['confidence'] >= 80]

    if not high_confidence:
        logger.info(f"{len(actionable)} signals found, but all below 80% confidence.")
        print("Action: Skip trading today or reduce position sizes.")
        return actionable  # Return them anyway for review

    # Sort by confidence (highest first)
    prioritized = sorted(high_confidence, key=lambda x: x['confidence'], reverse=True)

    logger.info(f"{len(prioritized)} high-confidence signals (>80%)")

    for i, signal in enumerate(prioritized, 1):
        print(f"{i}. {signal['stock']}: {signal['signal']}")
        print(f"   Confidence: {signal['confidence']:.1f}%")
        print(f"   Expected Return: {signal['expected_return']:.2f}%")
        print(f"   Hold Period: {signal['hold_period_days']} days")
        print(f"   Price: Rs {signal['price']:.2f}")
        print()

    return prioritized

# ============================================================================
# STEP 5: MANUAL TRADING GUIDE
# ============================================================================

def generate_trading_plan(signals):
    """Generate step-by-step trading plan for manual execution"""

    if not signals:
        return

    print("\n" + "="*80)
    print("STEP 4: YOUR TRADING PLAN (EXECUTE MANUALLY)")
    print("="*80)

    print("\nEXECUTE THESE TRADES ON YOUR BROKER PLATFORM:\n")

    for i, signal in enumerate(signals[:2], 1):  # Max 2 trades per day
        print(f"TRADE #{i}:")
        print(f"  Stock: {signal['stock']}")
        print(f"  Action: {signal['signal']}")
        print(f"  Entry Price: Rs {signal['price']:.2f} (or better)")

        if signal['signal'] == 'BUY':
            stop_loss = signal['price'] * 0.95
            target = signal['price'] * (1 + signal['expected_return']/100)
            print(f"  Stop Loss: Rs {stop_loss:.2f} (5% below entry)")
            print(f"  Target: Rs {target:.2f} ({signal['expected_return']:.1f}% profit)")
            print(f"  Hold Period: ~{signal['hold_period_days']} days")

        elif signal['signal'] == 'SELL':
            # For short selling (if allowed) or selling existing position
            stop_loss = signal['price'] * 1.05
            target = signal['price'] * (1 - signal['expected_return']/100)
            print(f"  Stop Loss: Rs {stop_loss:.2f} (5% above entry)")
            print(f"  Target: Rs {target:.2f} ({signal['expected_return']:.1f}% profit)")
            print(f"  Hold Period: ~{signal['hold_period_days']} days")

        print(f"  Suggested Position Size: 2-8% of portfolio")
        print(f"  Confidence: {signal['confidence']:.1f}%\n")

    if len(signals) > 2:
        logger.info(f"{len(signals)-2} additional signals available but holding to max 2 trades/day limit")

# ============================================================================
# STEP 5B: SEND EMAIL REPORT
# ============================================================================

def send_email_report(signals: list, prioritized: list) -> bool:
    """
    Send daily trading signals email via common.email_sender.
    Follows same pattern as Manipulation Detector v5.0 email.
    """
    from datetime import date as date_cls

    today = date_cls.today().isoformat()
    buy_signals  = [s for s in signals if s.get('signal') == 'BUY']
    sell_signals = [s for s in signals if s.get('signal') == 'SELL']
    hold_signals = [s for s in signals if s.get('signal') == 'HOLD']

    # ── Build report body ──────────────────────────────────────────────────────
    lines = [
        f"INVESTMENT OS — DAILY TRADING SIGNALS",
        f"Date: {today}",
        f"Stocks Analyzed: {len(signals)} (Ultra-Clean, 0% Manipulation)",
        "=" * 60,
        f"SUMMARY: {len(buy_signals)} BUY | {len(sell_signals)} SELL | {len(hold_signals)} HOLD",
        "",
    ]

    if prioritized:
        lines.append("HIGH-CONFIDENCE SIGNALS (ACTION REQUIRED):")
        lines.append("-" * 60)
        for i, s in enumerate(prioritized[:2], 1):
            lines.append(f"  TRADE #{i}: {s['stock']} — {s['signal']}")
            lines.append(f"    Entry:       Rs {s['price']:.2f}")
            lines.append(f"    Confidence:  {s['confidence']:.1f}%")
            lines.append(f"    Return:      {s['expected_return']:.2f}%")
            lines.append(f"    Hold Period: ~{s['hold_period_days']} days")
            if s['signal'] == 'BUY':
                lines.append(f"    Stop Loss:   Rs {s['price'] * 0.95:.2f}  (5% below entry)")
                lines.append(f"    Target:      Rs {s['price'] * (1 + s['expected_return']/100):.2f}")
            elif s['signal'] == 'SELL':
                lines.append(f"    Stop Loss:   Rs {s['price'] * 1.05:.2f}  (5% above entry)")
                lines.append(f"    Target:      Rs {s['price'] * (1 - s['expected_return']/100):.2f}")
            lines.append("")
    else:
        lines.append("No high-confidence signals today (all below 80%).")
        lines.append("Action: Hold positions. Review tomorrow.")
        lines.append("")

    lines.append("ALL SIGNALS:")
    lines.append("-" * 60)
    for s in signals:
        conf = f"{s['confidence']:.1f}%" if s.get('confidence') else "N/A"
        lines.append(f"  {s['stock']:<16}  {s['signal']:<6}  Confidence: {conf}")

    lines.append("")
    lines.append("Execute trades MANUALLY on your broker platform.")
    lines.append("Set stop loss orders (5% below entry for BUY).")

    body = "\n".join(lines)
    subject = f"Investment OS — Daily Signals {today}: {len(buy_signals)} BUY | {len(sell_signals)} SELL | {len(hold_signals)} HOLD"

    # ── Send ──────────────────────────────────────────────────────────────────
    try:
        sender = EmailSender()
        success = sender.send_report(subject=subject, body=body)
        if success:
            logger.info("Daily signals email sent successfully")
        else:
            logger.warning("Daily signals email failed — continuing without email")
        return success
    except Exception as e:
        logger.warning(f"Email step failed ({e}) — continuing without email")
        return False


# ============================================================================
# STEP 6: SAVE FOR RECORDS
# ============================================================================

def save_for_records(signals):
    """Save signals to file for tracking"""
    import os

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs(config.SIGNALS_DIR, exist_ok=True)
    filename = os.path.join(config.SIGNALS_DIR, f'investment_os_signals_{timestamp}.json')

    save_signals_to_file(signals, filename)

    logger.info(f"Signals saved to: {filename}")

# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def main():
    """
    Complete daily workflow:
    1. Load data from Supabase
    2. Generate signals
    3. Prioritize high-confidence signals
    4. Generate trading plan
    5. Save for records
    """

    print("""
    ================================================================
         INVESTMENT OS - DAILY TRADING WORKFLOW
                  (MANUAL EXECUTION MODE)
    ================================================================

    This script:
    1. Loads latest CSE data from your Supabase tables
    2. Generates trading signals using Granger-validated indicators
    3. Prioritizes high-confidence signals (>80%)
    4. Provides step-by-step trading plan
    5. Saves signals for tracking

    You then execute trades MANUALLY on your broker platform.
    """)

    logger.info("Daily Trading Workflow starting")

    # Setup
    if not setup_environment():
        return

    # Load data
    cse_data = load_data()
    if not cse_data:
        return

    # Generate signals
    signals = generate_signals(cse_data)

    # Prioritize
    prioritized = prioritize_signals(signals)

    # Trading plan
    generate_trading_plan(prioritized)

    # Email report
    send_email_report(signals, prioritized)

    # Save
    save_for_records(signals)

    print("="*80)
    print("WORKFLOW COMPLETE!")
    print("="*80)
    print("""
    NEXT STEPS:
    1. Review the trading plan above
    2. Login to your broker platform (NDB/Softlogic/etc)
    3. Execute the recommended trades
    4. Set stop loss orders (5% below entry for BUY)
    5. Record in your tracking spreadsheet
    6. Monitor positions daily

    Run this script again tomorrow for new signals!
    """)

    logger.info("Daily Trading Workflow complete")

if __name__ == '__main__':
    main()
