
#!/bin/bash
# ============================================================================
# INVESTMENT OS - AUTOMATED DAILY WORKFLOW (Phase 2 Migration)
# ============================================================================
# Runs daily at 6:30 PM SLK time
# Generates calendar signals + tier1 signals and sends email notification
#
# Location: /opt/investment-os/services/calendar-signals/run_daily_signals.sh
#
# Migration Notes:
# - WORKDIR changed: /opt/selenium_automation → /opt/investment-os
# - Python scripts use common library (no more inline create_client/load_dotenv)
# - Email sent via common.email_sender (no more inline SMTP or mail command)
# - PYTHONPATH already configured in ~/.bashrc (Phase 1)
#
# Original: /opt/selenium_automation/run_daily_signals.sh
#           /opt/selenium_automation/run_daily_signals_simple.sh
# ============================================================================

set -e  # Exit on any error

# ============================================================================
# CONFIGURATION
# ============================================================================

WORKDIR="/opt/investment-os"
SERVICE_DIR="${WORKDIR}/services/calendar-signals"
LOGDIR="${WORKDIR}/v5_logs"
SIGNALDIR="${WORKDIR}/signals"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DATE=$(date +%Y-%m-%d)
SIGNAL_FILE="${SIGNALDIR}/signals_${TIMESTAMP}.json"
LOG_FILE="${LOGDIR}/daily_signals_${TIMESTAMP}.log"
LATEST_SIGNAL="${SIGNALDIR}/latest_signal.json"

# ============================================================================
# SETUP
# ============================================================================

mkdir -p "${LOGDIR}"
mkdir -p "${SIGNALDIR}"

echo "======================================================================" | tee -a "${LOG_FILE}"
echo "INVESTMENT OS - AUTOMATED DAILY WORKFLOW" | tee -a "${LOG_FILE}"
echo "Started: $(date)" | tee -a "${LOG_FILE}"
echo "Working Directory: ${WORKDIR}" | tee -a "${LOG_FILE}"
echo "======================================================================" | tee -a "${LOG_FILE}"

cd "${WORKDIR}"

# ============================================================================
# STEP 1: VERIFY DATA FRESHNESS (uses common library)
# ============================================================================

echo "" | tee -a "${LOG_FILE}"
echo "Step 1: Verifying data freshness..." | tee -a "${LOG_FILE}"

python3 << 'PYTHON_CHECK' >> "${LOG_FILE}" 2>&1
from datetime import datetime, timedelta
from common.database import get_supabase_client

client = get_supabase_client()
today = datetime.now().date()
yesterday = today - timedelta(days=1)

response = client.table('cse_daily_prices').select('collection_date').order('collection_date', desc=True).limit(1).execute()

if response.data:
    latest_date = response.data[0]['collection_date']
    print(f"Latest data: {latest_date}")

    latest = datetime.strptime(latest_date, '%Y-%m-%d').date()

    if latest >= yesterday:
        print("Data is fresh")
        exit(0)
    else:
        print(f"Data is stale (last update: {latest})")
        exit(1)
else:
    print("No data found")
    exit(1)
PYTHON_CHECK

DATA_CHECK=$?

if [ $DATA_CHECK -ne 0 ]; then
    echo "WARNING: Data may be stale. Proceeding anyway..." | tee -a "${LOG_FILE}"
fi

# ============================================================================
# STEP 2: RUN CALENDAR SIGNAL MONITOR
# ============================================================================

echo "" | tee -a "${LOG_FILE}"
echo "Step 2: Running calendar signal monitor..." | tee -a "${LOG_FILE}"

cd "${SERVICE_DIR}"
python3 calendar_signal_monitor.py >> "${LOG_FILE}" 2>&1

echo "Calendar signals generated" | tee -a "${LOG_FILE}"

# ============================================================================
# STEP 3: GENERATE TIER 1 TRADING SIGNALS
# ============================================================================

echo "" | tee -a "${LOG_FILE}"
echo "Step 3: Generating tier 1 trading signals..." | tee -a "${LOG_FILE}"

cd "${SERVICE_DIR}"
python3 daily_trading_workflow.py >> "${LOG_FILE}" 2>&1

SIGNAL_GEN_EXIT=$?

if [ $SIGNAL_GEN_EXIT -ne 0 ]; then
    echo "ERROR: Signal generation failed!" | tee -a "${LOG_FILE}"

    # Send error notification via common library
    cd "${WORKDIR}"
    python3 << PYTHON_ERROR >> "${LOG_FILE}" 2>&1
from common.email_sender import EmailSender
sender = EmailSender()
sender.send_report(
    "Investment OS - Signal Generation FAILED",
    "Signal generation failed at $(date).\nCheck logs: ${LOG_FILE}"
)
PYTHON_ERROR

    exit 1
fi

echo "Tier 1 signals generated" | tee -a "${LOG_FILE}"

# ============================================================================
# STEP 4: COPY LATEST SIGNAL
# ============================================================================

echo "" | tee -a "${LOG_FILE}"
echo "Step 4: Saving latest signal..." | tee -a "${LOG_FILE}"

LATEST_GEN=$(ls -t ${SIGNALDIR}/investment_os_signals_*.json 2>/dev/null | head -1)

if [ -n "$LATEST_GEN" ]; then
    cp "$LATEST_GEN" "${LATEST_SIGNAL}"
    echo "Signal saved: ${LATEST_SIGNAL}" | tee -a "${LOG_FILE}"
else
    echo "No signal file found in ${SIGNALDIR}" | tee -a "${LOG_FILE}"
fi

# ============================================================================
# STEP 5: SEND EMAIL NOTIFICATION (via common library)
# ============================================================================

echo "" | tee -a "${LOG_FILE}"
echo "Step 5: Sending email notification..." | tee -a "${LOG_FILE}"

cd "${WORKDIR}"
python3 << 'PYTHON_EMAIL' >> "${LOG_FILE}" 2>&1
import json
from common.email_sender import EmailSender

try:
    # Load latest signal
    with open('/opt/investment-os/signals/latest_signal.json', 'r') as f:
        data = json.load(f)

    signals = data['signals']
    actionable = [s for s in signals if s['signal'] != 'HOLD' and s['confidence'] >= 80]

    # Build email body
    body = "Investment OS Daily Trading Signals\n"
    body += "=" * 60 + "\n"
    body += f"Generated: {data['generated_at']}\n"
    body += f"Stocks Analyzed: {data['stocks_analyzed']}\n"
    body += f"Manipulation Contamination: {data['manipulation_contamination']}\n"
    body += "=" * 60 + "\n\n"

    if not actionable:
        body += "NO HIGH-CONFIDENCE SIGNALS TODAY\n\n"
        body += "Market conditions unclear. No trades recommended.\n"
        body += "Action: HOLD all positions.\n"
    else:
        body += f"{len(actionable)} HIGH-CONFIDENCE SIGNAL(S) (>80%):\n\n"

        for i, signal in enumerate(actionable, 1):
            body += f"TRADE #{i}: {signal['stock']}\n"
            body += f"  Action: {signal['signal']}\n"
            body += f"  Price: Rs {signal['price']:.2f}\n"
            body += f"  Confidence: {signal['confidence']:.1f}%\n"
            body += f"  Expected Return: {signal['expected_return']:.2f}%\n"
            body += f"  Hold Period: {signal['hold_period_days']} days\n"

            if signal['signal'] == 'BUY':
                stop = signal['price'] * 0.95
                target = signal['price'] * (1 + signal['expected_return']/100)
                body += f"  Stop Loss: Rs {stop:.2f} (5% below)\n"
                body += f"  Target: Rs {target:.2f}\n"

            body += "\n"

    body += "=" * 60 + "\n"
    body += "Full details: /opt/investment-os/signals/latest_signal.json\n"

    # Send via common library
    sender = EmailSender()
    sender.send_report(f"Investment OS Daily Signals - {data['generated_at'][:10]}", body)
    print("Email sent successfully")

except Exception as e:
    print(f"Email failed: {e}")
    print("Signal files still available locally")

PYTHON_EMAIL

# ============================================================================
# STEP 6: CLEANUP OLD FILES
# ============================================================================

echo "" | tee -a "${LOG_FILE}"
echo "Step 6: Cleaning up old files..." | tee -a "${LOG_FILE}"

find "${SIGNALDIR}" -name "signals_*.json" -mtime +30 -delete 2>/dev/null || true
find "${LOGDIR}" -name "daily_signals_*.log" -mtime +30 -delete 2>/dev/null || true

echo "Cleaned up files older than 30 days" | tee -a "${LOG_FILE}"

# ============================================================================
# COMPLETION
# ============================================================================

echo "" | tee -a "${LOG_FILE}"
echo "======================================================================" | tee -a "${LOG_FILE}"
echo "WORKFLOW COMPLETE! $(date)" | tee -a "${LOG_FILE}"
echo "Signal File: ${SIGNAL_FILE}" | tee -a "${LOG_FILE}"
echo "Log File: ${LOG_FILE}" | tee -a "${LOG_FILE}"
echo "======================================================================" | tee -a "${LOG_FILE}"

exit 0
