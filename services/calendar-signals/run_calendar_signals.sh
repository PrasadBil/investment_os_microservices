#!/bin/bash
# =============================================================================
# run_calendar_signals.sh — Calendar Signal Monitor (9 AM daily)
# Cron: 0 9 * * 1-5  (9:00 AM SLK, Monday-Friday)
# Location: /opt/investment-os/services/calendar-signals/
#
# VERSION HISTORY:
#   v1.0.0  2026-02-28  Initial — cutover from /opt/selenium_automation/
#                        Old entry had log path bug: /log/ → /logs/
#                        New entry uses v5_logs + exec tee pattern (consistent
#                        with all other investment-os cron scripts).
#
# CRON ENTRY:
#   0 9 * * 1-5 /opt/investment-os/services/calendar-signals/run_calendar_signals.sh
#
# NOTES:
#   Runs calendar_signal_monitor.py only (payday cycle detection + DB log).
#   The 6:30 PM run_daily_signals.sh covers calendar + tier1 + email workflow.
#   This 9 AM run gives an early-morning DB record + console report for review.
# =============================================================================

set -eo pipefail

WORKDIR="/opt/investment-os/services/calendar-signals"
LOG_DIR="/opt/investment-os/v5_logs"
LOG_FILE="$LOG_DIR/calendar_signals_$(date +%Y%m%d).log"

export PYTHONPATH="/opt/investment-os/packages:$PYTHONPATH"
cd "$WORKDIR"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "======================================================"
echo "  Calendar Signal Monitor START: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

python3 calendar_signal_monitor.py
EXIT_CODE=${PIPESTATUS[0]}

echo "------------------------------------------------------"
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "  STATUS: SUCCESS"
else
    echo "  STATUS: FAILED (exit code $EXIT_CODE)"
    echo "  Check log: $LOG_FILE"
fi
echo "  END: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

exit $EXIT_CODE
