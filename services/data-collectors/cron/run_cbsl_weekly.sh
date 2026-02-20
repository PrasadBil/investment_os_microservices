
#!/bin/bash
# =============================================================================
# run_cbsl_weekly.sh — CBSL Weekly Economic Indicators Collector
# Cron: 30 12 * * 5  (6:00 PM SLK = 12:30 PM UTC, Fridays only)
# Location: /opt/investment-os/services/data-collectors/cron/
#
# VERSION HISTORY:
#   v1.0.0  2026-02-19  Sprint 2 — initial implementation
#
# CRON ENTRY (add to VPS crontab: crontab -e):
#   30 12 * * 5 /opt/investment-os/services/data-collectors/cron/run_cbsl_weekly.sh
#
# NOTES:
#   - CBSL publishes the WEI PDF on Fridays, typically by mid-afternoon SLK.
#   - discover() auto falls back to the previous Friday if current PDF not yet up.
#   - week_ending is the Friday date embedded in the PDF filename (YYYYMMDD).
# =============================================================================

set -eo pipefail

WORKDIR="/opt/investment-os/services/data-collectors"
LOG_DIR="/opt/investment-os/v5_logs"
LOG_FILE="$LOG_DIR/collector_cbsl_weekly_$(date +%Y%m%d).log"
SOURCE_ID="cbsl_weekly"

export PYTHONPATH="/opt/investment-os/packages:$PYTHONPATH"
cd "$WORKDIR"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "======================================================"
echo "  CBSL Weekly Collector START: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

# Pass today's date — discover() derives the correct Friday week_ending internally.
COLLECTION_DATE="$(date +%Y-%m-%d)"
echo "  Collection date passed to discover(): $COLLECTION_DATE"

python3 collector_runner.py --source "$SOURCE_ID" --date "$COLLECTION_DATE"
EXIT_CODE=${PIPESTATUS[0]}

echo "------------------------------------------------------"
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "  STATUS: SUCCESS"
elif [ "$EXIT_CODE" -eq 2 ]; then
    echo "  STATUS: SKIPPED (already collected this week)"
    EXIT_CODE=0
else
    echo "  STATUS: FAILED (exit code $EXIT_CODE)"
    echo "  Check log: $LOG_FILE"
fi
echo "  END: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

exit $EXIT_CODE
