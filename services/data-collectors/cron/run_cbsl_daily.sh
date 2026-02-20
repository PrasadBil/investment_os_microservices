#!/bin/bash
# =============================================================================
# run_cbsl_daily.sh — CBSL Daily Economic Indicators Collector
# Cron: 30 11 * * 1-5  (5:00 PM SLK = 11:30 AM UTC, Monday-Friday)
# Location: /opt/investment-os/services/data-collectors/cron/
#
# VERSION HISTORY:
#   v1.0.0  2026-02-18  Sprint 0 skeleton — ready for Sprint 1 implementation
#
# CRON ENTRY (add to VPS crontab: crontab -e):
#   30 11 * * 1-5 /opt/investment-os/services/data-collectors/cron/run_cbsl_daily.sh
# =============================================================================

set -eo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
WORKDIR="/opt/investment-os/services/data-collectors"
LOG_DIR="/opt/investment-os/v5_logs"
LOG_FILE="$LOG_DIR/collector_cbsl_daily_$(date +%Y%m%d).log"
SOURCE_ID="cbsl_daily"

# ── Environment ───────────────────────────────────────────────────────────────
export PYTHONPATH="/opt/investment-os/packages:$PYTHONPATH"
cd "$WORKDIR"

# ── Logging setup ─────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "======================================================"
echo "  CBSL Daily Collector START: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

# ── Run collector ─────────────────────────────────────────────────────────────
INDICATORS_DATE="$(python3 -c "
from datetime import date, timedelta
d = date.today() - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)
print(d.isoformat())
")""
echo "  Collecting indicators for: $INDICATORS_DATE"
python3 collector_runner.py --source "$SOURCE_ID" --date "$INDICATORS_DATE"
EXIT_CODE=${PIPESTATUS[0]}

# ── Result ────────────────────────────────────────────────────────────────────
echo "------------------------------------------------------"
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "  STATUS: SUCCESS"
elif [ "$EXIT_CODE" -eq 2 ]; then
    echo "  STATUS: SKIPPED (already collected today)"
    EXIT_CODE=0   # Treat skip as success for cron exit code
else
    echo "  STATUS: FAILED (exit code $EXIT_CODE)"
    echo "  Check log: $LOG_FILE"
fi
echo "  END: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

exit $EXIT_CODE
