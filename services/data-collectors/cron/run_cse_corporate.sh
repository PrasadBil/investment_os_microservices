]633;E;echo '#!/bin/bash';1ef7951c-d9fd-4991-bcec-38fe80372b05]633;C#!/bin/bash

#!/bin/bash
# =============================================================================
# run_cse_corporate.sh — CSE Daily Market Report Corporate Actions Collector
# Cron: 30 14 * * 1-5  (8:00 PM SLK = 14:30 UTC, Monday-Friday)
# Location: /opt/investment-os/services/data-collectors/cron/
#
# VERSION HISTORY:
#   v1.0.0  2026-02-18  Sprint 0 skeleton — ready for Sprint 3 implementation
#
# CRON ENTRY:
#   30 14 * * 1-5 /opt/investment-os/services/data-collectors/cron/run_cse_corporate.sh
#
# NOTES:
#   Runs AFTER CBSL daily (11:30 UTC) to avoid competing for Chrome/Selenium.
#   Selenium requires display: uses Xvfb virtual display (same as Service 5).
#   Large file (~25MB) → archived to Google Drive, NOT kept on VPS.
# =============================================================================

set -eo pipefail

WORKDIR="/opt/investment-os/services/data-collectors"
LOG_DIR="/opt/investment-os/v5_logs"
LOG_FILE="$LOG_DIR/collector_cse_corporate_$(date +%Y%m%d).log"
SOURCE_ID="cse_daily"

export PYTHONPATH="/opt/investment-os/packages:$PYTHONPATH"
export DISPLAY=:99   # Xvfb virtual display for headless Chrome (matches Service 5)
cd "$WORKDIR"

mkdir -p "$LOG_DIR"
mkdir -p "$WORKDIR/data/temp"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "======================================================"
echo "  CSE Corporate Actions Collector START: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

# Verify Chrome + ChromeDriver available (Selenium dependency)
if ! command -v google-chrome &> /dev/null && ! command -v chromium-browser &> /dev/null; then
    echo "  ERROR: Chrome not found. Install: apt-get install chromium-browser"
    exit 1
fi

python3 collector_runner.py --source "$SOURCE_ID" "$@"
EXIT_CODE=${PIPESTATUS[0]}

# Cleanup temp files older than 2 days (safety net)
find "$WORKDIR/data/temp" -name "*.pdf" -mtime +2 -delete 2>/dev/null || true

echo "------------------------------------------------------"
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "  STATUS: SUCCESS"
elif [ "$EXIT_CODE" -eq 2 ]; then
    echo "  STATUS: SKIPPED (already collected today)"
    EXIT_CODE=0
else
    echo "  STATUS: FAILED (exit code $EXIT_CODE)"
    echo "  Check log: $LOG_FILE"
    echo "  Screenshot (if exists): $WORKDIR/data/temp/screenshot_*.png"
fi
echo "  END: $(date '+%Y-%m-%d %H:%M:%S SLK')"
echo "======================================================"

exit $EXIT_CODE
