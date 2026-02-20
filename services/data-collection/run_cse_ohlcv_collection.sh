
#!/bin/bash
# CSE OHLCV Collection Pipeline - 3-Step Process
# Source: https://www.cse.lk/equity/trade-summary
# Data: OHLCV prices for all CSE-listed stocks
# Schedule: Mon-Fri 10:00 AM + 4:00 PM
# Runtime: ~30 seconds total
#
# Migration: Phase 2 (Feb 2026)
# - Renamed: run_cse_price_collection.sh -> run_cse_ohlcv_collection.sh
# - Changed: cd /opt/selenium_automation -> cd /opt/investment-os/services/data-collection
# - Changed: .env loading -> common library handles via PYTHONPATH
# - Changed: /tmp/ temp files -> $WORKDIR/temp/ temp files
# - Fixed: set -eo pipefail + PIPESTATUS for proper exit code capture through tee
# - Original: /opt/selenium_automation/run_cse_price_collection.sh

set -eo pipefail

WORKDIR=/opt/investment-os/services/data-collection
cd "$WORKDIR"
export TZ=Asia/Colombo

# Create directories
mkdir -p logs temp

LOG_FILE="logs/cse_ohlcv_collection_$(date +%Y%m%d_%H%M%S).log"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "CSE OHLCV COLLECTION PIPELINE - $(date)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Step 1: Collect CSV from CSE (~15 seconds)
echo "" | tee -a "$LOG_FILE"
echo "STEP 1: Collecting CSE trade summary..." | tee -a "$LOG_FILE"
python3 cse_ohlcv_collector.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: CSE collection FAILED (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: CSE collection successful" | tee -a "$LOG_FILE"

# Step 2: Process CSV (~5 seconds)
echo "" | tee -a "$LOG_FILE"
echo "STEP 2: Processing prices..." | tee -a "$LOG_FILE"
python3 cse_ohlcv_processor.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: CSE processing FAILED (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: CSE processing successful" | tee -a "$LOG_FILE"

# Step 3: Upload to Supabase (~10 seconds)
# Phase 2: common library loads .env automatically via PYTHONPATH
echo "" | tee -a "$LOG_FILE"
echo "STEP 3: Uploading to Supabase..." | tee -a "$LOG_FILE"
python3 cse_ohlcv_uploader.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: CSE upload FAILED (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: CSE upload successful" | tee -a "$LOG_FILE"

# Summary
echo "" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"
echo "CSE OHLCV COLLECTION COMPLETE - $(date)" | tee -a "$LOG_FILE"
echo "Runtime: ~30 seconds" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Cleanup temp files (Phase 2: local temp/ instead of /tmp/)
rm -f "$WORKDIR/temp/latest_cse_raw.txt" "$WORKDIR/temp/latest_cse_prices.txt"

# Keep only last 30 days of logs
find logs/ -name "cse_ohlcv_collection_*.log" -mtime +30 -delete 2>/dev/null

exit 0
