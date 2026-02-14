#!/bin/bash
# CSE Price Collection Pipeline - 3-Step Process
# Runs 2x daily: Market open (10:00 AM) + Market close (4:00 PM)
# Runtime: ~30 seconds total
#
# Migration: Phase 2 (Feb 2026)
# - Changed: cd /opt/selenium_automation -> cd /opt/investment-os/services/data-collection
# - Changed: .env loading from /opt/selenium_automation/.env -> /opt/investment-os/.env
# - Changed: /tmp/ temp files -> $WORKDIR/temp/ temp files
# - Original: /opt/selenium_automation/run_cse_price_collection.sh

set -e

WORKDIR=/opt/investment-os/services/data-collection
cd "$WORKDIR"
export TZ=Asia/Colombo

# Create directories
mkdir -p logs temp

LOG_FILE="logs/cse_price_collection_$(date +%Y%m%d_%H%M%S).log"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "CSE PRICE COLLECTION PIPELINE - $(date)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Step 1: Collect CSV from CSE (~15 seconds)
echo "" | tee -a "$LOG_FILE"
echo "STEP 1: Collecting CSE trade summary..." | tee -a "$LOG_FILE"
python3 cse_collector.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "PASS: CSE collection successful" | tee -a "$LOG_FILE"
else
    echo "FAIL: CSE collection FAILED" | tee -a "$LOG_FILE"
    exit 1
fi

# Step 2: Process CSV (~5 seconds)
echo "" | tee -a "$LOG_FILE"
echo "STEP 2: Processing prices..." | tee -a "$LOG_FILE"
python3 cse_processor.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "PASS: CSE processing successful" | tee -a "$LOG_FILE"
else
    echo "FAIL: CSE processing FAILED" | tee -a "$LOG_FILE"
    exit 1
fi

# Step 3: Upload to Supabase (~10 seconds)
# Phase 2: common library loads .env automatically via PYTHONPATH
echo "" | tee -a "$LOG_FILE"
echo "STEP 3: Uploading to Supabase..." | tee -a "$LOG_FILE"
python3 cse_uploader.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "PASS: CSE upload successful" | tee -a "$LOG_FILE"
else
    echo "FAIL: CSE upload FAILED" | tee -a "$LOG_FILE"
    exit 1
fi

# Summary
echo "" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"
echo "CSE PRICE COLLECTION COMPLETE - $(date)" | tee -a "$LOG_FILE"
echo "Runtime: ~30 seconds" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Cleanup temp files (Phase 2: local temp/ instead of /tmp/)
rm -f "$WORKDIR/temp/latest_cse_raw.txt" "$WORKDIR/temp/latest_cse_prices.txt"

# Keep only last 30 days of logs
find logs/ -name "cse_price_collection_*.log" -mtime +30 -delete 2>/dev/null

exit 0
