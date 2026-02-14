#!/bin/bash
# Price Collection Pipeline Orchestrator - DIRECT EXTRACTION VERSION
# Collector now outputs CSV directly, skipping HTML processor
# Schedule: Daily 3:45 PM (StockAnalysis backup price source)
#
# Migration: Phase 2 (Feb 2026)
# - Changed: Working directory to /opt/investment-os/services/data-collection
# - Changed: /tmp/ temp files -> $WORKDIR/temp/ temp files
# - Original: /opt/selenium_automation/run_price_collection.sh

set -e

WORKDIR=/opt/investment-os/services/data-collection
cd "$WORKDIR"
export TZ=Asia/Colombo

# Create directories
mkdir -p logs temp

LOG_FILE="logs/price_collection_$(date +%Y%m%d_%H%M%S).log"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "PRICE COLLECTION PIPELINE (StockAnalysis Backup) - $(date)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# STEP 1: Collect prices (now outputs CSV directly!)
echo "STEP 1: Collecting prices (direct extraction)..." | tee -a "$LOG_FILE"
python3 price_collector.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -ne 0 ]; then
    echo "FAIL: Price collection failed" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: Price collection successful" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# STEP 2: Upload to Supabase (reads CSV from temp/latest_prices.txt)
# Phase 2: common library loads .env automatically via PYTHONPATH
echo "STEP 2: Uploading to Supabase..." | tee -a "$LOG_FILE"
python3 price_uploader.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -ne 0 ]; then
    echo "FAIL: Price upload failed" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: Price upload successful" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "PRICE COLLECTION COMPLETE - $(date)" | tee -a "$LOG_FILE"
echo "Runtime: ~30 seconds" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Cleanup temp files (Phase 2: local temp/ instead of /tmp/)
rm -f "$WORKDIR/temp/latest_prices.txt"

# Keep only last 30 days of logs
find logs/ -name "price_collection_*.log" -mtime +30 -delete 2>/dev/null

exit 0
