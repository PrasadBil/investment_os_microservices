#!/bin/bash
# Daily Data Collection - Master Script (StockAnalysis.com Full Metrics)
# Schedule: Daily 6:00 PM (collects all 235 metrics from 13 views)
#
# Migration: Phase 2 (Feb 2026)
# - Changed: cd /opt/selenium_automation -> cd /opt/investment-os/services/data-collection
# - Changed: .env loading -> common library handles via PYTHONPATH
# - Original: /opt/selenium_automation/run_daily.sh

set -e

WORKDIR=/opt/investment-os/services/data-collection
cd "$WORKDIR"
export TZ=Asia/Colombo

# Create directories
mkdir -p logs

LOG_FILE="logs/daily_run_$(date +%Y%m%d_%H%M%S).log"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "DAILY DATA COLLECTION (StockAnalysis Full Metrics) - $(date)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Step 1: Collect data
echo "" | tee -a "$LOG_FILE"
echo "STEP 1: Collecting data from StockAnalysis.com..." | tee -a "$LOG_FILE"
python3 selenium_collector.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "PASS: Data collection successful" | tee -a "$LOG_FILE"
else
    echo "FAIL: Data collection FAILED" | tee -a "$LOG_FILE"
    exit 1
fi

# Step 2: Process data
echo "" | tee -a "$LOG_FILE"
echo "STEP 2: Processing data..." | tee -a "$LOG_FILE"
python3 data_processor.py 2>&1 | tee -a "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "PASS: Data processing successful" | tee -a "$LOG_FILE"
else
    echo "FAIL: Data processing FAILED" | tee -a "$LOG_FILE"
    exit 1
fi

# Step 3: Upload to Supabase
# Phase 2: common library loads .env automatically via PYTHONPATH
echo "" | tee -a "$LOG_FILE"
echo "STEP 3: Uploading to Supabase..." | tee -a "$LOG_FILE"

# Find latest CSV file
CSV_FILE=$(ls -t output/*/cleaned_data.csv 2>/dev/null | head -1)

if [ -f "$CSV_FILE" ]; then
    python3 upload_to_supabase.py "$CSV_FILE" 2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}

    if [ $EXIT_CODE -eq 0 ]; then
        echo "PASS: Supabase upload successful" | tee -a "$LOG_FILE"
    else
        echo "FAIL: Supabase upload FAILED (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
        exit 1
    fi
else
    echo "FAIL: CSV file not found" | tee -a "$LOG_FILE"
    exit 1
fi


# Summary
echo "" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"
echo "DAILY COLLECTION COMPLETE - $(date)" | tee -a "$LOG_FILE"
echo "SQL file: $(ls -t output/*/import_cse_*.sql 2>/dev/null | head -1)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Keep only last 30 days of logs
find logs/ -name "daily_run_*.log" -mtime +30 -delete 2>/dev/null
