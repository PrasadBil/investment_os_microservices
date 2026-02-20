
#!/bin/bash
# StockAnalysis Metrics Collection - Full 235 Parameters
# Source: https://stockanalysis.com/stocks/screener/ (13 views)
# Data: All 235 fundamental metrics for 296 CSE stocks
# Schedule: Mon-Fri 6:00 PM
# Runtime: ~10-15 minutes total
#
# Migration: Phase 2 (Feb 2026)
# - Renamed: run_daily.sh -> run_stockanalysis_metrics_collection.sh
# - Changed: cd /opt/selenium_automation -> cd /opt/investment-os/services/data-collection
# - Changed: .env loading -> common library handles via PYTHONPATH
# - Fixed: set -eo pipefail + PIPESTATUS for proper exit code capture through tee (all steps)
# - Original: /opt/selenium_automation/run_daily.sh

set -eo pipefail

WORKDIR=/opt/investment-os/services/data-collection
cd "$WORKDIR"
export TZ=Asia/Colombo

# Create directories
mkdir -p logs

LOG_FILE="logs/stockanalysis_metrics_collection_$(date +%Y%m%d_%H%M%S).log"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "STOCKANALYSIS METRICS COLLECTION (235 Parameters) - $(date)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Step 1: Collect data
echo "" | tee -a "$LOG_FILE"
echo "STEP 1: Collecting data from StockAnalysis.com..." | tee -a "$LOG_FILE"
python3 stockanalysis_metrics_collector.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: Data collection FAILED (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: Data collection successful" | tee -a "$LOG_FILE"

# Step 2: Process data
echo "" | tee -a "$LOG_FILE"
echo "STEP 2: Processing data..." | tee -a "$LOG_FILE"
python3 stockanalysis_metrics_processor.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: Data processing FAILED (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: Data processing successful" | tee -a "$LOG_FILE"

# Step 3: Upload to Supabase
# Phase 2: common library loads .env automatically via PYTHONPATH
echo "" | tee -a "$LOG_FILE"
echo "STEP 3: Uploading to Supabase..." | tee -a "$LOG_FILE"

# Find latest CSV file
CSV_FILE=$(ls -t output/*/cleaned_data.csv 2>/dev/null | head -1)

if [ -f "$CSV_FILE" ]; then
    python3 stockanalysis_metrics_uploader.py "$CSV_FILE" 2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}

    if [ $EXIT_CODE -ne 0 ]; then
        echo "FAIL: Supabase upload FAILED (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
        exit 1
    fi

    echo "PASS: Supabase upload successful" | tee -a "$LOG_FILE"
else
    echo "FAIL: CSV file not found" | tee -a "$LOG_FILE"
    exit 1
fi


# Summary
echo "" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"
echo "STOCKANALYSIS METRICS COLLECTION COMPLETE - $(date)" | tee -a "$LOG_FILE"
echo "SQL file: $(ls -t output/*/import_cse_*.sql 2>/dev/null | head -1)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Keep only last 30 days of logs
find logs/ -name "stockanalysis_metrics_collection_*.log" -mtime +30 -delete 2>/dev/null
