
#!/bin/bash
# StockAnalysis OHLCV Collection Pipeline - Backup Price Source
# Source: https://stockanalysis.com/watchlist/ (Daily_Price tab)
# Data: OHLCV backup prices for all CSE-listed stocks
# Schedule: Mon-Fri 3:45 PM
# Runtime: ~30 seconds total
#
# Migration: Phase 2 (Feb 2026)
# - Renamed: run_price_collection.sh -> run_stockanalysis_ohlcv_collection.sh
# - Changed: Working directory to /opt/investment-os/services/data-collection
# - Changed: /tmp/ temp files -> $WORKDIR/temp/ temp files
# - Fixed: set -eo pipefail + PIPESTATUS for proper exit code capture through tee
# - Original: /opt/selenium_automation/run_price_collection.sh

set -eo pipefail

WORKDIR=/opt/investment-os/services/data-collection
cd "$WORKDIR"
export TZ=Asia/Colombo

# Create directories
mkdir -p logs temp downloads output

LOG_FILE="logs/stockanalysis_ohlcv_collection_$(date +%Y%m%d_%H%M%S).log"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "STOCKANALYSIS OHLCV COLLECTION (Backup Prices) - $(date)" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# STEP 1: Collect prices (direct CSV extraction via spatial triangulation)
echo "STEP 1: Collecting prices (direct extraction)..." | tee -a "$LOG_FILE"
python3 stockanalysis_ohlcv_collector.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: Price collection failed (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: Price collection successful" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# STEP 2: Upload to Supabase (reads CSV path from temp/latest_prices.txt)
# Phase 2: common library loads .env automatically via PYTHONPATH
echo "STEP 2: Uploading to Supabase..." | tee -a "$LOG_FILE"
python3 stockanalysis_ohlcv_uploader.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "FAIL: Price upload failed (exit code: $EXIT_CODE)" | tee -a "$LOG_FILE"
    exit 1
fi

echo "PASS: Price upload successful" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

echo "=====================================================================" | tee -a "$LOG_FILE"
echo "STOCKANALYSIS OHLCV COLLECTION COMPLETE - $(date)" | tee -a "$LOG_FILE"
echo "Runtime: ~30 seconds" | tee -a "$LOG_FILE"
echo "=====================================================================" | tee -a "$LOG_FILE"

# Cleanup temp files (Phase 2: local temp/ instead of /tmp/)
rm -f "$WORKDIR/temp/latest_prices.txt"

# Keep only last 30 days of logs
find logs/ -name "stockanalysis_ohlcv_collection_*.log" -mtime +30 -delete 2>/dev/null

exit 0
