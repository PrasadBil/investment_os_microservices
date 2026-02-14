
#!/bin/bash
#
# INVESTMENT OS - WEEKLY SCORING (Phase 2 Migration)
# Uses standardized filenames + configurable data path
# Date: January 4, 2026
#
# Migration Notes:
# - SCRIPT_DIR changed: /opt/selenium_automation -> /opt/investment-os/services/scoring-7d
# - LOG_DIR changed: $SCRIPT_DIR/logs -> /opt/investment-os/v5_logs
# - DATA_FILE paths updated to /opt/investment-os/
# - Original: /opt/selenium_automation/weekly_scoring_complete.sh
#

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# ============================================================================
# CONFIGURATION (Phase 2: Updated paths)
# ============================================================================
WORKDIR="/opt/investment-os"
SCRIPT_DIR="${WORKDIR}/services/scoring-7d"
LOG_DIR="${WORKDIR}/v5_logs"

# INPUT DATA FILE - UPDATE THIS PATH!
# Option 1: If data is in dated folder
DATA_FILE="${WORKDIR}/output/$(date +%Y-%m-%d)/cleaned_data.csv"
# Option 2: Use latest file from output folder
# DATA_FILE=$(ls -t ${WORKDIR}/output/*/cleaned_data.csv 2>/dev/null | head -1)

# ============================================================================

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/weekly_scoring_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

# Redirect output to log
exec 1> >(tee -a "$LOG_FILE")
exec 2>&1

echo "================================================================================"
echo "INVESTMENT OS - WEEKLY SCORING AUTOMATION"
echo "================================================================================"
echo "Start Time: $(date)"
echo "Log File: $LOG_FILE"
echo "Data File: $DATA_FILE"
echo ""

cd "$SCRIPT_DIR"

# Check data file exists
if [ ! -f "$DATA_FILE" ]; then
    echo -e "${RED}ERROR: Data file not found: $DATA_FILE${NC}"
    echo ""
    echo "Please update DATA_FILE path in this script"
    echo "Available data files:"
    find "$WORKDIR" -name "cleaned_data.csv" -o -name "latest_data.csv" 2>/dev/null
    exit 1
fi

echo -e "${GREEN}Data file found: $DATA_FILE${NC}"
echo ""

# Function to run a scorer
run_scorer() {
    local dim=$1
    local scorer="dimension${dim}_scorer.py"
    local output="dimension${dim}_scores.csv"
    local report="dimension${dim}_report.txt"
    local description=$2

    echo "================================================================================"
    echo "STEP $dim/8: $description"
    echo "================================================================================"
    echo ""

    if [ ! -f "$scorer" ]; then
        echo -e "${RED}ERROR: $scorer not found${NC}"
        return 1
    fi

    python3 "$scorer" --input "$DATA_FILE" --output "$output" --report "$report"

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}$description complete${NC}"
        return 0
    else
        echo -e "${RED}$description failed${NC}"
        return 1
    fi
}

# Track results
TOTAL_STEPS=8
SUCCESSFUL_STEPS=0
FAILED_STEPS=0

# Run all 7 dimensions
if run_scorer 1 "DIMENSION 1 - PROFITABILITY"; then
    SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
else
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

if run_scorer 2 "DIMENSION 2 - FINANCIAL STRENGTH"; then
    SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
else
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

if run_scorer 3 "DIMENSION 3 - VALUATION"; then
    SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
else
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

if run_scorer 4 "DIMENSION 4 - GROWTH"; then
    SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
else
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

if run_scorer 5 "DIMENSION 5 - MANAGEMENT QUALITY"; then
    SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
else
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

if run_scorer 6 "DIMENSION 6 - BUSINESS QUALITY/MOAT"; then
    SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
else
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

if run_scorer 7 "DIMENSION 7 - MARKET SENTIMENT"; then
    SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
else
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

# Run composite scoring
echo ""
echo "================================================================================"
echo "STEP 8/8: COMPOSITE SCORING + SUPABASE UPLOAD"
echo "================================================================================"
echo ""

if [ -f "run_composite_scoring.sh" ]; then
    ./run_composite_scoring.sh

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Composite scoring + Supabase upload complete${NC}"
        SUCCESSFUL_STEPS=$((SUCCESSFUL_STEPS + 1))
    else
        echo -e "${RED}Composite scoring failed${NC}"
        FAILED_STEPS=$((FAILED_STEPS + 1))
    fi
else
    echo -e "${RED}run_composite_scoring.sh not found${NC}"
    FAILED_STEPS=$((FAILED_STEPS + 1))
fi

# Summary
echo ""
echo "================================================================================"
echo "WEEKLY SCORING SUMMARY"
echo "================================================================================"
echo "End Time: $(date)"
echo ""
echo "Results:"
echo "  Total Steps: $TOTAL_STEPS"
echo "  Successful: $SUCCESSFUL_STEPS"
echo "  Failed: $FAILED_STEPS"
echo ""

if [ $FAILED_STEPS -eq 0 ]; then
    echo -e "${GREEN}ALL STEPS COMPLETED SUCCESSFULLY!${NC}"
    echo ""
    echo "All 7 dimensions scored"
    echo "Composite scores calculated"
    echo "Watch list integration active"
    echo "Scores uploaded to Supabase"
    echo ""
    EXIT_CODE=0
else
    echo -e "${RED}$FAILED_STEPS STEP(S) FAILED${NC}"
    echo ""
    echo "Check log file: $LOG_FILE"
    echo ""
    EXIT_CODE=1
fi

echo "================================================================================"
echo "END OF WEEKLY SCORING"
echo "================================================================================"

# Cleanup old logs (keep 30 days)
find "$LOG_DIR" -name "weekly_scoring_*.log" -mtime +30 -delete 2>/dev/null || true

exit $EXIT_CODE
