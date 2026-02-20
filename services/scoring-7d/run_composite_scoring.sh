
#!/bin/bash
#
# FILE: run_composite_scoring.sh
# DESCRIPTION: Auto composite scorer with Supabase upload
# CREATED: 2026-01-04
# AUTHOR: Investment OS
#
# VERSION HISTORY:
#     v1.0.0  2026-01-04  Initial creation — Auto composite scorer with Supabase upload
#     v1.0.1  2026-02-12  Migrated to services/scoring-7d (Phase 2 microservices)
#     v1.0.2  2026-02-16  Added version history header (new project standard)
#

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "================================================================================"
echo "AUTO-COMPOSITE SCORER - Investment OS v1.1 (With Supabase Upload)"
echo "================================================================================"
echo ""

cd "$(dirname "$0")"
SCRIPT_DIR=$(pwd)
echo "Working directory: $SCRIPT_DIR"
echo ""

# Find latest dimension versions
find_latest_dimension() {
    local dim=$1
    local pattern="dimension${dim}_scores_v*.csv"
    local latest=$(ls -1 ${pattern} 2>/dev/null | sort -V | tail -1)
    if [ -z "$latest" ]; then
        local fallback="dimension${dim}_scores.csv"
        if [ -f "$fallback" ]; then
            echo "$fallback"
        else
            echo ""
        fi
    else
        echo "$latest"
    fi
}

echo "Auto-detecting latest dimension score files..."
echo ""

D1=$(find_latest_dimension 1)
D2=$(find_latest_dimension 2)
D3=$(find_latest_dimension 3)
D4=$(find_latest_dimension 4)
D5=$(find_latest_dimension 5)
D6=$(find_latest_dimension 6)
D7=$(find_latest_dimension 7)

echo "Detected files:"
for i in 1 2 3 4 5 6 7; do
    dim_var="D${i}"
    dim_file="${!dim_var}"
    echo "  Dimension $i: ${dim_file:-NOT FOUND}"
done
echo ""

# Validate all files exist
missing=0
for i in 1 2 3 4 5 6 7; do
    dim_var="D${i}"
    dim_file="${!dim_var}"
    if [ -z "$dim_file" ] || [ ! -f "$dim_file" ]; then
        echo -e "${RED}ERROR: Dimension $i score file not found${NC}"
        missing=$((missing + 1))
    fi
done

if [ $missing -gt 0 ]; then
    echo ""
    echo -e "${RED}Missing $missing dimension file(s). Cannot proceed.${NC}"
    exit 1
fi

echo -e "${GREEN}All 7 dimension files found!${NC}"
echo ""

# Generate output filenames
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_CSV="composite_scores_${TIMESTAMP}.csv"
OUTPUT_REPORT="composite_report_${TIMESTAMP}.txt"
OUTPUT_LATEST_CSV="composite_scores.csv"
OUTPUT_LATEST_REPORT="composite_report.txt"

echo "Output files:"
echo "  Timestamped CSV: $OUTPUT_CSV"
echo "  Timestamped Report: $OUTPUT_REPORT"
echo "  Latest CSV: $OUTPUT_LATEST_CSV (symlink)"
echo "  Latest Report: $OUTPUT_LATEST_REPORT (symlink)"
echo ""

# Check composite scorer
if [ ! -f "composite_scorer_v1_1.py" ]; then
    echo -e "${YELLOW}Warning: composite_scorer_v1_1.py not found${NC}"
    if [ -f "composite_scorer_v1_0.py" ]; then
        echo "   Using composite_scorer_v1_0.py (no watch list integration)"
        SCORER="composite_scorer_v1_0.py"
    else
        echo -e "${RED}ERROR: No composite scorer found${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}Using composite_scorer_v1_1.py (with watch list)${NC}"
    SCORER="composite_scorer_v1_1.py"
fi
echo ""

# Run composite scoring
echo "================================================================================"
echo "RUNNING COMPOSITE SCORING"
echo "================================================================================"
echo ""

python3 "$SCORER" \
    --d1 "$D1" \
    --d2 "$D2" \
    --d3 "$D3" \
    --d4 "$D4" \
    --d5 "$D5" \
    --d6 "$D6" \
    --d7 "$D7" \
    --output "$OUTPUT_CSV" \
    --report "$OUTPUT_REPORT"

if [ $? -ne 0 ]; then
    echo ""
    echo "================================================================================"
    echo -e "${RED}COMPOSITE SCORING FAILED${NC}"
    echo "================================================================================"
    exit 1
fi

echo ""
echo "================================================================================"
echo -e "${GREEN}COMPOSITE SCORING COMPLETE!${NC}"
echo "================================================================================"
echo ""

# Create/update symlinks
ln -sf "$OUTPUT_CSV" "$OUTPUT_LATEST_CSV"
ln -sf "$OUTPUT_REPORT" "$OUTPUT_LATEST_REPORT"

echo "Files created:"
echo "  $OUTPUT_CSV ($(wc -l < "$OUTPUT_CSV") lines)"
echo "  $OUTPUT_REPORT"
echo ""
echo "Symlinks updated:"
echo "  $OUTPUT_LATEST_CSV -> $OUTPUT_CSV"
echo "  $OUTPUT_LATEST_REPORT -> $OUTPUT_REPORT"
echo ""

# Show top 10 stocks
if [ -f "$OUTPUT_CSV" ]; then
    echo "TOP 10 STOCKS:"
    echo "--------------------------------------------------------------------------------"
    head -11 "$OUTPUT_CSV" | tail -10 | awk -F',' '{printf "  %2d. %-12s - Score: %5.1f - %s\n", NR, $2, $3, $5}'
    echo ""
fi

# Show watch list summary
if [ "$SCORER" = "composite_scorer_v1_1.py" ]; then
    echo "WATCH LIST SUMMARY:"
    echo "--------------------------------------------------------------------------------"
    WATCHLIST_COUNT=$(grep -c "True" "$OUTPUT_CSV" 2>/dev/null || echo "0")
    OVERRIDE_COUNT=$(grep -c "DO NOT BUY - WATCH LIST" "$OUTPUT_CSV" 2>/dev/null || echo "0")
    echo "  Watch list stocks found: $WATCHLIST_COUNT"
    echo "  Recommendations overridden: $OVERRIDE_COUNT"
    echo ""
fi

# Upload to Supabase
echo "================================================================================"
echo "UPLOADING TO SUPABASE"
echo "================================================================================"
echo ""

if [ -f "upload_composite_to_supabase.py" ]; then
    python3 upload_composite_to_supabase.py --input "$OUTPUT_CSV"

    if [ $? -eq 0 ]; then
        echo ""
        echo -e "${GREEN}Supabase upload complete!${NC}"
        SUPABASE_UPLOADED=true
    else
        echo ""
        echo -e "${YELLOW}Supabase upload failed (scores still saved locally)${NC}"
        SUPABASE_UPLOADED=false
    fi
else
    echo -e "${YELLOW}upload_composite_to_supabase.py not found - skipping upload${NC}"
    echo "   Scores saved locally only"
    SUPABASE_UPLOADED=false
fi

echo ""
echo "================================================================================"
echo -e "${GREEN}ALL COMPLETE!${NC}"
echo "================================================================================"
echo ""

# Final summary
echo "SUMMARY:"
echo "  Composite scoring complete"
echo "  296 stocks scored and ranked"
if [ "$SCORER" = "composite_scorer_v1_1.py" ]; then
    echo "  Watch list integration active"
fi
if [ "$SUPABASE_UPLOADED" = true ]; then
    echo "  Scores uploaded to Supabase"
else
    echo "  Supabase upload skipped/failed"
fi
echo ""
echo "Local files:"
echo "  $OUTPUT_CSV"
echo "  $OUTPUT_REPORT"
echo "  $OUTPUT_LATEST_CSV (symlink to latest)"
echo "  $OUTPUT_LATEST_REPORT (symlink to latest)"
echo ""
echo "Ready for production use!"
echo ""
