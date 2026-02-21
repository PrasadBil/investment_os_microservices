#!/bin/bash
#
# FILE: run_d7_comparison.sh
# DESCRIPTION: Standalone D7 v1.0 vs v2.0 comparison runner — on-demand or scheduled
# CREATED: 2026-02-21
# AUTHOR: Investment OS
#
# VERSION HISTORY:
#     v1.0.0  2026-02-21  Initial creation — standalone comparison outside weekly pipeline
#
# PURPOSE:
#   - Run Phase 1B-D scorer to regenerate fresh v2.0 scores
#   - Compare v1.0 (production) vs v2.0 (parallel) scores
#   - Save dated comparison CSV + report to comparisons/
#   - Can be run manually any time without triggering full weekly pipeline
#
# USAGE:
#   ./run_d7_comparison.sh                        # Auto-detect data, save report
#   ./run_d7_comparison.sh --data cse_metrics.csv # Specify input data file
#   ./run_d7_comparison.sh --no-rescore           # Compare existing CSVs (skip rescoring)
#
# SCHEDULE (optional — add to crontab for weekly validation):
#   0 18 * * 6   /opt/investment-os/services/scoring-7d/run_d7_comparison.sh >> /opt/investment-os/v5_logs/d7_comparison_cron.log 2>&1
#

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================

WORK_DIR="/opt/investment-os"
SERVICE_DIR="${WORK_DIR}/services/scoring-7d"
OUTPUT_DIR="${WORK_DIR}/output"
COMPARISON_DIR="${WORK_DIR}/comparisons"
LOG_DIR="${WORK_DIR}/v5_logs"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATE=$(date +"%Y-%m-%d")
LOG_FILE="${LOG_DIR}/d7_comparison_${TIMESTAMP}.log"
COMPARISON_PREFIX="${COMPARISON_DIR}/d7_comparison_${DATE}"

# Default data file: auto-detect latest cleaned_data.csv
DATA_FILE=""
SKIP_RESCORE=false

# =============================================================================
# PARSE ARGS
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --data)
            DATA_FILE="$2"
            shift 2
            ;;
        --no-rescore)
            SKIP_RESCORE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--data <path>] [--no-rescore]"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

# =============================================================================
# SETUP
# =============================================================================

mkdir -p "$LOG_DIR" "$OUTPUT_DIR" "$COMPARISON_DIR"
exec 1> >(tee -a "$LOG_FILE") 2>&1

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

log_section() {
    echo ""
    echo "========================================"
    echo "$1"
    echo "========================================"
}

log_section "D7 COMPARISON RUNNER  —  $DATE"
log "Log: $LOG_FILE"
log "Comparison output prefix: $COMPARISON_PREFIX"

cd "$SERVICE_DIR"

# =============================================================================
# STEP 1: LOCATE DATA
# =============================================================================

if [ "$SKIP_RESCORE" = false ]; then
    log_section "STEP 1: Locate Latest CSE Data"

    if [ -n "$DATA_FILE" ] && [ -f "$DATA_FILE" ]; then
        log "Using specified data file: $DATA_FILE"
    else
        # Try cse_metrics.csv in service dir first
        if [ -f "cse_metrics.csv" ]; then
            DATA_FILE="cse_metrics.csv"
            log "Found cse_metrics.csv in service dir"
        else
            # Auto-detect from output folder
            DATA_FILE=$(ls -t ${OUTPUT_DIR}/*/cleaned_data.csv 2>/dev/null | head -1)
            if [ -z "$DATA_FILE" ]; then
                log "WARNING: No data file found. Will skip Phase 1B-D rescore."
                log "   Searched: cse_metrics.csv, ${OUTPUT_DIR}/*/cleaned_data.csv"
                SKIP_RESCORE=true
            else
                log "Auto-detected data: $DATA_FILE"
            fi
        fi
    fi
fi

# =============================================================================
# STEP 2: RESCORE D7 V2.0 PHASE 1B-D (unless skipped)
# =============================================================================

if [ "$SKIP_RESCORE" = false ]; then
    log_section "STEP 2: Regenerate D7 v2.0 Phase 1B-D Scores"

    if [ -f "dimension7_scorer_v2_0_phase1bcd.py" ]; then
        log "Running Phase 1B-D scorer (5 components)..."
        log "   Input:  $DATA_FILE"
        log "   Output: dimension7_v2_scores.csv"

        python3 dimension7_scorer_v2_0_phase1bcd.py \
            --input "$DATA_FILE" \
            --output "dimension7_v2_scores.csv" \
            --report "dimension7_v2_report.txt"

        if [ $? -eq 0 ] && [ -f "dimension7_v2_scores.csv" ]; then
            V2_COUNT=$(tail -n +2 "dimension7_v2_scores.csv" | wc -l)
            log "Phase 1B-D scoring complete: $V2_COUNT stocks scored"
            # Backup to output dir
            cp "dimension7_v2_scores.csv" "${OUTPUT_DIR}/dimension7_v2_scores_${DATE}.csv"
            cp "dimension7_v2_report.txt" "${OUTPUT_DIR}/dimension7_v2_report_${DATE}.txt"
        else
            log "ERROR: Phase 1B-D scorer failed"
            log "Falling back to existing dimension7_v2_scores.csv if available..."
        fi

    elif [ -f "dimension7_scorer_v2_0_phase1a.py" ]; then
        log "WARNING: Phase 1B-D scorer not found. Falling back to Phase 1A (3 components)."
        python3 dimension7_scorer_v2_0_phase1a.py \
            --input "$DATA_FILE" \
            --output "dimension7_v2_scores.csv" \
            --report "dimension7_v2_report.txt"
    else
        log "ERROR: No v2.0 scorer found. Comparing existing CSVs."
        SKIP_RESCORE=true
    fi
else
    log_section "STEP 2: Skipped (--no-rescore)"
    log "Using existing dimension7_v2_scores.csv"
fi

# =============================================================================
# STEP 3: VERIFY INPUT FILES
# =============================================================================

log_section "STEP 3: Verify Input Files"

if [ ! -f "dimension7_scores.csv" ]; then
    log "ERROR: dimension7_scores.csv (v1.0) not found."
    log "   Run the weekly v1.0 scorer first: python3 dimension7_scorer.py"
    exit 1
fi

if [ ! -f "dimension7_v2_scores.csv" ]; then
    log "ERROR: dimension7_v2_scores.csv (v2.0) not found."
    log "   Re-run without --no-rescore, or generate scores manually."
    exit 1
fi

V1_COUNT=$(tail -n +2 "dimension7_scores.csv" | wc -l)
V2_COUNT=$(tail -n +2 "dimension7_v2_scores.csv" | wc -l)
log "v1.0 stocks: $V1_COUNT"
log "v2.0 stocks: $V2_COUNT"

# =============================================================================
# STEP 4: RUN COMPARISON
# =============================================================================

log_section "STEP 4: Run D7 Comparison"

if [ -f "compare_d7_versions.py" ]; then
    log "Running comparison script..."
    log "   v1.0 file: dimension7_scores.csv"
    log "   v2.0 file: dimension7_v2_scores.csv"
    log "   Saving to: ${COMPARISON_PREFIX}"

    python3 compare_d7_versions.py \
        dimension7_scores.csv \
        dimension7_v2_scores.csv \
        --save \
        --output-prefix "$COMPARISON_PREFIX"

    if [ $? -eq 0 ]; then
        log "Comparison complete"
        if [ -f "${COMPARISON_PREFIX}_report.txt" ]; then
            log "Report: ${COMPARISON_PREFIX}_report.txt"
        fi
        if [ -f "${COMPARISON_PREFIX}_merged.csv" ]; then
            log "Merged CSV: ${COMPARISON_PREFIX}_merged.csv"
        fi
    else
        log "ERROR: Comparison script failed"
        exit 1
    fi
else
    log "ERROR: compare_d7_versions.py not found"
    exit 1
fi

# =============================================================================
# STEP 5: SUMMARY
# =============================================================================

log_section "SUMMARY"
log "D7 Comparison Runner — COMPLETE"
log ""
log "Outputs:"
log "  Report:       ${COMPARISON_PREFIX}_report.txt"
log "  Merged CSV:   ${COMPARISON_PREFIX}_merged.csv"
log "  Full log:     $LOG_FILE"
log ""
log "Next step: Review report, then run full weekly_scoring_parallel.sh on Saturday"

# Cleanup old comparison files > 90 days
find "$COMPARISON_DIR" -name "d7_comparison_*" -mtime +90 -delete 2>/dev/null || true

exit 0
