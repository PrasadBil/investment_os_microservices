
#!/bin/bash
#
# FILE: weekly_scoring_parallel.sh
# DESCRIPTION: Weekly scoring automation — parallel v1.0 & v2.0 deployment
# CREATED: 2026-01-07
# AUTHOR: Investment OS
# SCHEDULE: Every Saturday 6:00 PM (after data collection)
#
# VERSION HISTORY:
#     v1.0.0  2026-01-07  Initial creation — parallel v1/v2 scoring pipeline
#     v1.0.1  2026-02-10  Migrated to services/scoring-7d (Phase 2 microservices)
#     v1.1.0  2026-02-15  Updated Step 3 to use Phase 1B-D scorer (5 components)
#     v1.2.0  2026-02-16  Fix: Timestamped output filenames (was writing to symlinks)
#                          Fix: Composite report/CSV use execution-date names
#                          Added: Version history header (new project standard)
#
# PURPOSE:
# - Run v1.0 (production) for composite scorer
# - Run v2.0 (parallel) for validation
# - Generate comparison report
# - Upload composite scores to Supabase
# - No disruption to production
#
# ORIGINAL: /opt/selenium_automation/weekly_scoring_parallel.sh
#

set -e  # Exit on error

# =============================================================================
# CONFIGURATION (Phase 2: Updated paths)
# =============================================================================

WORK_DIR="/opt/investment-os"
SERVICE_DIR="${WORK_DIR}/services/scoring-7d"
LOG_DIR="${WORK_DIR}/v5_logs"
OUTPUT_DIR="${WORK_DIR}/output"
COMPARISON_DIR="${WORK_DIR}/comparisons"

# Create directories if they don't exist
mkdir -p "$LOG_DIR"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$COMPARISON_DIR"

# Timestamp for this run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATE=$(date +"%Y-%m-%d")
LOG_FILE="$LOG_DIR/weekly_scoring_$TIMESTAMP.log"

# =============================================================================
# LOGGING
# =============================================================================

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log_section() {
    echo "" | tee -a "$LOG_FILE"
    echo "========================================" | tee -a "$LOG_FILE"
    echo "$1" | tee -a "$LOG_FILE"
    echo "========================================" | tee -a "$LOG_FILE"
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

log_section "WEEKLY SCORING START - $DATE"

cd "$SERVICE_DIR"

# Step 1: Find latest data
log_section "STEP 1: Locate Latest Data"

# Auto-detect latest data file
LATEST_DATA=$(ls -t ${OUTPUT_DIR}/*/cleaned_data.csv 2>/dev/null | head -1)

if [ -z "$LATEST_DATA" ]; then
    log "ERROR: No cleaned data found in ${OUTPUT_DIR}/*/"
    log "   Expected: ${OUTPUT_DIR}/YYYY-MM-DD/cleaned_data.csv"
    exit 1
fi

log "Found latest data: $LATEST_DATA"

# Also need CSE metrics
if [ ! -f "cse_metrics.csv" ]; then
    log "WARNING: cse_metrics.csv not found"
    log "   Generating CSE metrics..."

    python3 cse_data_connector.py --output cse_metrics.csv >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ]; then
        log "CSE metrics generated"
    else
        log "ERROR: Failed to generate CSE metrics"
        exit 1
    fi
fi

# =============================================================================
# STEP 2: RUN DIMENSION 7 V1.0 (PRODUCTION)
# =============================================================================

log_section "STEP 2: Dimension 7 v1.0 (PRODUCTION)"

if [ -f "dimension7_scorer.py" ]; then
    log "Running v1.0 scorer..."

    python3 dimension7_scorer.py \
        --input "$LATEST_DATA" \
        --output dimension7_scores.csv \
        >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ] && [ -f "dimension7_scores.csv" ]; then
        V1_COUNT=$(wc -l < dimension7_scores.csv)
        log "v1.0 scoring complete: $V1_COUNT stocks"

        # Backup v1.0 scores
        cp dimension7_scores.csv "$OUTPUT_DIR/dimension7_scores_$DATE.csv"
    else
        log "ERROR: v1.0 scoring failed"
        exit 1
    fi
else
    log "WARNING: dimension7_scorer.py not found"
    log "   Skipping v1.0 (will use existing scores if available)"
fi

# =============================================================================
# STEP 3: RUN DIMENSION 7 V2.0 (PARALLEL)
# =============================================================================

log_section "STEP 3: Dimension 7 v2.0 Phase 1B-D (PARALLEL)"

if [ -f "dimension7_scorer_v2_0_phase1bcd.py" ]; then
    log "Running v2.0 Phase 1B-D scorer (5 components)..."

    python3 dimension7_scorer_v2_0_phase1bcd.py \
        --input cse_metrics.csv \
        --output dimension7_v2_scores.csv \
        --report dimension7_v2_report.txt \
        >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ] && [ -f "dimension7_v2_scores.csv" ]; then
        V2_COUNT=$(wc -l < dimension7_v2_scores.csv)
        log "v2.0 Phase 1B-D scoring complete: $V2_COUNT stocks"

        # Backup v2.0 scores
        cp dimension7_v2_scores.csv "$OUTPUT_DIR/dimension7_v2_scores_$DATE.csv"
        cp dimension7_v2_report.txt "$OUTPUT_DIR/dimension7_v2_report_$DATE.txt"
    else
        log "ERROR: v2.0 scoring failed"
        # Don't exit - v1.0 is what matters for production
    fi
elif [ -f "dimension7_scorer_v2_0_phase1a.py" ]; then
    log "WARNING: Phase 1B-D scorer not found, falling back to Phase 1A"
    python3 dimension7_scorer_v2_0_phase1a.py \
        --input cse_metrics.csv \
        --output dimension7_v2_scores.csv \
        --report dimension7_v2_report.txt \
        >> "$LOG_FILE" 2>&1
    if [ $? -eq 0 ] && [ -f "dimension7_v2_scores.csv" ]; then
        V2_COUNT=$(wc -l < dimension7_v2_scores.csv)
        log "v2.0 Phase 1A (fallback) scoring complete: $V2_COUNT stocks"
        cp dimension7_v2_scores.csv "$OUTPUT_DIR/dimension7_v2_scores_$DATE.csv"
    else
        log "ERROR: v2.0 fallback scoring failed"
    fi
else
    log "ERROR: No v2.0 scorer found (neither phase1bcd nor phase1a)"
    log "   Skipping v2.0 parallel scoring"
fi

# =============================================================================
# STEP 4: COMPARISON REPORT
# =============================================================================

log_section "STEP 4: v1.0 vs v2.0 Comparison"

if [ -f "dimension7_scores.csv" ] && [ -f "dimension7_v2_scores.csv" ] && [ -f "compare_d7_versions.py" ]; then
    log "Generating comparison report..."

    COMPARISON_FILE="$COMPARISON_DIR/comparison_$DATE.txt"

    python3 compare_d7_versions.py \
        dimension7_scores.csv \
        dimension7_v2_scores.csv \
        > "$COMPARISON_FILE" 2>&1

    if [ $? -eq 0 ]; then
        log "Comparison report generated: $COMPARISON_FILE"
    else
        log "WARNING: Comparison failed (non-critical)"
    fi
else
    log "Skipping comparison (missing files or script)"
fi

# =============================================================================
# STEP 5: RUN DIMENSIONS 1-6 (EXISTING)
# =============================================================================

log_section "STEP 5: Dimensions 1-6 Scoring"

# Run all other dimension scorers
for i in 1 2 3 4 5 6; do
    SCORER="dimension${i}_scorer.py"
    OUTPUT="dimension${i}_scores.csv"

    if [ -f "$SCORER" ]; then
        log "Running Dimension $i..."

        python3 "$SCORER" \
            --input "$LATEST_DATA" \
            --output "$OUTPUT" \
            >> "$LOG_FILE" 2>&1

        if [ $? -eq 0 ]; then
            log "Dimension $i complete"
        else
            log "ERROR: Dimension $i failed"
            exit 1
        fi
    else
        log "WARNING: $SCORER not found"
    fi
done

# =============================================================================
# STEP 6: COMPOSITE SCORING (USES v1.0)
# =============================================================================

log_section "STEP 6: Composite Scoring (Production)"

# v1.2.0: Use timestamped filenames — prevents symlink overwrites
COMPOSITE_CSV="composite_scores_${TIMESTAMP}.csv"
COMPOSITE_REPORT="composite_report_${TIMESTAMP}.txt"

if [ -f "composite_scorer_v1_1.py" ]; then
    log "Running composite scorer v1.1 (with watch list)..."
    log "IMPORTANT: Using v1.0 dimension 7 scores (production unchanged)"

    python3 composite_scorer_v1_1.py \
        --d1 dimension1_scores.csv \
        --d2 dimension2_scores.csv \
        --d3 dimension3_scores.csv \
        --d4 dimension4_scores.csv \
        --d5 dimension5_scores.csv \
        --d6 dimension6_scores.csv \
        --d7 dimension7_scores.csv \
        --output "$COMPOSITE_CSV" \
        --report "$COMPOSITE_REPORT" \
        >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ] && [ -f "$COMPOSITE_CSV" ]; then
        COMPOSITE_FILE="$COMPOSITE_CSV"
        COMPOSITE_COUNT=$(tail -n +2 "$COMPOSITE_FILE" | wc -l)
        log "Composite scoring complete: $COMPOSITE_COUNT stocks"
        log "   CSV:    $COMPOSITE_CSV"
        log "   Report: $COMPOSITE_REPORT"

        # Update symlinks to latest
        ln -sf "$COMPOSITE_CSV" composite_scores.csv
        ln -sf "$COMPOSITE_REPORT" composite_report.txt

        # Backup composite scores
        cp "$COMPOSITE_FILE" "$OUTPUT_DIR/composite_scores_$DATE.csv"
        cp "$COMPOSITE_REPORT" "$OUTPUT_DIR/composite_report_$DATE.txt"
    else
        log "ERROR: Composite scoring failed"
        exit 1
    fi
elif [ -f "composite_scorer_v1_0.py" ]; then
    log "Running composite scorer v1.0 (no watch list)..."
    log "IMPORTANT: Using v1.0 dimension 7 scores (production unchanged)"

    python3 composite_scorer_v1_0.py \
        --d1 dimension1_scores.csv \
        --d2 dimension2_scores.csv \
        --d3 dimension3_scores.csv \
        --d4 dimension4_scores.csv \
        --d5 dimension5_scores.csv \
        --d6 dimension6_scores.csv \
        --d7 dimension7_scores.csv \
        --output "$COMPOSITE_CSV" \
        --report "$COMPOSITE_REPORT" \
        >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ] && [ -f "$COMPOSITE_CSV" ]; then
        COMPOSITE_FILE="$COMPOSITE_CSV"
        COMPOSITE_COUNT=$(tail -n +2 "$COMPOSITE_FILE" | wc -l)
        log "Composite scoring complete: $COMPOSITE_COUNT stocks"
        log "   CSV:    $COMPOSITE_CSV"
        log "   Report: $COMPOSITE_REPORT"

        ln -sf "$COMPOSITE_CSV" composite_scores.csv
        ln -sf "$COMPOSITE_REPORT" composite_report.txt

        cp "$COMPOSITE_FILE" "$OUTPUT_DIR/composite_scores_$DATE.csv"
        cp "$COMPOSITE_REPORT" "$OUTPUT_DIR/composite_report_$DATE.txt"
    else
        log "ERROR: Composite scoring failed"
        exit 1
    fi
else
    log "ERROR: No composite scorer found"
    exit 1
fi

# =============================================================================
# STEP 7: UPLOAD TO SUPABASE (PRODUCTION)
# =============================================================================

log_section "STEP 7: Supabase Upload"

if [ -f "upload_composite_to_supabase.py" ]; then
    log "Uploading composite scores to Supabase..."
    log "   Input file: $COMPOSITE_FILE"

    python3 upload_composite_to_supabase.py \
        --input "$COMPOSITE_FILE" \
        --date "$DATE" \
        >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ]; then
        log "Supabase upload complete"
    else
        log "ERROR: Supabase upload failed"
        log "   Check log for details: $LOG_FILE"
        exit 1
    fi
else
    log "WARNING: upload_composite_to_supabase.py not found"
fi

# =============================================================================
# STEP 8: CLEANUP & SUMMARY
# =============================================================================

log_section "STEP 8: Summary"

log "Weekly Scoring Complete!"
log ""
log "Production (v1.0):"
log "  Dimension 7 v1.0: dimension7_scores.csv"
log "  Composite CSV:    $COMPOSITE_CSV"
log "  Composite Report: $COMPOSITE_REPORT"
log "  Uploaded to Supabase"
log ""
log "Parallel Validation (v2.0):"
if [ -f "dimension7_v2_scores.csv" ]; then
    log "  Dimension 7 v2.0: dimension7_v2_scores.csv"
    log "  Comparison report: $COMPARISON_DIR/comparison_$DATE.txt"
else
    log "  v2.0 not run (check errors above)"
fi
log ""
log "Backups saved to: $OUTPUT_DIR"
log "Complete log: $LOG_FILE"

# Keep only last 30 days of logs
find "$LOG_DIR" -name "weekly_scoring_*.log" -mtime +30 -delete 2>/dev/null || true

# Keep only last 90 days of backups
find "$OUTPUT_DIR" -name "*_scores_*.csv" -mtime +90 -delete 2>/dev/null || true

log_section "WEEKLY SCORING COMPLETE - SUCCESS"

exit 0
