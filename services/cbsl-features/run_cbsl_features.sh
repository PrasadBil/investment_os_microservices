#!/bin/bash
#
# FILE: run_cbsl_features.sh
# DESCRIPTION: Cron runner for CBSL macro feature build pipeline
# CREATED: 2026-02-21
# AUTHOR: Investment OS
#
# VERSION HISTORY:
#     v1.0.0  2026-02-21  Initial creation
#
# SCHEDULE (add to VPS crontab):
#   Saturday 8:00 AM SLK — runs after Friday 8:30 PM CBSL weekly parse has completed
#   0 8 * * 6   /opt/investment-os/services/cbsl-features/run_cbsl_features.sh >> /opt/investment-os/v5_logs/cbsl_features_cron.log 2>&1
#
# WHY SATURDAY 8 AM:
#   Friday  8:30 PM → CBSL weekly parser runs (4 sector tables populated)
#   Saturday 8 AM   → Features built from fresh weekly data
#   Saturday ~8:30 AM → Granger causality service can use fresh features (if cron chained)
#   Saturday 6:30 PM → Weekly scoring pipeline runs (can read cbsl_macro_features from Supabase)
#
# USAGE:
#   ./run_cbsl_features.sh              # standard run (104 weeks)
#   ./run_cbsl_features.sh --full       # full history (260 weeks / 5 years)
#   ./run_cbsl_features.sh --no-upload  # CSV only, skip Supabase
#   ./run_cbsl_features.sh --dry-run    # validate only, no writes
#

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================

WORK_DIR="/opt/investment-os"
SERVICE_DIR="${WORK_DIR}/services/cbsl-features"
LOG_DIR="${WORK_DIR}/v5_logs"
OUTPUT_DIR="${WORK_DIR}/output/cbsl_features"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATE=$(date +"%Y-%m-%d")
LOG_FILE="${LOG_DIR}/cbsl_features_${TIMESTAMP}.log"

# =============================================================================
# PARSE ARGS
# =============================================================================

WEEKS=104
EXTRA_FLAGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --full)
            WEEKS=260
            shift
            ;;
        --no-upload)
            EXTRA_FLAGS="$EXTRA_FLAGS --no-upload"
            shift
            ;;
        --dry-run)
            EXTRA_FLAGS="$EXTRA_FLAGS --dry-run"
            shift
            ;;
        --weeks)
            WEEKS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--full] [--no-upload] [--dry-run] [--weeks N]"
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

mkdir -p "$LOG_DIR" "$OUTPUT_DIR"
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

log_section "CBSL MACRO FEATURE BUILD  —  ${DATE}"
log "Log: ${LOG_FILE}"
log "Window: ${WEEKS} weeks"
log "Output: ${OUTPUT_DIR}"
log "Flags: ${EXTRA_FLAGS:-<none>}"

# =============================================================================
# STEP 1: ENVIRONMENT CHECK
# =============================================================================

log_section "STEP 1: Environment Check"

cd "$SERVICE_DIR"

# Check .env
ENV_FILE="${WORK_DIR}/.env"
if [ -f "$ENV_FILE" ]; then
    log "Loading .env from ${ENV_FILE}"
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    log "WARNING: .env not found at ${ENV_FILE} — relying on environment variables"
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    log "ERROR: python3 not found"
    exit 1
fi
PYTHON_VER=$(python3 --version 2>&1)
log "Python: ${PYTHON_VER}"

# Check key file
if [ ! -f "cbsl_master_feature_builder.py" ]; then
    log "ERROR: cbsl_master_feature_builder.py not found in ${SERVICE_DIR}"
    exit 1
fi

# Check dependencies
python3 -c "import pandas, numpy, supabase, dotenv" 2>/dev/null || {
    log "WARNING: Some dependencies may be missing. Installing..."
    pip3 install pandas numpy supabase python-dotenv --quiet --break-system-packages || true
}

log "Environment check passed"

# =============================================================================
# STEP 2: RUN MASTER FEATURE BUILDER
# =============================================================================

log_section "STEP 2: Run cbsl_master_feature_builder"

PYTHONPATH="${WORK_DIR}/packages:${PYTHONPATH}" \
python3 cbsl_master_feature_builder.py \
    --weeks "${WEEKS}" \
    --output-dir "${OUTPUT_DIR}" \
    ${EXTRA_FLAGS}

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    log "ERROR: Master feature builder exited with code ${EXIT_CODE}"
    exit $EXIT_CODE
fi

log "Master feature builder completed successfully"

# =============================================================================
# STEP 3: VERIFY OUTPUT
# =============================================================================

log_section "STEP 3: Verify Output"

LATEST="${OUTPUT_DIR}/MASTER_CBSL_FEATURES_LATEST.csv"
DATED="${OUTPUT_DIR}/MASTER_CBSL_FEATURES_${DATE}.csv"

if [ -f "$LATEST" ]; then
    ROW_COUNT=$(tail -n +2 "$LATEST" | wc -l)
    COL_COUNT=$(head -1 "$LATEST" | tr ',' '\n' | wc -l)
    log "Latest CSV: ${LATEST}"
    log "  Rows:    ${ROW_COUNT}"
    log "  Columns: ${COL_COUNT}"
else
    log "WARNING: MASTER_CBSL_FEATURES_LATEST.csv not found in output dir"
fi

if [ -f "$DATED" ]; then
    log "Dated archive: ${DATED} ✓"
fi

# =============================================================================
# STEP 4: CLEANUP (keep last 90 days)
# =============================================================================

log_section "STEP 4: Cleanup"
find "$OUTPUT_DIR" -name "MASTER_CBSL_FEATURES_*.csv" \
     ! -name "MASTER_CBSL_FEATURES_LATEST.csv" \
     -mtime +90 -delete 2>/dev/null || true

find "$LOG_DIR" -name "cbsl_features_*.log" -mtime +30 -delete 2>/dev/null || true
log "Cleanup complete"

# =============================================================================
# SUMMARY
# =============================================================================

log_section "CBSL MACRO FEATURE BUILD — COMPLETE"
log "Run date:    ${DATE}"
log "Window:      ${WEEKS} weeks"
log "Output dir:  ${OUTPUT_DIR}"
log "Log:         ${LOG_FILE}"
log ""
log "Next steps:"
log "  → Granger causality service can now query cbsl_macro_features table"
log "  → Saturday 6:30 PM: weekly scoring pipeline reads macro context"
log "  → Monitor Supabase: SELECT COUNT(*), MAX(date) FROM cbsl_macro_features;"

exit 0
