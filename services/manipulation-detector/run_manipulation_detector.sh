#!/bin/bash
# =============================================================================
# run_manipulation_detector.sh
# Investment OS NEW PLATFORM — v5.0 Manipulation Detector + Supabase Upload
#
# Location: /opt/investment-os/services/manipulation-detector/run_manipulation_detector.sh
#
# Schedule: Mon–Fri 7:00 PM LKT
# Crontab:  30 13 * * 1-5   /opt/investment-os/services/manipulation-detector/run_manipulation_detector.sh
#           (13:30 UTC = 7:00 PM LKT)
#
# Detector: run_v5_detector.sh (new platform)
#   Saves report to: /opt/investment-os/v5_reports/v5_report_YYYY-MM-DD.json
#
# Reads:  cse_daily_prices (296 stocks)
# Writes: cse_manipulation_signals (upsert on run_date+symbol)
# =============================================================================

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_DIR="/opt/investment-os/services/manipulation-detector"
LOG_DIR="/opt/investment-os/v5_logs"
DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/manipulation_detector_${DATE}.log"
ENV_FILE="/opt/investment-os/.env"

# New platform detector
DETECTOR_SCRIPT="${SERVICE_DIR}/run_v5_detector.sh"
REPORT_JSON="/opt/investment-os/v5_reports/v5_report_${DATE}.json"
REPORT_TXT="/opt/investment-os/v5_reports/v5_report_${DATE}.txt"

UPLOAD_SCRIPT="${SERVICE_DIR}/upload_manipulation_signals.py"
PYTHONPATH_NEW="/opt/investment-os/packages"

# ── Setup ─────────────────────────────────────────────────────────────────────
mkdir -p "${LOG_DIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }

log "============================================"
log "MANIPULATION DETECTOR + UPLOAD — START"
log "============================================"
log "Date:       ${DATE}"
log "Log:        ${LOG_FILE}"
log "Detector:   ${DETECTOR_SCRIPT}"
log "Upload:     ${UPLOAD_SCRIPT}"

# ── Safe .env loading (never use source .env) ─────────────────────────────────
if [ -f "${ENV_FILE}" ]; then
  log "✅ Loading env from ${ENV_FILE}"
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    if [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
      export "$line" 2>/dev/null || true
    fi
  done < "${ENV_FILE}"
else
  log "⚠  No .env at ${ENV_FILE}"
fi

# ── Pre-flight ────────────────────────────────────────────────────────────────
if [ ! -f "${UPLOAD_SCRIPT}" ]; then
  log "❌ Upload script not found: ${UPLOAD_SCRIPT}"
  exit 1
fi

# ── STEP 1: Run detector (new platform) ───────────────────────────────────────
log ""
log "============================================"
log "STEP 1: Running v5 Manipulation Detector"
log "============================================"

if [ -f "${DETECTOR_SCRIPT}" ]; then
  bash "${DETECTOR_SCRIPT}" 2>&1 | tee -a "${LOG_FILE}"
  DETECTOR_EXIT=${PIPESTATUS[0]}
  if [ ${DETECTOR_EXIT} -ne 0 ]; then
    log "❌ Detector exited with code ${DETECTOR_EXIT}"
    exit ${DETECTOR_EXIT}
  fi
  log "✅ Detector complete"
else
  log "⚠  Detector script not found at ${DETECTOR_SCRIPT} — skipping (upload-only mode)"
fi

# ── STEP 2: Upload to Supabase (new platform) ─────────────────────────────────
log ""
log "============================================"
log "STEP 2: Uploading to Supabase (new platform)"
log "============================================"

# Prefer JSON report; fall back to TXT
if [ -f "${REPORT_JSON}" ]; then
  REPORT_PATH="${REPORT_JSON}"
  log "Report: ${REPORT_PATH} (JSON)"
elif [ -f "${REPORT_TXT}" ]; then
  REPORT_PATH="${REPORT_TXT}"
  log "Report: ${REPORT_PATH} (TXT fallback)"
else
  log "❌ No report found at ${REPORT_JSON} or ${REPORT_TXT}"
  exit 1
fi

PYTHONPATH="${PYTHONPATH_NEW}" python3 "${UPLOAD_SCRIPT}" \
  --report "${REPORT_PATH}" \
  --date "${DATE}" \
  2>&1 | tee -a "${LOG_FILE}"
UPLOAD_EXIT=${PIPESTATUS[0]}

if [ ${UPLOAD_EXIT} -ne 0 ]; then
  log "❌ Upload failed with code ${UPLOAD_EXIT}"
  exit ${UPLOAD_EXIT}
fi

log ""
log "============================================"
log "MANIPULATION DETECTOR + UPLOAD — DONE"
log "============================================"

exit 0
