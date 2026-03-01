#!/bin/bash
# =============================================================================
# run_daily_signals.sh
# Investment OS NEW PLATFORM — Daily Signals Generator + Supabase Upload
#
# Location: /opt/investment-os/services/calendar-signals/run_daily_signals.sh
#
# Schedule: Mon–Fri 6:30 PM LKT
# Crontab:  0 13 * * 1-5   /opt/investment-os/services/calendar-signals/run_daily_signals.sh
#           (13:00 UTC = 6:30 PM LKT)
#
# Generator: daily_trading_workflow.py (new platform)
#   Loads CSE data from Supabase → generates tier1 signals → saves JSON
#   Saves signal JSON to: config.SIGNALS_DIR/investment_os_signals_YYYYMMDD_HHMMSS.json
#                         i.e. /opt/investment-os/signals/investment_os_signals_YYYYMMDD_HHMMSS.json
#   Shell uses glob to find today's latest file (date-stamped, no fixed timestamp).
#
# Writes: cse_daily_signals (upsert on signal_date)
# =============================================================================

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_DIR="/opt/investment-os/services/calendar-signals"
LOG_DIR="/opt/investment-os/v5_logs"
DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/daily_signals_${DATE}.log"
ENV_FILE="/opt/investment-os/.env"

# New platform generator (Python — called with python3, NOT bash)
WORKFLOW_SCRIPT="${SERVICE_DIR}/daily_trading_workflow.py"
SIGNALS_DIR="/opt/investment-os/signals"
DATE_COMPACT=$(date +%Y%m%d)
# Signal file is date+time stamped — find latest for today (|| true prevents set -e from killing script when no file exists yet)
SIGNAL_JSON=$(ls "${SIGNALS_DIR}/investment_os_signals_${DATE_COMPACT}_"*.json 2>/dev/null | sort | tail -1 || true)
SIGNAL_TXT=""   # no txt fallback for new platform generator

UPLOAD_SCRIPT="${SERVICE_DIR}/upload_daily_signals.py"
PYTHONPATH_NEW="/opt/investment-os/packages"

# ── Setup ─────────────────────────────────────────────────────────────────────
mkdir -p "${LOG_DIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }

log "============================================"
log "DAILY SIGNALS + UPLOAD — START"
log "============================================"
log "Date:      ${DATE}"
log "Log:       ${LOG_FILE}"
log "Generator: python3 ${WORKFLOW_SCRIPT}"
log "Signals:   ${SIGNALS_DIR}/investment_os_signals_${DATE_COMPACT}_*.json"
log "Upload:    ${UPLOAD_SCRIPT}"

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

# ── STEP 1: Run signal generator (new platform) ───────────────────────────────
log ""
log "============================================"
log "STEP 1: Running Daily Signal Generator"
log "============================================"

if [ -f "${WORKFLOW_SCRIPT}" ]; then
  cd "${SERVICE_DIR}"   # daily_trading_workflow.py imports local module tier1_signal_generator
  PYTHONPATH="${PYTHONPATH_NEW}" python3 "${WORKFLOW_SCRIPT}" 2>&1 | tee -a "${LOG_FILE}"
  WORKFLOW_EXIT=${PIPESTATUS[0]}
  if [ ${WORKFLOW_EXIT} -ne 0 ]; then
    log "❌ Workflow exited with code ${WORKFLOW_EXIT}"
    exit ${WORKFLOW_EXIT}
  fi
  log "✅ Signal generator complete"
else
  log "⚠  Workflow script not found at ${WORKFLOW_SCRIPT} — skipping (upload-only mode)"
fi

# ── STEP 2: Upload to Supabase (new platform) ─────────────────────────────────
log ""
log "============================================"
log "STEP 2: Uploading to Supabase (new platform)"
log "============================================"

# Re-resolve glob after generator has run (in case it wasn't set before)
if [ -z "${SIGNAL_JSON}" ]; then
  SIGNAL_JSON=$(ls "${SIGNALS_DIR}/investment_os_signals_${DATE_COMPACT}_"*.json 2>/dev/null | sort | tail -1)
fi

if [ -n "${SIGNAL_JSON}" ] && [ -f "${SIGNAL_JSON}" ]; then
  SIGNAL_ARG="--signal-json ${SIGNAL_JSON}"
  log "Signal source: ${SIGNAL_JSON} (JSON)"
else
  log "❌ No signal file found matching ${SIGNALS_DIR}/investment_os_signals_${DATE_COMPACT}_*.json"
  exit 1
fi

PYTHONPATH="${PYTHONPATH_NEW}" python3 "${UPLOAD_SCRIPT}" \
  ${SIGNAL_ARG} \
  --date "${DATE}" \
  2>&1 | tee -a "${LOG_FILE}"
UPLOAD_EXIT=${PIPESTATUS[0]}

if [ ${UPLOAD_EXIT} -ne 0 ]; then
  log "❌ Upload failed with code ${UPLOAD_EXIT}"
  exit ${UPLOAD_EXIT}
fi

log ""
log "============================================"
log "DAILY SIGNALS + UPLOAD — DONE"
log "============================================"

exit 0
