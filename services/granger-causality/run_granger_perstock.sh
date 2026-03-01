
#!/bin/bash
# =============================================================================
# run_granger_perstock.sh
# Investment OS NEW PLATFORM — Granger Causality Per-Stock Analysis (v5)
#
# Location: /opt/investment-os/services/granger-causality/run_granger_perstock.sh
#
# Schedule: Saturday 10:00 AM LKT (UTC+5:30)
# Crontab:  0 4 * * 6   /opt/investment-os/services/granger-causality/run_granger_perstock.sh
#           (4:00 AM UTC = 10:00 AM LKT)
#
# Reads:    cse_daily_prices (296 stocks, all OHLCV history)
# Writes:   tier1_granger_results (789 rows: 3 tests × 296 stocks)
#           ~46 seconds runtime
# =============================================================================

set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
SCRIPT_DIR="/opt/investment-os/services/granger-causality"
PYTHON_SCRIPT="${SCRIPT_DIR}/tier1_granger_per_stock_v5.py"
LOG_DIR="/opt/investment-os/v5_logs"          # ← same dir as all other v5 logs
DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/granger_perstock_${DATE}.log"
ENV_FILE="/opt/investment-os/.env"

# ── Setup ────────────────────────────────────────────────────────────────────
mkdir -p "${LOG_DIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }

log "======================================"
log "GRANGER PER-STOCK ANALYSIS v5 — START"
log "======================================"
log "Date: ${DATE}"
log "Script: ${PYTHON_SCRIPT}"
log "Log: ${LOG_FILE}"

# ── Safe .env loading ─────────────────────────────────────────────────────────
# IMPORTANT: Do NOT use "source .env" — .env may contain bare values or special
# characters that bash interprets as commands (passwords without KEY= prefix).
# This loop exports ONLY valid KEY=VALUE lines and safely skips everything else.
if [ -f "${ENV_FILE}" ]; then
  log "✅ Loading env from ${ENV_FILE}"
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    if [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
      export "$line" 2>/dev/null || true
    fi
  done < "${ENV_FILE}"
else
  log "⚠  No .env at ${ENV_FILE} — relying on exported environment variables"
fi

# ── Supabase connectivity diagnostic ─────────────────────────────────────────
# Print which Supabase project the shell env is pointing to.
# NOTE: tier1_granger_per_stock_v5.py may also read granger_config.py directly —
# check that file if Supabase URL here doesn't match your Investment OS project.
if [ -n "${SUPABASE_URL:-}" ]; then
  SUPABASE_SHORT="${SUPABASE_URL:0:50}..."
  log "🔗 SUPABASE_URL (from .env): ${SUPABASE_SHORT}"
else
  log "⚠  SUPABASE_URL not set in shell env — Python script must load it independently"
  log "   (Check: /opt/investment-os/services/granger-causality/granger_config.py)"
fi

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [ ! -f "${PYTHON_SCRIPT}" ]; then
  log "❌ ERROR: Python script not found: ${PYTHON_SCRIPT}"
  log "   Verify: ls -la ${SCRIPT_DIR}/"
  exit 1
fi

# ── Run Granger analysis ──────────────────────────────────────────────────────
log ""
log "======================================"
log "STEP 1: Running Granger Causality Tests"
log "======================================"
log "296 stocks × 3 tests each = 789 rows → tier1_granger_results"
log ""

START_TS=$(date +%s)

python3 "${PYTHON_SCRIPT}" 2>&1 | tee -a "${LOG_FILE}"
PYTHON_EXIT=${PIPESTATUS[0]}

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))

log ""
if [ ${PYTHON_EXIT} -eq 0 ]; then
  log "✅ Granger analysis complete in ${ELAPSED}s"
  log "   Results written → tier1_granger_results"
else
  log "❌ Granger script exited with code ${PYTHON_EXIT}"
  exit ${PYTHON_EXIT}
fi

log ""
log "======================================"
log "GRANGER PER-STOCK ANALYSIS — DONE"
log "======================================"

exit 0
