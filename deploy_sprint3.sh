#!/bin/bash
# =============================================================================
# deploy_sprint3.sh — Investment OS Sprint 3 Deployment
# CSE Corporate Actions Collector (Selenium + PDF + Google Drive)
#
# Run this ON THE VPS after syncing the investment-os/ folder.
#
# Usage:
#   chmod +x deploy_sprint3.sh
#   ./deploy_sprint3.sh
#
# What this does:
#   1. Creates Sprint 3 directories (.secrets, parsers/, storage/)
#   2. Installs Python dependencies (pdfplumber, google-api-python-client)
#   3. Makes cron script executable
#   4. Prints next steps (GDrive setup + cron activation)
#
# What this does NOT do:
#   - Does NOT create cron entry (you do that at the end)
#   - Does NOT touch .env (already has SUPABASE_URL / SUPABASE_KEY)
#   - Does NOT configure Google Drive credentials (see GDRIVE_SETUP.md)
# =============================================================================

set -eo pipefail

INSTALL_DIR="/opt/investment-os"
COLLECTORS_DIR="$INSTALL_DIR/services/data-collectors"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo "  INVESTMENT OS — SPRINT 3 DEPLOYMENT"
echo "  CSE Corporate Actions Collector"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=================================================================="
echo ""

# ---------------------------------------------------------------------------
# STEP 1: Directory structure
# ---------------------------------------------------------------------------
echo "STEP 1: Creating Sprint 3 directories..."

mkdir -p "$INSTALL_DIR/.secrets"
chmod 700 "$INSTALL_DIR/.secrets"

mkdir -p "$COLLECTORS_DIR/parsers"
mkdir -p "$COLLECTORS_DIR/storage"
mkdir -p "$COLLECTORS_DIR/data/temp"

# Ensure __init__.py files exist (parsers and storage are Python packages)
touch "$COLLECTORS_DIR/parsers/__init__.py"
touch "$COLLECTORS_DIR/storage/__init__.py"

echo "  ✓  $INSTALL_DIR/.secrets/  (mode 700, for gdrive_credentials.json)"
echo "  ✓  $COLLECTORS_DIR/parsers/"
echo "  ✓  $COLLECTORS_DIR/storage/"
echo "  ✓  $COLLECTORS_DIR/data/temp/"

# ---------------------------------------------------------------------------
# STEP 2: Copy Sprint 3 Python files (if run from repo root)
# ---------------------------------------------------------------------------
echo ""
echo "STEP 2: Copying Sprint 3 Python modules..."

if [ -f "$SCRIPT_DIR/services/data-collectors/parsers/cse_report_parser.py" ]; then
    cp "$SCRIPT_DIR/services/data-collectors/parsers/cse_report_parser.py" \
       "$COLLECTORS_DIR/parsers/cse_report_parser.py"
    echo "  ✓  cse_report_parser.py copied"
else
    echo "  ⚠  cse_report_parser.py not found in $SCRIPT_DIR — copy manually:"
    echo "     cp parsers/cse_report_parser.py $COLLECTORS_DIR/parsers/"
fi

if [ -f "$SCRIPT_DIR/services/data-collectors/storage/gdrive_uploader.py" ]; then
    cp "$SCRIPT_DIR/services/data-collectors/storage/gdrive_uploader.py" \
       "$COLLECTORS_DIR/storage/gdrive_uploader.py"
    echo "  ✓  gdrive_uploader.py copied"
else
    echo "  ⚠  gdrive_uploader.py not found — copy manually:"
    echo "     cp storage/gdrive_uploader.py $COLLECTORS_DIR/storage/"
fi

if [ -f "$SCRIPT_DIR/services/data-collectors/cron/run_cse_corporate.sh" ]; then
    cp "$SCRIPT_DIR/services/data-collectors/cron/run_cse_corporate.sh" \
       "$COLLECTORS_DIR/cron/run_cse_corporate.sh"
    chmod +x "$COLLECTORS_DIR/cron/run_cse_corporate.sh"
    echo "  ✓  run_cse_corporate.sh copied + made executable"
fi

# ---------------------------------------------------------------------------
# STEP 3: Install Python dependencies
# ---------------------------------------------------------------------------
echo ""
echo "STEP 3: Installing Python dependencies..."

pip3 install pdfplumber --break-system-packages -q && echo "  ✓  pdfplumber installed"
pip3 install google-api-python-client google-auth --break-system-packages -q && echo "  ✓  google-api-python-client installed"
pip3 install selenium webdriver-manager --break-system-packages -q && echo "  ✓  selenium + webdriver-manager installed"

# ---------------------------------------------------------------------------
# STEP 4: Verify Chromium / Chrome available for Selenium
# ---------------------------------------------------------------------------
echo ""
echo "STEP 4: Checking Selenium dependencies..."

if command -v google-chrome &>/dev/null; then
    CHROME_VER=$(google-chrome --version 2>&1 | head -1)
    echo "  ✓  Chrome found: $CHROME_VER"
elif command -v chromium-browser &>/dev/null; then
    CHROME_VER=$(chromium-browser --version 2>&1 | head -1)
    echo "  ✓  Chromium found: $CHROME_VER"
else
    echo "  ✗  Chrome/Chromium NOT found. Install with:"
    echo "     sudo apt-get install -y chromium-browser"
    echo "     (or: sudo apt-get install -y google-chrome-stable)"
fi

if command -v Xvfb &>/dev/null; then
    echo "  ✓  Xvfb found (virtual display for headless mode)"
else
    echo "  ⚠  Xvfb not found. Install with: sudo apt-get install -y xvfb"
fi

# ---------------------------------------------------------------------------
# STEP 5: Quick parser smoke test (no Supabase, no GDrive, just PDF parsing)
# ---------------------------------------------------------------------------
echo ""
echo "STEP 5: Quick import check..."

python3 -c "
import sys
sys.path.insert(0, '$COLLECTORS_DIR')
try:
    from parsers.cse_report_parser import CSEReportParser
    print('  ✓  CSEReportParser imports OK')
except ImportError as e:
    print(f'  ✗  Import failed: {e}')
"

# ---------------------------------------------------------------------------
# STEP 6: Print remaining manual steps
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo "  DEPLOYMENT COMPLETE — MANUAL STEPS REMAINING:"
echo "=================================================================="
echo ""
echo "  1. RUN SCHEMA IN SUPABASE (one-time):"
echo "     → Open: https://supabase.com/dashboard"
echo "     → SQL Editor → New query"
echo "     → Paste contents of: sprint3_schema.sql"
echo "     → Click Run"
echo ""
echo "  2. SETUP GOOGLE DRIVE (see GDRIVE_SETUP.md for full guide):"
echo "     a) Create GCP project + enable Drive API"
echo "     b) Create service account → download credentials.json"
echo "     c) Copy to: $INSTALL_DIR/.secrets/gdrive_credentials.json"
echo "     d) Share 'Investment OS' Drive folder with service account email"
echo "     e) Add to /opt/investment-os/.env:"
echo "        GDRIVE_CREDENTIALS_PATH=$INSTALL_DIR/.secrets/gdrive_credentials.json"
echo "        GDRIVE_ROOT_FOLDER_ID=<folder-id-from-drive-url>"
echo ""
echo "  3. TEST GDRIVE AUTH:"
echo "     cd $COLLECTORS_DIR"
echo "     python3 storage/gdrive_uploader.py"
echo ""
echo "  4. ACTIVATE CRON (Monday–Friday, 8:00 PM SLK = 14:30 UTC):"
echo "     crontab -e"
echo "     Add this line:"
echo "     30 14 * * 1-5 $COLLECTORS_DIR/cron/run_cse_corporate.sh"
echo ""
echo "  5. MANUAL TEST RUN (first live run):"
echo "     $COLLECTORS_DIR/cron/run_cse_corporate.sh"
echo ""
echo "=================================================================="
echo "  LOG FILE: /opt/investment-os/v5_logs/collector_cse_corporate_*.log"
echo "=================================================================="
echo ""
