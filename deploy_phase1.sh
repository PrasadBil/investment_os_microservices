#!/bin/bash
# ============================================================================
# Investment OS - Phase 1 Deployment Script
# ============================================================================
# Run this ON THE VPS after scp'ing the investment-os/ folder.
#
# What it does:
#   1. Creates /opt/investment-os/packages/common/ directory
#   2. Copies all 6 Python modules + .env.example
#   3. Copies .env from old system (credentials stay intact)
#   4. Creates output directories (v5_reports, v5_logs, signals, backups)
#   5. Configures PYTHONPATH (persistent via .bashrc)
#   6. Runs full verification
#
# What it does NOT do:
#   - Does NOT touch /opt/selenium_automation/ (old system untouched)
#   - Does NOT modify cron jobs (that's Phase 4)
#   - Does NOT stop any running services
#
# Usage:
#   chmod +x deploy_phase1.sh
#   ./deploy_phase1.sh
#
# Rollback:
#   rm -rf /opt/investment-os/packages
#   sed -i '/investment-os\/packages/d' ~/.bashrc
#   source ~/.bashrc
# ============================================================================

set -e  # Exit on any error

echo "=================================================================="
echo "  INVESTMENT OS - PHASE 1 DEPLOYMENT"
echo "  Common Library Installation"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=================================================================="
echo ""

# ----------------------------------------------------------------------------
# STEP 1: Create directory structure
# ----------------------------------------------------------------------------
echo "STEP 1: Creating directory structure..."

mkdir -p /opt/investment-os/packages/common
mkdir -p /opt/investment-os/v5_reports
mkdir -p /opt/investment-os/v5_logs
mkdir -p /opt/investment-os/signals
mkdir -p /opt/investment-os/backups

echo "  /opt/investment-os/"
echo "  ├── packages/common/     (library code)"
echo "  ├── v5_reports/           (daily reports)"
echo "  ├── v5_logs/              (service logs)"
echo "  ├── signals/              (signal output)"
echo "  └── backups/              (weekly archives)"
echo "  ✅ Directories created"
echo ""

# ----------------------------------------------------------------------------
# STEP 2: Copy common library files
# ----------------------------------------------------------------------------
echo "STEP 2: Copying common library modules..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC="${SCRIPT_DIR}/packages/common"
DST="/opt/investment-os/packages/common"

for file in __init__.py config.py database.py data_loader.py email_sender.py logging_config.py requirements.txt; do
    if [ -f "${SRC}/${file}" ]; then
        cp "${SRC}/${file}" "${DST}/${file}"
        echo "  ✅ ${file} → ${DST}/${file}"
    else
        echo "  ❌ MISSING: ${SRC}/${file}"
        exit 1
    fi
done
echo ""

# ----------------------------------------------------------------------------
# STEP 3: Copy .env from old system
# ----------------------------------------------------------------------------
echo "STEP 3: Setting up .env..."

if [ -f "/opt/investment-os/.env" ]; then
    echo "  ⏭️  .env already exists at /opt/investment-os/.env (keeping existing)"
elif [ -f "/opt/selenium_automation/.env" ]; then
    cp /opt/selenium_automation/.env /opt/investment-os/.env
    echo "  ✅ Copied .env from /opt/selenium_automation/.env"
else
    cp "${SCRIPT_DIR}/.env.example" /opt/investment-os/.env
    echo "  ⚠️  No existing .env found — copied .env.example"
    echo "     EDIT /opt/investment-os/.env with your credentials before use!"
fi

# Also copy .env.example for reference
cp "${SCRIPT_DIR}/.env.example" /opt/investment-os/.env.example
echo ""

# ----------------------------------------------------------------------------
# STEP 4: Configure PYTHONPATH
# ----------------------------------------------------------------------------
echo "STEP 4: Configuring PYTHONPATH..."

PYPATH_LINE='export PYTHONPATH=/opt/investment-os/packages:$PYTHONPATH'

if grep -q "investment-os/packages" ~/.bashrc 2>/dev/null; then
    echo "  ⏭️  PYTHONPATH already configured in .bashrc"
else
    echo "" >> ~/.bashrc
    echo "# Investment OS - Common Library (Phase 1, $(date '+%Y-%m-%d'))" >> ~/.bashrc
    echo "${PYPATH_LINE}" >> ~/.bashrc
    echo "  ✅ Added PYTHONPATH to ~/.bashrc"
fi

# Apply immediately for this session
export PYTHONPATH=/opt/investment-os/packages:$PYTHONPATH
echo "  ✅ PYTHONPATH active for current session"
echo ""

# ----------------------------------------------------------------------------
# STEP 5: Install Python dependencies (if missing)
# ----------------------------------------------------------------------------
echo "STEP 5: Checking Python dependencies..."

python3 -c "import supabase" 2>/dev/null && echo "  ✅ supabase already installed" || {
    echo "  Installing supabase..."
    pip3 install supabase python-dotenv pandas --quiet
    echo "  ✅ Dependencies installed"
}
echo ""

# ----------------------------------------------------------------------------
# STEP 6: Full verification
# ----------------------------------------------------------------------------
echo "STEP 6: Running verification..."
echo ""

cd /opt/investment-os

python3 -c "
import sys
print('  Python:', sys.version.split()[0])
print('  PYTHONPATH includes packages:', '/opt/investment-os/packages' in sys.path)
print()

# Test all imports
try:
    from common import __version__
    print(f'  1. Package import:    ✅ v{__version__}')
except Exception as e:
    print(f'  1. Package import:    ❌ {e}')
    sys.exit(1)

try:
    from common.config import get_config
    config = get_config()
    has_db = bool(config.SUPABASE_URL)
    has_email = bool(config.V5_EMAIL_FROM)
    print(f'  2. Config singleton:  ✅ supabase={\"✅\" if has_db else \"❌\"} email={\"✅\" if has_email else \"❌\"}')
except Exception as e:
    print(f'  2. Config singleton:  ❌ {e}')
    sys.exit(1)

try:
    from common.database import get_supabase_client, health_check, reset_client
    print(f'  3. Database module:   ✅ 3 functions')
except Exception as e:
    print(f'  3. Database module:   ❌ {e}')
    sys.exit(1)

try:
    from common.data_loader import load_stock_data, load_cse_data, validate_data_quality
    print(f'  4. Data loader:       ✅ 3 functions')
except Exception as e:
    print(f'  4. Data loader:       ❌ {e}')
    sys.exit(1)

try:
    from common.email_sender import EmailSender
    sender = EmailSender()
    print(f'  5. Email sender:      ✅ EmailSender class')
except Exception as e:
    print(f'  5. Email sender:      ❌ {e}')
    sys.exit(1)

try:
    from common.logging_config import setup_logging
    logger = setup_logging('deploy-verify')
    print(f'  6. Logging config:    ✅ setup_logging function')
except Exception as e:
    print(f'  6. Logging config:    ❌ {e}')
    sys.exit(1)

# Database connectivity test (only if credentials present)
if has_db:
    try:
        result = health_check()
        print(f'  7. DB connectivity:   {\"✅ CONNECTED\" if result else \"❌ FAILED\"}')
    except Exception as e:
        print(f'  7. DB connectivity:   ❌ {e}')
else:
    print(f'  7. DB connectivity:   ⏭️  SKIPPED (no credentials in .env)')

print()
print('  OLD SYSTEM CHECK:')
import os
old_ok = os.path.exists('/opt/selenium_automation/cse_data_loader.py')
print(f'  8. Old system intact: {\"✅ /opt/selenium_automation/ untouched\" if old_ok else \"⚠️  Old system not found (OK if fresh install)\"}')
"

VERIFY_EXIT=$?

echo ""
echo "=================================================================="
if [ $VERIFY_EXIT -eq 0 ]; then
    echo "  ✅ PHASE 1 DEPLOYMENT COMPLETE"
    echo ""
    echo "  Common library installed at:"
    echo "    /opt/investment-os/packages/common/"
    echo ""
    echo "  Files deployed:"
    ls -la /opt/investment-os/packages/common/*.py | awk '{printf "    %-25s %6s bytes\n", $NF, $5}'
    echo ""
    echo "  Next steps:"
    echo "    → Phase 2: Migrate services to use common library"
    echo "    → Old system continues running (zero disruption)"
else
    echo "  ❌ DEPLOYMENT VERIFICATION FAILED"
    echo "  Check error messages above and re-run."
fi
echo "=================================================================="
