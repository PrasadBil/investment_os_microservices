# Phase 1: Deployment Notes
# Investment OS - Monolithic to Microservices Migration
# Date: February 9, 2026

## Pre-Phase 2: VPS PYTHONPATH Configuration

Before migrating any service code (Phase 2), the VPS must know where to find
the new `common` package. Run this ONCE on the VPS.

### Steps

```bash
# 1. SSH into production VPS
ssh root@srv1127544.hstgr.cloud

# 2. Add PYTHONPATH (persistent across reboots/sessions)
echo 'export PYTHONPATH=/opt/investment-os/packages:$PYTHONPATH' >> ~/.bashrc
source ~/.bashrc

# 3. Verify it worked
python3 -c "import sys; '/opt/investment-os/packages' in sys.path and print('✅ PYTHONPATH configured')"
```

### What this does

- All Python processes launched by root (including cron jobs) will be able to
  resolve `from common.database import get_supabase_client` without any
  sys.path hacks in individual scripts.
- The existing `/opt/selenium_automation/` system is UNAFFECTED — this only
  adds a new path, it doesn't change any existing paths.
- Cron jobs inherit the shell environment, so this covers the automated
  6:30 PM, 7:00 PM, and Saturday 6:00 PM schedules.

### Verification checklist

```
[ ] PYTHONPATH includes /opt/investment-os/packages
[ ] python3 -c "from common.config import get_config" works (after config.py is deployed)
[ ] python3 -c "from common.database import get_supabase_client" works (after database.py is deployed)
[ ] Old system still runs: bash /opt/selenium_automation/run_v5_detector.sh
```

### Rollback (if needed)

```bash
# Remove the PYTHONPATH line from .bashrc
sed -i '/investment-os\/packages/d' ~/.bashrc
source ~/.bashrc
```

## File Inventory: Phase 1 Common Library

| File | Status | Description |
|------|--------|-------------|
| `packages/common/__init__.py` | Pending | Package initializer with version |
| `packages/common/database.py` | **APPROVED** | Singleton Supabase client |
| `packages/common/config.py` | Pending | Singleton Config class (.env loader) |
| `packages/common/data_loader.py` | Pending | Unified OHLCV data loader (from cse_data_loader.py) |
| `packages/common/email_sender.py` | Pending | Unified email sender (Phase 1B) |
| `packages/common/logging_config.py` | Pending | Standardized logging (Phase 1B) |
