#!/usr/bin/env python3
"""
setup_gdrive_oauth.py — One-time Google Drive OAuth token setup

Run this ONCE on your LOCAL MACHINE (not the VPS) to generate a
token.json file. The token contains a refresh token that lets the
VPS upload files as pra.bil.wicky@gmail.com — counting against your
15 GB Google Drive quota instead of the service account's 0 quota.

PREREQUISITES (already done):
  - GCP project "investment-os" exists
  - Google Drive API is enabled

STEPS:
  1. In GCP Console → APIs & Services → Credentials
     → + Create Credentials → OAuth client ID
     → Application type: Desktop app
     → Name: "investment-os-uploader"
     → Download the JSON → save as oauth_client_secret.json in this folder

  2. Run this script on your LOCAL machine:
       pip install google-auth-oauthlib --break-system-packages
       python3 setup_gdrive_oauth.py

  3. A browser window opens → sign in as pra.bil.wicky@gmail.com → Allow
     (You may see "Google hasn't verified this app" → click Advanced → Go to app)

  4. token.json is created in this folder.

  5. SCP to VPS:
       scp token.json root@YOUR_VPS_IP:/opt/investment-os/.secrets/gdrive_token.json

  6. Add to /opt/investment-os/.env:
       GDRIVE_TOKEN_PATH=/opt/investment-os/.secrets/gdrive_token.json

  That's it! The VPS will now upload as you, auto-refreshing the token.

SECURITY NOTE:
  token.json contains a long-lived refresh token — treat it like a password.
  It is already in .gitignore and the .secrets/ folder has mode 700.
"""

import json
import os
import sys
from pathlib import Path

# ── Dependency check ──────────────────────────────────────────────────────────

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.oauth2.credentials import Credentials
except ImportError:
    print("ERROR: google-auth-oauthlib not installed.")
    print("Run:  pip install google-auth-oauthlib --break-system-packages")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

CLIENT_SECRET_FILE = Path("oauth_client_secret.json")
TOKEN_OUT           = Path("token.json")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not CLIENT_SECRET_FILE.exists():
        print(f"""
ERROR: {CLIENT_SECRET_FILE} not found.

Steps to get it:
  1. Go to: https://console.cloud.google.com/apis/credentials
  2. Click + Create Credentials → OAuth client ID
  3. Application type: Desktop app   Name: investment-os-uploader
  4. Click Create → Download JSON
  5. Rename the downloaded file to: oauth_client_secret.json
  6. Place it in the same folder as this script
  7. Re-run: python3 setup_gdrive_oauth.py
""")
        sys.exit(1)

    print("\n=== Investment OS — Google Drive OAuth Setup ===\n")
    print("A browser window will open. Sign in as pra.bil.wicky@gmail.com")
    print("and grant the requested permissions.\n")
    print("If you see 'Google hasn't verified this app':")
    print("  → Click 'Advanced' → 'Go to investment-os-uploader (unsafe)'\n")

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_SECRET_FILE), SCOPES
    )
    creds = flow.run_local_server(port=0, open_browser=True)

    TOKEN_OUT.write_text(creds.to_json())
    print(f"\n✓ Token saved to: {TOKEN_OUT.resolve()}\n")

    # Show next steps
    print("=" * 60)
    print("NEXT STEPS:")
    print("=" * 60)
    print()
    print("1. Copy token to VPS:")
    print("   scp token.json root@YOUR_VPS_IP:/opt/investment-os/.secrets/gdrive_token.json")
    print()
    print("2. Add to /opt/investment-os/.env:")
    print("   GDRIVE_TOKEN_PATH=/opt/investment-os/.secrets/gdrive_token.json")
    print()
    print("3. Set permissions on VPS:")
    print("   chmod 600 /opt/investment-os/.secrets/gdrive_token.json")
    print()
    print("4. Force re-archive today's PDF:")
    print("   bash /opt/investment-os/services/data-collectors/cron/run_cse_corporate.sh --force")
    print()
    print("The token auto-refreshes — no further action needed.")
    print()


if __name__ == "__main__":
    main()
