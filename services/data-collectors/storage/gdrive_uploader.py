
"""
storage/gdrive_uploader.py — Google Drive Uploader for Investment OS

Sprint 3: Used by CSEReportCollector to archive ~25MB daily PDFs to Drive.
CBSL files (~200KB-1MB) go to VPS local storage instead (vps_local path).

VERSION HISTORY:
  v1.0.0  2026-02-19  Sprint 3 — CSE Corporate Actions archival

SETUP (one-time, done by user):
  1. Create a Google Cloud project at console.cloud.google.com
  2. Enable Google Drive API
  3. Create a Service Account, download credentials.json
  4. Share your "Investment OS" Drive folder with the service account email
  5. Set GDRIVE_CREDENTIALS_PATH in .env → path to credentials.json
  6. Set GDRIVE_ROOT_FOLDER_ID in .env → the folder ID of "Investment OS"
     (get it from the URL: drive.google.com/drive/folders/<THIS_PART>)

USAGE:
  uploader = GDriveUploader()
  result = uploader.upload(
      local_path="/tmp/cse_daily_20260219.pdf",
      drive_folder="CSE Daily Reports/2026/02",
      filename="CSE_Daily_Report_2026-02-19.pdf",
  )
  print(result["web_view_link"])  # Shareable Drive URL

FOLDER STRUCTURE CREATED IN DRIVE:
  Investment OS/
  └── CSE Daily Reports/
      └── 2026/
          └── 02/
              └── CSE_Daily_Report_2026-02-19.pdf

NOTE ON QUOTA:
  Free tier: 15 GB storage, 15,000 queries/day.
  We upload ~25MB/day = ~7.5 GB/year. Within free tier with headroom.
  Queries: 1 upload + ~3 folder lookups/day = ~4/day. No risk of hitting quota.
"""

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Try to import Google API client — degrade gracefully if not installed
# ---------------------------------------------------------------------------

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    _GDRIVE_AVAILABLE = True
except ImportError:
    _GDRIVE_AVAILABLE = False
    logger.warning(
        "[GDrive] google-api-python-client not installed. "
        "Run: pip install google-api-python-client google-auth --break-system-packages"
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
MIME_PDF = "application/pdf"
MIME_FOLDER = "application/vnd.google-apps.folder"


# ---------------------------------------------------------------------------
# GDriveUploader
# ---------------------------------------------------------------------------

class GDriveUploader:
    """
    Uploads files to Google Drive using a service account.

    Service account credentials are loaded from the path specified in:
      - env var GDRIVE_CREDENTIALS_PATH (preferred)
      - fallback: /opt/investment-os/.secrets/gdrive_credentials.json

    Root folder ID (the "Investment OS" folder) is loaded from:
      - env var GDRIVE_ROOT_FOLDER_ID
    """

    DEFAULT_CREDENTIALS_PATH = (
        "/opt/investment-os/.secrets/gdrive_credentials.json"
    )

    def __init__(
        self,
        credentials_path: Optional[str] = None,
        root_folder_id: Optional[str] = None,
        token_path: Optional[str] = None,
    ):
        self._credentials_path = (
            credentials_path
            or os.environ.get("GDRIVE_CREDENTIALS_PATH")
            or self.DEFAULT_CREDENTIALS_PATH
        )
        # OAuth token path (preferred over service account for personal Gmail)
        self._token_path = (
            token_path
            or os.environ.get("GDRIVE_TOKEN_PATH")   # e.g. .secrets/gdrive_token.json
        )
        self._root_folder_id = (
            root_folder_id
            or os.environ.get("GDRIVE_ROOT_FOLDER_ID")
        )
        self._service = None   # Lazy-initialized on first upload call

    # ------------------------------------------------------------------
    # Public: upload a file
    # ------------------------------------------------------------------

    def upload(
        self,
        local_path: str | Path,
        drive_folder: str,
        filename: Optional[str] = None,
        mime_type: str = MIME_PDF,
    ) -> dict:
        """
        Upload a local file to the specified Drive folder path.

        Args:
            local_path:   Absolute path to the local file.
            drive_folder: Subfolder path within root, e.g. "CSE Daily Reports/2026/02"
                          Folders are created automatically if they don't exist.
            filename:     Name for the file in Drive. Defaults to local filename.
            mime_type:    MIME type (default: application/pdf).

        Returns:
            {
              "file_id": str,           # Google Drive file ID
              "web_view_link": str,     # https://drive.google.com/file/d/...
              "file_name": str,         # Filename in Drive
              "folder_id": str,         # ID of the parent folder
              "size_bytes": int,        # File size
            }

        Raises:
            GDriveUploaderError on any failure.
        """
        if not _GDRIVE_AVAILABLE:
            raise GDriveUploaderError(
                "google-api-python-client not installed. "
                "Run: pip install google-api-python-client google-auth --break-system-packages"
            )

        local_path = Path(local_path)
        if not local_path.exists():
            raise GDriveUploaderError(f"Local file not found: {local_path}")

        filename = filename or local_path.name
        file_size = local_path.stat().st_size

        logger.info(
            f"[GDrive] Uploading {filename} ({file_size:,} bytes) "
            f"→ Drive/{drive_folder}/"
        )

        try:
            service = self._get_service()
            folder_id = self._ensure_folder_path(service, drive_folder)

            file_metadata = {
                "name": filename,
                "parents": [folder_id],
            }
            media = MediaFileUpload(
                str(local_path),
                mimetype=mime_type,
                resumable=True,   # Resumable upload handles large files gracefully
            )

            uploaded = (
                service.files()
                .create(
                    body=file_metadata,
                    media_body=media,
                    fields="id,name,webViewLink,size",
                )
                .execute()
            )

            result = {
                "file_id":       uploaded.get("id"),
                "web_view_link": uploaded.get("webViewLink"),
                "file_name":     uploaded.get("name"),
                "folder_id":     folder_id,
                "size_bytes":    file_size,
            }

            logger.info(
                f"[GDrive] Upload complete | id={result['file_id']} "
                f"| link={result['web_view_link']}"
            )
            return result

        except HttpError as e:
            raise GDriveUploaderError(
                f"Google Drive API error during upload: {e}"
            ) from e
        except Exception as e:
            raise GDriveUploaderError(
                f"Unexpected error during Drive upload: {e}"
            ) from e

    # ------------------------------------------------------------------
    # Public: verify a file exists in Drive
    # ------------------------------------------------------------------

    def file_exists(self, filename: str, drive_folder: str) -> Optional[str]:
        """
        Check if a file with the given name exists in the Drive folder.

        Returns:
            File ID if found, None if not found.

        Use for idempotency: skip upload if file already in Drive.
        """
        if not _GDRIVE_AVAILABLE:
            return None
        try:
            service = self._get_service()
            folder_id = self._get_folder_id(service, drive_folder)
            if not folder_id:
                return None

            results = (
                service.files()
                .list(
                    q=(
                        f"name='{filename}' "
                        f"and '{folder_id}' in parents "
                        f"and trashed=false"
                    ),
                    fields="files(id, name)",
                )
                .execute()
            )
            files = results.get("files", [])
            return files[0]["id"] if files else None

        except Exception as e:
            logger.warning(f"[GDrive] file_exists check failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Internal: lazy service initialization
    # ------------------------------------------------------------------

    def _get_service(self):
        """
        Initialize and cache the Drive API service.

        Auth priority:
          1. OAuth user token (GDRIVE_TOKEN_PATH) — uploads as you, uses your
             15 GB quota. Required for personal Gmail accounts. Generated once
             by running setup_gdrive_oauth.py on your local machine.
          2. Service account (GDRIVE_CREDENTIALS_PATH) — fallback. Works for
             Google Workspace / Shared Drives but NOT personal Gmail (0 quota).
        """
        if self._service is not None:
            return self._service

        creds = None

        # ── Strategy 1: OAuth user token (preferred for personal Gmail) ──
        if self._token_path and Path(self._token_path).exists():
            try:
                from google.oauth2.credentials import Credentials
                from google.auth.transport.requests import Request

                creds = Credentials.from_authorized_user_file(
                    self._token_path, SCOPES
                )

                # Auto-refresh if expired
                if creds.expired and creds.refresh_token:
                    logger.info("[GDrive] OAuth token expired — refreshing...")
                    creds.refresh(Request())
                    # Persist refreshed token
                    Path(self._token_path).write_text(creds.to_json())
                    logger.info("[GDrive] OAuth token refreshed and saved.")

                self._service = build("drive", "v3", credentials=creds)
                logger.info(
                    f"[GDrive] Service initialized | auth=OAuth "
                    f"| token={Path(self._token_path).name}"
                )
                return self._service

            except Exception as e:
                logger.warning(
                    f"[GDrive] OAuth token load failed ({e}) — "
                    "falling back to service account."
                )
                creds = None

        # ── Strategy 2: Service account ───────────────────────────────────
        if not Path(self._credentials_path).exists():
            raise GDriveUploaderError(
                "No valid GDrive auth found.\n"
                f"  OAuth token not found at: {self._token_path}\n"
                f"  Service account not found at: {self._credentials_path}\n"
                "  → Run setup_gdrive_oauth.py on your local machine to fix this."
            )

        try:
            creds = service_account.Credentials.from_service_account_file(
                self._credentials_path,
                scopes=SCOPES,
            )
            self._service = build("drive", "v3", credentials=creds)
            logger.info(
                f"[GDrive] Service initialized | auth=ServiceAccount "
                f"| credentials={Path(self._credentials_path).name}"
            )
            logger.warning(
                "[GDrive] Using service account auth. For personal Gmail accounts "
                "this will fail with storageQuotaExceeded. "
                "Run setup_gdrive_oauth.py to switch to OAuth user credentials."
            )
            return self._service
        except Exception as e:
            raise GDriveUploaderError(
                f"Failed to initialize Drive service: {e}"
            ) from e

    # ------------------------------------------------------------------
    # Internal: folder management
    # ------------------------------------------------------------------

    def _ensure_folder_path(self, service, path: str) -> str:
        """
        Ensure the full folder path exists under the root folder.
        Creates intermediate folders as needed. Returns the leaf folder ID.

        Example: "CSE Daily Reports/2026/02"
          → creates "CSE Daily Reports" if needed (under root)
          → creates "2026" if needed (under CSE Daily Reports)
          → creates "02" if needed (under 2026)
          → returns the ID of "02"
        """
        if not self._root_folder_id:
            raise GDriveUploaderError(
                "GDRIVE_ROOT_FOLDER_ID not set.\n"
                "  → Open 'Investment OS' folder in Google Drive\n"
                "  → Copy the folder ID from the URL: "
                "drive.google.com/drive/folders/<FOLDER_ID>\n"
                "  → Set GDRIVE_ROOT_FOLDER_ID in .env"
            )

        parts = [p for p in path.split("/") if p]
        parent_id = self._root_folder_id

        for part in parts:
            existing_id = self._get_subfolder_id(service, part, parent_id)
            if existing_id:
                parent_id = existing_id
            else:
                parent_id = self._create_folder(service, part, parent_id)

        return parent_id

    def _get_folder_id(self, service, path: str) -> Optional[str]:
        """Get folder ID for a path without creating missing folders. Returns None if not found."""
        if not self._root_folder_id:
            return None
        parts = [p for p in path.split("/") if p]
        parent_id = self._root_folder_id
        for part in parts:
            fid = self._get_subfolder_id(service, part, parent_id)
            if not fid:
                return None
            parent_id = fid
        return parent_id

    def _get_subfolder_id(
        self, service, name: str, parent_id: str
    ) -> Optional[str]:
        """Find an existing subfolder by name within a parent folder."""
        try:
            results = (
                service.files()
                .list(
                    q=(
                        f"name='{name}' "
                        f"and mimeType='{MIME_FOLDER}' "
                        f"and '{parent_id}' in parents "
                        f"and trashed=false"
                    ),
                    fields="files(id, name)",
                )
                .execute()
            )
            files = results.get("files", [])
            return files[0]["id"] if files else None
        except Exception:
            return None

    def _create_folder(self, service, name: str, parent_id: str) -> str:
        """Create a new folder inside parent_id. Returns the new folder's ID."""
        try:
            folder_metadata = {
                "name": name,
                "mimeType": MIME_FOLDER,
                "parents": [parent_id],
            }
            folder = (
                service.files()
                .create(body=folder_metadata, fields="id")
                .execute()
            )
            folder_id = folder.get("id")
            logger.info(f"[GDrive] Created folder: {name} (id={folder_id})")
            return folder_id
        except HttpError as e:
            raise GDriveUploaderError(
                f"Failed to create Drive folder '{name}': {e}"
            ) from e


# ---------------------------------------------------------------------------
# Custom Exception
# ---------------------------------------------------------------------------

class GDriveUploaderError(Exception):
    """Raised on any Google Drive upload failure."""
    pass


# ---------------------------------------------------------------------------
# CLI helper — verify credentials and connection
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Quick connectivity test:
      python storage/gdrive_uploader.py

    Creates a test folder "Investment OS/_test" and verifies auth is working.
    """
    # Load .env so GDRIVE_ROOT_FOLDER_ID and GDRIVE_CREDENTIALS_PATH are visible
    # when running this script directly (production path loads .env via common.config)
    try:
        from dotenv import load_dotenv as _load_dotenv
        _load_dotenv("/opt/investment-os/.env")
    except ImportError:
        pass  # Fall back to whatever is already exported in the shell

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    print("\n=== Google Drive Uploader — Connectivity Test ===\n")
    token_path = os.environ.get("GDRIVE_TOKEN_PATH")
    if token_path and Path(token_path).exists():
        print(f"  Auth mode : OAuth user token  ({token_path})")
    else:
        creds_path = os.environ.get("GDRIVE_CREDENTIALS_PATH",
                                    GDriveUploader.DEFAULT_CREDENTIALS_PATH)
        print(f"  Auth mode : Service account   ({creds_path})")
    print()
    try:
        uploader = GDriveUploader()
        service = uploader._get_service()
        print("✓ Drive API authenticated successfully.")

        # List root folder contents as a sanity check
        if uploader._root_folder_id:
            results = (
                service.files()
                .list(
                    q=f"'{uploader._root_folder_id}' in parents and trashed=false",
                    fields="files(id, name, mimeType)",
                    pageSize=10,
                )
                .execute()
            )
            files = results.get("files", [])
            print(f"✓ Root folder accessible — {len(files)} items:")
            for f in files:
                icon = "📁" if f["mimeType"] == MIME_FOLDER else "📄"
                print(f"  {icon}  {f['name']}  ({f['id']})")
        else:
            print("⚠  GDRIVE_ROOT_FOLDER_ID not set — skipping folder listing.")

        print("\n✓ GDriveUploader is ready for production use.\n")

    except GDriveUploaderError as e:
        print(f"\n✗ Setup required:\n{e}\n")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}\n")
