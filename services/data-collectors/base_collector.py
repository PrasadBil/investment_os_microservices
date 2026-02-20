
"""
base_collector.py — Abstract Base Collector for Investment OS Data Collection Framework

VERSION HISTORY:
  v1.0.0  2026-02-18  Initial implementation — Sprint 0 (Framework Skeleton)
                       5-stage pipeline: DISCOVER → DOWNLOAD → PARSE → STORE → ARCHIVE
                       Retry logic with exponential backoff
                       Idempotency via Supabase date+source composite key check
                       Structured logging with per-run execution context

DESIGN PRINCIPLE (Factory Pattern):
  Every data source is an assembly line using the same factory floor.
  The factory floor (this file) never changes.
  Each assembly line (subclass) only implements source-specific jigs (parsers).

USAGE:
  Subclass BaseCollector and implement all 5 abstract methods.
  Instantiate via collector_runner.py — never call directly.
"""

import sys
import time
import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, date
from pathlib import Path
from typing import Any

# Common library (on VPS: /opt/investment-os/packages in PYTHONPATH)
try:
    from common.config import get_config
    from common.database import get_supabase_client
    from common.logging_config import setup_logging
    from common.email_sender import EmailSender
    _COMMON_AVAILABLE = True
except ImportError:
    _COMMON_AVAILABLE = False  # Graceful degradation during local dev/testing


# ---------------------------------------------------------------------------
# Pipeline Stage Enum
# ---------------------------------------------------------------------------

class PipelineStage:
    DISCOVER  = "DISCOVER"
    DOWNLOAD  = "DOWNLOAD"
    PARSE     = "PARSE"
    STORE     = "STORE"
    ARCHIVE   = "ARCHIVE"


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

class CollectionResult:
    """Immutable result returned by each pipeline stage and the full run."""

    def __init__(
        self,
        success: bool,
        stage: str,
        source_id: str,
        collection_date: date,
        rows_stored: int = 0,
        file_path: str | None = None,
        archive_path: str | None = None,
        error: str | None = None,
        duration_seconds: float = 0.0,
        metadata: dict | None = None,
    ):
        self.success          = success
        self.stage            = stage
        self.source_id        = source_id
        self.collection_date  = collection_date
        self.rows_stored      = rows_stored
        self.file_path        = file_path
        self.archive_path     = archive_path
        self.error            = error
        self.duration_seconds = duration_seconds
        self.metadata         = metadata or {}

    def __repr__(self) -> str:
        status = "OK" if self.success else "FAIL"
        return (
            f"CollectionResult({status} | stage={self.stage} | "
            f"source={self.source_id} | date={self.collection_date} | "
            f"rows={self.rows_stored} | {self.duration_seconds:.1f}s)"
        )


# ---------------------------------------------------------------------------
# Abstract Base Collector
# ---------------------------------------------------------------------------

class BaseCollector(ABC):
    """
    Abstract base class for all Investment OS data collectors.

    Every collector follows the same 5-stage pipeline:
      1. DISCOVER  — Determine today's URL; check if already collected (idempotent)
      2. DOWNLOAD  — Fetch the publication file with retry + exponential backoff
      3. PARSE     — Extract structured data rows from the raw file
      4. STORE     — Upsert parsed rows into Supabase (idempotent)
      5. ARCHIVE   — Move raw file to final storage; clean up temp

    Subclasses implement the 5 abstract methods below.
    The run() method orchestrates the full pipeline and handles all errors.
    """

    # Retry configuration (overridable per source via source_config.py)
    MAX_RETRIES:     int   = 3
    RETRY_BACKOFF_S: float = 2.0   # doubles each attempt: 2s, 4s, 8s
    DOWNLOAD_TIMEOUT_S: int = 60

    def __init__(self, source_id: str, collection_date: date | None = None):
        self.source_id        = source_id
        self.collection_date  = collection_date or date.today()
        self._run_id          = self._generate_run_id()
        self._start_time: float | None = None

        # Set up logger (falls back to basic logging if common lib unavailable)
        if _COMMON_AVAILABLE:
            setup_logging(self.source_id, log_to_file=True)
        self.logger = logging.getLogger(f"collector.{source_id}")
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter(
                "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            ))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        # Supabase client (None in test mode)
        self._db = get_supabase_client() if _COMMON_AVAILABLE else None

    # ------------------------------------------------------------------
    # 5 Abstract Methods — implement in each source-specific subclass
    # ------------------------------------------------------------------

    @abstractmethod
    def discover(self) -> dict:
        """
        Stage 1: DISCOVER

        Determine the URL (or navigation target) for today's publication.
        Check whether this date has already been collected to ensure idempotency.

        Returns:
            {
              "url": str,                  # Target URL to download
              "already_collected": bool,   # True → skip entire pipeline
              "date_str": str,             # Date formatted per source convention
              "metadata": dict,            # Any extra context for downstream stages
            }

        Raises:
            CollectorDiscoverError if URL cannot be determined.
        """

    @abstractmethod
    def download(self, discover_result: dict) -> dict:
        """
        Stage 2: DOWNLOAD

        Fetch the publication file to a local temp path.
        Uses built-in retry logic from _download_with_retry().

        Args:
            discover_result: Output from discover()

        Returns:
            {
              "file_path": str,    # Absolute path to downloaded file
              "file_size_bytes": int,
              "content_hash": str, # SHA256 for integrity verification
              "metadata": dict,
            }

        Raises:
            CollectorDownloadError after all retries exhausted.
        """

    @abstractmethod
    def parse(self, download_result: dict) -> list[dict]:
        """
        Stage 3: PARSE

        Extract structured data rows from the downloaded file.
        Must be deterministic — same file → same output always.

        Args:
            download_result: Output from download()

        Returns:
            List of dicts, each matching the target Supabase table schema.
            Example: [{"date": "2026-02-18", "usd_tt_buy": 298.50, ...}]

        Raises:
            CollectorParseError if file cannot be parsed or yields 0 rows.
        """

    @abstractmethod
    def store(self, parsed_rows: list[dict]) -> dict:
        """
        Stage 4: STORE

        Upsert parsed rows into Supabase. Must be idempotent:
        running twice on the same data produces the same DB state.

        Args:
            parsed_rows: Output from parse()

        Returns:
            {
              "rows_stored": int,
              "rows_skipped": int,   # Duplicates detected + skipped
              "tables_written": list[str],
            }

        Raises:
            CollectorStoreError on Supabase write failure.
        """

    @abstractmethod
    def archive(self, download_result: dict, store_result: dict) -> dict:
        """
        Stage 5: ARCHIVE

        Move the raw file to its final resting place.
        - Large files (CSE ~25MB) → Google Drive
        - Small files (CBSL ~200KB–1MB) → VPS /data/ directory

        Delete the local temp file after archival is confirmed.

        Args:
            download_result: Output from download()
            store_result:    Output from store()

        Returns:
            {
              "archive_path": str,   # Final location (Drive URL or local path)
              "local_deleted": bool,
            }

        Raises:
            CollectorArchiveError on upload/move failure.
        """

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> CollectionResult:
        """
        Execute the full 5-stage pipeline for this source + date.

        Handles all exceptions at the pipeline level — never raises.
        Returns a CollectionResult describing the outcome.

        Idempotency: If DISCOVER determines data is already collected,
        the method returns immediately with success=True and rows_stored=0.
        """
        self._start_time = time.time()
        self.logger.info(
            "=" * 60 + f"\n  RUN START | source={self.source_id} | "
            f"date={self.collection_date} | run_id={self._run_id}\n" + "=" * 60
        )

        try:
            # ── Stage 1: DISCOVER ─────────────────────────────────────
            discover_result = self._run_stage(PipelineStage.DISCOVER, self.discover)
            if discover_result.get("already_collected"):
                elapsed = time.time() - self._start_time
                self.logger.info(
                    f"[DISCOVER] Already collected for {self.collection_date} — skipping pipeline."
                )
                return CollectionResult(
                    success=True, stage=PipelineStage.DISCOVER,
                    source_id=self.source_id, collection_date=self.collection_date,
                    rows_stored=0, duration_seconds=elapsed,
                    metadata={"skipped_reason": "already_collected"},
                )

            # ── Stage 2: DOWNLOAD ─────────────────────────────────────
            download_result = self._run_stage(
                PipelineStage.DOWNLOAD, self.download, discover_result
            )

            # ── Stage 3: PARSE ────────────────────────────────────────
            parsed_rows = self._run_stage(
                PipelineStage.PARSE, self.parse, download_result
            )

            # ── Stage 4: STORE ────────────────────────────────────────
            store_result = self._run_stage(
                PipelineStage.STORE, self.store, parsed_rows
            )

            # ── Stage 5: ARCHIVE ──────────────────────────────────────
            archive_result = self._run_stage(
                PipelineStage.ARCHIVE, self.archive, download_result, store_result
            )

            elapsed = time.time() - self._start_time
            self.logger.info(
                f"PIPELINE COMPLETE | rows={store_result.get('rows_stored', 0)} | "
                f"archive={archive_result.get('archive_path')} | {elapsed:.1f}s"
            )

            return CollectionResult(
                success=True,
                stage=PipelineStage.ARCHIVE,
                source_id=self.source_id,
                collection_date=self.collection_date,
                rows_stored=store_result.get("rows_stored", 0),
                file_path=download_result.get("file_path"),
                archive_path=archive_result.get("archive_path"),
                duration_seconds=elapsed,
                metadata={
                    "run_id": self._run_id,
                    "file_size_bytes": download_result.get("file_size_bytes", 0),
                    "content_hash": download_result.get("content_hash", ""),
                    "tables_written": store_result.get("tables_written", []),
                },
            )

        except CollectorError as exc:
            elapsed = time.time() - self._start_time
            self.logger.error(f"PIPELINE FAILED at {exc.stage} | {exc}")
            self._send_failure_alert(exc)
            return CollectionResult(
                success=False,
                stage=exc.stage,
                source_id=self.source_id,
                collection_date=self.collection_date,
                error=str(exc),
                duration_seconds=elapsed,
                metadata={"run_id": self._run_id},
            )

        except Exception as exc:
            elapsed = time.time() - self._start_time
            self.logger.exception(f"UNEXPECTED ERROR | {exc}")
            self._send_failure_alert(exc)
            return CollectionResult(
                success=False,
                stage="UNKNOWN",
                source_id=self.source_id,
                collection_date=self.collection_date,
                error=f"Unexpected: {exc}",
                duration_seconds=elapsed,
                metadata={"run_id": self._run_id},
            )

    # ------------------------------------------------------------------
    # Helper: idempotency check (call from discover() in subclasses)
    # ------------------------------------------------------------------

    def is_already_collected(self, table: str, date_column: str = "date") -> bool:
        """
        Check Supabase whether this source + date is already stored.
        Returns True if a row exists → pipeline should be skipped.

        Usage in subclass discover():
            if self.is_already_collected("cbsl_daily_indicators"):
                return {"already_collected": True, ...}
        """
        if self._db is None:
            self.logger.debug("DB not available — skipping idempotency check.")
            return False
        try:
            result = (
                self._db.table(table)
                .select(date_column)
                .eq(date_column, self.collection_date.isoformat())
                .limit(1)
                .execute()
            )
            exists = len(result.data) > 0
            if exists:
                self.logger.info(
                    f"[IDEMPOTENCY] Row exists in {table} for {self.collection_date}."
                )
            return exists
        except Exception as e:
            self.logger.warning(f"Idempotency check failed (proceeding anyway): {e}")
            return False

    # ------------------------------------------------------------------
    # Helper: download with retry + exponential backoff
    # ------------------------------------------------------------------

    def _download_with_retry(
        self,
        url: str,
        dest_path: str | Path,
        headers: dict | None = None,
    ) -> dict:
        """
        HTTP GET download with retry logic. Used by http_get sources (CBSL).
        For Selenium-based downloads (CSE), implement download() directly.

        Returns:
            {"file_path": str, "file_size_bytes": int, "content_hash": str}
        """
        import requests

        dest_path = Path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        headers = headers or {
            "User-Agent": (
                "Mozilla/5.0 (compatible; InvestmentOS/1.0; "
                "+https://github.com/investment-os)"
            )
        }

        last_error: Exception | None = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                self.logger.info(
                    f"[DOWNLOAD] Attempt {attempt}/{self.MAX_RETRIES} → {url}"
                )
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=self.DOWNLOAD_TIMEOUT_S,
                    stream=True,
                )
                response.raise_for_status()

                sha256 = hashlib.sha256()
                with open(dest_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        f.write(chunk)
                        sha256.update(chunk)

                file_size = dest_path.stat().st_size
                content_hash = sha256.hexdigest()
                self.logger.info(
                    f"[DOWNLOAD] OK | {file_size:,} bytes | sha256={content_hash[:12]}..."
                )
                return {
                    "file_path": str(dest_path),
                    "file_size_bytes": file_size,
                    "content_hash": content_hash,
                }

            except Exception as e:
                last_error = e
                wait = self.RETRY_BACKOFF_S * (2 ** (attempt - 1))
                self.logger.warning(
                    f"[DOWNLOAD] Attempt {attempt} failed: {e} — retrying in {wait:.0f}s"
                )
                if attempt < self.MAX_RETRIES:
                    time.sleep(wait)

        raise CollectorDownloadError(
            f"All {self.MAX_RETRIES} download attempts failed: {last_error}"
        )

    # ------------------------------------------------------------------
    # Helper: Supabase upsert
    # ------------------------------------------------------------------

    def _upsert_rows(self, table: str, rows: list[dict], conflict_columns: list[str]) -> int:
        """
        Upsert rows into Supabase table. Idempotent — safe to call multiple times.

        Args:
            table:            Supabase table name
            rows:             List of row dicts matching table schema
            conflict_columns: Columns that form the unique key (e.g., ["date"])

        Returns:
            Number of rows upserted.
        """
        if not rows:
            self.logger.warning(f"[STORE] No rows to upsert into {table}.")
            return 0
        if self._db is None:
            self.logger.warning("[STORE] DB not available — skipping upsert (test mode).")
            return len(rows)

        try:
            result = (
                self._db.table(table)
                .upsert(rows, on_conflict=",".join(conflict_columns))
                .execute()
            )
            count = len(result.data) if result.data else len(rows)
            self.logger.info(f"[STORE] Upserted {count} rows → {table}")
            return count
        except Exception as e:
            raise CollectorStoreError(f"Supabase upsert failed for {table}: {e}") from e

    # ------------------------------------------------------------------
    # Internal: stage runner with timing
    # ------------------------------------------------------------------

    def _run_stage(self, stage_name: str, fn, *args) -> Any:
        """Execute a pipeline stage function with timing + error wrapping."""
        t0 = time.time()
        self.logger.info(f"[{stage_name}] Starting...")
        try:
            result = fn(*args)
            elapsed = time.time() - t0
            self.logger.info(f"[{stage_name}] Complete in {elapsed:.2f}s")
            return result
        except CollectorError:
            raise  # Already typed — let run() handle it
        except Exception as e:
            elapsed = time.time() - t0
            self.logger.error(f"[{stage_name}] Failed in {elapsed:.2f}s: {e}")
            # Wrap in the appropriate CollectorError type
            error_map = {
                PipelineStage.DISCOVER: CollectorDiscoverError,
                PipelineStage.DOWNLOAD: CollectorDownloadError,
                PipelineStage.PARSE:    CollectorParseError,
                PipelineStage.STORE:    CollectorStoreError,
                PipelineStage.ARCHIVE:  CollectorArchiveError,
            }
            ErrorClass = error_map.get(stage_name, CollectorError)
            # Typed subclasses (e.g. CollectorParseError) hard-code their own stage
            # Only the base CollectorError accepts an explicit stage kwarg
            if ErrorClass is CollectorError:
                raise ErrorClass(str(e), stage=stage_name) from e
            else:
                raise ErrorClass(str(e)) from e

    def _generate_run_id(self) -> str:
        """Generate a short unique ID for this pipeline run (for log correlation)."""
        raw = f"{self.source_id}:{self.collection_date}:{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:12]

    def _send_failure_alert(self, error: Exception) -> None:
        """Send email alert on pipeline failure. Silently degrades if email unavailable."""
        if not _COMMON_AVAILABLE:
            return
        try:
            sender  = EmailSender()
            subject = f"[Investment OS] Collector FAILED: {self.source_id} ({self.collection_date})"
            body    = (
                f"<h2>Collector Failure Alert</h2>"
                f"<p><b>Source:</b> {self.source_id}<br>"
                f"<b>Date:</b> {self.collection_date}<br>"
                f"<b>Run ID:</b> {self._run_id}<br>"
                f"<b>Error:</b> {error}</p>"
                f"<p>Check VPS logs at "
                f"/opt/investment-os/v5_logs/collector_{self.source_id}.log</p>"
            )
            sender.send_html(subject=subject, html_content=body, plain_fallback=f"Collector FAILED: {self.source_id} | {error}")
            self.logger.info("[ALERT] Failure email sent.")
        except Exception as e:
            self.logger.warning(f"[ALERT] Could not send failure email: {e}")


# ---------------------------------------------------------------------------
# Custom Exception Hierarchy
# ---------------------------------------------------------------------------

class CollectorError(Exception):
    """Base exception for all collector errors. Always carries the failing stage."""
    def __init__(self, message: str, stage: str = "UNKNOWN"):
        super().__init__(message)
        self.stage = stage

class CollectorDiscoverError(CollectorError):
    def __init__(self, message: str):
        super().__init__(message, stage=PipelineStage.DISCOVER)

class CollectorDownloadError(CollectorError):
    def __init__(self, message: str):
        super().__init__(message, stage=PipelineStage.DOWNLOAD)

class CollectorParseError(CollectorError):
    def __init__(self, message: str):
        super().__init__(message, stage=PipelineStage.PARSE)

class CollectorStoreError(CollectorError):
    def __init__(self, message: str):
        super().__init__(message, stage=PipelineStage.STORE)

class CollectorArchiveError(CollectorError):
    def __init__(self, message: str):
        super().__init__(message, stage=PipelineStage.ARCHIVE)
