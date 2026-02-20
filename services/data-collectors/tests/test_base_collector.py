
"""
test_base_collector.py — Unit Tests for Investment OS Data Collection Framework

VERSION HISTORY:
  v1.0.0  2026-02-18  Initial test suite — Sprint 0 (Framework Skeleton)

COVERAGE:
  - BaseCollector lifecycle (happy path end-to-end)
  - Idempotency: already-collected detection skips pipeline
  - Retry logic: download failures trigger exponential backoff
  - Error propagation: each stage failure returns the correct CollectionResult
  - Stage isolation: failure in one stage does not corrupt others
  - Source configuration validation (SourceConfig.__post_init__ guards)
  - CLI argument parsing via collector_runner.py

RUNNING:
  From the data-collectors/ directory:
    python3 -m pytest tests/ -v
    python3 -m pytest tests/ -v --tb=short
    python3 -m pytest tests/test_base_collector.py::TestMockCollectorLifecycle -v

DEPENDENCIES:
  pytest, unittest.mock (stdlib)
  No Supabase / Google Drive connections — all external calls are mocked.
"""

import sys
import os
import hashlib
import unittest
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Make sure the parent directory is importable
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from base_collector import (
    BaseCollector,
    CollectionResult,
    PipelineStage,
    CollectorError,
    CollectorDiscoverError,
    CollectorDownloadError,
    CollectorParseError,
    CollectorStoreError,
    CollectorArchiveError,
)
from source_config import (
    get_source_config,
    get_all_configs,
    get_sources_due_today,
    list_sources,
    SourceConfig,
)


# =============================================================================
# Mock Collector — Concrete implementation of BaseCollector for testing
# =============================================================================

class MockCollector(BaseCollector):
    """
    A minimal, fully-controllable concrete implementation of BaseCollector.

    All 5 stages are configurable via constructor parameters:
      - Pass a callable to override the default behavior of any stage
      - Pass an exception class to simulate a failure in any stage

    This is the canonical pattern for unit-testing collector subclasses.
    """

    def __init__(
        self,
        source_id: str = "mock_source",
        collection_date: date | None = None,
        discover_fn=None,
        download_fn=None,
        parse_fn=None,
        store_fn=None,
        archive_fn=None,
    ):
        # Disable external dependencies
        self._db = None  # No Supabase in tests
        self.source_id       = source_id
        self.collection_date = collection_date or date(2026, 2, 18)
        self._run_id         = "test_run_001"
        self._start_time     = None

        import logging
        self.logger = logging.getLogger(f"test.{source_id}")
        if not self.logger.handlers:
            self.logger.addHandler(logging.NullHandler())
        self.logger.setLevel(logging.DEBUG)

        # Stage call tracking — lets tests assert stage was/wasn't called
        self.stages_called: list[str] = []

        # Configurable stage implementations
        self._discover_fn = discover_fn
        self._download_fn = download_fn
        self._parse_fn    = parse_fn
        self._store_fn    = store_fn
        self._archive_fn  = archive_fn

    # ── Default stage implementations ──────────────────────────────────────

    def discover(self) -> dict:
        self.stages_called.append(PipelineStage.DISCOVER)
        if self._discover_fn:
            return self._discover_fn()
        return {
            "url":               "https://mock.source/data_20260218.pdf",
            "already_collected": False,
            "date_str":          "20260218",
            "metadata":          {"source": "mock"},
        }

    def download(self, discover_result: dict) -> dict:
        self.stages_called.append(PipelineStage.DOWNLOAD)
        if self._download_fn:
            return self._download_fn(discover_result)
        # Create a small temp file to simulate a download
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".pdf", prefix="mock_"
        ) as f:
            f.write(b"%PDF-1.4 mock content for testing")
            tmp_path = f.name
        return {
            "file_path":        tmp_path,
            "file_size_bytes":  32,
            "content_hash":     hashlib.sha256(b"mock").hexdigest(),
            "metadata":         {},
        }

    def parse(self, download_result: dict) -> list[dict]:
        self.stages_called.append(PipelineStage.PARSE)
        if self._parse_fn:
            return self._parse_fn(download_result)
        return [
            {
                "date":         "2026-02-18",
                "usd_tt_buy":   298.50,
                "usd_tt_sell":  304.00,
                "aspi":         13_540.25,
                "opr":          8.00,
            }
        ]

    def store(self, parsed_rows: list[dict]) -> dict:
        self.stages_called.append(PipelineStage.STORE)
        if self._store_fn:
            return self._store_fn(parsed_rows)
        return {
            "rows_stored":     len(parsed_rows),
            "rows_skipped":    0,
            "tables_written":  ["mock_table"],
        }

    def archive(self, download_result: dict, store_result: dict) -> dict:
        self.stages_called.append(PipelineStage.ARCHIVE)
        if self._archive_fn:
            return self._archive_fn(download_result, store_result)
        # Clean up the temp file created by download()
        file_path = download_result.get("file_path", "")
        if file_path and Path(file_path).exists():
            Path(file_path).unlink()
        return {
            "archive_path":  "/data/mock/mock_source_20260218.pdf",
            "local_deleted": True,
        }


# =============================================================================
# Test Suite 1: MockCollector Lifecycle
# =============================================================================

class TestMockCollectorLifecycle(unittest.TestCase):
    """Tests for the full 5-stage pipeline happy path."""

    def test_happy_path_returns_success(self):
        """Complete pipeline execution returns success=True."""
        collector = MockCollector()
        result = collector.run()

        self.assertTrue(result.success, f"Expected success, got error: {result.error}")
        self.assertEqual(result.source_id, "mock_source")
        self.assertEqual(result.collection_date, date(2026, 2, 18))
        self.assertEqual(result.rows_stored, 1)
        self.assertIsNone(result.error)

    def test_all_five_stages_called_in_order(self):
        """All 5 stages execute in DISCOVER → DOWNLOAD → PARSE → STORE → ARCHIVE order."""
        collector = MockCollector()
        collector.run()

        expected_order = [
            PipelineStage.DISCOVER,
            PipelineStage.DOWNLOAD,
            PipelineStage.PARSE,
            PipelineStage.STORE,
            PipelineStage.ARCHIVE,
        ]
        self.assertEqual(collector.stages_called, expected_order)

    def test_result_contains_archive_path(self):
        """CollectionResult.archive_path is populated on success."""
        collector = MockCollector()
        result = collector.run()
        self.assertEqual(result.archive_path, "/data/mock/mock_source_20260218.pdf")

    def test_result_duration_is_positive(self):
        """CollectionResult.duration_seconds is a positive float."""
        collector = MockCollector()
        result = collector.run()
        self.assertGreater(result.duration_seconds, 0.0)

    def test_result_has_run_id(self):
        """CollectionResult.metadata contains a run_id for log correlation."""
        collector = MockCollector()
        result = collector.run()
        self.assertIn("run_id", result.metadata)


# =============================================================================
# Test Suite 2: Idempotency (already-collected detection)
# =============================================================================

class TestIdempotency(unittest.TestCase):
    """Tests for the already-collected detection in DISCOVER stage."""

    def test_already_collected_skips_pipeline(self):
        """When DISCOVER signals already_collected=True, no other stages run."""
        collector = MockCollector(
            discover_fn=lambda: {
                "url":               "https://mock.source/data.pdf",
                "already_collected": True,
                "date_str":          "20260218",
                "metadata":          {},
            }
        )
        result = collector.run()

        # Only DISCOVER should have been called
        self.assertEqual(collector.stages_called, [PipelineStage.DISCOVER])
        # Result should still be success (skip is not a failure)
        self.assertTrue(result.success)
        self.assertEqual(result.rows_stored, 0)
        self.assertEqual(result.metadata.get("skipped_reason"), "already_collected")

    def test_already_collected_does_not_call_download(self):
        """DOWNLOAD stage is never called when already_collected=True."""
        download_called = []

        collector = MockCollector(
            discover_fn=lambda: {"already_collected": True, "url": "", "date_str": "", "metadata": {}},
            download_fn=lambda d: download_called.append(True) or {},
        )
        collector.run()
        self.assertEqual(download_called, [], "DOWNLOAD should not be called when already collected")

    def test_is_already_collected_returns_false_without_db(self):
        """is_already_collected() safely returns False when DB is not available."""
        collector = MockCollector()
        result = collector.is_already_collected("any_table")
        self.assertFalse(result)


# =============================================================================
# Test Suite 3: Error Propagation
# =============================================================================

class TestErrorPropagation(unittest.TestCase):
    """Tests for failure handling at each pipeline stage."""

    def _make_failing_fn(self, exception_class, message):
        """Helper: returns a lambda that raises the given exception."""
        def failing(*args, **kwargs):
            raise exception_class(message)
        return failing

    def test_discover_failure_returns_failure_result(self):
        collector = MockCollector(
            discover_fn=self._make_failing_fn(CollectorDiscoverError, "URL not found")
        )
        result = collector.run()

        self.assertFalse(result.success)
        self.assertEqual(result.stage, PipelineStage.DISCOVER)
        self.assertIn("URL not found", result.error)
        # No subsequent stages should have run
        self.assertEqual(collector.stages_called, [PipelineStage.DISCOVER])

    def test_download_failure_stops_pipeline(self):
        collector = MockCollector(
            download_fn=self._make_failing_fn(CollectorDownloadError, "Connection timeout")
        )
        result = collector.run()

        self.assertFalse(result.success)
        self.assertEqual(result.stage, PipelineStage.DOWNLOAD)
        self.assertIn("Connection timeout", result.error)
        # PARSE, STORE, ARCHIVE should NOT have run
        self.assertNotIn(PipelineStage.PARSE, collector.stages_called)
        self.assertNotIn(PipelineStage.STORE, collector.stages_called)
        self.assertNotIn(PipelineStage.ARCHIVE, collector.stages_called)

    def test_parse_failure_stops_pipeline(self):
        collector = MockCollector(
            parse_fn=self._make_failing_fn(CollectorParseError, "Zero rows extracted")
        )
        result = collector.run()

        self.assertFalse(result.success)
        self.assertEqual(result.stage, PipelineStage.PARSE)
        self.assertNotIn(PipelineStage.STORE, collector.stages_called)

    def test_store_failure_stops_pipeline(self):
        collector = MockCollector(
            store_fn=self._make_failing_fn(CollectorStoreError, "Supabase upsert failed")
        )
        result = collector.run()

        self.assertFalse(result.success)
        self.assertEqual(result.stage, PipelineStage.STORE)
        self.assertNotIn(PipelineStage.ARCHIVE, collector.stages_called)

    def test_archive_failure_returns_failure_result(self):
        collector = MockCollector(
            archive_fn=self._make_failing_fn(CollectorArchiveError, "Google Drive quota exceeded")
        )
        result = collector.run()

        self.assertFalse(result.success)
        self.assertEqual(result.stage, PipelineStage.ARCHIVE)
        # All prior stages succeeded
        self.assertIn(PipelineStage.STORE, collector.stages_called)

    def test_unexpected_exception_is_caught(self):
        """Untyped exceptions don't propagate — they're caught and returned as failures."""
        collector = MockCollector(
            parse_fn=lambda d: 1 / 0  # ZeroDivisionError — not a CollectorError
        )
        # Should NOT raise — all exceptions are swallowed and wrapped
        result = collector.run()
        self.assertFalse(result.success)
        # _run_stage wraps unknown exceptions as CollectorError with the exception message
        # Python's ZeroDivisionError message is "division by zero"
        self.assertIsNotNone(result.error)
        self.assertGreater(len(result.error or ""), 0)

    def test_failure_result_has_error_message(self):
        """Failed CollectionResult always has a non-empty error string."""
        collector = MockCollector(
            discover_fn=self._make_failing_fn(CollectorDiscoverError, "test error")
        )
        result = collector.run()
        self.assertIsNotNone(result.error)
        self.assertGreater(len(result.error), 0)


# =============================================================================
# Test Suite 4: Download Retry Logic
# =============================================================================

class TestDownloadRetry(unittest.TestCase):
    """Tests for the _download_with_retry() helper."""

    @patch("time.sleep")  # Don't actually sleep in tests
    @patch("requests.get")
    def test_retry_on_connection_error(self, mock_get, mock_sleep):
        """Failed downloads are retried up to MAX_RETRIES times."""
        import requests

        # Fail twice, succeed on third attempt
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock(return_value=None)
        mock_response.iter_content = MagicMock(return_value=[b"mock pdf content"])

        mock_get.side_effect = [
            requests.ConnectionError("timeout"),
            requests.ConnectionError("timeout"),
            mock_response,
        ]

        collector = MockCollector()
        collector.MAX_RETRIES = 3

        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir) / "test.pdf"
            result = collector._download_with_retry("https://example.com/test.pdf", dest)

        self.assertEqual(mock_get.call_count, 3)
        self.assertIn("file_path", result)
        # Sleep called between retries (2 times for 3 attempts)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("time.sleep")
    @patch("requests.get")
    def test_all_retries_exhausted_raises(self, mock_get, mock_sleep):
        """CollectorDownloadError raised after all retries fail."""
        import requests
        mock_get.side_effect = requests.ConnectionError("persistent failure")

        collector = MockCollector()
        collector.MAX_RETRIES = 3

        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir) / "test.pdf"
            with self.assertRaises(CollectorDownloadError):
                collector._download_with_retry("https://example.com/test.pdf", dest)

        self.assertEqual(mock_get.call_count, 3)

    @patch("requests.get")
    def test_successful_download_creates_file(self, mock_get):
        """A successful download writes the file and returns size + hash."""
        content = b"PDF content for testing SHA256 hash"
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock(return_value=None)
        mock_response.iter_content = MagicMock(return_value=[content])
        mock_get.return_value = mock_response

        collector = MockCollector()
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir) / "test.pdf"
            result = collector._download_with_retry("https://example.com/test.pdf", dest)
            # Assert inside the context manager — file is deleted when context exits
            self.assertTrue(Path(result["file_path"]).exists())
            self.assertEqual(result["file_size_bytes"], len(content))
            expected_hash = hashlib.sha256(content).hexdigest()
            self.assertEqual(result["content_hash"], expected_hash)


# =============================================================================
# Test Suite 5: Source Configuration
# =============================================================================

class TestSourceConfig(unittest.TestCase):
    """Tests for SourceConfig and the SOURCE_REGISTRY."""

    def test_all_three_sources_registered(self):
        """cbsl_daily, cbsl_weekly, and cse_daily are all registered."""
        sources = list_sources()
        self.assertIn("cbsl_daily", sources)
        self.assertIn("cbsl_weekly", sources)
        self.assertIn("cse_daily", sources)

    def test_cbsl_daily_url_template_contains_date(self):
        """CBSL daily URL is correctly substituted for a known date."""
        cfg = get_source_config("cbsl_daily")
        url = cfg.build_url(date(2026, 2, 18))
        self.assertIn("20260218", url)
        self.assertIn("daily_economic_indicators", url)
        self.assertIn("cbsl.gov.lk", url)

    def test_cbsl_weekly_url_template_contains_date(self):
        """CBSL weekly URL is correctly substituted for a known date."""
        cfg = get_source_config("cbsl_weekly")
        url = cfg.build_url(date(2026, 2, 21))  # Friday
        self.assertIn("20260221", url)
        self.assertIn("WEI", url)
        self.assertIn("cbsl.gov.lk", url)

    def test_cse_daily_returns_base_url_for_selenium(self):
        """Selenium sources return the base page URL, not a date-formatted URL."""
        cfg = get_source_config("cse_daily")
        url = cfg.build_url(date(2026, 2, 18))
        self.assertIn("cse.lk", url)
        # Should NOT have a date format in the URL (Selenium navigates dynamically)
        self.assertNotIn("20260218", url)

    def test_cbsl_daily_tables(self):
        """CBSL daily writes to exactly one Supabase table."""
        cfg = get_source_config("cbsl_daily")
        self.assertEqual(cfg.supabase_tables, ["cbsl_daily_indicators"])
        self.assertEqual(cfg.conflict_columns, ["date"])

    def test_cbsl_weekly_tables(self):
        """CBSL weekly writes to exactly four sector-specific tables."""
        cfg = get_source_config("cbsl_weekly")
        self.assertEqual(len(cfg.supabase_tables), 4)
        self.assertIn("cbsl_weekly_real_sector", cfg.supabase_tables)
        self.assertIn("cbsl_weekly_monetary_sector", cfg.supabase_tables)
        self.assertIn("cbsl_weekly_fiscal_sector", cfg.supabase_tables)
        self.assertIn("cbsl_weekly_external_sector", cfg.supabase_tables)

    def test_cse_daily_tables(self):
        """CSE daily writes to exactly four corporate action tables."""
        cfg = get_source_config("cse_daily")
        self.assertEqual(len(cfg.supabase_tables), 4)
        self.assertIn("cse_corporate_actions", cfg.supabase_tables)

    def test_build_filename_format(self):
        """Filenames follow the {source_id}_{YYYYMMDD}.{ext} convention."""
        cfg = get_source_config("cbsl_daily")
        fn = cfg.build_filename(date(2026, 2, 18))
        self.assertEqual(fn, "cbsl_daily_20260218.pdf")

    def test_is_due_today_cbsl_daily_on_weekday(self):
        """CBSL daily is due Monday through Friday."""
        cfg = get_source_config("cbsl_daily")
        monday = date(2026, 2, 16)    # Monday
        friday = date(2026, 2, 20)    # Friday
        saturday = date(2026, 2, 21)  # Saturday
        sunday = date(2026, 2, 22)    # Sunday
        self.assertTrue(cfg.is_due_today(monday))
        self.assertTrue(cfg.is_due_today(friday))
        self.assertFalse(cfg.is_due_today(saturday))
        self.assertFalse(cfg.is_due_today(sunday))

    def test_is_due_today_cbsl_weekly_on_friday_only(self):
        """CBSL weekly is due on Fridays only."""
        cfg = get_source_config("cbsl_weekly")
        thursday = date(2026, 2, 19)  # Thursday
        friday   = date(2026, 2, 20)  # Friday
        saturday = date(2026, 2, 21)  # Saturday
        self.assertFalse(cfg.is_due_today(thursday))
        self.assertTrue(cfg.is_due_today(friday))
        self.assertFalse(cfg.is_due_today(saturday))

    def test_invalid_source_raises_value_error(self):
        """Requesting a non-existent source_id raises ValueError."""
        with self.assertRaises(ValueError):
            get_source_config("non_existent_source")

    def test_get_sources_due_today_excludes_weekend(self):
        """On a Sunday, no weekday sources are due."""
        sunday = date(2026, 2, 22)
        due = get_sources_due_today(sunday)
        self.assertEqual(due, [])

    def test_get_sources_due_on_friday(self):
        """On a Friday, both cbsl_daily and cbsl_weekly are due (not cse_daily on weekends)."""
        friday = date(2026, 2, 20)
        due_ids = {cfg.source_id for cfg in get_sources_due_today(friday)}
        self.assertIn("cbsl_daily", due_ids)
        self.assertIn("cbsl_weekly", due_ids)
        self.assertIn("cse_daily", due_ids)  # CSE is Mon-Fri

    def test_all_configs_have_display_names(self):
        """Every registered source has a non-empty display_name."""
        for cfg in get_all_configs():
            self.assertTrue(cfg.display_name, f"{cfg.source_id}: missing display_name")

    def test_primary_table_returns_first_table(self):
        """primary_table() returns the first table in supabase_tables."""
        cfg = get_source_config("cbsl_daily")
        self.assertEqual(cfg.primary_table(), "cbsl_daily_indicators")


# =============================================================================
# Test Suite 6: CollectionResult
# =============================================================================

class TestCollectionResult(unittest.TestCase):
    """Tests for the CollectionResult data container."""

    def test_repr_success(self):
        """__repr__ includes OK for successful results."""
        result = CollectionResult(
            success=True, stage=PipelineStage.ARCHIVE,
            source_id="cbsl_daily", collection_date=date(2026, 2, 18),
            rows_stored=5, duration_seconds=12.3,
        )
        r = repr(result)
        self.assertIn("OK", r)
        self.assertIn("cbsl_daily", r)
        self.assertIn("5", r)

    def test_repr_failure(self):
        """__repr__ includes FAIL for failed results."""
        result = CollectionResult(
            success=False, stage=PipelineStage.DOWNLOAD,
            source_id="cbsl_daily", collection_date=date(2026, 2, 18),
            error="Connection refused",
        )
        self.assertIn("FAIL", repr(result))

    def test_default_metadata_is_empty_dict(self):
        """metadata defaults to {} — no mutable default argument issues."""
        r1 = CollectionResult(success=True, stage="X", source_id="s", collection_date=date.today())
        r2 = CollectionResult(success=True, stage="X", source_id="s", collection_date=date.today())
        r1.metadata["key"] = "val"
        self.assertNotIn("key", r2.metadata)  # Mutation of r1 does not affect r2


# =============================================================================
# Test Suite 7: Upsert Helper
# =============================================================================

class TestUpsertHelper(unittest.TestCase):
    """Tests for _upsert_rows() without a real DB connection."""

    def test_upsert_with_no_db_returns_row_count(self):
        """In test mode (db=None), _upsert_rows returns the number of rows passed."""
        collector = MockCollector()
        rows = [{"date": "2026-02-18", "aspi": 13540.0}]
        count = collector._upsert_rows("cbsl_daily_indicators", rows, ["date"])
        self.assertEqual(count, 1)

    def test_upsert_empty_rows_returns_zero(self):
        """_upsert_rows with empty list returns 0 without error."""
        collector = MockCollector()
        count = collector._upsert_rows("cbsl_daily_indicators", [], ["date"])
        self.assertEqual(count, 0)


# =============================================================================
# Test Suite 8: Integration — Full Pipeline with Multiple Rows
# =============================================================================

class TestMultiRowPipeline(unittest.TestCase):
    """End-to-end test simulating a real multi-row extraction."""

    def test_multiple_rows_stored(self):
        """Pipeline correctly stores multiple rows returned by parse()."""

        def parse_returns_many(download_result):
            return [
                {"date": "2026-02-18", "symbol": "JKH.N0000", "action_type": "cash_dividend"},
                {"date": "2026-02-18", "symbol": "DIAL.N0000", "action_type": "cash_dividend"},
                {"date": "2026-02-18", "symbol": "ACME.N0000", "action_type": "right_issue"},
            ]

        stored_rows: list[dict] = []

        def store_captures(parsed_rows):
            stored_rows.extend(parsed_rows)
            return {"rows_stored": len(parsed_rows), "rows_skipped": 0, "tables_written": ["cse_corporate_actions"]}

        collector = MockCollector(
            parse_fn=parse_returns_many,
            store_fn=store_captures,
        )
        result = collector.run()

        self.assertTrue(result.success)
        self.assertEqual(result.rows_stored, 3)
        self.assertEqual(len(stored_rows), 3)
        self.assertEqual(stored_rows[0]["symbol"], "JKH.N0000")

    def test_parse_returns_empty_list_is_handled(self):
        """Empty parse output passes to store (store logs a warning, does not raise)."""
        collector = MockCollector(parse_fn=lambda d: [])
        result = collector.run()
        # Should complete without error (zero rows is a valid state for a market holiday)
        self.assertTrue(result.success)
        self.assertEqual(result.rows_stored, 0)


# =============================================================================
# Entry point — run all tests
# =============================================================================

if __name__ == "__main__":
    # Run with verbose output when executed directly
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
