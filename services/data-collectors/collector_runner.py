
"""
collector_runner.py — CLI Entry Point for Investment OS Data Collectors

VERSION HISTORY:
  v1.0.0  2026-02-18  Initial implementation — Sprint 0 (Framework Skeleton)
                       CLI with --source, --date, --dry-run, --force flags
                       Dynamic parser import — no hardcoded class references
                       Exit codes: 0=success, 1=failure, 2=already-collected

USAGE:
  # Collect today's CBSL daily indicators
  python3 collector_runner.py --source cbsl_daily

  # Collect a specific date (backfill)
  python3 collector_runner.py --source cbsl_daily --date 2026-01-15

  # Dry-run: discover + validate URL without downloading
  python3 collector_runner.py --source cbsl_weekly --dry-run

  # Force re-collection even if date already exists in Supabase
  python3 collector_runner.py --source cbsl_daily --force

  # List all available sources
  python3 collector_runner.py --list

  # Collect all sources due today (used by orchestration scripts)
  python3 collector_runner.py --all

ENVIRONMENT:
  Requires PYTHONPATH to include /opt/investment-os/packages (set in ~/.bashrc on VPS)
  Reads credentials from /opt/investment-os/.env via common.config
"""

import sys
import os
import argparse
import importlib
import logging
from datetime import date, datetime
from pathlib import Path

# Add packages to path (for local dev without PYTHONPATH set)
_PACKAGES_DIR = Path(__file__).resolve().parent.parent.parent.parent / "packages"
if _PACKAGES_DIR.exists() and str(_PACKAGES_DIR) not in sys.path:
    sys.path.insert(0, str(_PACKAGES_DIR))

from source_config import get_source_config, list_sources, get_sources_due_today, SourceConfig
from base_collector import BaseCollector, CollectionResult

# ---------------------------------------------------------------------------
# Logger for the runner itself (separate from collector-level loggers)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | runner | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("collector_runner")


# ---------------------------------------------------------------------------
# Dynamic collector loader
# ---------------------------------------------------------------------------

# Maps source_id → (module path, class name) within the parsers/ directory.
# Updated as each Sprint adds a new collector.
_COLLECTOR_MAP: dict[str, tuple[str, str]] = {
    "cbsl_daily":  ("parsers.cbsl_daily_parser",  "CBSLDailyCollector"),
    "cbsl_weekly": ("parsers.cbsl_weekly_parser", "CBSLWeeklyCollector"),
    "cse_daily":   ("parsers.cse_report_parser",  "CSEReportCollector"),
}


def load_collector_class(source_id: str) -> type[BaseCollector]:
    """
    Dynamically import and return the collector class for a given source_id.

    This avoids circular imports and makes it trivial to add new sources
    without touching runner code.

    Raises:
        NotImplementedError if the source's Sprint hasn't been built yet.
        ImportError if the module exists but cannot be imported.
    """
    if source_id not in _COLLECTOR_MAP:
        raise ValueError(
            f"No collector registered for source_id='{source_id}'. "
            f"Valid sources: {', '.join(sorted(_COLLECTOR_MAP.keys()))}"
        )

    module_path, class_name = _COLLECTOR_MAP[source_id]

    # Check if module file exists before attempting import (Sprint guard)
    module_file = Path(__file__).parent / module_path.replace(".", "/")
    module_file_py = module_file.with_suffix(".py")
    if not module_file_py.exists():
        raise NotImplementedError(
            f"Collector for '{source_id}' not yet implemented.\n"
            f"Expected: {module_file_py}\n"
            f"This source is scheduled for a future Sprint. "
            f"Run --dry-run to validate the URL pattern now."
        )

    module = importlib.import_module(module_path)
    collector_class = getattr(module, class_name)

    if not issubclass(collector_class, BaseCollector):
        raise TypeError(
            f"{class_name} does not extend BaseCollector. "
            f"All collectors must inherit from base_collector.BaseCollector."
        )

    return collector_class


# ---------------------------------------------------------------------------
# Dry-run mode — validate URL without downloading
# ---------------------------------------------------------------------------

def dry_run(source_id: str, target_date: date) -> int:
    """
    DISCOVER stage only — validates URL accessibility without downloading.

    Returns:
        0 = URL accessible
        1 = URL not accessible
    """
    import requests

    cfg = get_source_config(source_id)
    url = cfg.build_url(target_date)

    logger.info(f"[DRY-RUN] source={source_id} | date={target_date}")
    logger.info(f"[DRY-RUN] URL: {url}")
    logger.info(f"[DRY-RUN] Method: {cfg.download_method} | Tables: {cfg.supabase_tables}")
    logger.info(f"[DRY-RUN] Schedule: {cfg.schedule_desc}")

    if cfg.download_method == "selenium":
        logger.info("[DRY-RUN] Selenium source — cannot validate URL via HTTP GET.")
        logger.info(f"[DRY-RUN] Navigate to: {url}")
        return 0

    try:
        logger.info("[DRY-RUN] Sending HEAD request to validate URL...")
        headers = {"User-Agent": "Mozilla/5.0 (compatible; InvestmentOS/1.0)"}
        response = requests.head(url, headers=headers, timeout=15, allow_redirects=True)
        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            content_length = response.headers.get("Content-Length", "unknown")
            logger.info(
                f"[DRY-RUN] URL ACCESSIBLE ✓ | "
                f"status={response.status_code} | "
                f"type={content_type} | "
                f"size={content_length} bytes"
            )
            return 0
        elif response.status_code == 405:
            # Some servers don't support HEAD — try GET with stream=True
            response = requests.get(url, headers=headers, timeout=15, stream=True)
            if response.status_code == 200:
                logger.info(f"[DRY-RUN] URL ACCESSIBLE ✓ (GET fallback) | status=200")
                response.close()
                return 0
        logger.error(f"[DRY-RUN] URL NOT ACCESSIBLE ✗ | status={response.status_code}")
        return 1
    except Exception as e:
        logger.error(f"[DRY-RUN] URL CHECK FAILED: {e}")
        return 1


# ---------------------------------------------------------------------------
# Single source run
# ---------------------------------------------------------------------------

def run_source(
    source_id: str,
    target_date: date,
    force: bool = False,
    dry_run_mode: bool = False,
) -> int:
    """
    Load and execute the collector for one source + date.

    Args:
        source_id:    Source identifier from source_config.py
        target_date:  Date to collect
        force:        If True, skip idempotency check (re-collect even if exists)
        dry_run_mode: If True, only validate URL — don't download or parse

    Returns:
        Exit code: 0=success, 1=failure, 2=already-collected (skipped)
    """
    if dry_run_mode:
        return dry_run(source_id, target_date)

    cfg = get_source_config(source_id)
    logger.info(
        f"\n{'=' * 60}\n"
        f"  source={source_id} | date={target_date} | force={force}\n"
        f"  {cfg.display_name}\n"
        f"{'=' * 60}"
    )

    # Load collector class (raises NotImplementedError if Sprint not built yet)
    try:
        CollectorClass = load_collector_class(source_id)
    except NotImplementedError as e:
        logger.warning(str(e))
        logger.info("Tip: Run with --dry-run to validate the URL now.")
        return 1
    except Exception as e:
        logger.error(f"Failed to load collector for '{source_id}': {e}")
        return 1

    # Instantiate and run
    collector: BaseCollector = CollectorClass(
        source_id=source_id,
        collection_date=target_date,
    )

    # Force mode: monkey-patch idempotency check
    if force:
        collector.is_already_collected = lambda *args, **kwargs: False
        logger.info("[FORCE] Idempotency check disabled — will re-collect.")

    result: CollectionResult = collector.run()

    # Print result summary
    _print_result(result)

    if result.success:
        if result.rows_stored == 0 and result.metadata.get("skipped_reason") == "already_collected":
            logger.info(f"Skipped (already collected). Use --force to re-collect.")
            return 2
        return 0
    else:
        return 1


# ---------------------------------------------------------------------------
# All-sources run (orchestration mode)
# ---------------------------------------------------------------------------

def run_all(target_date: date, force: bool = False) -> int:
    """
    Run all sources that are due on target_date.

    Used by the nightly orchestration cron job (not typically called by hand).
    Returns the number of failures (0 = all succeeded).
    """
    due_sources = get_sources_due_today(target_date)
    if not due_sources:
        logger.info(f"No sources due on {target_date} (weekend or no configured sources).")
        return 0

    logger.info(f"Running {len(due_sources)} source(s) due on {target_date}:")
    for cfg in due_sources:
        logger.info(f"  • {cfg.source_id} ({cfg.schedule_desc})")

    results: dict[str, int] = {}
    for cfg in due_sources:
        exit_code = run_source(cfg.source_id, target_date, force=force)
        results[cfg.source_id] = exit_code

    # Summary
    logger.info(f"\n{'=' * 60}")
    logger.info("  ALL-SOURCES RUN SUMMARY")
    logger.info(f"{'=' * 60}")
    failures = 0
    for source_id, code in results.items():
        status = "✓ OK" if code == 0 else ("↷ SKIPPED" if code == 2 else "✗ FAILED")
        logger.info(f"  {source_id:<16}  {status}")
        if code == 1:
            failures += 1
    logger.info(f"{'=' * 60}")
    logger.info(f"  {len(results)} sources | {failures} failures")

    return failures


# ---------------------------------------------------------------------------
# Result display
# ---------------------------------------------------------------------------

def _print_result(result: CollectionResult) -> None:
    """Pretty-print a CollectionResult to stdout."""
    status = "✓ SUCCESS" if result.success else "✗ FAILURE"
    print(f"\n{'─' * 50}")
    print(f"  {status}")
    print(f"  source:   {result.source_id}")
    print(f"  date:     {result.collection_date}")
    print(f"  stage:    {result.stage}")
    print(f"  rows:     {result.rows_stored}")
    print(f"  time:     {result.duration_seconds:.1f}s")
    if result.archive_path:
        print(f"  archive:  {result.archive_path}")
    if result.error:
        print(f"  error:    {result.error}")
    if result.metadata.get("tables_written"):
        print(f"  tables:   {', '.join(result.metadata['tables_written'])}")
    print(f"{'─' * 50}\n")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="collector_runner.py",
        description=(
            "Investment OS Data Collector — runs the 5-stage pipeline "
            "(DISCOVER → DOWNLOAD → PARSE → STORE → ARCHIVE) for a given source."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python3 collector_runner.py --source cbsl_daily
  python3 collector_runner.py --source cbsl_daily --date 2026-01-15
  python3 collector_runner.py --source cbsl_weekly --dry-run
  python3 collector_runner.py --source cbsl_daily --force
  python3 collector_runner.py --all
  python3 collector_runner.py --list
        """,
    )

    # Mutually exclusive: --source, --all, --list
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--source", "-s",
        metavar="SOURCE_ID",
        help="Source ID to collect. Use --list to see options.",
    )
    mode_group.add_argument(
        "--all",
        action="store_true",
        help="Run all sources due today (orchestration mode).",
    )
    mode_group.add_argument(
        "--list",
        action="store_true",
        help="List all registered source IDs and exit.",
    )

    parser.add_argument(
        "--date", "-d",
        metavar="YYYY-MM-DD",
        help="Date to collect (default: today). Used for backfilling.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate URL accessibility only — do not download or parse.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-collect even if this date already exists in Supabase.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)

    # --list mode
    if args.list:
        print("\nRegistered data sources:\n")
        for source_id in list_sources():
            cfg = get_source_config(source_id)
            print(f"  {source_id:<16}  {cfg.display_name}")
            print(f"                    {cfg.schedule_desc}")
        print()
        return 0

    # Parse date
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: '{args.date}'. Use YYYY-MM-DD.")
            return 1
    else:
        target_date = date.today()

    # --all mode
    if args.all:
        failure_count = run_all(target_date, force=args.force)
        return 0 if failure_count == 0 else 1

    # --source mode
    return run_source(
        source_id=args.source,
        target_date=target_date,
        force=args.force,
        dry_run_mode=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
