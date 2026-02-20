"""
source_config.py — Source Configuration Registry for Investment OS Data Collectors

VERSION HISTORY:
  v1.0.0  2026-02-18  Initial implementation — Sprint 0 (Framework Skeleton)
                       3 sources: CSE Daily Report, CBSL Daily, CBSL Weekly
                       Validated URL patterns (stable since 2019)

DESIGN PRINCIPLE (Registry Pattern):
  Adding a new data source = adding one entry to SOURCE_REGISTRY.
  No framework code changes required. The factory floor (base_collector.py) is
  untouched — only a new jig (parser + config entry) is needed.

USAGE:
  from source_config import get_source_config, list_sources
  cfg = get_source_config("cbsl_daily")
  print(cfg.url_template)

SOURCE IDs (used in CLI and cron scripts):
  cbsl_daily   → CBSL Daily Economic Indicators (daily, 5PM SLK)
  cbsl_weekly  → CBSL Weekly Economic Indicators (Friday, 6PM SLK)
  cse_daily    → CSE Daily Market Report (daily, 8PM SLK)
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Callable


# ---------------------------------------------------------------------------
# SourceConfig dataclass
# ---------------------------------------------------------------------------

@dataclass
class SourceConfig:
    """
    Complete configuration for a single data source collector.

    All fields are read by collector_runner.py and BaseCollector subclasses.
    Add new fields here as the framework evolves (e.g., proxy_required).
    """

    # Identity
    source_id:       str    # Unique key: "cbsl_daily", "cbsl_weekly", "cse_daily"
    display_name:    str    # Human-readable: "CBSL Daily Economic Indicators"
    description:     str    # One-line description for monitoring dashboards

    # Download
    download_method: str    # "http_get" | "selenium"
    url_template:    str    # f-string template — use {date_str} as placeholder
                            # Or "SELENIUM_DYNAMIC" for Selenium-navigated sources
    date_format:     str    # strftime format for {date_str}: "%Y%m%d", ""
    base_url:        str    # Base domain for display/logging (not download target)

    # Parse
    parse_pages:     list[int]   # 1-indexed PDF page numbers to extract ([] = all)
    parser_class:    str         # Class name in parsers/ — imported dynamically by runner

    # Store
    supabase_tables: list[str]   # Target tables — first is the primary (idempotency check)
    conflict_columns: list[str]  # Columns forming the unique key for upsert

    # Archive
    storage_target:  str    # "vps_local" | "google_drive"
    archive_dir:     str    # VPS path (vps_local) or Drive folder path (google_drive)

    # Schedule
    schedule_cron:   str    # Standard cron expression (all times SLK = UTC+5:30)
    schedule_desc:   str    # Human: "Daily at 5:00 PM SLK (business days only)"

    # File characteristics
    typical_size_kb: int    # Expected file size — used for download validation
    file_extension:  str    # "pdf", "html", etc.

    # Retry overrides (None = use BaseCollector defaults)
    max_retries:     int | None = None
    retry_backoff_s: float | None = None
    download_timeout_s: int | None = None

    # URL reliability note (documents known-stable patterns)
    url_stable_since: str = ""      # e.g., "2019" — documented for future engineers

    # Extra metadata (source-specific config passed to parser)
    extra: dict = field(default_factory=dict)

    # ---------------------------------------------------------------------------
    # Computed helpers
    # ---------------------------------------------------------------------------

    def build_url(self, target_date: date) -> str:
        """
        Resolve the download URL for a specific date.

        For http_get sources: substitutes {date_str} in url_template.
        For selenium sources: returns the base page URL (navigation handled by collector).

        >>> cfg = get_source_config("cbsl_daily")
        >>> cfg.build_url(date(2026, 2, 18))
        'https://www.cbsl.gov.lk/sites/default/files/daily_economic_indicators_20260218_e.pdf'
        """
        if self.download_method == "selenium":
            return self.url_template  # Selenium navigates dynamically
        if self.date_format and "{date_str}" in self.url_template:
            date_str = target_date.strftime(self.date_format)
            return self.url_template.format(date_str=date_str)
        return self.url_template

    def build_filename(self, target_date: date) -> str:
        """
        Generate a canonical local filename for the downloaded file.

        Convention: {source_id}_{YYYYMMDD}.{ext}
        Example: cbsl_daily_20260218.pdf

        This ensures files from different sources never collide in the temp dir.
        """
        return f"{self.source_id}_{target_date.strftime('%Y%m%d')}.{self.file_extension}"

    def primary_table(self) -> str:
        """The first Supabase table — used for idempotency checks."""
        return self.supabase_tables[0]

    def is_due_today(self, target_date: date | None = None) -> bool:
        """
        Quick check: should this source be collected on the given date?

        Implements source-specific frequency logic:
          - cbsl_daily:  Monday–Friday only (business days)
          - cbsl_weekly: Friday only
          - cse_daily:   Monday–Friday only (CSE trading days)

        Note: Does not account for public holidays (handled by Supabase idempotency).
        """
        target_date = target_date or date.today()
        weekday = target_date.weekday()  # 0=Monday … 6=Sunday

        if self.source_id == "cbsl_weekly":
            return weekday == 4   # Friday only
        elif self.source_id in ("cbsl_daily", "cse_daily"):
            return weekday < 5    # Monday–Friday
        return True  # Default: always collect


# ---------------------------------------------------------------------------
# Source Registry — THE SINGLE SOURCE OF TRUTH for all configured sources
# ---------------------------------------------------------------------------

SOURCE_REGISTRY: dict[str, SourceConfig] = {

    # ────────────────────────────────────────────────────────────────────────
    # CBSL DAILY ECONOMIC INDICATORS
    # Sprint 1 target — simplest source, validates framework end-to-end
    # ────────────────────────────────────────────────────────────────────────
    "cbsl_daily": SourceConfig(
        source_id       = "cbsl_daily",
        display_name    = "CBSL Daily Economic Indicators",
        description     = (
            "Single-page daily PDF dashboard from Central Bank of Sri Lanka. "
            "Captures 31 metrics: exchange rates, T-bill yields, money market, "
            "share market indices, energy prices, and macro headlines."
        ),

        download_method = "http_get",
        url_template    = (
            "https://www.cbsl.gov.lk/sites/default/files/"
            "daily_economic_indicators_{date_str}_e.pdf"
        ),
        date_format     = "%Y%m%d",
        base_url        = "https://www.cbsl.gov.lk",

        parse_pages     = [1],   # Single-page dashboard — extract all of page 1
        parser_class    = "CBSLDailyParser",   # parsers/cbsl_daily_parser.py

        supabase_tables  = ["cbsl_daily_indicators"],
        conflict_columns = ["date"],

        storage_target  = "vps_local",
        archive_dir     = "/opt/investment-os/services/data-collectors/data/cbsl_daily/",

        schedule_cron   = "30 11 * * 1-5",    # 5:00 PM SLK = 11:30 AM UTC (UTC+5:30)
        schedule_desc   = "Daily at 5:00 PM SLK, Monday–Friday (business days)",

        typical_size_kb     = 200,
        file_extension      = "pdf",
        max_retries         = 3,
        retry_backoff_s     = 2.0,
        download_timeout_s  = 30,
        url_stable_since    = "2019",

        extra = {
            # pdfplumber table extraction settings for the single-page dashboard
            "table_settings": {
                "vertical_strategy":   "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance":      3,
            },
            # Charts generate garbled text — suppress them in parsing
            "ignore_chart_text":     True,
            # Column map: PDF table header → Supabase column name
            # Populated by CBSLDailyParser after first real PDF inspection
            "column_map":            {},
        },
    ),

    # ────────────────────────────────────────────────────────────────────────
    # CBSL WEEKLY ECONOMIC INDICATORS
    # Sprint 2 target — 16-page sectoral report, 4 sub-parsers
    # ────────────────────────────────────────────────────────────────────────
    "cbsl_weekly": SourceConfig(
        source_id       = "cbsl_weekly",
        display_name    = "CBSL Weekly Economic Indicators",
        description     = (
            "16-page Friday sectoral report from Central Bank of Sri Lanka. "
            "Four sectors: Real (CPI/GDP/IIP/PMI), Monetary (rates/money supply), "
            "Fiscal (T-bill auctions/govt securities), External (FX/trade/reserves)."
        ),

        download_method = "http_get",
        url_template    = (
            "https://www.cbsl.gov.lk/sites/default/files/"
            "cbslweb_documents/statistics/wei/WEI_{date_str}_e.pdf"
        ),
        date_format     = "%Y%m%d",
        base_url        = "https://www.cbsl.gov.lk",

        # Pages 1-2: Highlights narrative (qualitative, skip)
        # Pages 3-6:  Real Sector
        # Pages 7-9:  Monetary Sector
        # Pages 10-12: Fiscal Sector
        # Pages 13-16: External Sector
        parse_pages     = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        parser_class    = "CBSLWeeklyParser",   # parsers/cbsl_weekly_parser.py

        supabase_tables  = [
            "cbsl_weekly_real_sector",
            "cbsl_weekly_monetary_sector",
            "cbsl_weekly_fiscal_sector",
            "cbsl_weekly_external_sector",
        ],
        conflict_columns = ["week_ending"],   # Friday date is the PK for all 4 tables

        storage_target  = "vps_local",
        archive_dir     = "/opt/investment-os/services/data-collectors/data/cbsl_weekly/",

        schedule_cron   = "30 12 * * 5",      # Friday 6:00 PM SLK = 12:30 PM UTC
        schedule_desc   = "Every Friday at 6:00 PM SLK",

        typical_size_kb     = 1000,
        file_extension      = "pdf",
        max_retries         = 3,
        retry_backoff_s     = 2.0,
        download_timeout_s  = 60,
        url_stable_since    = "2019",

        extra = {
            # Page ranges per sector (used by CBSLWeeklyParser to route to sub-parsers)
            "sector_pages": {
                "real_sector":      [3, 4, 5, 6],
                "monetary_sector":  [7, 8, 9],
                "fiscal_sector":    [10, 11, 12],
                "external_sector":  [13, 14, 15, 16],
            },
            # Charts produce reversed/garbled text (e.g., 'keeW', 'tnec reP')
            # Strategy: ignore chart text, extract data from table regions only
            "ignore_chart_text":   True,
            "chart_noise_tokens":  ["keeW", "tnec reP", "etar", "htworG"],
            "table_settings": {
                "vertical_strategy":   "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance":      5,
            },
        },
    ),

    # ────────────────────────────────────────────────────────────────────────
    # CSE DAILY MARKET REPORT (Corporate Actions)
    # Sprint 3 target — largest file, Selenium + Google Drive
    # ────────────────────────────────────────────────────────────────────────
    "cse_daily": SourceConfig(
        source_id       = "cse_daily",
        display_name    = "CSE Daily Market Report (Corporate Actions)",
        description     = (
            "Daily ~25MB PDF from Colombo Stock Exchange. "
            "Pages 11-15 contain: right issues, share subdivisions, scrip dividends, "
            "cash dividends, watch list, and trading suspensions. "
            "Full PDF archived to Google Drive; only pages 11-15 parsed."
        ),

        download_method = "selenium",
        # Selenium navigates to this page and clicks the latest daily report link
        url_template    = "https://www.cse.lk/publications/cse-daily",
        date_format     = "",            # Dynamic navigation, not date-in-URL
        base_url        = "https://www.cse.lk",

        # Only pages 11-15 contain corporate actions data
        # Page 11: Right Issues | Page 12: Share Subdivisions + Scrip Dividends
        # Page 13: Cash Dividends | Page 14: Watch List | Page 15: Suspended
        parse_pages     = [11, 12, 13, 14, 15],
        parser_class    = "CSEReportParser",   # parsers/cse_report_parser.py

        supabase_tables  = [
            "cse_corporate_actions",    # Master log (all action types in one place)
            "cse_right_issues",         # Right issue dates + ratios + prices
            "cse_dividends",            # Cash/scrip dividends with XD/record/payment dates
            "cse_watch_list_history",   # Watch list and suspension entry/exit
        ],
        conflict_columns = ["date", "symbol", "action_type"],

        storage_target  = "google_drive",
        # Drive folder structure mirrors date hierarchy for easy Cowork access
        archive_dir     = "Investment OS/CSE Daily Reports/{year}/{month:02d}/",

        schedule_cron   = "30 14 * * 1-5",    # 8:00 PM SLK = 14:30 PM UTC
        schedule_desc   = "Daily at 8:00 PM SLK, Monday–Friday",

        typical_size_kb     = 25_000,      # ~25MB per file
        file_extension      = "pdf",
        max_retries         = 3,
        retry_backoff_s     = 5.0,         # Longer backoff for Selenium
        download_timeout_s  = 120,         # Large file, slow download

        url_stable_since    = "2020",      # Published consistently since ~2020

        extra = {
            # Section headers used for page-content detection
            # (more resilient than hard page numbers if layout shifts)
            "section_headers": {
                "right_issues":        "Right Issues",
                "share_subdivisions":  "Share Subdivisions",
                "scrip_dividends":     "Scrip Dividends",
                "cash_dividends":      "Cash Dividends",
                "watch_list":          "Watch List",
                "trading_suspended":   "Trading Suspended",
            },
            # Selenium: ChromeDriver settings (reuse config from Service 5 cse_ohlcv_collector)
            "selenium": {
                "headless":           True,
                "download_dir":       "/opt/investment-os/services/data-collectors/data/temp/",
                "page_load_timeout":  30,
                "implicit_wait":      10,
            },
            # Google Drive: folder ID for "Investment OS" root
            # Set from environment variable GDRIVE_FOLDER_ID (populated in .env)
            "gdrive_folder_env_var": "GDRIVE_FOLDER_ID",
            "gdrive_subfolder":      "CSE Daily Reports",
        },
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_source_config(source_id: str) -> SourceConfig:
    """
    Retrieve configuration for a given source_id.

    Args:
        source_id: One of "cbsl_daily", "cbsl_weekly", "cse_daily"

    Returns:
        SourceConfig instance

    Raises:
        ValueError if source_id is not registered.

    Usage:
        cfg = get_source_config("cbsl_daily")
        url = cfg.build_url(date.today())
    """
    if source_id not in SOURCE_REGISTRY:
        valid = ", ".join(sorted(SOURCE_REGISTRY.keys()))
        raise ValueError(
            f"Unknown source_id: '{source_id}'. "
            f"Valid options: {valid}"
        )
    return SOURCE_REGISTRY[source_id]


def list_sources() -> list[str]:
    """Return all registered source IDs, sorted alphabetically."""
    return sorted(SOURCE_REGISTRY.keys())


def get_all_configs() -> list[SourceConfig]:
    """Return all registered SourceConfig objects, sorted by source_id."""
    return [SOURCE_REGISTRY[sid] for sid in sorted(SOURCE_REGISTRY.keys())]


def get_sources_due_today(target_date: date | None = None) -> list[SourceConfig]:
    """
    Return configs for sources that should be collected today.

    Used by monitoring/briefing scripts to know which collectors should have run.

    Args:
        target_date: Date to check (defaults to today)

    Returns:
        List of SourceConfig for sources due on target_date.
    """
    target_date = target_date or date.today()
    return [cfg for cfg in get_all_configs() if cfg.is_due_today(target_date)]


# ---------------------------------------------------------------------------
# Validation (run at import time to catch config typos early)
# ---------------------------------------------------------------------------

def _validate_registry() -> None:
    """Sanity-check the registry at import time. Catches typos before deployment."""
    for sid, cfg in SOURCE_REGISTRY.items():
        assert cfg.source_id == sid, f"source_id mismatch: key={sid}, value.source_id={cfg.source_id}"
        assert cfg.supabase_tables, f"{sid}: supabase_tables cannot be empty"
        assert cfg.conflict_columns, f"{sid}: conflict_columns cannot be empty"
        assert cfg.download_method in ("http_get", "selenium"), \
            f"{sid}: download_method must be 'http_get' or 'selenium'"
        assert cfg.storage_target in ("vps_local", "google_drive"), \
            f"{sid}: storage_target must be 'vps_local' or 'google_drive'"
        assert cfg.parser_class, f"{sid}: parser_class cannot be empty"
        if cfg.download_method == "http_get":
            assert "{date_str}" in cfg.url_template, \
                f"{sid}: http_get sources must have {{date_str}} in url_template"

_validate_registry()


# ---------------------------------------------------------------------------
# CLI helper — python source_config.py (list all registered sources)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from datetime import date as _date
    print("\n═══ Investment OS — Registered Data Sources ═══\n")
    for cfg in get_all_configs():
        print(f"  {cfg.source_id:<16}  {cfg.display_name}")
        print(f"    Method:   {cfg.download_method}")
        print(f"    Tables:   {', '.join(cfg.supabase_tables)}")
        print(f"    Schedule: {cfg.schedule_desc}")
        print(f"    Size:     ~{cfg.typical_size_kb:,} KB")
        print(f"    URL stable since: {cfg.url_stable_since}")
        print(f"    Sample URL ({_date.today()}): {cfg.build_url(_date.today())}")
        print()

    print("Sources due today:", [c.source_id for c in get_sources_due_today()])
    print()
