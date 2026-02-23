
"""
parsers/cse_report_parser.py — CSE Daily Market Report Collector & Parser

Sprint 3 (corporate actions) + Sprint A (market summary + foreign flow).

Collects the daily ~25MB PDF from Colombo Stock Exchange publications page,
extracts market-level signals from pages 1-5 and corporate actions from pages
11-15, stores in 7 Supabase tables, and archives the raw PDF to Google Drive.

VERSION HISTORY:
  v1.0.0  2026-02-19  Sprint 3 — Initial implementation, PDF structure validated
                       from Feb 13, 2026 sample report.
  v1.1.0  2026-02-20  Format update fix — CSE redesigned Corporate Announcements
                       section with a new index/TOC page listing all section names.
                       Fix 1: _detect_section_pages skips TOC pages (≥4 headers).
                       Fix 2: _parse_right_issues falls back to tables[0] when the
                       new single-table format is used (old format had 2 tables).
  v1.2.0  2026-02-22  Sprint A — Market summary + foreign flow extraction.
                       New tables: cse_daily_market_summary, cse_foreign_flow.
                       New methods: _detect_market_pages, _parse_market_summary,
                         _parse_foreign_flow, _extract_index_row,
                         _extract_labeled_value, _extract_labeled_int,
                         _validate_market_row.
                       Layer 1 validation: MARKET_VALIDATION range dict.
                       parse_confidence (0-100) stored per row.
                       Cross-validation: aspi_close vs cbsl_daily_indicators ±0.5%.

SOURCE:
  URL:       https://www.cse.lk/publications/cse-daily  (Angular SPA, Selenium required)
  Format:    ~270-page PDF (~25MB)
  Published: Every trading day, after market close (~7-8 PM SLK)

PAGES PARSED:
  Sprint A — Market level (pages 1-5):
    Page 1-2:  Market overview → cse_daily_market_summary
               (ASPI, S&P SL20, turnover, market cap, breadth, volume)
    Page 3-5:  Foreign trading → cse_foreign_flow
               (foreign buy / sell / net LKR, net_flow_direction IN/OUT)

  Sprint 3 — Corporate actions (pages 11-15):
    Page 11: Right Issues      → cse_right_issues + cse_corporate_actions
    Page 12: Share Subdivisions → cse_share_splits + cse_corporate_actions
             Scrip Dividends   → cse_dividends    + cse_corporate_actions
    Page 13: Cash Dividends    → cse_dividends    + cse_corporate_actions
    Page 14: Watch List        → cse_watch_list_history (symbol PRESENT)
    Page 15: Trading Suspended → cse_watch_list_history (symbol PRESENT)

PARSING STRATEGIES:
  Market Summary (p1-2):  Text scanning — label/value patterns for indices +
                           scalars (turnover, market cap, volume, breadth).
  Foreign Flow (p3-5):    Text scanning — "Foreign Purchases / Sales / Net"
                           with LKR comma-formatted amounts.
  Right Issues (p11):     Compressed TABLE 2 → column-position mapping.
  Share Subdivisions(p12): Text-based line parsing (company name + dates).
  Scrip Dividends (p12):  Text-based line parsing (company name + 2 dates).
  Cash Dividends (p13):   Text-based line parsing with regex.
  Watch List (p14):       Text-based regex on symbol pattern (XXX.N0000).
  Suspended (p15):        Text-based regex on symbol pattern.

SYMBOL NOTE:
  - Watch List and Trading Suspended tables include the symbol (ALHP.N0000 etc.)
  - Right Issues, Dividends, Splits tables include company name ONLY — no symbol.
  - Symbols for those sections are left NULL in DB. A downstream enrichment job
    can match company_name → symbol using the stock universe table.

PIPELINE:
  DISCOVER  → Check Supabase whether today's report already stored.
  DOWNLOAD  → Selenium: navigate CSE publications, download latest PDF.
  PARSE     → pdfplumber: text + table extraction per section.
  STORE     → Upsert into 7 tables (market_summary, foreign_flow, master,
              right_issues, splits, dividends, watch_list).
  ARCHIVE   → Upload to Google Drive: Investment OS/CSE Daily Reports/{YYYY}/{MM}/.
"""

import json
import re
import shutil
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pdfplumber

# ---------------------------------------------------------------------------
# Path bootstrap — resolve sibling packages without PYTHONPATH
# ---------------------------------------------------------------------------

_SERVICES_DIR = Path(__file__).resolve().parent.parent
if str(_SERVICES_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVICES_DIR))

from base_collector import (
    BaseCollector,
    CollectorDiscoverError,
    CollectorDownloadError,
    CollectorArchiveError,
    CollectorParseError,
    CollectorStoreError,
)
from storage.gdrive_uploader import GDriveUploader, GDriveUploaderError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_ID = "cse_daily"

TABLES = {
    "master":          "cse_corporate_actions",
    "right_issues":    "cse_right_issues",
    "dividends":       "cse_dividends",
    "watch_list":      "cse_watch_list_history",
    "splits":          "cse_share_splits",
    # ── Sprint A: market-level data (pages 1-5) ───────────────────────────
    "market_summary":  "cse_daily_market_summary",
    "foreign_flow":    "cse_foreign_flow",
}

# Validation ranges for market summary fields (Layer 1 robustness)
MARKET_VALIDATION = {
    "aspi_close":       (3_000,   25_000),    # CSE ASPI historical range
    "sp20_close":       (1_000,   10_000),    # S&P SL 20 range
    "turnover_lkr":     (50e6,    50e9),      # LKR 50M – 50B (typical trading day)
    "market_cap_lkr":   (1e12,    25e12),     # LKR 1T – 25T
    "volume_shares":    (1e6,     5e9),       # 1M – 5B shares
    "trade_count":      (100,     500_000),   # 100 – 500K trades
    "stocks_advancing": (0,       350),       # CSE has ~340 listed
    "stocks_declining": (0,       350),
    "stocks_unchanged": (0,       350),
}

TEMP_DIR = Path("/opt/investment-os/services/data-collectors/data/temp")
GDRIVE_SUBFOLDER_TEMPLATE = "CSE Daily Reports/{year}/{month:02d}"

# Section headers — used for content-based page detection
SECTION_HEADERS = {
    "right_issues":       ["Right Issues", "Right Issue"],
    "share_subdivisions": ["Sub Division of Shares", "Share Subdivision"],
    "scrip_dividends":    ["Scrip Dividends", "Scrip Dividend"],
    "cash_dividends":     ["Cash Dividends", "Cash Dividend"],
    "watch_list":         ["Watch List", "Watch Listed"],
    "trading_suspended":  ["Trading Suspended", "Suspended"],
}

CSE_PUBLICATIONS_URL = "https://www.cse.lk/publications/cse-daily"

# Date formats found in CSE PDFs
DATE_FORMATS = [
    "%m/%d/%Y",   # 2/13/2026 (most common in CSE daily report)
    "%d/%m/%Y",   # 13/02/2026
    "%d-%b-%y",   # 13-FEB-26
    "%d-%b-%Y",   # 13-FEB-2026
    "%d/%m/%y",   # 13/02/26
    "%m/%d/%y",   # 2/13/26
    "%Y-%m-%d",   # ISO
    "%d.%m.%Y",
]

# CSE symbol regex pattern
SYMBOL_RE = re.compile(r"\b([A-Z]{2,10})\.(N|X|Y|W|R)\d{4}\b")

# "Dates to be notified" variants (return None for these)
TBN_PATTERN = re.compile(r"dates?\s+to\s+be\s+notif", re.IGNORECASE)


# ---------------------------------------------------------------------------
# CSEReportCollector — extends BaseCollector
# ---------------------------------------------------------------------------

class CSEReportCollector(BaseCollector):
    """
    Collects CSE Daily Market Report PDFs and extracts corporate actions.

    Extends BaseCollector with:
      - Selenium-based download (Angular SPA)
      - Google Drive archival (~25MB files)
      - 5-section PDF parser (right issues, splits, dividends, watchlist, suspended)
    """

    def __init__(self, source_id: str = SOURCE_ID, collection_date: date | None = None):
        super().__init__(source_id, collection_date)
        self._parser = CSEReportParser()
        self._gdrive = GDriveUploader()

    # ------------------------------------------------------------------
    # Stage 1: DISCOVER
    # ------------------------------------------------------------------

    def discover(self) -> dict:
        already = self.is_already_collected(TABLES["master"], date_column="report_date")
        if already:
            return {"already_collected": True, "url": CSE_PUBLICATIONS_URL,
                    "date_str": self.collection_date.strftime("%Y-%m-%d"), "metadata": {}}

        self.logger.info(f"[DISCOVER] No existing row for {self.collection_date} — proceeding.")
        return {
            "already_collected": False,
            "url": CSE_PUBLICATIONS_URL,
            "date_str": self.collection_date.strftime("%Y-%m-%d"),
            "metadata": {
                "target_filename": f"cse_daily_{self.collection_date.strftime('%Y%m%d')}.pdf",
            },
        }

    # ------------------------------------------------------------------
    # Stage 2: DOWNLOAD (Selenium)
    # ------------------------------------------------------------------

    def download(self, discover_result: dict) -> dict:
        """Selenium download from CSE Angular publications page."""
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        dest_filename = discover_result["metadata"]["target_filename"]
        dest_path = TEMP_DIR / dest_filename

        # Reuse local cache if already downloaded
        if dest_path.exists() and dest_path.stat().st_size > 1_000_000:
            import hashlib
            sha256 = hashlib.sha256(dest_path.read_bytes()).hexdigest()
            self.logger.info(f"[DOWNLOAD] Using cached file: {dest_path}")
            return {"file_path": str(dest_path), "file_size_bytes": dest_path.stat().st_size,
                    "content_hash": sha256, "metadata": {"source": "local_cache"}}

        driver = None
        try:
            driver = self._setup_selenium_driver(str(TEMP_DIR))
            return self._selenium_download(driver, dest_path)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    def _setup_selenium_driver(self, download_dir: str):
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            from webdriver_manager.chrome import ChromeDriverManager
        except ImportError:
            raise CollectorDownloadError(
                "selenium and webdriver-manager not installed. "
                "Run: pip install selenium webdriver-manager --break-system-packages"
            )

        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_experimental_option("prefs", {
            "download.default_directory":         download_dir,
            "download.prompt_for_download":       False,
            "download.directory_upgrade":         True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled":               True,
        })

        svc = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=svc, options=opts)
        driver.set_page_load_timeout(30)
        self.logger.info("[DOWNLOAD] Chrome driver initialized.")
        return driver

    def _selenium_download(self, driver, dest_path: Path) -> dict:
        import hashlib
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        self.logger.info(f"[DOWNLOAD] Navigating to {CSE_PUBLICATIONS_URL}")
        driver.get(CSE_PUBLICATIONS_URL)

        # ── Wait for Angular to render ─────────────────────────────────────
        # Primary: wait for any element containing the text "Download"
        # Fallback: sleep 15s (Angular SPAs are slow on first paint)
        try:
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//*[contains(translate(normalize-space(.), "
                               "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                               "'abcdefghijklmnopqrstuvwxyz'), 'download')]")
                )
            )
            self.logger.info("[DOWNLOAD] Angular render detected — page has download content.")
        except Exception:
            self.logger.warning("[DOWNLOAD] Angular wait timed out — using 15s fallback sleep.")
            time.sleep(15)

        # ── Selector cascade ───────────────────────────────────────────────
        # CSE uses an Angular SPA. PDF links are rendered dynamically and
        # may NOT have href=*.pdf — the Angular click handler initiates the
        # download. XPath by visible text "Download" is the most reliable
        # approach regardless of Angular version or class-name changes.

        pdf_link = None

        # Strategy A: XPath by visible text (handles Angular Material buttons,
        # <a> tags, <button> tags, <span> wrappers, etc.)
        xpath_candidates = [
            # Exact-text button or anchor
            "//button[normalize-space(.)='Download']",
            "//a[normalize-space(.)='Download']",
            # Contains text — covers "Download Now", "Download PDF" etc.
            "//button[contains(normalize-space(.), 'Download')]",
            "//a[contains(normalize-space(.), 'Download')]",
            # Case-insensitive contains (Angular may capitalise differently)
            "//button[contains(translate(normalize-space(.),"
            " 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download')]",
            "//a[contains(translate(normalize-space(.),"
            " 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download')]",
            # Span/div inside a clickable parent
            "//*[contains(@class,'download') or contains(@class,'Download')]",
        ]

        for xpath in xpath_candidates:
            try:
                elems = driver.find_elements(By.XPATH, xpath)
                visible = [e for e in elems if e.is_displayed()]
                if visible:
                    pdf_link = visible[0]   # first = most recent / featured report
                    self.logger.info(
                        f"[DOWNLOAD] Found element via XPath: {xpath} "
                        f"| text='{pdf_link.text.strip()}'"
                    )
                    break
            except Exception:
                continue

        # Strategy B: CSS selectors (href-based — works if Angular sets href)
        if pdf_link is None:
            css_candidates = [
                "a[href*='.pdf']",
                "a[href*='download']",
                "a[href*='Download']",
                ".download-link",
                "button.download",
                "button[class*='download']",
                "button[class*='btn-download']",
                "td a",
            ]
            for selector in css_candidates:
                try:
                    links = driver.find_elements(By.CSS_SELECTOR, selector)
                    visible = [e for e in links if e.is_displayed()]
                    if visible:
                        pdf_link = visible[0]
                        self.logger.info(f"[DOWNLOAD] Found link via CSS: {selector}")
                        break
                except Exception:
                    continue

        # ── Save debug artefacts and raise if nothing found ────────────────
        if pdf_link is None:
            dbg_png  = TEMP_DIR / "cse_download_debug.png"
            dbg_html = TEMP_DIR / "cse_download_debug.html"
            driver.save_screenshot(str(dbg_png))
            try:
                dbg_html.write_text(driver.page_source, encoding="utf-8")
            except Exception:
                pass
            # Log every visible clickable element to help diagnose
            try:
                all_clickable = driver.find_elements(
                    By.XPATH, "//button | //a[@href] | //a[@ng-click] | //a[@(click)]"
                )
                self.logger.warning(
                    f"[DOWNLOAD] {len(all_clickable)} clickable elements found on page:"
                )
                for el in all_clickable[:20]:
                    self.logger.warning(
                        f"  tag={el.tag_name} | text='{el.text.strip()[:60]}' "
                        f"| class='{el.get_attribute('class')}' "
                        f"| href='{el.get_attribute('href')}'"
                    )
            except Exception:
                pass
            raise CollectorDownloadError(
                f"Could not find PDF download element on {CSE_PUBLICATIONS_URL}. "
                f"Screenshot: {dbg_png}  HTML: {dbg_html}. "
                "Check the HTML dump for the correct selector."
            )

        driver.execute_script("arguments[0].scrollIntoView(true);", pdf_link)
        time.sleep(1)
        pre_click = set(TEMP_DIR.glob("*.pdf"))
        pdf_link.click()
        self.logger.info("[DOWNLOAD] Click triggered — waiting for download...")

        downloaded = self._wait_for_download(pre_click, timeout_seconds=120)
        shutil.move(str(downloaded), str(dest_path))
        self.logger.info(f"[DOWNLOAD] Moved to {dest_path} ({dest_path.stat().st_size:,} bytes)")

        sha256 = hashlib.sha256()
        with open(dest_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256.update(chunk)

        return {
            "file_path":       str(dest_path),
            "file_size_bytes": dest_path.stat().st_size,
            "content_hash":    sha256.hexdigest(),
            "metadata":        {"download_method": "selenium"},
        }

    def _wait_for_download(self, pre_click: set, timeout_seconds: int = 120) -> Path:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            new_pdfs = [
                f for f in set(TEMP_DIR.glob("*.pdf")) - pre_click
                if not str(f).endswith(".crdownload") and f.stat().st_size > 100_000
            ]
            if new_pdfs:
                candidate = max(new_pdfs, key=lambda p: p.stat().st_mtime)
                prev_size, stable = -1, 0
                for _ in range(10):
                    curr = candidate.stat().st_size
                    if curr == prev_size:
                        stable += 1
                        if stable >= 2:
                            return candidate
                    prev_size = curr
                    time.sleep(2)
            time.sleep(3)
        raise CollectorDownloadError(
            f"Download did not complete in {timeout_seconds}s. Check {TEMP_DIR}"
        )

    # ------------------------------------------------------------------
    # Stage 3: PARSE
    # ------------------------------------------------------------------

    def parse(self, download_result: dict) -> list[dict]:
        file_path = download_result["file_path"]
        self.logger.info(f"[PARSE] Parsing: {file_path}")

        parsed = self._parser.parse(pdf_path=file_path, report_date=self.collection_date)

        total = sum(len(v) for v in parsed.values())
        self.logger.info(
            f"[PARSE] right_issues={len(parsed.get('right_issues', []))} | "
            f"splits={len(parsed.get('splits', []))} | "
            f"dividends={len(parsed.get('dividends', []))} | "
            f"watch_list={len(parsed.get('watch_list', []))} | "
            f"total={total}"
        )

        if total == 0:
            self.logger.warning(
                "[PARSE] 0 rows extracted from all sections. "
                "This may be a NIL corporate actions day (public holiday or no announcements). "
                "To inspect the PDF run: python parsers/cse_report_parser.py --analyze <pdf_path>"
            )
            # Return empty list — pipeline will store 0 rows and archive the PDF.
            # This is valid: CSE publishes daily PDFs even on NIL days.
            return []

        tagged = []
        for table_key, rows in parsed.items():
            for row in rows:
                tagged.append({"_table": table_key, **row})
        return tagged

    # ------------------------------------------------------------------
    # Stage 4: STORE
    # ------------------------------------------------------------------

    def store(self, parsed_rows: list[dict]) -> dict:
        groups: dict[str, list[dict]] = {k: [] for k in TABLES}
        for row in parsed_rows:
            key = row.pop("_table", "master")
            if key in groups:
                groups[key].append(row)

        conflict_map = {
            "master":          ["company_name", "action_type", "report_date"],
            "right_issues":    ["company_name", "report_date"],
            "dividends":       ["company_name", "dividend_type", "report_date"],
            "watch_list":      ["symbol", "report_date"],
            "splits":          ["company_name", "report_date"],
            # Sprint A: market-level (single row per trading day — PK is report_date)
            "market_summary":  ["report_date"],
            "foreign_flow":    ["report_date"],
        }

        # ── Deduplicate each group on its conflict key ────────────────────
        # PostgreSQL raises "ON CONFLICT DO UPDATE command cannot affect row
        # a second time" if the SAME conflict key appears twice in one batch.
        # This happens when a company has multiple share classes (e.g. N0000
        # and X0000) — both appear in watch_list/suspended, generating two
        # master-table rows with identical (company_name, action_type, date).
        # We keep the first row; where a later row has a non-null symbol we
        # prefer that (more informative).
        for key, cols in conflict_map.items():
            seen: dict[tuple, dict] = {}
            for row in groups[key]:
                dedup_key = tuple(row.get(c) for c in cols)
                if dedup_key not in seen:
                    seen[dedup_key] = row
                elif any(row.get(c) is not None for c in ["symbol", "effective_date"]):
                    seen[dedup_key] = row   # prefer richer row
            before = len(groups[key])
            groups[key] = list(seen.values())
            after = len(groups[key])
            if before != after:
                self.logger.info(
                    f"[STORE] Deduplicated {key}: {before} → {after} rows "
                    f"(removed {before - after} duplicate conflict keys)"
                )

        rows_stored = 0
        tables_written = []
        for key, rows in groups.items():
            if not rows:
                continue
            n = self._upsert_rows(TABLES[key], rows, conflict_map[key])
            rows_stored += n
            tables_written.append(TABLES[key])

        return {"rows_stored": rows_stored, "rows_skipped": 0, "tables_written": tables_written}

    # ------------------------------------------------------------------
    # Stage 5: ARCHIVE (Google Drive)
    # ------------------------------------------------------------------

    def archive(self, download_result: dict, store_result: dict) -> dict:
        local_path = Path(download_result["file_path"])
        drive_folder = GDRIVE_SUBFOLDER_TEMPLATE.format(
            year=self.collection_date.year,
            month=self.collection_date.month,
        )
        drive_filename = f"CSE_Daily_Report_{self.collection_date.strftime('%Y-%m-%d')}.pdf"

        try:
            existing = self._gdrive.file_exists(drive_filename, drive_folder)
            if existing:
                self.logger.info(f"[ARCHIVE] Already in Drive (id={existing}) — skipping.")
                archive_path = f"GDrive:{drive_folder}/{drive_filename}"
            else:
                result = self._gdrive.upload(local_path, drive_folder, drive_filename)
                archive_path = result["web_view_link"]
                self.logger.info(f"[ARCHIVE] Uploaded: {archive_path}")
        except GDriveUploaderError as e:
            self.logger.warning(f"[ARCHIVE] Drive upload failed (non-fatal): {e}")
            return {"archive_path": str(local_path), "local_deleted": False,
                    "warning": f"Drive upload failed: {e}"}

        try:
            local_path.unlink()
            local_deleted = True
        except Exception as e:
            self.logger.warning(f"[ARCHIVE] Could not delete temp file: {e}")
            local_deleted = False

        return {"archive_path": archive_path, "local_deleted": local_deleted}


# ---------------------------------------------------------------------------
# CSEReportParser — extracts corporate actions from pages 11-15
# ---------------------------------------------------------------------------

class CSEReportParser:
    """
    Production parser for CSE Daily Market Report corporate actions (pages 11-15).

    Extraction strategies (validated against Feb 13, 2026 report):
      - Right Issues:       Compressed TABLE 2 from page 11 (column-position mapped)
      - Share Subdivisions: Text-line parsing from page 12 (between section headers)
      - Scrip Dividends:    Text-line parsing from page 12 (after "Scrip Dividends" header)
      - Cash Dividends:     Text-line parsing from page 13 (regex: company + amounts + dates)
      - Watch List:         Text-line parsing from page 14 (regex for symbol pattern)
      - Suspended:          Text-line parsing from page 15 (regex for symbol pattern)
    """

    LINES_SETTINGS = {
        "vertical_strategy":   "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance":      3,
    }

    def parse(self, pdf_path: str, report_date: date) -> dict:
        result = {
            "right_issues":   [],
            "splits":         [],
            "dividends":      [],
            "watch_list":     [],
            "master":         [],
            # Sprint A: market-level sections
            "market_summary": [],
            "foreign_flow":   [],
        }

        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                logger.info(f"[PARSER] PDF opened: {total_pages} pages")

                # ── Sprint A: early pages (1-6) — market summary & foreign flow ──
                market_pages = self._detect_market_pages(pdf)
                logger.info(f"[PARSER] Market pages: {market_pages}")

                if "market_summary" in market_pages:
                    ms_idx = market_pages["market_summary"]
                    # Market data spans two adjacent pages:
                    #   ms_idx   → Index Performance (ASPI, S&P SL20)
                    #   ms_idx+1 → Turnover Overview (turnover, market cap, volume, trades)
                    ms_pages = [pdf.pages[ms_idx]]
                    if ms_idx + 1 < total_pages:
                        ms_pages.append(pdf.pages[ms_idx + 1])
                    summary, warnings = self._parse_market_summary(ms_pages, report_date)
                    result["market_summary"].append(summary)
                    if warnings:
                        logger.warning(f"[VALIDATE:MARKET] {'; '.join(warnings)}")

                # Foreign flow may be on the same page as market summary, or adjacent
                ff_page_idx = market_pages.get(
                    "foreign_trading",
                    market_pages.get("market_summary")  # fallback: same page
                )
                if ff_page_idx is not None:
                    flow, ff_warnings = self._parse_foreign_flow(
                        pdf.pages[ff_page_idx], report_date
                    )
                    # If same-page fallback returned nulls, try the next page
                    if (flow.get("foreign_buy_lkr") is None
                            and flow.get("net_flow_lkr") is None
                            and ff_page_idx + 1 < len(pdf.pages)):
                        flow, ff_warnings = self._parse_foreign_flow(
                            pdf.pages[ff_page_idx + 1], report_date
                        )
                    result["foreign_flow"].append(flow)
                    if ff_warnings:
                        logger.warning(f"[VALIDATE:FOREIGN] {'; '.join(ff_warnings)}")

                # ── Corporate actions (pages 11-15) ──────────────────────────────
                section_pages = self._detect_section_pages(pdf)
                logger.info(f"[PARSER] Sections detected: {section_pages}")

                if "right_issues" in section_pages:
                    result["right_issues"] = self._parse_right_issues(
                        pdf.pages[section_pages["right_issues"]], report_date
                    )

                if "share_subdivisions" in section_pages:
                    result["splits"] = self._parse_share_subdivisions(
                        pdf.pages[section_pages["share_subdivisions"]], report_date
                    )

                if "scrip_dividends" in section_pages:
                    result["dividends"].extend(
                        self._parse_scrip_dividends(
                            pdf.pages[section_pages["scrip_dividends"]], report_date
                        )
                    )

                if "cash_dividends" in section_pages:
                    result["dividends"].extend(
                        self._parse_cash_dividends(
                            pdf.pages[section_pages["cash_dividends"]], report_date
                        )
                    )

                if "watch_list" in section_pages:
                    result["watch_list"].extend(
                        self._parse_watch_or_suspended(
                            pdf.pages[section_pages["watch_list"]], report_date, "WATCH"
                        )
                    )

                if "trading_suspended" in section_pages:
                    result["watch_list"].extend(
                        self._parse_watch_or_suspended(
                            pdf.pages[section_pages["trading_suspended"]], report_date, "SUSPENDED"
                        )
                    )

        except CollectorParseError:
            raise
        except Exception as e:
            raise CollectorParseError(f"PDF parsing failed: {e}") from e

        result["master"] = self._build_master_log(result, report_date)
        return result

    # ------------------------------------------------------------------
    # Section detection
    # ------------------------------------------------------------------

    def _detect_section_pages(self, pdf) -> dict:
        """Scan pages 8-25 for section headers. Returns section → 0-indexed page number.

        Two-pass strategy robust against TOC/index pages:

        Pass 1 — collect every page where each section header appears, and count
                  how many distinct section types appear on each page.
        Pass 2 — identify TOC/index pages (≥ 3 distinct section types on one page).
                  For each section take its first occurrence on a NON-TOC page.
                  Fallback: if every occurrence is on a TOC page, use the last one.

        This handles the new CSE format (post Feb 2026) which adds a Corporate
        Announcements index page listing all section names before the data pages.
        """
        # ── Pass 1: scan all candidate pages ──────────────────────────────
        section_all_pages: dict[str, list[int]] = {k: [] for k in SECTION_HEADERS}
        page_hit_counts:   dict[int, int]        = {}

        for idx in range(min(7, len(pdf.pages)), min(25, len(pdf.pages))):
            try:
                text = (pdf.pages[idx].extract_text() or "").lower()
                hits = 0
                for key, headers in SECTION_HEADERS.items():
                    if any(h.lower() in text for h in headers):
                        section_all_pages[key].append(idx)
                        hits += 1
                if hits:
                    page_hit_counts[idx] = hits
            except Exception:
                pass

        logger.info(f"[DETECT] Per-page hit counts (0-indexed): {page_hit_counts}")

        # ── Identify TOC / index pages ─────────────────────────────────────
        # A page with 3+ distinct section types is almost certainly a TOC.
        # Legitimate data pages share at most 2 sections (Sub Division + Scrip).
        toc_pages: set[int] = {
            idx for idx, cnt in page_hit_counts.items() if cnt >= 3
        }
        if toc_pages:
            logger.info(
                f"[DETECT] TOC pages skipped (1-indexed): "
                f"{sorted(p + 1 for p in toc_pages)}"
            )

        # ── Pass 2: best page per section ─────────────────────────────────
        found: dict[str, int] = {}
        for key, pages in section_all_pages.items():
            non_toc = [p for p in pages if p not in toc_pages]
            if non_toc:
                found[key] = non_toc[0]
            elif pages:
                found[key] = pages[-1]   # all on TOC — use last as best guess
                logger.warning(
                    f"[DETECT] '{key}' only found on TOC page(s) "
                    f"— falling back to page {pages[-1] + 1}"
                )

        return found

    # ------------------------------------------------------------------
    # Right Issues — TABLE 2 compressed column-position extraction
    # ------------------------------------------------------------------

    def _parse_right_issues(self, page, report_date: date) -> list[dict]:
        """
        Extract right issues using TABLE 2 (compressed).

        Compressed column positions (after removing empty/None cols):
          0: company_name    1: proportion_prefix  2: proportion_text
          3: egm_date        4: xr_date            5: record_date
          6: despatch_date   7: trading_commences   8: renunciation_date
          9: last_acceptance

        Note: rows with company continuation text (e.g. 'PLC' alone) are skipped.
        """
        rows = []
        try:
            tables = page.extract_tables(self.LINES_SETTINGS)
            if not tables:
                logger.warning("[PARSER:RIGHT_ISSUES] No tables found on right issues page.")
                return rows

            # Old format (pre Feb 2026): TABLE 2 (index 1) was the compressed data table.
            # New format (post Feb 2026): single well-bordered table at index 0.
            if len(tables) >= 2:
                t2 = tables[1]
            else:
                t2 = tables[0]
                logger.info("[PARSER:RIGHT_ISSUES] Single table — using table[0] (new format).")
            in_data = False
            header_found = False

            for raw_row in t2:
                compressed = [
                    cell.strip() for cell in raw_row
                    if cell not in (None, "", " ") and str(cell).strip()
                ]

                if not compressed:
                    continue

                # Detect header row (contains 'company' and 'xr')
                joined = " ".join(compressed).lower()
                if "company" in joined and ("xr" in joined or "allotment" in joined):
                    header_found = True
                    in_data = True
                    continue

                if not in_data:
                    continue

                # Skip rows that look like continuation text only (1-2 words, no dates)
                if len(compressed) <= 2 and not any(
                    re.search(r"\d{1,2}/\d{1,2}/\d{4}", c) for c in compressed
                ):
                    continue

                # Data row — extract by position
                try:
                    company_raw = compressed[0] if len(compressed) > 0 else ""
                    company = self._clean_company_name(company_raw)
                    # Reject blank / too-short / standalone suffix words (continuation rows)
                    _SUFFIX_ONLY = {"PLC", "LTD", "CO.", "LIMITED", "HOLDINGS", "CORP"}
                    if not company or len(company) < 5 or company.upper().strip() in _SUFFIX_ONLY:
                        continue

                    # Clean up proportion text (remove line artifacts)
                    proportion = " ".join(
                        compressed[1:3]
                    ).replace("\n", " ").strip() if len(compressed) >= 3 else None

                    record = {
                        "symbol":              None,   # Not available in right issues table
                        "company_name":        company,
                        "proportion":          self._clean_text(proportion),
                        "issue_price_lkr":     None,   # Not in PDF table
                        "egm_date":            self._parse_date(compressed[3] if len(compressed) > 3 else None),
                        "xr_date":             self._parse_date(compressed[4] if len(compressed) > 4 else None),
                        "record_date":         self._parse_date(compressed[5] if len(compressed) > 5 else None),
                        "despatch_date":       self._parse_date(compressed[6] if len(compressed) > 6 else None),
                        "trading_commences":   self._parse_date(compressed[7] if len(compressed) > 7 else None),
                        "renunciation_date":   self._parse_date(compressed[8] if len(compressed) > 8 else None),
                        "acceptance_deadline": self._parse_date(compressed[9] if len(compressed) > 9 else None),
                        "report_date":         report_date.isoformat(),
                    }
                    rows.append(record)
                except Exception as e:
                    logger.debug(f"[PARSER:RIGHT_ISSUES] Row skip: {e} | row={compressed}")

        except Exception as e:
            logger.warning(f"[PARSER:RIGHT_ISSUES] Section failed: {e}")

        logger.info(f"[PARSER:RIGHT_ISSUES] {len(rows)} records extracted")
        return rows

    # ------------------------------------------------------------------
    # Share Subdivisions — text-line parsing (page 12, after header)
    # ------------------------------------------------------------------

    def _parse_share_subdivisions(self, page, report_date: date) -> list[dict]:
        """
        Parse "Sub Division of Shares" section from page 12.

        Text format (confirmed from PDF):
          COMPANY NAME PLC  date1  date2  date3  date4
          - date1: EGM/Prov. Allotment
          - date2: Sub Division Based on Shareholdings as at
          - date3: Period of Dealing Suspension
          - date4: Date of Commencement of Trading
        """
        rows = []
        try:
            text = page.extract_text() or ""
            lines = text.split("\n")

            in_section = False
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue

                # Start extraction after "Sub Division of Shares" header
                if "sub division" in stripped.lower() or "subdivision" in stripped.lower():
                    in_section = True
                    continue

                # Stop at next section
                if in_section and any(
                    h.lower() in stripped.lower()
                    for h in ["Scrip Dividend", "Cash Dividend", "Watch List", "Suspended",
                               "Company Name EGM"]
                ):
                    break

                if not in_section:
                    continue

                # Skip non-data lines
                if not self._is_data_line(stripped):
                    continue

                # Parse: COMPANY NAME PLC  d1  d2  d3  d4
                result = self._parse_subdivision_line(stripped, report_date)
                if result:
                    rows.append(result)

        except Exception as e:
            logger.warning(f"[PARSER:SPLITS] Section failed: {e}")

        logger.info(f"[PARSER:SPLITS] {len(rows)} records extracted")
        return rows

    def _parse_subdivision_line(self, line: str, report_date: date) -> Optional[dict]:
        """Parse one share subdivision line: 'COMPANY PLC date1 date2 date3 date4'"""
        # Extract all dates from the line
        dates = re.findall(
            r"\b(\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}|Dates?\s+to\s+be\s+notified)\b",
            line, re.IGNORECASE
        )

        # Company name = everything before the first date or "Dates to be notified"
        company = re.split(r"\s+\d{1,2}/\d|\s+Dates?\s+to\s+be", line, maxsplit=1)[0].strip()
        company = self._clean_company_name(company)
        if not company:
            return None

        return {
            "symbol":            None,
            "company_name":      company,
            "split_ratio":       None,   # Ratio not shown in current CSE format
            "old_par_value":     None,
            "new_par_value":     None,
            "effective_date":    self._parse_date(dates[0] if dates else None),
            "trading_commences": self._parse_date(dates[3] if len(dates) > 3 else None),
            "report_date":       report_date.isoformat(),
        }

    # ------------------------------------------------------------------
    # Scrip Dividends — text-line parsing (page 12, after "Scrip Dividends")
    # ------------------------------------------------------------------

    def _parse_scrip_dividends(self, page, report_date: date) -> list[dict]:
        """
        Parse "Scrip Dividends" section from page 12.

        Text format (confirmed from PDF):
          COMPANY NAME PLC  DD/MM/YYYY  DD/MM/YYYY
          - col1: Announcement Date
          - col2: Record Date
        """
        rows = []
        try:
            text = page.extract_text() or ""
            lines = text.split("\n")

            in_section = False
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue

                if "scrip dividend" in stripped.lower():
                    in_section = True
                    continue

                if in_section and any(
                    h.lower() in stripped.lower()
                    for h in ["Cash Dividend", "Watch List", "Suspended", "Company Name Ann"]
                ):
                    break

                if not in_section:
                    continue

                if "company name" in stripped.lower():
                    continue  # skip header row

                if not self._is_data_line(stripped):
                    continue

                # Format: COMPANY PLC  date1  date2
                dates = re.findall(r"\b\d{1,2}/\d{1,2}/\d{4}\b", stripped)
                company = re.split(r"\s+\d{1,2}/\d|\s+Dates?\s+to\s+be", stripped, maxsplit=1)[0].strip()
                company = self._clean_company_name(company)
                if not company:
                    continue

                rows.append({
                    "symbol":             None,
                    "company_name":       company,
                    "dividend_type":      "SCRIP",
                    "dividend_per_share": None,
                    "scrip_ratio":        None,   # Ratio not in current PDF format
                    "xd_date":            None,   # Not in scrip dividends section
                    "record_date":        self._parse_date(dates[1] if len(dates) > 1 else dates[0] if dates else None),
                    "payment_date":       None,
                    "financial_year":     None,
                    "interim_or_final":   None,
                    "report_date":        report_date.isoformat(),
                })

        except Exception as e:
            logger.warning(f"[PARSER:SCRIP_DIV] Section failed: {e}")

        logger.info(f"[PARSER:SCRIP_DIV] {len(rows)} records extracted")
        return rows

    # ------------------------------------------------------------------
    # Cash Dividends — text-line regex parsing (page 13)
    # ------------------------------------------------------------------

    def _parse_cash_dividends(self, page, report_date: date) -> list[dict]:
        """
        Parse "Cash Dividends" section from page 13.

        Text format (confirmed from PDF):
          COMPANY NAME PLC  voting_div  [non_voting_div]  XD_Date  Record_Date  Payment_Date
          - Dates can be M/D/YYYY or DD-MON-YY (e.g. 03-MAR-26)
          - "Dates to be notified" means NULL
        """
        rows = []
        try:
            text = page.extract_text() or ""
            lines = text.split("\n")

            in_section = False
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue

                if "cash dividend" in stripped.lower():
                    in_section = True
                    continue

                if in_section and any(
                    h.lower() in stripped.lower()
                    for h in ["Watch List", "Suspended", "Second Board"]
                ):
                    break

                if not in_section:
                    continue

                # Skip header and sub-header lines
                if any(kw in stripped.lower() for kw in [
                    "company name", "dividend per", "xd date", "record date",
                    "date of payment", "click to view", "ශ"
                ]):
                    continue

                if not self._is_data_line(stripped):
                    continue

                result = self._parse_cash_dividend_line(stripped, report_date)
                if result:
                    rows.append(result)

        except Exception as e:
            logger.warning(f"[PARSER:CASH_DIV] Section failed: {e}")

        logger.info(f"[PARSER:CASH_DIV] {len(rows)} records extracted")
        return rows

    def _parse_cash_dividend_line(self, line: str, report_date: date) -> Optional[dict]:
        """
        Parse one cash dividend line.

        Expected pattern: COMPANY NAME PLC  amount  [amount]  date  date  date

        Amounts: decimal numbers (e.g. 0.25, 1.50, 15.00)
        Dates: M/D/YYYY or DD-MON-YY or "Dates to be notified"
        """
        # Extract the company name (everything before the first number)
        company_match = re.match(r"^([A-Z][A-Z\s&().,']+?(?:PLC|LTD|CO\.))\s+", line)
        if not company_match:
            return None

        company = self._clean_company_name(company_match.group(1))
        if not company:
            return None

        remainder = line[company_match.end():]

        # Extract all decimal numbers (dividend amounts)
        amounts = re.findall(r"\b(\d+\.\d+)\b", remainder)
        voting_div = float(amounts[0]) if amounts else None
        non_voting_div = float(amounts[1]) if len(amounts) > 1 else None

        # Extract all dates (M/D/YYYY or DD-MON-YY or "Dates to be notified")
        date_pattern = re.compile(
            r"\b(\d{1,2}/\d{1,2}/\d{4}|\d{2}-[A-Z]{3}-\d{2}|Dates?\s+to\s+be\s+notified)\b",
            re.IGNORECASE
        )
        raw_dates = date_pattern.findall(remainder)

        xd_date      = self._parse_date(raw_dates[0]) if len(raw_dates) > 0 else None
        record_date  = self._parse_date(raw_dates[1]) if len(raw_dates) > 1 else None
        payment_date = self._parse_date(raw_dates[2]) if len(raw_dates) > 2 else None

        return {
            "symbol":             None,   # Not in cash dividends table
            "company_name":       company,
            "dividend_type":      "CASH",
            "dividend_per_share": voting_div,    # Voting shares dividend
            "scrip_ratio":        None,
            "xd_date":            xd_date,
            "record_date":        record_date,
            "payment_date":       payment_date,
            "financial_year":     None,
            "interim_or_final":   None,
            "report_date":        report_date.isoformat(),
        }

    # ------------------------------------------------------------------
    # Watch List & Trading Suspended — text regex on symbol (pages 14-15)
    # ------------------------------------------------------------------

    def _parse_watch_or_suspended(self, page, report_date: date, status: str) -> list[dict]:
        """
        Parse Watch List (page 14) or Trading Suspended (page 15).

        Text format (confirmed from PDF):
          [Time]  COMPANY NAME PLC  SYMBOL.N0000  price  volume  [change]
          - Symbol is always present (e.g. ALHP.N0000, ACAP.N0000)
          - Some rows have no time prefix (static watch list entries)
        """
        rows = []
        seen_symbols = set()
        try:
            text = page.extract_text() or ""
            lines = text.split("\n")

            section_header = "watch list" if status == "WATCH" else "trading suspended"
            in_section = False

            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue

                if section_header in stripped.lower():
                    in_section = True
                    continue

                # Stop at "Dealing Suspended" or next major section
                if in_section and any(
                    h.lower() in stripped.lower()
                    for h in ["Dealing Suspended", "Second Board", "Corporate Ann"]
                ):
                    break

                if not in_section:
                    continue

                # Skip header and footer lines
                if any(kw in stripped.lower() for kw in [
                    "last traded", "company name", "trade volume", "click to view",
                    "ෙවා", "அவதானிப்"
                ]):
                    continue

                # Extract symbol (the definitive anchor for watch/suspended rows)
                sym_match = SYMBOL_RE.search(stripped)
                if not sym_match:
                    continue

                symbol = sym_match.group(0)
                if symbol in seen_symbols:
                    continue
                seen_symbols.add(symbol)

                # Company name = everything before the symbol
                before_symbol = stripped[:sym_match.start()].strip()
                # Remove leading timestamp if present (e.g. "2:12:44 PM ")
                before_symbol = re.sub(r"^\d{1,2}:\d{2}:\d{2}\s+[AP]M\s+", "", before_symbol)
                company = self._clean_company_name(before_symbol)

                rows.append({
                    "symbol":         symbol,
                    "company_name":   company or None,
                    "entry_date":     report_date.isoformat(),  # Treat report_date as entry date
                    "exit_date":      None,
                    "reason":         None,
                    "category":       None,
                    "trading_status": status,
                    "report_date":    report_date.isoformat(),
                })

        except Exception as e:
            logger.warning(f"[PARSER:{status}] Section failed: {e}")

        logger.info(f"[PARSER:{status}] {len(rows)} records extracted")
        return rows

    # ------------------------------------------------------------------
    # Sprint A: Market summary + Foreign flow  (pages 1-6)
    # ------------------------------------------------------------------

    def _detect_market_pages(self, pdf) -> dict:
        """
        Scan pages 1-6 (0-indexed 0-5) for market summary and foreign flow content.

        Returns dict with 0-indexed page numbers:
            "market_summary"  → page with ASPI / S&P SL 20 / turnover
            "foreign_trading" → page with Foreign Purchases / Net Foreign
        """
        found: dict[str, int] = {}
        MARKET_LABELS  = ["all share price index", "aspi", "market turnover", "s&p sl 20"]
        FOREIGN_LABELS = ["foreign purchases", "net foreign", "foreign trading"]

        for idx in range(min(6, len(pdf.pages))):
            try:
                text = (pdf.pages[idx].extract_text() or "").lower()
                if "market_summary" not in found and any(lbl in text for lbl in MARKET_LABELS):
                    found["market_summary"] = idx
                if "foreign_trading" not in found and any(lbl in text for lbl in FOREIGN_LABELS):
                    found["foreign_trading"] = idx
            except Exception:
                pass

        return found

    def _parse_market_summary(self, pages: list, report_date: date) -> tuple[dict, list[str]]:
        """
        Extract market-level summary from early pages (typically pages 1-2, 0-indexed).

        CSE PDF actual layout (confirmed from Feb 20, 2026 report):

          Page 1 (0-indexed) — Index Performance:
            "All Share Price Index (ASPI) S&P Sri Lanka 20 Index"  ← combined label line
            [2-4 multilingual / arrow lines]
            "23,773.64 -96.43 -0.40% 6,721.47 -21.72 -0.32%"     ← data line (6 numbers)

          Page 2 (0-indexed) — Turnover Overview:
            "Total Turnover (Rs.) Market Capitalization (Rs.)"     ← combined label line
            [1-3 multilingual lines]
            "3,890,609,725.35  8,411,382,637,987.40"              ← both values, one line
            "Volume of Turnover (Total)"
            "139,059,091  224,631"                                 ← total is first number
            "No. of Trades (Total)"
            "34,795  195"                                          ← total is first number

        Strategy: combine text from both pages into one line list, then apply
        label-aware multi-line scanning rather than same-line extraction.

        Returns (row_dict, warnings_list).
        """
        row: dict = {
            "report_date":      report_date.isoformat(),
            "aspi_close":       None, "aspi_change":    None, "aspi_change_pct":    None,
            "sp20_close":       None, "sp20_change":    None, "sp20_change_pct":    None,
            "turnover_lkr":     None,
            "market_cap_lkr":   None,
            "volume_shares":    None,
            "trade_count":      None,
            "stocks_advancing": None,
            "stocks_declining": None,
            "stocks_unchanged": None,
            "parse_confidence": 0,
            "parse_warnings":   None,
        }

        try:
            # Combine text lines from all provided pages into one flat list
            all_lines: list[str] = []
            for page in pages:
                text = page.extract_text() or ""
                all_lines.extend(ln.strip() for ln in text.splitlines())
            lines = [ln for ln in all_lines if ln]   # drop blanks

            # ── Market Indices ────────────────────────────────────────────────
            # Label and values are on DIFFERENT lines in the CSE PDF.
            # _parse_dual_index_line finds the label then scans forward for
            # the numeric data line (first number > 1,000 = index value).
            aspi, sp20 = self._parse_dual_index_line(lines)
            row["aspi_close"]      = aspi.get("close")
            row["aspi_change"]     = aspi.get("change")
            row["aspi_change_pct"] = aspi.get("pct")
            row["sp20_close"]      = sp20.get("close")
            row["sp20_change"]     = sp20.get("change")
            row["sp20_change_pct"] = sp20.get("pct")

            # ── Turnover + Market Cap ─────────────────────────────────────────
            # Both appear as two large numbers on a single data line that follows
            # the combined label "Total Turnover (Rs.) Market Capitalization (Rs.)"
            turnover, market_cap = self._parse_turnover_cap_line(lines)
            row["turnover_lkr"]  = turnover
            row["market_cap_lkr"] = market_cap

            # ── Volume ────────────────────────────────────────────────────────
            # "Volume of Turnover (Total)" on one line, "139,059,091 224,631" on next.
            # Total (all shares) is the first number.
            row["volume_shares"] = self._find_int_after_label_in_lines(
                lines,
                ["Volume of Turnover (Total)", "Volume of Turnover", "Volume Traded",
                 "Total Volume"],
            )

            # ── Trade Count ───────────────────────────────────────────────────
            # "No. of Trades (Total)" on one line, "34,795 195" on next.
            # Total is the first number.
            row["trade_count"] = self._find_int_after_label_in_lines(
                lines,
                ["No. of Trades (Total)", "No. of Trades", "Number of Trades",
                 "No of Trades"],
            )

            # ── Market Breadth ────────────────────────────────────────────────
            # Not present in pages 1-2 of the current CSE report format.
            # Deferred to Sprint C (requires scanning a later breadth/statistics page).
            row["stocks_advancing"] = None
            row["stocks_declining"] = None
            row["stocks_unchanged"] = None

        except Exception as exc:
            logger.warning(f"[PARSER:MARKET_SUMMARY] Extraction error: {exc}")

        # ── Parse confidence: % of non-null fields ────────────────────────────
        # Breadth fields excluded — not available in this page range.
        value_fields = [
            "aspi_close", "aspi_change", "aspi_change_pct",
            "sp20_close", "turnover_lkr", "market_cap_lkr",
            "volume_shares", "trade_count",
        ]
        filled = sum(1 for f in value_fields if row.get(f) is not None)
        row["parse_confidence"] = int(filled / len(value_fields) * 100)

        # ── Layer 1 validation ────────────────────────────────────────────────
        warnings = self._validate_market_row(row)
        if warnings:
            row["parse_warnings"] = "; ".join(warnings)

        _t = f"{row['turnover_lkr']:,.0f}" if row["turnover_lkr"] is not None else "N/A"
        logger.info(
            f"[PARSER:MARKET_SUMMARY] ASPI={row['aspi_close']} | "
            f"SP20={row['sp20_close']} | "
            f"Turnover={_t} | "
            f"Confidence={row['parse_confidence']}%"
        )
        return row, warnings

    def _parse_foreign_flow(self, page, report_date: date) -> tuple[dict, list[str]]:
        """
        Extract foreign trading summary from the Trading Statistics page.

        CSE PDF actual layout (confirmed from Feb 20, 2026 report, page 3 0-indexed):

          "Foreign Purchases  Foreign Sales  Net foreign flow"   ← all 3 labels on one line
          [0-8 multilingual / decorative lines]
          "89,840,401.25  101,155,924.90  -11,315,523.65"       ← all 3 values on one line

        Strategy:
          1. Find the header line containing both "Foreign Purchases" and "Foreign Sales".
          2. Scan the next lines for a row with ≥2 positive decimal numbers (buy, sell).
          3. Map col 1 → buy, col 2 → sell, col 3 → net (or compute net from buy-sell).
          4. Fallback: try label+inline-value pattern (Turnover Overview page format).

        Returns (row_dict, warnings_list).
        """
        row: dict = {
            "report_date":        report_date.isoformat(),
            "foreign_buy_lkr":    None,
            "foreign_sell_lkr":   None,
            "net_flow_lkr":       None,
            "net_flow_direction": None,
            "parse_confidence":   0,
            "parse_warnings":     None,
        }

        try:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            # ── Strategy 1: three-column header → next data line ───────────────
            # "Foreign Purchases  Foreign Sales  Net foreign flow"
            # [multilingual lines]
            # "89,840,401.25  101,155,924.90  -11,315,523.65"
            header_idx = None
            for i, line in enumerate(lines):
                ll = line.lower()
                if "foreign purchases" in ll and "foreign sales" in ll:
                    header_idx = i
                    break

            if header_idx is not None:
                for j in range(1, 12):
                    if header_idx + j >= len(lines):
                        break
                    next_line = lines[header_idx + j]
                    # Match decimal numbers including leading minus
                    nums_raw = re.findall(r"-?[\d,]+\.\d+", next_line)
                    nums = []
                    for n in nums_raw:
                        try:
                            nums.append(float(n.replace(",", "")))
                        except ValueError:
                            pass
                    # First two must be positive (buy and sell volumes)
                    if len(nums) >= 2 and nums[0] > 0 and nums[1] > 0:
                        row["foreign_buy_lkr"]  = nums[0]
                        row["foreign_sell_lkr"] = nums[1]
                        row["net_flow_lkr"] = (
                            nums[2] if len(nums) > 2 else nums[0] - nums[1]
                        )
                        break

            # ── Strategy 2: inline label+value fallback (page 2 format) ────────
            # "Foreign Purchase (Rs.) 89,840,401.25 0.00"
            # "Foreign Sales (Rs.)"
            # "101,155,924.90 0.00"
            if row["foreign_buy_lkr"] is None:
                row["foreign_buy_lkr"] = self._find_value_after_label_in_lines(
                    lines,
                    ["Foreign Purchase (Rs.)", "Foreign Purchases", "Foreign Buys",
                     "Foreign Buying", "Fgn. Purchases"],
                )
            if row["foreign_sell_lkr"] is None:
                row["foreign_sell_lkr"] = self._find_value_after_label_in_lines(
                    lines,
                    ["Foreign Sales (Rs.)", "Foreign Sales", "Foreign Sells",
                     "Foreign Selling", "Fgn. Sales"],
                )
            if row["net_flow_lkr"] is None:
                row["net_flow_lkr"] = self._find_value_after_label_in_lines(
                    lines,
                    ["Net foreign flow", "Net Foreign Flow", "Net Foreign", "Net Fgn"],
                )

            # Compute net if still missing
            if row["net_flow_lkr"] is None:
                b, s = row["foreign_buy_lkr"], row["foreign_sell_lkr"]
                if b is not None and s is not None:
                    row["net_flow_lkr"] = b - s

            # Direction
            if row["net_flow_lkr"] is not None:
                row["net_flow_direction"] = "IN" if row["net_flow_lkr"] >= 0 else "OUT"

        except Exception as exc:
            logger.warning(f"[PARSER:FOREIGN_FLOW] Extraction error: {exc}")

        # ── Parse confidence ──────────────────────────────────────────────────
        filled = sum(
            1 for f in ["foreign_buy_lkr", "foreign_sell_lkr", "net_flow_lkr"]
            if row.get(f) is not None
        )
        row["parse_confidence"] = int(filled / 3 * 100)

        # ── Layer 1 validation ────────────────────────────────────────────────
        warnings: list[str] = []
        if row["foreign_buy_lkr"] is not None and row["foreign_buy_lkr"] < 0:
            warnings.append(f"foreign_buy_lkr is negative: {row['foreign_buy_lkr']}")
        if row["foreign_sell_lkr"] is not None and row["foreign_sell_lkr"] < 0:
            warnings.append(f"foreign_sell_lkr is negative: {row['foreign_sell_lkr']}")
        if row["foreign_buy_lkr"] and row["foreign_sell_lkr"]:
            computed_net = row["foreign_buy_lkr"] - row["foreign_sell_lkr"]
            if row["net_flow_lkr"] is not None:
                divergence = abs(computed_net - row["net_flow_lkr"])
                if divergence > 1e6:   # LKR 1M tolerance
                    warnings.append(
                        f"Net flow divergence: stated={row['net_flow_lkr']:,.0f} "
                        f"computed={computed_net:,.0f}"
                    )

        if warnings:
            row["parse_warnings"] = "; ".join(warnings)

        logger.info(
            f"[PARSER:FOREIGN_FLOW] Buy={row['foreign_buy_lkr']} | "
            f"Sell={row['foreign_sell_lkr']} | Net={row['net_flow_lkr']} | "
            f"Dir={row['net_flow_direction']} | Confidence={row['parse_confidence']}%"
        )
        return row, warnings

    # ── Line-list extraction helpers (Sprint A v2 — multi-line aware) ─────────
    # The CSE PDF puts labels and values on DIFFERENT lines, often with
    # multilingual text in between.  These helpers scan forward in the line
    # list rather than matching label+value on a single line.

    def _parse_dual_index_line(self, lines: list[str]) -> tuple[dict, dict]:
        """
        Find ASPI and SP20 values from page text lines.

        Actual CSE PDF layout:
          "All Share Price Index (ASPI) S&P Sri Lanka 20 Index"  ← combined label
          [2-5 multilingual / arrow lines]
          "23,773.64 -96.43 -0.40% 6,721.47 -21.72 -0.32%"     ← 6-number data line

        Strategy: locate the label line (contains both "aspi" and "s&p"), then scan
        the next lines until the first number > 1,000 is found (index value).
        Extract 6 numbers → ASPI(close, change, pct) + SP20(close, change, pct).
        """
        aspi: dict = {}
        sp20: dict = {}

        label_idx = None
        for i, line in enumerate(lines):
            ll = line.lower()
            if ("all share price index" in ll or "aspi" in ll) and \
               ("s&p" in ll or "sl 20" in ll or "sl20" in ll):
                label_idx = i
                break

        if label_idx is None:
            return aspi, sp20

        for j in range(1, 14):
            if label_idx + j >= len(lines):
                break
            line = lines[label_idx + j]
            # Strip % signs so we can parse percent values as floats
            clean = line.replace("%", "")
            nums_raw = re.findall(r"[+-]?[\d,]+\.?\d*", clean)
            nums = []
            for n in nums_raw:
                try:
                    nums.append(float(n.replace(",", "")))
                except ValueError:
                    pass
            # Need ≥4 numbers; first must be > 1,000 (actual index value, not a ratio)
            if len(nums) >= 4 and nums[0] > 1_000:
                aspi = {
                    "close":  nums[0],
                    "change": nums[1] if len(nums) > 1 else None,
                    "pct":    nums[2] if len(nums) > 2 else None,
                }
                sp20 = {
                    "close":  nums[3] if len(nums) > 3 else None,
                    "change": nums[4] if len(nums) > 4 else None,
                    "pct":    nums[5] if len(nums) > 5 else None,
                }
                break

        return aspi, sp20

    def _parse_turnover_cap_line(
        self, lines: list[str]
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Find Total Turnover and Market Cap from the Turnover Overview page.

        Actual CSE PDF layout:
          "Total Turnover (Rs.) Market Capitalization (Rs.)"  ← combined label
          [1-4 multilingual lines]
          "3,890,609,725.35  8,411,382,637,987.40"           ← both values, one line

        Both values are > LKR 1M; filter by magnitude to avoid picking up small numbers.
        """
        turnover   = None
        market_cap = None

        for i, line in enumerate(lines):
            ll = line.lower()
            if "total turnover" in ll and ("market cap" in ll or "capitaliz" in ll):
                for j in range(1, 10):
                    if i + j >= len(lines):
                        break
                    next_line = lines[i + j]
                    nums_raw = re.findall(r"[\d,]+\.\d+", next_line)
                    nums = []
                    for n in nums_raw:
                        try:
                            v = float(n.replace(",", ""))
                            if v > 1_000_000:   # must be LKR > 1M to be turnover/cap
                                nums.append(v)
                        except ValueError:
                            pass
                    if len(nums) >= 2:
                        turnover   = nums[0]
                        market_cap = nums[1]
                        break
                    elif len(nums) == 1:
                        turnover = nums[0]   # partial — market cap on next line
                        break
                break

        return turnover, market_cap

    def _find_value_after_label_in_lines(
        self,
        lines: list[str],
        labels: list[str],
        lookahead: int = 6,
    ) -> Optional[float]:
        """
        Find a label in the line list; return the first numeric value found:
          - on the same line (after the label text), OR
          - on any of the next `lookahead` lines.

        Handles comma-formatted numbers (e.g., "139,059,091").
        Skips values ≤ 1 to avoid picking up ratios/percentages on mixed pages.
        """
        for label in labels:
            label_lower = label.lower()
            for i, line in enumerate(lines):
                if label_lower in line.lower():
                    # ── Same-line: text after the label ───────────────────────
                    after_pos = line.lower().find(label_lower) + len(label)
                    after = line[after_pos:]
                    for n in re.findall(r"[\d,]+\.?\d*", after):
                        try:
                            v = float(n.replace(",", ""))
                            if v > 1:
                                return v
                        except ValueError:
                            pass
                    # ── Next N lines ───────────────────────────────────────────
                    for j in range(1, lookahead + 1):
                        if i + j >= len(lines):
                            break
                        for n in re.findall(r"[\d,]+\.?\d*", lines[i + j]):
                            try:
                                v = float(n.replace(",", ""))
                                if v > 1:
                                    return v
                            except ValueError:
                                pass
        return None

    def _find_int_after_label_in_lines(
        self,
        lines: list[str],
        labels: list[str],
        lookahead: int = 6,
    ) -> Optional[int]:
        """Wrapper over _find_value_after_label_in_lines returning an integer."""
        v = self._find_value_after_label_in_lines(lines, labels, lookahead)
        return int(round(v)) if v is not None else None

    # ── Legacy extraction helpers (single-line text, kept for compatibility) ──

    def _extract_index_row(self, text: str, labels: list[str]) -> dict:
        """
        Extract index close, change, and pct_change from a labelled text row.

        Handles formats:
          "All Share Price Index   6,523.45   +12.34   +0.19%"
          "ASPI  6523.45  12.34  0.19"
          "All Share Price Index 6,523.45 (12.34 / 0.19%)"
        """
        result: dict = {}
        for label in labels:
            # Find line containing this label
            pattern = re.compile(re.escape(label), re.IGNORECASE)
            m = pattern.search(text)
            if not m:
                continue

            # Extract the rest of the line after the label
            line_end = text.find("\n", m.end())
            snippet = text[m.end(): line_end if line_end > 0 else m.end() + 200]

            # Pull all numeric tokens (handles commas, signs, parentheses for negatives)
            nums = re.findall(r"[+-]?[\d,]+\.?\d*", snippet.replace("(", "-").replace(")", ""))
            cleaned = []
            for n in nums:
                v = self._parse_decimal(n.replace(",", ""))
                if v is not None:
                    cleaned.append(v)

            if cleaned:
                result["close"]  = cleaned[0] if len(cleaned) > 0 else None
                result["change"] = cleaned[1] if len(cleaned) > 1 else None
                result["pct"]    = cleaned[2] if len(cleaned) > 2 else None
            break

        return result

    def _extract_labeled_value(self, text: str, labels: list[str]) -> Optional[float]:
        """
        Find a label in text, return the next numeric value on that line.
        Handles comma-formatted numbers (e.g., "3,245,678,901").
        """
        for label in labels:
            pattern = re.compile(re.escape(label) + r"[^\d\n]*?([\d,]+\.?\d*)", re.IGNORECASE)
            m = pattern.search(text)
            if m:
                return self._parse_decimal(m.group(1).replace(",", ""))
        return None

    def _extract_labeled_int(self, text: str, labels: list[str]) -> Optional[int]:
        """Find a label and return the next value as integer."""
        val = self._extract_labeled_value(text, labels)
        if val is not None:
            return int(round(val))
        return None

    # ── Validation ────────────────────────────────────────────────────────────

    def _validate_market_row(self, row: dict) -> list[str]:
        """
        Layer 1 validation: check extracted values against MARKET_VALIDATION ranges.
        Returns list of warning strings (empty = all clear).
        """
        warnings: list[str] = []
        for field, (lo, hi) in MARKET_VALIDATION.items():
            val = row.get(field)
            if val is None:
                continue
            if not (lo <= val <= hi):
                warnings.append(
                    f"{field}={val:,.2f} outside expected [{lo:,.0f} – {hi:,.0f}]"
                )

        # Breadth sanity: total should be > 0 if any component is filled
        adv = row.get("stocks_advancing") or 0
        dec = row.get("stocks_declining") or 0
        unc = row.get("stocks_unchanged") or 0
        total = adv + dec + unc
        if total > 0 and (adv + dec) == 0:
            warnings.append("All stocks unchanged — likely parse error in breadth section")

        return warnings

    # ------------------------------------------------------------------
    # Master log builder
    # ------------------------------------------------------------------

    def _build_master_log(self, sections: dict, report_date: date) -> list[dict]:
        master = []

        for row in sections.get("right_issues", []):
            master.append({
                "symbol":            row.get("symbol"),
                "company_name":      row.get("company_name"),
                "action_type":       "RIGHT_ISSUE",
                "announcement_date": None,
                "effective_date":    row.get("xr_date"),
                "expiry_date":       row.get("acceptance_deadline"),
                "details_json":      json.dumps({
                    "proportion":        row.get("proportion"),
                    "trading_commences": row.get("trading_commences"),
                }),
                "status":    "ACTIVE",
                "source":    "PDF",
                "report_date": report_date.isoformat(),
            })

        for row in sections.get("splits", []):
            master.append({
                "symbol":            row.get("symbol"),
                "company_name":      row.get("company_name"),
                "action_type":       "SPLIT",
                "announcement_date": None,
                "effective_date":    row.get("effective_date"),
                "expiry_date":       row.get("trading_commences"),
                "details_json":      json.dumps({"ratio": row.get("split_ratio")}),
                "status":    "ACTIVE",
                "source":    "PDF",
                "report_date": report_date.isoformat(),
            })

        for row in sections.get("dividends", []):
            dtype = row.get("dividend_type", "CASH")
            master.append({
                "symbol":            row.get("symbol"),
                "company_name":      row.get("company_name"),
                "action_type":       f"{dtype}_DIVIDEND",
                "announcement_date": None,
                "effective_date":    row.get("xd_date"),
                "expiry_date":       row.get("payment_date"),
                "details_json":      json.dumps({
                    "per_share": row.get("dividend_per_share"),
                }),
                "status":    "ACTIVE",
                "source":    "PDF",
                "report_date": report_date.isoformat(),
            })

        for row in sections.get("watch_list", []):
            master.append({
                "symbol":            row.get("symbol"),
                "company_name":      row.get("company_name"),
                "action_type":       row.get("trading_status", "WATCH_LIST"),
                "announcement_date": row.get("entry_date"),
                "effective_date":    row.get("entry_date"),
                "expiry_date":       None,
                "details_json":      json.dumps({"reason": row.get("reason")}),
                "status":    "ACTIVE",
                "source":    "PDF",
                "report_date": report_date.isoformat(),
            })

        return master

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _is_data_line(self, line: str) -> bool:
        """
        Heuristic: is this line a data row (not a header, footer, or noise)?
        Data lines must start with an uppercase letter and contain PLC or a symbol.
        """
        if not line or len(line) < 5:
            return False
        # Must start with uppercase letter (company name)
        if not line[0].isupper():
            return False
        # Must contain PLC (company name) or a CSE symbol
        if "PLC" not in line and "LTD" not in line and not SYMBOL_RE.search(line):
            return False
        return True

    def _clean_company_name(self, raw: str) -> Optional[str]:
        """Normalize company name: clean whitespace, remove artifacts."""
        if not raw:
            return None
        # Remove non-printable and Unicode characters from Sinhala/Tamil
        cleaned = re.sub(r"[^\x20-\x7E]", " ", raw)
        # Collapse multiple spaces
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        # Remove trailing symbols
        cleaned = re.sub(r"[^A-Z0-9&().,'\- ]+$", "", cleaned).strip()
        if len(cleaned) < 3:
            return None
        return cleaned

    def _clean_text(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        return re.sub(r"\s+", " ", re.sub(r"[^\x20-\x7E]", " ", raw)).strip() or None

    def _parse_date(self, raw: Optional[str]) -> Optional[str]:
        """Parse date string → ISO YYYY-MM-DD. Returns None for 'Dates to be notified'."""
        if not raw:
            return None
        raw = str(raw).strip()
        if not raw or TBN_PATTERN.search(raw):
            return None
        if raw in ("-", "N/A", "n/a", "TBD", "0"):
            return None

        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue

        # Regex fallback
        m = re.search(r"(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})", raw)
        if m:
            a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if c < 100:
                c += 2000
            for (d, mo, y) in [(a, b, c), (b, a, c)]:
                try:
                    return date(y, mo, d).isoformat()
                except ValueError:
                    continue

        logger.debug(f"[PARSER] Unparseable date: '{raw}'")
        return None

    def _parse_decimal(self, raw: Optional[str]) -> Optional[float]:
        if not raw:
            return None
        try:
            return float(re.sub(r"[,\s]", "", str(raw)))
        except (ValueError, TypeError):
            return None


# ---------------------------------------------------------------------------
# CLI: Analyze / parse in dry-run mode
# ---------------------------------------------------------------------------

def analyze_pdf_structure(pdf_path: str, pages: list[int] = None):
    """Analyze PDF page structure for debugging. No DB writes."""
    pages = pages or list(range(10, 16))
    parser = CSEReportParser()

    print(f"\n{'='*70}")
    print(f"PDF STRUCTURE ANALYSIS: {Path(pdf_path).name}")
    print(f"{'='*70}")

    with pdfplumber.open(pdf_path) as pdf:
        print(f"\nTotal pages: {len(pdf.pages)}")
        section_pages = parser._detect_section_pages(pdf)
        print("\nDetected sections:")
        for k, v in section_pages.items():
            print(f"  {k:<25} → page {v+1} (0-idx: {v})")

        for pn in pages:
            pi = pn - 1
            if pi >= len(pdf.pages):
                continue
            pg = pdf.pages[pi]
            text = (pg.extract_text() or "")[:600]
            tables = pg.extract_tables({"vertical_strategy": "lines", "horizontal_strategy": "lines"})
            print(f"\n{'─'*60}")
            print(f"PAGE {pn}: {len(tables)} tables")
            print(f"Text preview: {repr(text[:300])}")
            for ti, t in enumerate(tables):
                if not t:
                    continue
                compressed_hdr = [c for c in (t[2] if len(t) > 2 else t[0]) if c not in (None, "")]
                print(f"\n  Table {ti+1}: {len(t)} rows × {len(t[0])} cols")
                print(f"  Header (compressed): {compressed_hdr}")
                for r in t[3:6]:
                    cmp = [c for c in r if c not in (None, "")]
                    if cmp:
                        print(f"  Row: {cmp}")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")

    ap = argparse.ArgumentParser(description="CSE Report Parser — Sprint 3")
    ap.add_argument("--analyze", metavar="PDF")
    ap.add_argument("--pages", metavar="PAGES", help="e.g. 11,12,13")
    ap.add_argument("--parse", metavar="PDF")
    ap.add_argument("--date", default=date.today().isoformat())
    args = ap.parse_args()

    if args.analyze:
        pages = [int(p) for p in args.pages.split(",")] if args.pages else None
        analyze_pdf_structure(args.analyze, pages)

    elif args.parse:
        rdate = date.fromisoformat(args.date)
        p = CSEReportParser()
        result = p.parse(args.parse, rdate)
        print(f"\n{'='*60}")
        print("PARSE RESULTS (dry run)")
        print(f"{'='*60}")
        for section, rows in result.items():
            print(f"\n{section.upper()}: {len(rows)} rows")
            for r in rows[:5]:
                print(f"  {r}")
            if len(rows) > 5:
                print(f"  ... ({len(rows)-5} more)")
    else:
        print("Use --analyze <pdf> or --parse <pdf>")
