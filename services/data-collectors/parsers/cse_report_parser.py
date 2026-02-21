
"""
parsers/cse_report_parser.py — CSE Daily Market Report Collector & Parser

Sprint 3 implementation: CSEReportCollector + CSEReportParser

Collects the daily ~25MB PDF from Colombo Stock Exchange publications page,
extracts corporate actions from pages 11-15, stores in 5 Supabase tables,
and archives the raw PDF to Google Drive.

VERSION HISTORY:
  v1.0.0  2026-02-19  Sprint 3 — Initial implementation, PDF structure validated
                       from Feb 13, 2026 sample report.
  v1.1.0  2026-02-20  Format update fix — CSE redesigned Corporate Announcements
                       section with a new index/TOC page listing all section names.
                       Fix 1: _detect_section_pages skips TOC pages (≥4 headers).
                       Fix 2: _parse_right_issues falls back to tables[0] when the
                       new single-table format is used (old format had 2 tables).

SOURCE:
  URL:       https://www.cse.lk/publications/cse-daily  (Angular SPA, Selenium required)
  Format:    ~270-page PDF (~25MB)
  Published: Every trading day, after market close (~7-8 PM SLK)

PAGES PARSED (corporate actions only — confirmed from PDF analysis):
  Page 11: Right Issues      → cse_right_issues + cse_corporate_actions (no symbol in PDF)
  Page 12: Share Subdivisions → cse_share_splits + cse_corporate_actions (no symbol in PDF)
           Scrip Dividends   → cse_dividends    + cse_corporate_actions (no symbol in PDF)
  Page 13: Cash Dividends    → cse_dividends    + cse_corporate_actions (no symbol in PDF)
  Page 14: Watch List        → cse_watch_list_history (symbol PRESENT in PDF)
  Page 15: Trading Suspended → cse_watch_list_history (symbol PRESENT in PDF)

PARSING STRATEGIES (from PDF structure analysis):
  Right Issues (p11):     Compressed TABLE 2 → column-position mapping
  Share Subdivisions(p12): Text-based line parsing (company name + dates)
  Scrip Dividends (p12):  Text-based line parsing (company name + 2 dates)
  Cash Dividends (p13):   Text-based line parsing with regex (company + amounts + dates)
  Watch List (p14):       Text-based regex on symbol pattern (XXX.N0000)
  Suspended (p15):        Text-based regex on symbol pattern

SYMBOL NOTE:
  - Watch List and Trading Suspended tables include the symbol (ALHP.N0000 etc.)
  - Right Issues, Dividends, Splits tables include company name ONLY — no symbol.
  - Symbols for those sections are left NULL in DB. A downstream enrichment job
    can match company_name → symbol using the stock universe table.

PIPELINE:
  DISCOVER  → Check Supabase whether today's report already stored.
  DOWNLOAD  → Selenium: navigate CSE publications, download latest PDF.
  PARSE     → pdfplumber: text + table extraction per section.
  STORE     → Upsert into 5 tables (master, right_issues, splits, dividends, watch_list).
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
    "master":       "cse_corporate_actions",
    "right_issues": "cse_right_issues",
    "dividends":    "cse_dividends",
    "watch_list":   "cse_watch_list_history",
    "splits":       "cse_share_splits",
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
            "master":       ["company_name", "action_type", "report_date"],
            "right_issues": ["company_name", "report_date"],
            "dividends":    ["company_name", "dividend_type", "report_date"],
            "watch_list":   ["symbol", "report_date"],
            "splits":       ["company_name", "report_date"],
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
        result = {"right_issues": [], "splits": [], "dividends": [], "watch_list": [], "master": []}

        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                logger.info(f"[PARSER] PDF opened: {total_pages} pages")

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
