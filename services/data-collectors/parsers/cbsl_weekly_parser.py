
"""
parsers/cbsl_weekly_parser.py — CBSL Weekly Economic Indicators Collector

Sprint 2 implementation: CBSLWeeklyCollector

Collects the Central Bank of Sri Lanka (CBSL) Weekly Economic Indicators PDF
(WEI_YYYYMMDD_e.pdf) and upserts data into 4 Supabase tables:
  - cbsl_weekly_real_sector
  - cbsl_weekly_monetary_sector
  - cbsl_weekly_fiscal_sector
  - cbsl_weekly_external_sector

VERSION HISTORY:
  v1.0.0  2026-02-19  Sprint 2 — Initial implementation

SOURCE:
  URL:       https://www.cbsl.gov.lk/sites/default/files/cbslweb_documents/statistics/
             wei/WEI_YYYYMMDD_e.pdf   (YYYYMMDD = Friday date)
  Format:    16-page A4 PDF (~800KB)
  Published: Every Friday, covering the week ending that Friday.
             Falls back to the previous Friday if current week is not yet published.

PIPELINE:
  DISCOVER  → Build URL for most recent Friday. Check weekly tables for idempotency.
  DOWNLOAD  → HTTP GET with 3-attempt exponential backoff retry.
  PARSE     → pdfplumber: hybrid text + structured table extraction.
              Returns 4 rows (one per sector table) tagged with _table.
  STORE     → Upsert 4 rows into 4 Supabase tables. Returns combined count.
  ARCHIVE   → Copy PDF to /data/cbsl_weekly/YYYY/MM/ on VPS, delete temp.

PDF STRUCTURE (16 pages):
  Page 1  : Date header only
  Page 2  : Highlights of the Week (text summary)
  Pages 3-4: Real Sector — price indices (NCPI/CCPI), market prices
  Page 5  : Real Sector — GDP, agricultural production, IIP, PMI
  Page 6  : Real Sector — employment, Brent crude, energy
  Page 7  : Monetary Sector — interest rates (OPR, SDFR, SLFR, AWCMR, AWPR, etc.)
  Page 8  : Monetary Sector — money supply (M1, M2, M2b, reserve money, credit)
  Page 9  : Monetary Sector — open market operations (OMO)
  Page 10 : Monetary Sector — credit cards, commercial paper, share market (ASPI, S&P SL20)
  Page 11 : Fiscal Sector — government finance, debt, T-bill/T-bond yields
  Page 12 : Fiscal Sector — primary & secondary market auction results, ISBs
  Page 13 : Fiscal Sector — two-way quotes (T-bills and T-bonds)
  Page 14 : External Sector — exchange rates, remittances, tourism
  Page 15 : External Sector — official reserve assets, BOP
  Page 16 : External Sector — balance of payments, trade data

KNOWN QUIRKS:
  1. NCPI/CCPI: Table col[2] stores space-separated triplets (Year Ago / Month Ago / This Month).
     _last_num() always takes the last token.
  2. GDP row in the table has 2025 Q3 value in a concatenated cell "5.3 5.4" → last = 5.4.
  3. PMI Manufacturing row col[3] = "55.5 60.9" → last = Dec 2025 value = 60.9.
  4. Money supply: raw text is cleanest source — pattern "Label a b c" → last = c.
  5. OMO: All weekly dashes except Friday date column — take last non-dash value.
  6. Page 14 FX table has OCR artifacts in currency labels (e.g., "UESxDchange") → match by row index.
  7. Official reserves = 6,824 USD mn → store as 6.824 USD bn (÷ 1000).
  8. T-bill/T-bond auction amounts in Rs. Mn → store as Rs. Bn (÷ 1000).
"""

import re
import shutil
import logging
import sys

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pdfplumber
import requests

# ---------------------------------------------------------------------------
# Path bootstrap — resolve sibling packages without PYTHONPATH
# ---------------------------------------------------------------------------

_SERVICES_DIR = Path(__file__).resolve().parent.parent
if str(_SERVICES_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVICES_DIR))

from base_collector import (
    BaseCollector,
    CollectorDiscoverError,
    CollectorDownloadError,   # noqa: F401 (re-exported for caller convenience)
    CollectorArchiveError,
    CollectorParseError,
    CollectorStoreError,
)


# ---------------------------------------------------------------------------
# CBSLWeeklyCollector
# ---------------------------------------------------------------------------

class CBSLWeeklyCollector(BaseCollector):
    """
    Collector for the CBSL Weekly Economic Indicators (WEI) publication.

    Writes to 4 Supabase tables in a single pipeline run:
      cbsl_weekly_real_sector, cbsl_weekly_monetary_sector,
      cbsl_weekly_fiscal_sector, cbsl_weekly_external_sector

    parse() returns a list of 4 dicts, each tagged with '_table' so that
    store() can route each row to the correct Supabase table.
    """

    # ── Identity ─────────────────────────────────────────────────────────────
    SOURCE_ID = "cbsl_weekly"

    REAL_TABLE     = "cbsl_weekly_real_sector"
    MONETARY_TABLE = "cbsl_weekly_monetary_sector"
    FISCAL_TABLE   = "cbsl_weekly_fiscal_sector"
    EXTERNAL_TABLE = "cbsl_weekly_external_sector"

    CONFLICT_COLUMNS = ["week_ending"]

    # ── Paths ─────────────────────────────────────────────────────────────────
    ARCHIVE_BASE_DIR = Path(
        "/opt/investment-os/services/data-collectors/data/cbsl_weekly"
    )
    TEMP_DIR = Path(
        "/opt/investment-os/services/data-collectors/data/temp"
    )

    # ── URL template — date_str = Friday YYYYMMDD ─────────────────────────────
    URL_TEMPLATE = (
        "https://www.cbsl.gov.lk/sites/default/files/"
        "cbslweb_documents/statistics/wei/WEI_{date_str}_e.pdf"
    )

    def __init__(self, source_id: str = SOURCE_ID, collection_date: date | None = None):
        # source_id is accepted for runner compatibility (always hard-coded to SOURCE_ID)
        super().__init__(self.SOURCE_ID, collection_date)

    # =========================================================================
    # ── Static helpers ────────────────────────────────────────────────────────
    # =========================================================================

    @staticmethod
    def _pf(s) -> float | None:
        """
        Parse a float from a raw string.
        Handles commas, spaces, parenthetical negatives, and null sentinels.
        """
        if s is None:
            return None
        cleaned = re.sub(r"[,\s]", "", str(s).strip())
        null_set = {"", "-", "–", "—", "N/A", "n/a", "nil", "Nil",
                    "n.a.", "n.a", "N.A.", "NA"}
        if cleaned in null_set:
            return None
        neg = cleaned.startswith("(") and cleaned.endswith(")")
        cleaned = re.sub(r"[()]", "", cleaned)
        try:
            val = float(cleaned)
            return -val if neg else val
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _month_to_date(month_str: str) -> str | None:
        """Convert 'January 2026' → '2026-01-01' (ISO date, first of month)."""
        if not month_str:
            return None
        try:
            from datetime import datetime
            dt = datetime.strptime(month_str.strip(), "%B %Y")
            return dt.date().replace(day=1).isoformat()
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _last_num(s: str) -> float | None:
        """
        From a space-separated list (possibly with parenthetical negatives),
        return the last parseable number.
        Example: "(2.0) 2.4 2.9" → 2.9
        Example: "55.5 60.9" → 60.9
        """
        tokens = re.findall(r"\([^)]+\)|[\d,]+(?:\.[\d]+)?", str(s))
        if not tokens:
            return None
        for tok in reversed(tokens):
            v = CBSLWeeklyCollector._pf(tok)
            if v is not None:
                return v
        return None

    @staticmethod
    def _get_friday(d: date) -> date:
        """Return the Friday on or before d (weekday 4 = Friday)."""
        days_back = (d.weekday() - 4) % 7
        return d - timedelta(days=days_back)

    @staticmethod
    def _prev_friday(d: date) -> date:
        """Return the Friday exactly one week before d."""
        this_fri = CBSLWeeklyCollector._get_friday(d)
        return this_fri - timedelta(weeks=1)

    # ── Safe table cell access ────────────────────────────────────────────────

    @staticmethod
    def _tval(table: list, row: int, col: int) -> str:
        """Safely retrieve a cell value from a pdfplumber table as a stripped string."""
        try:
            return str(table[row][col] or "").strip()
        except (IndexError, TypeError):
            return ""

    @staticmethod
    def _table_rows_with_label(table: list, pattern: str) -> list:
        """Return all rows whose first non-empty cell matches the regex pattern."""
        matches = []
        for row in table:
            for cell in row:
                cell_s = str(cell or "").strip()
                if cell_s and re.search(pattern, cell_s, re.IGNORECASE):
                    matches.append(row)
                    break
        return matches

    @staticmethod
    def _last_non_empty_in_row(row: list) -> str:
        """Return the last non-empty, non-dash cell value in a table row."""
        for cell in reversed(row):
            v = str(cell or "").strip()
            if v and v not in ("-", "–", "—", "None"):
                return v
        return ""

    # ── URL availability check ────────────────────────────────────────────────

    def _check_url(self, url: str) -> tuple[bool, int]:
        """HEAD request to confirm URL exists. Falls back to streaming GET on 405."""
        try:
            r = requests.head(url, timeout=10, allow_redirects=True,
                              headers={"User-Agent": "InvestmentOS/1.0"})
            if r.status_code == 405:
                r = requests.get(url, timeout=15, stream=True,
                                 headers={"User-Agent": "InvestmentOS/1.0"})
            return r.status_code == 200, r.status_code
        except Exception as e:
            self.logger.warning(f"URL check error: {e}")
            return False, 0

    # =========================================================================
    # ── Stage 1: DISCOVER ─────────────────────────────────────────────────────
    # =========================================================================

    def discover(self) -> dict:
        """
        Build URL for the most recent Friday on or before collection_date.
        Falls back to the previous Friday if current week PDF is not yet published.
        Checks idempotency against cbsl_weekly_real_sector (week_ending column).
        """
        d = self.collection_date
        friday = self._get_friday(d)
        url = self.URL_TEMPLATE.format(date_str=friday.strftime("%Y%m%d"))
        self.logger.info(f"[DISCOVER] Trying Friday {friday}: {url}")

        ok, status = self._check_url(url)
        if not ok:
            prev = self._prev_friday(d)
            url2 = self.URL_TEMPLATE.format(date_str=prev.strftime("%Y%m%d"))
            self.logger.info(
                f"[DISCOVER] {friday} → HTTP {status}. Falling back to {prev}: {url2}"
            )
            ok2, status2 = self._check_url(url2)
            if not ok2:
                raise CollectorDiscoverError(
                    f"WEI PDF not found for {friday} ({status}) "
                    f"or {prev} ({status2})"
                )
            friday = prev
            url = url2

        # Idempotency — use week_ending date for the check
        # Temporarily set collection_date to friday for the DB query
        _saved = self.collection_date
        self.collection_date = friday
        already = self.is_already_collected(
            self.REAL_TABLE, date_column="week_ending"
        )
        self.collection_date = _saved

        if already:
            return {
                "url": url,
                "already_collected": True,
                "date_str": friday.strftime("%Y%m%d"),
                "metadata": {"week_ending": friday.isoformat()},
            }

        self.logger.info(f"[DISCOVER] WEI PDF confirmed for week ending {friday}")
        return {
            "url": url,
            "already_collected": False,
            "date_str": friday.strftime("%Y%m%d"),
            "metadata": {"week_ending": friday.isoformat()},
        }

    # =========================================================================
    # ── Stage 2: DOWNLOAD ─────────────────────────────────────────────────────
    # =========================================================================

    def download(self, discover_result: dict) -> dict:
        """Download WEI PDF to temp directory using BaseCollector retry logic."""
        url = discover_result["url"]
        date_str = discover_result["date_str"]
        self.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        dest = self.TEMP_DIR / f"cbsl_weekly_{date_str}.pdf"
        result = self._download_with_retry(url, dest)
        result["metadata"] = discover_result["metadata"]
        return result

    # =========================================================================
    # ── Stage 3: PARSE ────────────────────────────────────────────────────────
    # =========================================================================

    def parse(self, download_result: dict) -> list[dict]:
        """
        Extract all 4 sector rows from the WEI PDF.

        Returns a list of 4 dicts, each tagged with '_table' indicating
        the target Supabase table. The store() method routes accordingly.
        """
        pdf_path = Path(download_result["file_path"])
        week_ending = download_result["metadata"]["week_ending"]
        source_file = pdf_path.name
        now_str = datetime.utcnow().isoformat()
        parse_notes: list[str] = []

        self.logger.info(f"[PARSE] Opening {pdf_path}")

        with pdfplumber.open(pdf_path) as pdf:
            # Extract text and tables for all 16 pages (0-indexed)
            pages_text: list[str] = []
            pages_tables: list[list] = []
            for page in pdf.pages:
                pages_text.append(page.extract_text() or "")
                pages_tables.append(
                    page.extract_tables(
                        {"vertical_strategy": "lines",
                         "horizontal_strategy": "lines"}
                    ) or []
                )

        # ── Extract each sector ───────────────────────────────────────────────
        real     = self._extract_real(pages_text, pages_tables, parse_notes)
        monetary = self._extract_monetary(pages_text, pages_tables, parse_notes)
        fiscal   = self._extract_fiscal(pages_text, pages_tables, parse_notes)
        external = self._extract_external(pages_text, pages_tables, parse_notes)

        # ── Canary check (monetary OPR must be present) ───────────────────────
        if monetary.get("opr") is None:
            raise CollectorParseError(
                "Canary field 'opr' is None — PDF parse likely failed. "
                f"parse_notes: {parse_notes}"
            )

        notes_str = "; ".join(parse_notes) if parse_notes else None
        base = {
            "week_ending":  week_ending,
            "collected_at": now_str,
            "source_file":  source_file,
            "parse_notes":  notes_str,
        }

        self.logger.info(
            f"[PARSE] Complete. parse_notes count={len(parse_notes)}"
        )

        return [
            {"_table": self.REAL_TABLE,     **base, **real},
            {"_table": self.MONETARY_TABLE, **base, **monetary},
            {"_table": self.FISCAL_TABLE,   **base, **fiscal},
            {"_table": self.EXTERNAL_TABLE, **base, **external},
        ]

    # =========================================================================
    # ── Stage 4: STORE ────────────────────────────────────────────────────────
    # =========================================================================

    def store(self, parsed_rows: list[dict]) -> dict:
        """
        Upsert 4 rows into 4 Supabase tables.
        Pops the '_table' routing key before upserting.
        """
        tables_written = []
        total_stored = 0

        for row in parsed_rows:
            row = dict(row)  # copy so we can mutate
            table = row.pop("_table")
            n = self._upsert_rows(table, [row], self.CONFLICT_COLUMNS)
            total_stored += n
            tables_written.append(table)
            self.logger.info(f"[STORE] {table}: {n} row upserted")

        return {
            "rows_stored":    total_stored,
            "rows_skipped":   0,
            "tables_written": tables_written,
        }

    # =========================================================================
    # ── Stage 5: ARCHIVE ──────────────────────────────────────────────────────
    # =========================================================================

    def archive(self, download_result: dict, store_result: dict) -> dict:
        """Copy PDF to /data/cbsl_weekly/YYYY/MM/, delete temp file."""
        src = Path(download_result["file_path"])
        week_ending = download_result["metadata"]["week_ending"]
        yr, mo, _ = week_ending.split("-")

        dest_dir = self.ARCHIVE_BASE_DIR / yr / mo
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / src.name

        shutil.copy2(src, dest)
        src.unlink(missing_ok=True)
        self.logger.info(f"[ARCHIVE] Moved to {dest}")

        return {
            "archive_path":  str(dest),
            "local_deleted": True,
        }

    # =========================================================================
    # ── Sector extractors ─────────────────────────────────────────────────────
    # =========================================================================

    def _extract_real(
        self,
        texts: list[str],
        tables: list[list],
        notes: list[str],
    ) -> dict[str, Any]:
        """
        Pages 3, 5, 6 → NCPI, CCPI, GDP, IIP, PMI, Agriculture, Brent.

        Key table locations (0-indexed pages):
          Page 3 (idx 2), Table 2 (18r×5c): NCPI/CCPI index rows
          Page 5 (idx 4), Table 0 (7r×8c):  GDP
          Page 5 (idx 4), Table 3 (5r×6c):  Tea/Rubber/Coconut
          Page 5 (idx 4), Table 4 (10r×5c): IIP
          Page 5 (idx 4), Table 7 (11r×5c): PMI Manufacturing + Services
          Page 6 (idx 5), text: Brent crude, Unemployment Rate
        """
        d: dict[str, Any] = {}

        # ── Page 3: NCPI / CCPI ──────────────────────────────────────────────
        # Table 2 (18r×5c): col[2] holds space-separated value triplets
        p3_tables = tables[2] if len(tables) > 2 else []
        ncpi_tbl = p3_tables[2] if len(p3_tables) > 2 else []

        def _tbl2_last(row_idx: int) -> float | None:
            return self._last_num(self._tval(ncpi_tbl, row_idx, 2))

        d["ncpi_headline"] = _tbl2_last(2)   # NCPI Headline Dec 2025 = 210.5
        d["ncpi_mom_pct"]  = _tbl2_last(3)   # Monthly Change %  = 1.6
        d["ncpi_yoy_pct"]  = _tbl2_last(5)   # Year-on-Year %    = 2.9
        d["ccpi_headline"] = _tbl2_last(11)  # CCPI Headline Jan 2026 = 197.0
        d["ccpi_mom_pct"]  = _tbl2_last(12)  # Monthly Change %  = 0.6
        d["ccpi_yoy_pct"]  = _tbl2_last(14)  # Year-on-Year %    = 2.3

        # Fallback: text regex on page 3 if table extraction failed
        p3_text = texts[2] if len(texts) > 2 else ""
        if d["ncpi_headline"] is None:
            m = re.search(
                r'Consumer Price Index.*?Headline\s+'
                r'([\d.]+)\s+([\d.]+)\s+([\d.]+)',
                p3_text
            )
            if m:
                d["ncpi_headline"] = self._pf(m.group(3))
            else:
                notes.append("ncpi_headline: not found")

        yoy_matches = re.findall(
            r'Year-on-Year Change %\s+'
            r'(\([^)]+\)|[\d.]+)\s+(\([^)]+\)|[\d.]+)\s+(\([^)]+\)|[\d.]+)',
            p3_text
        )
        if len(yoy_matches) >= 1 and d["ncpi_yoy_pct"] is None:
            d["ncpi_yoy_pct"] = self._pf(yoy_matches[0][2])
        if len(yoy_matches) >= 2 and d["ccpi_yoy_pct"] is None:
            d["ccpi_yoy_pct"] = self._pf(yoy_matches[1][2])

        mom_matches = re.findall(
            r'Monthly Change %\s+'
            r'(\([^)]+\)|[\d.]+)\s+(\([^)]+\)|[\d.]+)\s+(\([^)]+\)|[\d.]+)',
            p3_text
        )
        if len(mom_matches) >= 1 and d["ncpi_mom_pct"] is None:
            d["ncpi_mom_pct"] = self._pf(mom_matches[0][2])
        if len(mom_matches) >= 2 and d["ccpi_mom_pct"] is None:
            d["ccpi_mom_pct"] = self._pf(mom_matches[1][2])

        # ── Page 5: GDP, Agriculture, IIP, PMI ───────────────────────────────
        p5_text   = texts[4] if len(texts) > 4 else ""
        p5_tables = tables[4] if len(tables) > 4 else []

        # GDP — Table 0 (7r×8c), Row 6 "GDP", col[4] = "5.3 5.4" → last = 5.4
        gdp_tbl = p5_tables[0] if len(p5_tables) > 0 else []
        gdp_rows = self._table_rows_with_label(gdp_tbl, r'^GDP$')
        if gdp_rows:
            # col[4] contains the 2024Q3+2025Q3 concatenated value
            d["gdp_growth_pct"] = self._last_num(
                self._tval(gdp_rows[0], 0, 4) or
                self._last_non_empty_in_row(gdp_rows[0])
            )
        else:
            # Fallback: text regex
            m = re.search(
                r'GDP\s+(\([^)]+\)|[\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)',
                p5_text
            )
            d["gdp_growth_pct"] = self._pf(m.group(4)) if m else None
            if d["gdp_growth_pct"] is None:
                notes.append("gdp_growth_pct: not found")

        # Tea/Rubber/Coconut — Table 3 (5r×6c)
        # Row 2: Tea (mn kg)  → col[1] = "21.8 21.4" → last = 21.4
        # Row 3: Rubber       → col[1] = "4.5 5.0"   → last = 5.0
        # Row 4: Coconut      → col[1] = "170.1 254.5" → last = 254.5
        agr_tbl = p5_tables[3] if len(p5_tables) > 3 else []
        d["tea_production_mn_kg"]    = self._last_num(self._tval(agr_tbl, 2, 1))
        d["rubber_production_mn_kg"] = self._last_num(self._tval(agr_tbl, 3, 1))
        d["coconut_production_mn_nuts"] = self._last_num(self._tval(agr_tbl, 4, 1))

        # Fallback: regex on p5_text if table miss
        if d["tea_production_mn_kg"] is None:
            m = re.search(r'Tea\s+\(mn kg\).*?([\d.]+)\s+([\d.]+)', p5_text)
            d["tea_production_mn_kg"] = self._pf(m.group(2)) if m else None
            if d["tea_production_mn_kg"] is None:
                notes.append("tea_production_mn_kg: not found")

        if d["rubber_production_mn_kg"] is None:
            m = re.search(r'Rubber\s+\(mn kg\).*?([\d.]+)\s+([\d.]+)', p5_text)
            d["rubber_production_mn_kg"] = self._pf(m.group(2)) if m else None
            if d["rubber_production_mn_kg"] is None:
                notes.append("rubber_production_mn_kg: not found")

        if d["coconut_production_mn_nuts"] is None:
            m = re.search(r'Coconut\s+\(mn nuts\).*?([\d.]+)\s+([\d.]+)', p5_text)
            d["coconut_production_mn_nuts"] = self._pf(m.group(2)) if m else None
            if d["coconut_production_mn_nuts"] is None:
                notes.append("coconut_production_mn_nuts: not found")

        # IIP — Table 4 (10r×5c)
        # Row 2: "Index of Industrial Production" → col[1]="94.8 99.0" → last=99.0
        #                                           col[4]="4.4" → change
        iip_tbl = p5_tables[4] if len(p5_tables) > 4 else []
        d["iip_index"]   = self._last_num(self._tval(iip_tbl, 2, 1))
        d["iip_yoy_pct"] = self._pf(self._tval(iip_tbl, 2, 4))

        if d["iip_index"] is None:
            m = re.search(
                r'Index of Industrial Produc[^\n]*([\d.]+)\s+([\d.]+)', p5_text
            )
            d["iip_index"] = self._pf(m.group(2)) if m else None
            if d["iip_index"] is None:
                notes.append("iip_index: not found")

        # PMI — Table 7 (11r×5c)
        # Row 2 (Manufacturing Index): col[3] = "55.5 60.9" → last = Dec2025 = 60.9
        # Row 6 (Services Business Activity): col[3] = "50.5 67.9" → last = 67.9
        pmi_tbl = p5_tables[7] if len(p5_tables) > 7 else []
        d["pmi_manufacturing"] = self._last_num(self._tval(pmi_tbl, 2, 3))
        d["pmi_services"]      = self._last_num(self._tval(pmi_tbl, 6, 3))

        # Fallback: text regex
        if d["pmi_manufacturing"] is None:
            m = re.search(
                r'Index\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)', p5_text
            )
            d["pmi_manufacturing"] = self._pf(m.group(4)) if m else None
            if d["pmi_manufacturing"] is None:
                notes.append("pmi_manufacturing: not found")

        if d["pmi_services"] is None:
            m = re.search(
                r'Business Ac.*?Index\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)',
                p5_text, re.IGNORECASE
            )
            d["pmi_services"] = self._pf(m.group(4)) if m else None
            if d["pmi_services"] is None:
                notes.append("pmi_services: not found")

        # ── Page 6: Brent crude, Unemployment ────────────────────────────────
        p6_text = texts[5] if len(texts) > 5 else ""

        # Brent: last month row in table (December row)
        # Pattern: "December XX.XX YY.YY ZZ.ZZ" → first value = CPC or Brent
        # From actual PDF: "December 61.81 58.02 68.40" where col[1]=Brent
        m = re.search(r'December\s+([\d.]+)', p6_text)
        d["brent_crude_monthly_avg"] = self._pf(m.group(1)) if m else None
        if d["brent_crude_monthly_avg"] is None:
            # Try November as fallback
            m = re.search(r'November\s+([\d.]+)', p6_text)
            d["brent_crude_monthly_avg"] = self._pf(m.group(1)) if m else None
            if d["brent_crude_monthly_avg"] is None:
                notes.append("brent_crude_monthly_avg: not found")

        # Unemployment Rate (quarterly data)
        # "Unemployment Rate 4.4 4.2 4.3" → last = 4.3
        m = re.search(r'Unemployment Rate\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)', p6_text)
        d["unemployment_rate"] = self._pf(m.group(3)) if m else None
        if d["unemployment_rate"] is None:
            # Try 2-column version (only 2 values)
            m = re.search(r'Unemployment Rate\s+([\d.]+)\s+([\d.]+)', p6_text)
            d["unemployment_rate"] = self._pf(m.group(2)) if m else None
            if d["unemployment_rate"] is None:
                notes.append("unemployment_rate: not found (quarterly data may be stale)")

        return d

    # -------------------------------------------------------------------------

    def _extract_monetary(
        self,
        texts: list[str],
        tables: list[list],
        notes: list[str],
    ) -> dict[str, Any]:
        """
        Pages 7-9 → policy rates, AWPR, AWDR, AWLR, AWFDR, money supply, OMO.

        Key table locations:
          Page 7 (idx 6), Table 0 (33r×15c): interest rates
          Page 8 (idx 7), text: money supply (cleanest extraction)
          Page 9 (idx 8), text: open market operations
        """
        d: dict[str, Any] = {}

        p7_text   = texts[6] if len(texts) > 6 else ""
        p7_tables = tables[6] if len(tables) > 6 else []
        p8_text   = texts[7] if len(texts) > 7 else ""
        p9_text   = texts[8] if len(texts) > 8 else ""

        # ── Policy rates from Table 0 (33r×15c) ──────────────────────────────
        # Column layout per row: col[0]="", col[1]=label, col[2..4]=Year Ago,
        #                        col[5]=Week Ago, col[6]=This Week
        # Row 2:  OPR           → col[6] = "7.75"
        # Row 5:  SDFR          → col[6] = "7.25"
        # Row 6:  SLFR          → col[6] = "8.25"
        # Row 8:  AWCMR (end wk)→ col[6] = "7.66"
        # (T-bill rows 11-13 go to fiscal table, not monetary)
        mon_tbl = p7_tables[0] if p7_tables else []

        def _rate_from_table(row_idx: int) -> float | None:
            return self._pf(self._last_non_empty_in_row(mon_tbl[row_idx])
                            if row_idx < len(mon_tbl) else "")

        d["opr"]           = _rate_from_table(2)
        d["sdfr"]          = _rate_from_table(5)
        d["slfr"]          = _rate_from_table(6)
        d["awcmr_end_week"] = _rate_from_table(8)

        # AWPR, AWDR, AWFDR, AWLR — search by label in the full table
        label_map = {
            "awpr": r'Average Weighted Prime Lending Rate|AWPR',
            "awdr": r'Average Weighted Deposit Rate\b|AWDR',
            "awfdr": r'Average Weighted Fixed Deposit Rate|AWFDR',
            "awlr": r'Average Weighted Lending Rate\b|AWLR',
        }
        for col_key, pattern in label_map.items():
            rows = self._table_rows_with_label(mon_tbl, pattern)
            if rows:
                d[col_key] = self._pf(self._last_non_empty_in_row(rows[0]))
            else:
                d[col_key] = None

        # Fallback: text regex for all rate fields
        rate_patterns = {
            "opr":            r'Overnight Policy Rate.*?([\d.]+)',
            "sdfr":           r'Standing Deposit Facility Rate.*?([\d.]+)',
            "slfr":           r'Standing Lending Facility Rate.*?([\d.]+)',
            "awcmr_end_week": r'AWCMR.*?([\d.]+)',
            "awpr":           r'AWPR.*?([\d.]+)',
            "awdr":           r'Average Weighted Deposit Rate.*?(\d+\.\d+)',
            "awfdr":          r'Average Weighted Fixed Deposit Rate.*?(\d+\.\d+)',
            "awlr":           r'Average Weighted Lending Rate\b.*?(\d+\.\d+)',
        }
        for col_key, pattern in rate_patterns.items():
            if d.get(col_key) is None:
                m = re.search(pattern, p7_text, re.IGNORECASE)
                if m:
                    # Take the last number in the match (= This Week)
                    nums = re.findall(r'[\d.]+', m.group(0))
                    d[col_key] = self._pf(nums[-1]) if nums else None
                if d[col_key] is None:
                    notes.append(f"{col_key}: not found in table or text")

        # ── Money supply from Page 8 text ─────────────────────────────────────
        # Pattern: "Label A,BBB.B C,DDD.D E,FFF.G" → take last (= Dec 2025)
        money_patterns = {
            "reserve_money_bn":        r'Reserve Money\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
            "m1_bn":                   r'\bM1\b\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
            "m2_bn":                   r'\bM2\b\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
            "m2b_bn":                  r'\bM2b\b\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
            "nfa_banking_bn":          r'Net Foreign Assets of the Banking System\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
            "credit_govt_bn":          r'Net Credit to the Government\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
            "credit_private_sector_bn": r'Credit to the Private Sector\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
        }
        for col_key, pattern in money_patterns.items():
            m = re.search(pattern, p8_text, re.IGNORECASE)
            if m:
                d[col_key] = self._pf(m.group(3))  # group(3) = This Week / latest
            else:
                d[col_key] = None
                notes.append(f"{col_key}: not found")

        # ── OMO from Page 9 text ──────────────────────────────────────────────
        # "Repo Amount Offered (Rs. bn) - - - - 20.00" → last non-dash = 20.00
        # "Reverse Repo Amount Offered (Rs. bn) - - - - -" → null
        m = re.search(
            r'Repo Amount Offered.*?([\d.]+)',
            p9_text, re.IGNORECASE
        )
        d["omo_repo_bn"] = self._pf(m.group(1)) if m else None

        m = re.search(
            r'Reverse\s+Repo Amount Offered.*?([\d.]+)',
            p9_text, re.IGNORECASE
        )
        d["omo_reverse_repo_bn"] = self._pf(m.group(1)) if m else None
        # Note: all-dash rows mean None (no OMO that day) — already None above

        return d

    # -------------------------------------------------------------------------

    def _extract_fiscal(
        self,
        texts: list[str],
        tables: list[list],
        notes: list[str],
    ) -> dict[str, Any]:
        """
        Pages 11-12 → T-bill yields, auction results, foreign holdings, secondary mkt.

        Key table locations:
          Page 11 (idx 10), Table 3 (15r×13c): primary/secondary yields
          Page 12 (idx 11), Table 0 (25r×18c): outstanding stock + foreign holdings
          Page 12 (idx 11), Table 1 (3r×3c):  T-bill auction (offered/accepted)
          Page 12 (idx 11), Table 3 (3r×3c):  T-bond auction (offered/accepted)
          Page 12 (idx 11), Table 4 (5r×4c):  secondary market volumes
        """
        d: dict[str, Any] = {}

        p11_text   = texts[10] if len(texts) > 10 else ""
        p12_text   = texts[11] if len(texts) > 11 else ""
        p11_tables = tables[10] if len(tables) > 10 else []
        p12_tables = tables[11] if len(tables) > 11 else []

        # ── Primary T-bill yields — Page 11, Table 3 ─────────────────────────
        # Structure (15r×13c):
        #   Row 3: 91 Day  → col[6] = "7.72" (This Week primary)
        #   Row 4: 182 Day → col[6] = "8.07"
        #   Row 5: 364 Day → col[6] = "8.31"
        yield_tbl = p11_tables[3] if len(p11_tables) > 3 else []
        d["tbill_91d_yield"]  = self._pf(self._tval(yield_tbl, 3, 6))
        d["tbill_182d_yield"] = self._pf(self._tval(yield_tbl, 4, 6))
        d["tbill_364d_yield"] = self._pf(self._tval(yield_tbl, 5, 6))

        # Fallback: text regex
        if d["tbill_91d_yield"] is None:
            m = re.search(r'91[\s-]*Day.*?(\d+\.\d+)', p11_text)
            d["tbill_91d_yield"] = self._pf(m.group(1)) if m else None
        if d["tbill_182d_yield"] is None:
            m = re.search(r'182[\s-]*Day.*?(\d+\.\d+)', p11_text)
            d["tbill_182d_yield"] = self._pf(m.group(1)) if m else None
        if d["tbill_364d_yield"] is None:
            m = re.search(r'364[\s-]*Day.*?(\d+\.\d+)', p11_text)
            d["tbill_364d_yield"] = self._pf(m.group(1)) if m else None

        # ── Foreign holdings — Page 12, Table 0 ──────────────────────────────
        # "of which T-Bills and T-Bonds held by Foreigners 154,020 163,229"
        # Row in Table 0: find by label → last non-empty = 163,229 (mn)
        stock_tbl = p12_tables[0] if len(p12_tables) > 0 else []
        foreign_rows = self._table_rows_with_label(stock_tbl, r'Foreigners|Foreign')
        if foreign_rows:
            total_foreign_mn = self._pf(self._last_non_empty_in_row(foreign_rows[0]))
            d["total_foreign_holdings_bn"] = (
                total_foreign_mn / 1000 if total_foreign_mn else None
            )
        else:
            # Text fallback
            m = re.search(
                r'Foreigners\s+([\d,]+)\s+([\d,]+)', p12_text
            )
            if m:
                d["total_foreign_holdings_bn"] = self._pf(m.group(2)) / 1000
            else:
                d["total_foreign_holdings_bn"] = None
                notes.append("total_foreign_holdings_bn: not found")

        # Individual T-bill / T-bond foreign holdings not separately shown → None
        d["tbill_foreign_holdings_bn"] = None
        d["tbond_foreign_holdings_bn"] = None

        # ── T-bill auction — Page 12, Table 1 (3r×3c) ───────────────────────
        # Row 0: Amount Offered  [Last Week, This Week] → col[2] = 90,000 (mn)
        # Row 2: Amount Accepted → col[2] = 90,000 (mn)
        tbill_auc = p12_tables[1] if len(p12_tables) > 1 else []
        tbill_offered_mn  = self._pf(self._tval(tbill_auc, 0, 2))
        tbill_accepted_mn = self._pf(self._tval(tbill_auc, 2, 2))

        d["tbill_amount_offered_bn"]  = (
            tbill_offered_mn / 1000 if tbill_offered_mn else None
        )
        d["tbill_amount_accepted_bn"] = (
            tbill_accepted_mn / 1000 if tbill_accepted_mn else None
        )
        if tbill_offered_mn and tbill_accepted_mn and tbill_offered_mn > 0:
            d["tbill_subscription_ratio"] = round(
                tbill_accepted_mn / tbill_offered_mn, 4
            )
        else:
            d["tbill_subscription_ratio"] = None

        # T-bill text fallback
        if d["tbill_amount_offered_bn"] is None:
            m = re.search(
                r'Treasury\s*Bills.*?Amount Offered\s+([\d,]+)\s+([\d,]+)',
                p12_text, re.DOTALL | re.IGNORECASE
            )
            if m:
                d["tbill_amount_offered_bn"] = self._pf(m.group(2)) / 1000
        if d["tbill_amount_accepted_bn"] is None:
            m = re.search(
                r'Treasury\s*Bills.*?Amount Accepted\s+([\d,]+)\s+([\d,]+)',
                p12_text, re.DOTALL | re.IGNORECASE
            )
            if m:
                d["tbill_amount_accepted_bn"] = self._pf(m.group(2)) / 1000

        # ── T-bond auction — Page 12, Table 3 (3r×3c) ───────────────────────
        # Row 0: Amount Offered  → col[2] = 51,000 (mn); may be "-" if no auction
        # Row 2: Amount Accepted → col[2] = 51,000 (mn)
        tbond_auc = p12_tables[3] if len(p12_tables) > 3 else []
        tbond_offered_mn  = self._pf(self._tval(tbond_auc, 0, 2))
        tbond_accepted_mn = self._pf(self._tval(tbond_auc, 2, 2))

        d["tbond_amount_offered_bn"]  = (
            tbond_offered_mn / 1000 if tbond_offered_mn else None
        )
        d["tbond_amount_accepted_bn"] = (
            tbond_accepted_mn / 1000 if tbond_accepted_mn else None
        )
        if tbond_offered_mn and tbond_accepted_mn and tbond_offered_mn > 0:
            d["tbond_subscription_ratio"] = round(
                tbond_accepted_mn / tbond_offered_mn, 4
            )
        else:
            d["tbond_subscription_ratio"] = None

        # ── Secondary market volume — Page 12, Table 4 (5r×4c) ──────────────
        # Rows: T-bill Outright, T-bill Repo, T-bond placeholder, T-bond Outright, T-bond Repo
        # Summing all Outright transactions (col[3] = This Week):
        # T-bill Outright: row[0] col[3] = 105,343
        # T-bond Outright: row[3] col[3] = 443,174
        # Total outright = 548,517 mn = 548.517 bn
        sec_tbl = p12_tables[4] if len(p12_tables) > 4 else []
        tbill_outright_mn = self._pf(self._tval(sec_tbl, 0, 3))
        tbond_outright_mn = self._pf(self._tval(sec_tbl, 3, 3))

        if tbill_outright_mn is not None and tbond_outright_mn is not None:
            d["secondary_market_volume_bn"] = (
                tbill_outright_mn + tbond_outright_mn
            ) / 1000
        elif tbill_outright_mn is not None:
            d["secondary_market_volume_bn"] = tbill_outright_mn / 1000
        else:
            d["secondary_market_volume_bn"] = None
            notes.append("secondary_market_volume_bn: not found")

        return d

    # -------------------------------------------------------------------------

    def _extract_external(
        self,
        texts: list[str],
        tables: list[list],
        notes: list[str],
    ) -> dict[str, Any]:
        """
        Pages 14-16 → FX rates, remittances, tourism, reserves, trade, BOP.

        Key table locations:
          Page 14 (idx 13), Table 0 (13r×11c): FX rates
          Page 14 (idx 13), Table 5 (10r×6c):  tourism & remittances
          Page 15 (idx 14), Table 0 (40r×7c):  official reserve assets
          Page 16 (idx 15), text: trade data
        """
        d: dict[str, Any] = {}

        p14_text   = texts[13] if len(texts) > 13 else ""
        p15_text   = texts[14] if len(texts) > 14 else ""
        p16_text   = texts[15] if len(texts) > 15 else ""
        p14_tables = tables[13] if len(tables) > 13 else []
        p15_tables = tables[14] if len(tables) > 14 else []

        # ── FX rates — Page 14, Table 0 (13r×11c) ───────────────────────────
        # OCR artifacts in currency labels (e.g., "UESxDchangeRates")
        # Structure: col[0]=label, col[2]=Buying, col[3]=Selling, col[5]=Average,
        #            col[8]=Week Ago, col[10]=Year Ago
        # Row 2: USD → col[5] = "309.29"
        # Row 3: GBP → col[5] = "421.21"
        # Row 4: JPY → col[5] = "2.02"
        # Row 5: EUR → col[5] = "367.14"
        fx_tbl = p14_tables[0] if len(p14_tables) > 0 else []
        d["usd_lkr_indicative"] = self._pf(self._tval(fx_tbl, 2, 5))
        d["gbp_lkr"]            = self._pf(self._tval(fx_tbl, 3, 5))
        d["eur_lkr"]            = self._pf(self._tval(fx_tbl, 5, 5))

        # Fallback: text regex (robust against OCR label artifacts)
        if d["usd_lkr_indicative"] is None:
            # "305.52 313.06 309.29" → 3rd value = average
            m = re.search(r'([\d.]+)\s+([\d.]+)\s+(309\.[\d]+)', p14_text)
            if not m:
                m = re.search(r'USD.*?([\d.]+)\s+([\d.]+)\s+([\d.]+)', p14_text)
            d["usd_lkr_indicative"] = self._pf(m.group(3)) if m else None
            if d["usd_lkr_indicative"] is None:
                notes.append("usd_lkr_indicative: not found")

        if d["gbp_lkr"] is None:
            m = re.search(r'GBP.*?([\d.]+)\s+([\d.]+)\s+([\d.]+)', p14_text)
            d["gbp_lkr"] = self._pf(m.group(3)) if m else None

        if d["eur_lkr"] is None:
            m = re.search(r'EUR.*?([\d.]+)\s+([\d.]+)\s+([\d.]+)', p14_text)
            d["eur_lkr"] = self._pf(m.group(3)) if m else None

        # LKR YTD change — not directly shown; compute from USD Year Ago vs This Week
        usd_year_ago = self._pf(self._tval(fx_tbl, 2, 10))
        if d["usd_lkr_indicative"] and usd_year_ago and usd_year_ago > 0:
            d["lkr_ytd_change_pct"] = round(
                (d["usd_lkr_indicative"] - usd_year_ago) / usd_year_ago * 100, 2
            )
        else:
            d["lkr_ytd_change_pct"] = None

        # ── Tourism & Remittances — Page 14, Table 5 (10r×6c) ───────────────
        # Row 3: Tourism Earnings USD mn → col[1]="400.7", col[2]="378.3(g)" → latest
        # Row 8: Workers Remittances USD mn → col[1]="573.0 751.1" → last
        tour_tbl = p14_tables[5] if len(p14_tables) > 5 else []

        # Tourism — col[2] of row 3 = "378.3(g)" → strip footnote
        tour_raw = self._tval(tour_tbl, 3, 2)
        tour_raw = re.sub(r'\(.*?\)', '', tour_raw).strip()  # remove "(g)"
        d["tourism_earnings_usd_mn"] = self._pf(tour_raw)

        # Workers remittances — col[1] of row 8 = "573.0 751.1" → last
        d["workers_remittances_usd_mn"] = self._last_num(
            self._tval(tour_tbl, 8, 1)
        )

        # Remittances data month — anchor to Tourism/Remittances section only
        d["remittances_data_month"] = None
        remit_anchor = re.search(r'Tourism|Remittances', p14_text, re.IGNORECASE)
        remit_search_text = p14_text[remit_anchor.start():] if remit_anchor else p14_text
        m = re.search(
            r'(January|February|March|April|May|June|July|'
            r'August|September|October|November|December)\s+202[0-9]',
            remit_search_text
        )
        if m:
            d["remittances_data_month"] = self._month_to_date(m.group(0))

        # Fallback: text regex for remittances / tourism
        if d["workers_remittances_usd_mn"] is None:
            m = re.search(
                r"Workers'?\s+Rem.*?USD\s*mn\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
                p14_text, re.IGNORECASE
            )
            d["workers_remittances_usd_mn"] = self._pf(m.group(2)) if m else None
            if d["workers_remittances_usd_mn"] is None:
                notes.append("workers_remittances_usd_mn: not found")

        if d["tourism_earnings_usd_mn"] is None:
            m = re.search(
                r'Earnings from Tourism.*?USD\s*mn\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)',
                p14_text, re.IGNORECASE
            )
            d["tourism_earnings_usd_mn"] = self._pf(m.group(2)) if m else None
            if d["tourism_earnings_usd_mn"] is None:
                notes.append("tourism_earnings_usd_mn: not found")

        # ── Official Reserves — Page 15, Table 0 (40r×7c) ───────────────────
        # Row 1: "Official Reserve Assets(b)" → col[5] = "6,824" (USD mn)
        # Store as bn: 6,824 / 1000 = 6.824
        res_tbl = p15_tables[0] if len(p15_tables) > 0 else []
        res_mn = self._pf(self._tval(res_tbl, 1, 5))
        d["gross_official_reserves_usd_bn"] = (
            res_mn / 1000 if res_mn and res_mn > 100 else res_mn  # already bn if < 100
        )

        if d["gross_official_reserves_usd_bn"] is None:
            m = re.search(
                r'Official Reserve Assets.*?([\d,]+)', p15_text, re.IGNORECASE
            )
            if m:
                val = self._pf(m.group(1))
                d["gross_official_reserves_usd_bn"] = (
                    val / 1000 if val and val > 100 else val
                )
            if d["gross_official_reserves_usd_bn"] is None:
                notes.append("gross_official_reserves_usd_bn: not found")

        # Import cover months — often in footnote text; not always in table
        m = re.search(
            r'import cover.*?([\d.]+)\s*months?', p15_text, re.IGNORECASE
        )
        d["import_cover_months"] = self._pf(m.group(1)) if m else None

        # ── Trade data — Page 16 text ─────────────────────────────────────────
        # Annual totals: "Exports 13,581.4" / "Imports 21,480.0"
        m = re.search(r'Exports\s+([\d,]+\.?\d*)', p16_text, re.IGNORECASE)
        d["exports_usd_mn"] = self._pf(m.group(1)) if m else None

        m = re.search(r'Imports\s+([\d,]+\.?\d*)', p16_text, re.IGNORECASE)
        d["imports_usd_mn"] = self._pf(m.group(1)) if m else None

        if d["exports_usd_mn"] and d["imports_usd_mn"]:
            d["trade_balance_usd_mn"] = round(
                d["exports_usd_mn"] - d["imports_usd_mn"], 2
            )
        else:
            d["trade_balance_usd_mn"] = None

        # Trade data month
        m = re.search(
            r'(January|February|March|April|May|June|July|'
            r'August|September|October|November|December)\s+202[0-9]',
            p16_text
        )
        d["trade_data_month"] = self._month_to_date(m.group(0)) if m else None

        # BOP Overall and Current Account — may be on page 15 or 16
        p1516_text = p15_text + p16_text
        m = re.search(
            r'Overall.*?Balance.*?([+-]?[\d,]+\.?\d*)', p1516_text, re.IGNORECASE
        )
        d["bop_overall_usd_mn"] = self._pf(m.group(1)) if m else None

        m = re.search(
            r'Current Account.*?([+-]?\(?\d[\d,]*\.?\d*\)?)', p1516_text, re.IGNORECASE
        )
        d["current_account_usd_mn"] = self._pf(m.group(1)) if m else None

        return d


# ---------------------------------------------------------------------------
# Self-registration (used by collector_runner.py)
# ---------------------------------------------------------------------------

COLLECTOR_CLASS = CBSLWeeklyCollector
