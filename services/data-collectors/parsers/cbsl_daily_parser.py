
"""
parsers/cbsl_daily_parser.py — CBSL Daily Economic Indicators Collector

Sprint 1 implementation: CBSLDailyCollector

Collects the Central Bank of Sri Lanka (CBSL) Daily Economic Indicators PDF,
extracts ~31 macro metrics, and stores them in the cbsl_daily_indicators table.

VERSION HISTORY:
  v1.0.0  2026-02-18  Sprint 1 — Initial implementation

SOURCE:
  URL:       https://www.cbsl.gov.lk/sites/default/files/daily_economic_indicators_YYYYMMDD_e.pdf
  Format:    Single-page A4 PDF (~200-310KB)
  Published: Every business day, with a 1-business-day lag.
             Example: Published 18-Feb-2026 → indicators dated 17-Feb-2026

PIPELINE:
  DISCOVER  → Build URL for collection_date (with prev-business-day fallback).
              Check cbsl_daily_indicators for existing row (idempotency).
  DOWNLOAD  → HTTP GET with 3-attempt exponential backoff retry.
  PARSE     → pdfplumber: hybrid text + table extraction → 31 metrics.
  STORE     → Upsert single row to cbsl_daily_indicators (PK: date).
  ARCHIVE   → Copy PDF to /data/cbsl_daily/YYYY/MM/ on VPS, delete temp.

KNOWN QUIRKS:
  1. Publication lag: CBSL publishes N-1 business day's indicators on day N.
     discover() automatically falls back to prev_business_day if today 404s.
     Best practice: call runner with --date $(date -d 'yesterday' +%Y-%m-%d).

  2. CPC local prices have inter-digit spaces in the PDF rendering (artifact).
     e.g., "2 9 2 . 0 0" → parsed as 292.00 via whitespace-stripping regex.

  3. AWCMR and AWRR may be identical on low-activity overnight market days.

  4. T-bill secondary market yields may be '-' (NULL) if no trades occurred.

  5. GDP growth, NCPI, CCPI are quarterly/monthly updates — same value repeats
     across many daily reports until the next release. Stored as-is.

FIELDS EXTRACTED (31 columns in cbsl_daily_indicators):
  Exchange Rates:  usd_tt_buy, usd_tt_sell, gbp_tt_buy, gbp_tt_sell,
                   eur_tt_buy, eur_tt_sell, jpy_tt_buy, jpy_tt_sell
  T-Bills:         tbill_91d, tbill_182d, tbill_364d          (secondary market)
  Money Market:    opr, awpr, awcmr, awrr, overnight_liquidity_bn
                   (sdfr, slfr → NULL: not shown in current report format)
  Currency/Rsrv:   currency_in_circ_mn, reserve_money_mn
  Share Market:    aspi, sp_sl20, daily_turnover_mn, market_cap_bn, pe_ratio,
                   foreign_purchases_mn, foreign_sales_mn
  Energy:          total_energy_gwh, peak_demand_mw
  Petroleum:       brent_crude_usd, wti_crude_usd, petrol_local_lkr, diesel_local_lkr
  Macro:           gdp_growth_pct, ncpi_yoy_pct, ccpi_yoy_pct
"""

import re
import shutil
import logging
import sys

from datetime import date, timedelta
from pathlib import Path

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
    CollectorDownloadError,
    CollectorArchiveError,
    CollectorParseError,
    CollectorStoreError,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_ID        = "cbsl_daily"
TABLE_NAME       = "cbsl_daily_indicators"
CONFLICT_COLUMNS = ["date"]

ARCHIVE_BASE_DIR = Path(
    "/opt/investment-os/services/data-collectors/data/cbsl_daily"
)
TEMP_DIR = Path(
    "/opt/investment-os/services/data-collectors/data/temp"
)

URL_TEMPLATE = (
    "https://www.cbsl.gov.lk/sites/default/files/"
    "daily_economic_indicators_{date_str}_e.pdf"
)

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(compatible; InvestmentOS/1.0)"
    ),
    "Accept": "application/pdf,*/*",
    "Referer": (
        "https://www.cbsl.gov.lk/en/statistics/"
        "statistical-tables/economic-indicators"
    ),
}

MONTHS_PATTERN = (
    r"(?:January|February|March|April|May|June|July|August|"
    r"September|October|November|December)"
)


# ---------------------------------------------------------------------------
# Business-day helpers
# ---------------------------------------------------------------------------

def _prev_business_day(d: date) -> date:
    """Return the nearest business day strictly before d."""
    d = d - timedelta(days=1)
    while d.weekday() >= 5:   # 5=Saturday, 6=Sunday
        d = d - timedelta(days=1)
    return d


def _build_url(d: date) -> str:
    return URL_TEMPLATE.format(date_str=d.strftime("%Y%m%d"))


# ---------------------------------------------------------------------------
# CBSLDailyCollector
# ---------------------------------------------------------------------------

class CBSLDailyCollector(BaseCollector):
    """
    Collects CBSL Daily Economic Indicators from a single-page A4 PDF.

    Inherits the 5-stage pipeline skeleton from BaseCollector.
    Implements only the 5 abstract methods with CBSL-specific logic.

    Design decisions:
    - Parsing is regex-first (raw text is more reliable than table coordinates
      because embedded chart labels pollute pdfplumber tables).
    - Tables are used as primary source only for clean structured sections
      (T-bill table, share market table) where coordinates are stable.
    - All extraction is defensive: a failed field returns None + logs a note.
      The row is still stored; downstream dashboards show NULL as expected.
    """

    # =========================================================================
    # Stage 1 — DISCOVER
    # =========================================================================

    def discover(self) -> dict:
        """
        Build and validate the CBSL daily URL for collection_date.

        Strategy:
          1. Try URL for self.collection_date (the caller-supplied date).
          2. If 404, fall back to prev_business_day(self.collection_date).
             (Handles the common case where the runner uses date.today()
             but CBSL publishes yesterday's indicators.)
          3. If both fail, raise CollectorDiscoverError.

        Returns:
            {
              "url": str,
              "already_collected": bool,
              "date_str": str,           # YYYYMMDD used in URL
              "indicators_date": date,   # the date the indicators represent
              "metadata": dict,
            }
        """
        candidates = [
            self.collection_date,
            _prev_business_day(self.collection_date),
        ]
        url: str | None = None
        indicators_date: date | None = None

        for candidate in candidates:
            candidate_url = _build_url(candidate)
            self.logger.info(f"[DISCOVER] Trying: {candidate_url}")
            try:
                resp = requests.head(
                    candidate_url,
                    headers=HTTP_HEADERS,
                    timeout=15,
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    url = candidate_url
                    indicators_date = candidate
                    break
                elif resp.status_code == 405:
                    # Server doesn't support HEAD — use streaming GET
                    resp2 = requests.get(
                        candidate_url, headers=HTTP_HEADERS,
                        timeout=15, stream=True,
                    )
                    if resp2.status_code == 200:
                        resp2.close()
                        url = candidate_url
                        indicators_date = candidate
                        break
                else:
                    self.logger.debug(
                        f"[DISCOVER] {candidate} → HTTP {resp.status_code}"
                    )
            except requests.RequestException as e:
                self.logger.warning(
                    f"[DISCOVER] Request error for {candidate}: {e}"
                )

        if url is None or indicators_date is None:
            raise CollectorDiscoverError(
                f"CBSL Daily PDF not found for {self.collection_date} "
                f"or {_prev_business_day(self.collection_date)}. "
                f"CBSL may not have published yet, or it is a public holiday."
            )

        if indicators_date != self.collection_date:
            self.logger.info(
                f"[DISCOVER] Date fallback applied: "
                f"{self.collection_date} → {indicators_date} "
                f"(CBSL publication lag)"
            )

        already_collected = self.is_already_collected(TABLE_NAME, date_column="date")

        return {
            "url": url,
            "already_collected": already_collected,
            "date_str": indicators_date.strftime("%Y%m%d"),
            "indicators_date": indicators_date,
            "metadata": {"indicators_date": indicators_date.isoformat()},
        }

    # =========================================================================
    # Stage 2 — DOWNLOAD
    # =========================================================================

    def download(self, discover_result: dict) -> dict:
        """
        HTTP GET download to a temp file with exponential-backoff retry.
        Delegates entirely to BaseCollector._download_with_retry().

        Returns:
            {
              "file_path": str,
              "file_size_bytes": int,
              "content_hash": str,   # SHA256 hex
              "metadata": dict,
            }
        """
        TEMP_DIR.mkdir(parents=True, exist_ok=True)

        url      = discover_result["url"]
        date_str = discover_result["date_str"]
        dest     = TEMP_DIR / f"cbsl_daily_{date_str}.pdf"

        result = self._download_with_retry(url, dest, headers=HTTP_HEADERS)
        result["metadata"] = {
            "indicators_date": discover_result["indicators_date"].isoformat()
        }
        return result

    # =========================================================================
    # Stage 3 — PARSE
    # =========================================================================

    def parse(self, download_result: dict) -> list[dict]:
        """
        Extract 31 economic metrics from the single-page CBSL daily PDF.

        Approach:
          - pdfplumber.extract_text() for most metrics (regex on raw text).
          - pdfplumber.extract_tables(lines strategy) for clean table sections.
          - All fields are optional (None on failure) except usd_tt_sell,
            which is used as a canary to detect format changes.

        Returns:
            [row_dict]  — exactly one row matching cbsl_daily_indicators schema.

        Raises:
            CollectorParseError if the PDF cannot be opened, is corrupt,
            or the critical USD rate is missing (indicates format change).
        """
        file_path       = Path(download_result["file_path"])
        indicators_date = date.fromisoformat(
            download_result["metadata"]["indicators_date"]
        )
        self.logger.info(f"[PARSE] Opening {file_path.name}")

        try:
            with pdfplumber.open(str(file_path)) as pdf:
                if not pdf.pages:
                    raise CollectorParseError("PDF has no pages")
                page   = pdf.pages[0]
                text   = page.extract_text() or ""
                tables = page.extract_tables({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                })
        except CollectorParseError:
            raise
        except Exception as e:
            raise CollectorParseError(f"pdfplumber failed to open PDF: {e}") from e

        self.logger.info(
            f"[PARSE] text={len(text):,} chars | tables={len(tables)}"
        )
        if len(text) < 100:
            raise CollectorParseError(
                f"Extracted text too short ({len(text)} chars) — PDF may be corrupt"
            )

        notes: list[str] = []
        row: dict = {"date": indicators_date.isoformat()}

        # ── Extract each metric group ─────────────────────────────────────────
        row.update(self._extract_exchange_rates(text, notes))
        row.update(self._extract_tbill_yields(tables, text, notes))
        row.update(self._extract_money_market(text, notes))
        row.update(self._extract_currency_reserves(text, notes))
        row.update(self._extract_share_market(tables, text, notes))
        row.update(self._extract_energy(text, notes))
        row.update(self._extract_petroleum(text, notes))
        row.update(self._extract_macro_headlines(text, notes))

        # Columns present in schema but not extractable from daily PDF
        row.setdefault("sdfr", None)   # Standing Deposit Facility Rate
        row.setdefault("slfr", None)   # Standing Lending Facility Rate

        # ── Metadata ─────────────────────────────────────────────────────────
        row["source_file"]  = str(file_path)
        row["parse_notes"]  = "; ".join(notes) if notes else None

        # ── Canary validation ─────────────────────────────────────────────────
        if row.get("usd_tt_sell") is None:
            raise CollectorParseError(
                "Critical validation failed: USD TT Sell rate not found. "
                "The CBSL PDF format may have changed. "
                "Manual inspection of the archive PDF is required."
            )

        non_null = sum(1 for v in row.values() if v is not None)
        self.logger.info(
            f"[PARSE] {non_null}/{len(row)} non-null fields | "
            f"warnings={len(notes)}"
        )
        if notes:
            self.logger.warning(f"[PARSE] Notes: {'; '.join(notes)}")

        return [row]

    # =========================================================================
    # Stage 4 — STORE
    # =========================================================================

    def store(self, parsed_rows: list[dict]) -> dict:
        """Upsert single row to cbsl_daily_indicators."""
        rows_stored = self._upsert_rows(TABLE_NAME, parsed_rows, CONFLICT_COLUMNS)
        return {
            "rows_stored": rows_stored,
            "rows_skipped": 0,
            "tables_written": [TABLE_NAME],
        }

    # =========================================================================
    # Stage 5 — ARCHIVE
    # =========================================================================

    def archive(self, download_result: dict, store_result: dict) -> dict:
        """
        Copy PDF to /data/cbsl_daily/YYYY/MM/ on VPS, then delete temp.

        Small file (~200-310KB) → kept on VPS (no Google Drive needed).
        Structure: /data/cbsl_daily/2026/02/cbsl_daily_20260217.pdf

        Returns:
            {"archive_path": str, "local_deleted": bool}
        """
        src             = Path(download_result["file_path"])
        indicators_date = date.fromisoformat(
            download_result["metadata"]["indicators_date"]
        )

        archive_dir = (
            ARCHIVE_BASE_DIR
            / indicators_date.strftime("%Y")
            / indicators_date.strftime("%m")
        )
        archive_dir.mkdir(parents=True, exist_ok=True)
        dest = archive_dir / src.name

        try:
            shutil.copy2(str(src), str(dest))
            self.logger.info(f"[ARCHIVE] {src.name} → {dest}")
        except Exception as e:
            raise CollectorArchiveError(
                f"Failed to copy to archive directory: {e}"
            ) from e

        local_deleted = False
        try:
            src.unlink()
            local_deleted = True
            self.logger.info(f"[ARCHIVE] Temp file deleted: {src.name}")
        except Exception as e:
            self.logger.warning(f"[ARCHIVE] Could not delete temp file: {e}")

        return {
            "archive_path": str(dest),
            "local_deleted": local_deleted,
        }

    # =========================================================================
    # Private extraction helpers
    # =========================================================================

    # ── Low-level utilities ───────────────────────────────────────────────────

    @staticmethod
    def _pf(s) -> float | None:
        """
        Parse a float from string, stripping commas, spaces, and whitespace.
        Returns None for empty, dash, or unparseable values.
        """
        if s is None:
            return None
        cleaned = re.sub(r"[,\s]", "", str(s).strip())
        if cleaned in ("", "-", "\u2014", "N/A", "n/a", "nil", "Nil"):
            return None
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _get_cell(table: list, row_idx: int, col_idx: int) -> str | None:
        """
        Safely retrieve a cell from a pdfplumber table.
        Returns stripped string or None if any index is out of bounds.
        """
        try:
            val = table[row_idx][col_idx]
            return str(val).strip() if val is not None else None
        except (IndexError, TypeError):
            return None

    # ── Exchange Rates ────────────────────────────────────────────────────────

    def _extract_exchange_rates(self, text: str, notes: list) -> dict:
        """
        Extract USD/GBP/EUR/JPY TT buying and selling rates.

        Raw text pattern (single block, one currency per line):
          USD 305.4869 313.0220
          GBP 414.6944 427.4178
          EUR 360.3427 372.0821
          JPY 1.9873   2.0542

        Note on JPY: schema comment says "LKR per 100 JPY" but the rate
        printed is ~1.99, consistent with LKR per 1 JPY. Stored as printed.
        """
        result    = {}
        field_map = {
            "USD": ("usd_tt_buy", "usd_tt_sell"),
            "GBP": ("gbp_tt_buy", "gbp_tt_sell"),
            "EUR": ("eur_tt_buy", "eur_tt_sell"),
            "JPY": ("jpy_tt_buy", "jpy_tt_sell"),
        }
        for ccy, (buy_col, sell_col) in field_map.items():
            m = re.search(
                rf"\b{ccy}\s+([\d,]+\.?\d+)\s+([\d,]+\.?\d+)",
                text,
                re.IGNORECASE,
            )
            if m:
                result[buy_col]  = self._pf(m.group(1))
                result[sell_col] = self._pf(m.group(2))
            else:
                result[buy_col]  = None
                result[sell_col] = None
                notes.append(f"fx_{ccy.lower()}: not found in text")
        return result

    # ── T-Bill Yields ─────────────────────────────────────────────────────────

    def _extract_tbill_yields(
        self, tables: list, text: str, notes: list
    ) -> dict:
        """
        Extract T-bill SECONDARY market yields: 91-day, 182-day, 364-day.

        Primary source: the T-bills table (identified by "Primary"/"Secondary"
        in the header row). Structure:
          row[0]: header (contains "Primary" and "Secondary")
          row[1]: auction dates
          row[2]: 91-day   → primary=col[0], secondary=col[1]
          row[3]: (blank spacer or empty)
          row[4]: 182-day  → primary=col[0], secondary=col[1]
          row[5]: 364-day  → primary=col[0], secondary=col[1]

        Fallback: regex on raw text block between "Yield Rates" and next section.
        """
        result = {"tbill_91d": None, "tbill_182d": None, "tbill_364d": None}

        # Find T-bill table by header row containing "rimary" and "econdary"
        tbill_tbl = None
        for t in tables:
            if t and t[0]:
                header_text = " ".join(str(c) for c in t[0] if c)
                if "rimary" in header_text and "econdary" in header_text:
                    tbill_tbl = t
                    break

        if tbill_tbl and len(tbill_tbl) >= 6:
            # Column 1 = secondary market
            result["tbill_91d"]  = self._pf(self._get_cell(tbill_tbl, 2, 1))
            result["tbill_182d"] = self._pf(self._get_cell(tbill_tbl, 4, 1))
            result["tbill_364d"] = self._pf(self._get_cell(tbill_tbl, 5, 1))
            self.logger.debug(
                f"[PARSE:tbill] 91d={result['tbill_91d']} "
                f"182d={result['tbill_182d']} "
                f"364d={result['tbill_364d']}"
            )
        else:
            # Text fallback: find 6 numbers (3 primary, 3 secondary) after header
            # Pattern: "7.72 -\n8.07 8.00\n8.31 8.25" (secondary may be "-")
            tbill_block = re.search(
                r"Yield Rates of T-Bills.*?"
                r"(\d[\d.]*)\s+([\d.-]+)\s+"
                r"(\d[\d.]*)\s+([\d.-]+)\s+"
                r"(\d[\d.]*)\s+([\d.-]+)",
                text,
                re.DOTALL,
            )
            if tbill_block:
                result["tbill_91d"]  = self._pf(tbill_block.group(2))
                result["tbill_182d"] = self._pf(tbill_block.group(4))
                result["tbill_364d"] = self._pf(tbill_block.group(6))
            else:
                notes.append("tbill: table not found and text fallback failed")

        return result

    # ── Money Market ──────────────────────────────────────────────────────────

    def _extract_money_market(self, text: str, notes: list) -> dict:
        """
        Extract OPR, AWPR, AWCMR, AWRR, and overnight liquidity.

        Text patterns:
          OPR (1st value in Money Market section chart Y-axis):
            "Money Market\n2.00%\n8.10\n7.90\n..."  → OPR=8.10
          AWPR (appears just before "91 Day" T-bill label):
            "Primary Market Secondary Market\n7.70\n91 Day"  → AWPR=7.70
          AWCMR/AWRR (in Overnight Money Market section):
            "Overnight Money Market\n7.68\n(b) 7.68\nAWCMR AWRR"
          Overnight liquidity (two-date row, take 2nd = latest):
            "Overnight Liquidity (Rs. bn) 270.99 270.41"
        """
        result = {
            "opr": None,
            "awpr": None,
            "awcmr": None,
            "awrr": None,
            "overnight_liquidity_bn": None,
        }

        # OPR: first floating-point value in the money-market chart block
        # The block starts with "Money Market" followed by a percentage scale.
        opr_m = re.search(
            r"Money Market\s+[\d.]+%\s+([\d.]+)\s+([\d.]+)",
            text,
        )
        if opr_m:
            result["opr"] = self._pf(opr_m.group(1))
            # group(2) = SRR (7.90%) — not a schema column; intentionally skipped
        else:
            notes.append("opr: money-market chart pattern not found")

        # AWPR: float immediately after "Primary Market Secondary Market" label
        awpr_m = re.search(
            r"Primary Market\s+Secondary Market\s+([\d.]+)",
            text,
        )
        if awpr_m:
            result["awpr"] = self._pf(awpr_m.group(1))
        else:
            notes.append("awpr: not found after 'Primary Market Secondary Market'")

        # AWCMR / AWRR: published in the Overnight Money Market sub-section
        # "(b)" indicates value based on actual transactions > Rs.50mn
        awcmr_m = re.search(
            r"Overnight Money Market\s+([\d.]+)\s*\(b\)\s*([\d.]+)\s*AWCMR\s*AWRR",
            text,
        )
        if awcmr_m:
            result["awcmr"] = self._pf(awcmr_m.group(1))
            result["awrr"]  = self._pf(awcmr_m.group(2))
        else:
            notes.append("awcmr/awrr: overnight money market pattern not found")

        # Overnight liquidity — appears with two dates; take the 2nd (current day)
        liq_m = re.search(
            r"Overnight Liquidity \(Rs\. bn\)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
            text,
        )
        if liq_m:
            result["overnight_liquidity_bn"] = self._pf(liq_m.group(2))
        else:
            notes.append("overnight_liquidity_bn: not found")

        return result

    # ── Currency and Reserve Money ────────────────────────────────────────────

    def _extract_currency_reserves(self, text: str, notes: list) -> dict:
        """
        Extract Currency in Circulation and Reserve Money (latest date value).

        Two-date pattern — second value is always the current indicators date:
          "Currency in Circulation  1,594,680.86  1,604,280.08"
          "Reserve Money            1,844,800.86  1,852,535.11"
        """
        result = {
            "currency_in_circ_mn": None,
            "reserve_money_mn": None,
        }

        cic_m = re.search(
            r"Currency in Circulation\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
            text,
        )
        if cic_m:
            result["currency_in_circ_mn"] = self._pf(cic_m.group(2))
        else:
            notes.append("currency_in_circ_mn: not found")

        rm_m = re.search(
            r"Reserve Money\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
            text,
        )
        if rm_m:
            result["reserve_money_mn"] = self._pf(rm_m.group(2))
        else:
            notes.append("reserve_money_mn: not found")

        return result

    # ── Share Market ─────────────────────────────────────────────────────────

    def _extract_share_market(
        self, tables: list, text: str, notes: list
    ) -> dict:
        """
        Extract CSE market statistics: ASPI, S&P SL20, turnover, market cap,
        PE ratio, foreign purchases, and foreign sales.

        Source strategy:
          - ASPI, SP_SL20: from raw text (adjacent numeric values near labels).
          - Turnover/Market Cap/PE, Foreign flows: text regex (most reliable).
          - Table fallback for any still-None values after text extraction.
        """
        result = {
            "aspi": None, "sp_sl20": None,
            "daily_turnover_mn": None, "market_cap_bn": None,
            "pe_ratio": None,
            "foreign_purchases_mn": None, "foreign_sales_mn": None,
        }

        # ASPI: large number immediately before "Daily Turnover"
        # Text: "23,882.82 Daily Turnover (Rs. mn) 4,234.53"
        aspi_m = re.search(r"([\d,]+\.\d{2})\s+Daily Turnover", text)
        if aspi_m:
            result["aspi"] = self._pf(aspi_m.group(1))
        else:
            notes.append("aspi: not found near 'Daily Turnover' label")

        # Daily turnover
        dt_m = re.search(r"Daily Turnover \(Rs\. mn\)\s+([\d,]+\.?\d*)", text)
        if dt_m:
            result["daily_turnover_mn"] = self._pf(dt_m.group(1))

        # Market cap + SP_SL20: appear together on one line
        # "Market Capitalization (Rs. bn) 8,475.97 6,708.71 PE Ratio 11.27"
        # market_cap = group(1), sp_sl20 = group(2)
        mkt_m = re.search(
            r"Market Capitalization \(Rs\. bn\)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s*PE Ratio",
            text,
        )
        if mkt_m:
            result["market_cap_bn"] = self._pf(mkt_m.group(1))
            result["sp_sl20"]       = self._pf(mkt_m.group(2))
        else:
            # Separate fallback patterns
            mc_m = re.search(
                r"Market Capitalization \(Rs\. bn\)\s+([\d,]+\.?\d*)", text
            )
            if mc_m:
                result["market_cap_bn"] = self._pf(mc_m.group(1))
            else:
                notes.append("market_cap_bn: not found")
            notes.append("sp_sl20: not found near market cap (pattern mismatch)")

        # PE ratio
        pe_m = re.search(r"PE Ratio\s+([\d,]+\.?\d*)", text)
        if pe_m:
            result["pe_ratio"] = self._pf(pe_m.group(1))

        # Foreign purchases and sales
        fp_m = re.search(r"Foreign Purchases\s+([\d,]+\.?\d*)", text)
        if fp_m:
            result["foreign_purchases_mn"] = self._pf(fp_m.group(1))
        else:
            notes.append("foreign_purchases_mn: not found")

        fs_m = re.search(r"Foreign Sales\s+([\d,]+\.?\d*)", text)
        if fs_m:
            result["foreign_sales_mn"] = self._pf(fs_m.group(1))
        else:
            notes.append("foreign_sales_mn: not found")

        # Table fallback for turnover / PE ratio / foreign flows if still None
        for t in tables:
            for r_idx, row in enumerate(t):
                if not row or not row[0]:
                    continue
                label = str(row[0]).strip()
                val   = self._pf(row[1]) if len(row) > 1 else None

                if "Daily Turnover" in label and result["daily_turnover_mn"] is None:
                    result["daily_turnover_mn"] = val
                elif "Market Capitalization" in label and result["market_cap_bn"] is None:
                    result["market_cap_bn"] = val
                elif "PE Ratio" in label and result["pe_ratio"] is None:
                    result["pe_ratio"] = val
                elif "Foreign Purchases" in label and result["foreign_purchases_mn"] is None:
                    result["foreign_purchases_mn"] = val
                elif "Foreign Sales" in label and result["foreign_sales_mn"] is None:
                    result["foreign_sales_mn"] = val

        return result

    # ── Energy ────────────────────────────────────────────────────────────────

    def _extract_energy(self, text: str, notes: list) -> dict:
        """
        Extract total electricity generation (GWh) and peak demand (MW).

        Two-date pattern — second value is always the current indicators date:
          "Total Energy (GWh)  50.71  51.47"
          "Peak Demand (MW)    2,785.80  2,726.20"
        """
        result = {
            "total_energy_gwh": None,
            "peak_demand_mw": None,
        }

        energy_m = re.search(
            r"Total Energy \(GWh\)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
            text,
        )
        if energy_m:
            result["total_energy_gwh"] = self._pf(energy_m.group(2))
        else:
            notes.append("total_energy_gwh: not found")

        peak_m = re.search(
            r"Peak Demand \(MW\)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
            text,
        )
        if peak_m:
            result["peak_demand_mw"] = self._pf(peak_m.group(2))
        else:
            notes.append("peak_demand_mw: not found")

        return result

    # ── Petroleum ─────────────────────────────────────────────────────────────

    def _extract_petroleum(self, text: str, notes: list) -> dict:
        """
        Extract Brent/WTI crude prices (USD/barrel) and CPC local pump prices.

        Crude:
          Text block: "Brent WTI OPEC Petrol Diesel Kerosene"
          Values:     "67.41 62.29 66.81 73.90 86.32 85.77"
          → brent=67.41, wti=62.29  (OPEC and Singapore CIF not in schema)

        CPC local prices (LKR per litre):
          Text: "Petrol (92 octane): 2 9 2 . 0 0 Auto Diesel: 2 7 7 . 0 0"
          Note: PDF renders digits with inter-character spaces — remove all
          whitespace from the captured group to recover the numeric string.
        """
        result = {
            "brent_crude_usd": None,
            "wti_crude_usd": None,
            "petrol_local_lkr": None,
            "diesel_local_lkr": None,
        }

        # Crude: the label and values appear on consecutive lines
        crude_m = re.search(
            r"Brent\s+WTI\s+OPEC\s+Petrol\s+Diesel\s+Kerosene\s+"
            r"([\d.]+)\s+([\d.]+)\s+([\d.]+)",
            text,
        )
        if crude_m:
            result["brent_crude_usd"] = self._pf(crude_m.group(1))
            result["wti_crude_usd"]   = self._pf(crude_m.group(2))
        else:
            # Fallback: find the numeric block near the "Brent" label
            brent_near = re.search(
                r"Brent\b[^\n]*\n([\d.]+)\s+([\d.]+)", text
            )
            if brent_near:
                result["brent_crude_usd"] = self._pf(brent_near.group(1))
                result["wti_crude_usd"]   = self._pf(brent_near.group(2))
            else:
                notes.append("brent/wti: not found in text")

        # CPC Petrol 92 — capture everything between "Petrol (92 octane):" and "Auto Diesel"
        petrol_m = re.search(
            r"Petrol \(92 octane\):\s*([\d\s.]+?)\s*Auto Diesel",
            text,
        )
        if petrol_m:
            result["petrol_local_lkr"] = self._pf(
                re.sub(r"\s+", "", petrol_m.group(1))  # strip inter-digit spaces
            )
        else:
            notes.append("petrol_local_lkr: not found")

        # CPC Auto Diesel — capture between "Auto Diesel:" and "Kerosene"
        diesel_m = re.search(
            r"Auto Diesel:\s*([\d\s.]+?)\s*Kerosene",
            text,
        )
        if diesel_m:
            result["diesel_local_lkr"] = self._pf(
                re.sub(r"\s+", "", diesel_m.group(1))
            )
        else:
            notes.append("diesel_local_lkr: not found")

        return result

    # ── Macro Headlines ───────────────────────────────────────────────────────

    def _extract_macro_headlines(self, text: str, notes: list) -> dict:
        """
        Extract real GDP growth, NCPI y-o-y, and CCPI y-o-y headline figures.

        These are quarterly/monthly releases — same value repeats for weeks.
        The PDF shows them in the top header bar.

        Text patterns (concatenated, no spaces between year and value):
          "2025Q35.4%"         → gdp_growth=5.4
          "December2025 2.9%"  → ncpi_yoy=2.9
          "January 20262.3%"   → ccpi_yoy=2.3

        The NCPI/CCPI order in the header is always NCPI first, CCPI second.
        """
        result = {
            "gdp_growth_pct": None,
            "ncpi_yoy_pct":   None,
            "ccpi_yoy_pct":   None,
        }

        # GDP growth: digit after "Q<n>"
        gdp_m = re.search(r"Q\d\s*([\d.]+)%", text)
        if gdp_m:
            result["gdp_growth_pct"] = self._pf(gdp_m.group(1))
        else:
            notes.append("gdp_growth_pct: not found (Q<n>% pattern missing)")

        # NCPI and CCPI: month-name + 4-digit year followed by value+%
        inflation_matches = re.findall(
            rf"{MONTHS_PATTERN}\s*\d{{4}}\s*([\d.]+)%",
            text,
        )
        if len(inflation_matches) >= 1:
            result["ncpi_yoy_pct"] = self._pf(inflation_matches[0])
        else:
            notes.append("ncpi_yoy_pct: month+year+% pattern not found")

        if len(inflation_matches) >= 2:
            result["ccpi_yoy_pct"] = self._pf(inflation_matches[1])
        else:
            notes.append("ccpi_yoy_pct: second month+year+% pattern not found")

        return result
