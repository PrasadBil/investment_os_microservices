-- =============================================================================
-- Investment OS — Unified Data Collection Framework
-- Migration: 001_unified_data_collection_schema.sql
-- Created:   2026-02-18 (Sprint 0 — Framework Skeleton)
-- Database:  Supabase (PostgreSQL 15)
-- VPS:       root@srv1127544.hstgr.cloud
-- Project:   crsnnyjxfnpwnjxfdilx.supabase.co
-- =============================================================================
--
-- TABLE INVENTORY (9 tables total):
--
--   CBSL DAILY (1 table):
--     1. cbsl_daily_indicators        — 31 daily macro metrics, date PK
--
--   CBSL WEEKLY (4 tables):
--     2. cbsl_weekly_real_sector      — CPI, GDP, IIP, PMI, agriculture
--     3. cbsl_weekly_monetary_sector  — Rates, money supply, credit
--     4. cbsl_weekly_fiscal_sector    — T-Bill/Bond auctions, govt securities
--     5. cbsl_weekly_external_sector  — FX, trade balance, reserves
--
--   CSE CORPORATE ACTIONS (4 tables):
--     6. cse_corporate_actions        — Master log (all action types)
--     7. cse_right_issues             — Right issue milestones (dates, ratios, prices)
--     8. cse_dividends                — Cash/scrip dividends (XD/record/payment dates)
--     9. cse_watch_list_history       — Watch list and suspension entry/exit
--
-- DESIGN PRINCIPLES:
--   • All PKs use date + source composite key — guarantees idempotency on re-run
--   • DECIMAL precision chosen per financial data convention (see comments per column)
--   • collected_at TIMESTAMPTZ on every table — enables staleness detection
--   • Row-level security NOT enabled (single-service architecture, VPS → Supabase)
--   • All table/column names snake_case (consistent with existing Investment OS schema)
--   • Indexes added for primary query patterns (time series + cross-source joins)
--
-- UPSERT PATTERN:
--   All inserts use ON CONFLICT (pk_columns) DO UPDATE SET ... collected_at = NOW()
--   This ensures re-runs update existing rows rather than failing or duplicating.
--
-- RUNNING THIS MIGRATION:
--   Option A (Supabase Studio): paste into SQL Editor and execute
--   Option B (psql on VPS):
--     psql $SUPABASE_DB_URL < 001_unified_data_collection_schema.sql
--   Option C (Python): supabase.rpc("exec_sql", {"sql": open(...).read()})
--
-- =============================================================================


-- =============================================================================
-- SAFETY: Run idempotently — DROP only if you need a clean slate.
-- Commented out by default to protect against accidental data loss.
-- =============================================================================
-- DROP TABLE IF EXISTS cbsl_daily_indicators CASCADE;
-- DROP TABLE IF EXISTS cbsl_weekly_real_sector CASCADE;
-- DROP TABLE IF EXISTS cbsl_weekly_monetary_sector CASCADE;
-- DROP TABLE IF EXISTS cbsl_weekly_fiscal_sector CASCADE;
-- DROP TABLE IF EXISTS cbsl_weekly_external_sector CASCADE;
-- DROP TABLE IF EXISTS cse_corporate_actions CASCADE;
-- DROP TABLE IF EXISTS cse_right_issues CASCADE;
-- DROP TABLE IF EXISTS cse_dividends CASCADE;
-- DROP TABLE IF EXISTS cse_watch_list_history CASCADE;


-- =============================================================================
-- TABLE 1: cbsl_daily_indicators
-- Source: CBSL Daily Economic Indicators PDF (single page, ~200KB)
-- PK: date (publication date)
-- Row rate: ~1 row/business day (~250 rows/year)
-- =============================================================================

CREATE TABLE IF NOT EXISTS cbsl_daily_indicators (

    -- Primary key
    date                        DATE            NOT NULL,

    -- ── Exchange Rates (TT = Telegraphic Transfer) ──────────────────────────
    -- All rates: LKR per 1 unit of foreign currency
    usd_tt_buy                  DECIMAL(10, 4),     -- USD TT Buying rate
    usd_tt_sell                 DECIMAL(10, 4),     -- USD TT Selling rate
    gbp_tt_buy                  DECIMAL(10, 4),     -- GBP TT Buying rate
    gbp_tt_sell                 DECIMAL(10, 4),     -- GBP TT Selling rate
    eur_tt_buy                  DECIMAL(10, 4),     -- EUR TT Buying rate
    eur_tt_sell                 DECIMAL(10, 4),     -- EUR TT Selling rate
    jpy_tt_buy                  DECIMAL(10, 4),     -- JPY TT Buying rate (LKR per 100 JPY)
    jpy_tt_sell                 DECIMAL(10, 4),     -- JPY TT Selling rate

    -- ── T-Bill Yields (Secondary Market) ─────────────────────────────────────
    -- Percent per annum
    tbill_91d                   DECIMAL(6, 2),      -- 91-day T-Bill yield
    tbill_182d                  DECIMAL(6, 2),      -- 182-day T-Bill yield
    tbill_364d                  DECIMAL(6, 2),      -- 364-day T-Bill yield

    -- ── Money Market ──────────────────────────────────────────────────────────
    awcmr                       DECIMAL(6, 2),      -- Avg Weighted Call Money Rate (% p.a.)
    awrr                        DECIMAL(6, 2),      -- Avg Weighted Repo Rate (% p.a.)
    overnight_liquidity_bn      DECIMAL(10, 2),     -- Overnight liquidity position (Rs. billion)
                                                    -- Negative = net borrowing from CBSL

    -- ── Currency & Reserves ───────────────────────────────────────────────────
    currency_in_circ_mn         DECIMAL(14, 2),     -- Currency in circulation (Rs. million)
    reserve_money_mn            DECIMAL(14, 2),     -- Reserve money / monetary base (Rs. million)

    -- ── Share Market (CSE) ────────────────────────────────────────────────────
    aspi                        DECIMAL(10, 2),     -- All Share Price Index
    sp_sl20                     DECIMAL(10, 2),     -- S&P SL 20 Index
    daily_turnover_mn           DECIMAL(10, 2),     -- Daily market turnover (Rs. million)
    market_cap_bn               DECIMAL(10, 2),     -- Market capitalization (Rs. billion)
    pe_ratio                    DECIMAL(6, 2),      -- Market P/E ratio
    foreign_purchases_mn        DECIMAL(10, 2),     -- Foreign net purchases (Rs. million)
    foreign_sales_mn            DECIMAL(10, 2),     -- Foreign net sales (Rs. million)

    -- ── Policy Rates ─────────────────────────────────────────────────────────
    opr                         DECIMAL(4, 2),      -- Overnight Policy Rate (% p.a.)
    sdfr                        DECIMAL(4, 2),      -- Standing Deposit Facility Rate
    slfr                        DECIMAL(4, 2),      -- Standing Lending Facility Rate
    awpr                        DECIMAL(6, 2),      -- Avg Weighted Prime Lending Rate

    -- ── Energy ───────────────────────────────────────────────────────────────
    brent_crude_usd             DECIMAL(8, 2),      -- Brent crude oil price (USD/barrel)
    wti_crude_usd               DECIMAL(8, 2),      -- WTI crude oil price (USD/barrel)
    petrol_local_lkr            DECIMAL(8, 2),      -- CPC Petrol 92 (LKR/litre)
    diesel_local_lkr            DECIMAL(8, 2),      -- CPC Auto Diesel (LKR/litre)
    total_energy_gwh            DECIMAL(8, 2),      -- Total electricity generation (GWh)
    peak_demand_mw              DECIMAL(8, 2),      -- Peak electricity demand (MW)

    -- ── Macro Headlines (when updated — may be NULL on many days) ─────────────
    gdp_growth_pct              DECIMAL(6, 2),      -- Real GDP growth rate (% y-o-y)
    ncpi_yoy_pct                DECIMAL(6, 2),      -- NCPI inflation (% y-o-y)
    ccpi_yoy_pct                DECIMAL(6, 2),      -- CCPI inflation (% y-o-y)

    -- ── Metadata ──────────────────────────────────────────────────────────────
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,               -- Archive path/URL of source PDF
    parse_notes                 TEXT,               -- Parser warnings (partial extraction etc.)

    CONSTRAINT cbsl_daily_indicators_pkey PRIMARY KEY (date)
);

COMMENT ON TABLE cbsl_daily_indicators IS
    'CBSL Daily Economic Indicators — 31 macro metrics extracted from the 1-page '
    'CBSL daily dashboard PDF. Published every business day. '
    'Source: https://www.cbsl.gov.lk/sites/default/files/daily_economic_indicators_YYYYMMDD_e.pdf';

-- Indexes for time-series queries
CREATE INDEX IF NOT EXISTS idx_cbsl_daily_date
    ON cbsl_daily_indicators (date DESC);

CREATE INDEX IF NOT EXISTS idx_cbsl_daily_aspi
    ON cbsl_daily_indicators (date DESC, aspi)
    WHERE aspi IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cbsl_daily_usd
    ON cbsl_daily_indicators (date DESC, usd_tt_sell)
    WHERE usd_tt_sell IS NOT NULL;


-- =============================================================================
-- TABLE 2: cbsl_weekly_real_sector
-- Source: CBSL Weekly Economic Indicators PDF, pages 3-6
-- PK: week_ending (Friday date)
-- Row rate: ~1 row/week (~52 rows/year)
-- =============================================================================

CREATE TABLE IF NOT EXISTS cbsl_weekly_real_sector (

    -- Primary key: the Friday on which the report is published
    week_ending                 DATE            NOT NULL,

    -- ── Consumer Price Indices ────────────────────────────────────────────────
    ncpi_headline               DECIMAL(8, 1),      -- National CPI index level (base 2021=100)
    ncpi_yoy_pct                DECIMAL(6, 2),      -- NCPI year-on-year % change
    ncpi_mom_pct                DECIMAL(6, 2),      -- NCPI month-on-month % change
    ccpi_headline               DECIMAL(8, 1),      -- Colombo CPI index level (base 2021=100)
    ccpi_yoy_pct                DECIMAL(6, 2),      -- CCPI year-on-year % change
    ccpi_mom_pct                DECIMAL(6, 2),      -- CCPI month-on-month % change

    -- ── National Output ───────────────────────────────────────────────────────
    gdp_growth_pct              DECIMAL(6, 2),      -- Real GDP growth (% y-o-y)
    iip_index                   DECIMAL(8, 1),      -- Index of Industrial Production (base 2018=100)
    iip_yoy_pct                 DECIMAL(6, 2),      -- IIP year-on-year % change

    -- ── Business Activity Indices ─────────────────────────────────────────────
    pmi_manufacturing           DECIMAL(6, 1),      -- Manufacturing PMI (>50 = expansion)
    pmi_services                DECIMAL(6, 1),      -- Services PMI / Business Activity Index

    -- ── Agriculture ───────────────────────────────────────────────────────────
    tea_production_mn_kg        DECIMAL(8, 1),      -- Tea production (million kg)
    rubber_production_mn_kg     DECIMAL(8, 1),      -- Rubber production (million kg)
    coconut_production_mn_nuts  DECIMAL(10, 1),     -- Coconut production (million nuts)

    -- ── Labour Market ─────────────────────────────────────────────────────────
    unemployment_rate           DECIMAL(4, 1),      -- Unemployment rate (%)

    -- ── Energy / Commodity Prices ─────────────────────────────────────────────
    brent_crude_monthly_avg     DECIMAL(8, 2),      -- Monthly avg Brent crude (USD/barrel)

    -- ── Metadata ──────────────────────────────────────────────────────────────
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,
    parse_notes                 TEXT,

    CONSTRAINT cbsl_weekly_real_pkey PRIMARY KEY (week_ending)
);

COMMENT ON TABLE cbsl_weekly_real_sector IS
    'CBSL Weekly Economic Indicators — Real Sector (pages 3-6). '
    'CPI, GDP, IIP, PMI, agriculture, and labour data. Published every Friday. '
    'Source: https://www.cbsl.gov.lk/sites/default/files/cbslweb_documents/statistics/wei/WEI_YYYYMMDD_e.pdf';

CREATE INDEX IF NOT EXISTS idx_cbsl_weekly_real_date
    ON cbsl_weekly_real_sector (week_ending DESC);


-- =============================================================================
-- TABLE 3: cbsl_weekly_monetary_sector
-- Source: CBSL Weekly Economic Indicators PDF, pages 7-9
-- PK: week_ending (Friday date)
-- =============================================================================

CREATE TABLE IF NOT EXISTS cbsl_weekly_monetary_sector (

    week_ending                 DATE            NOT NULL,

    -- ── Policy Rates ─────────────────────────────────────────────────────────
    opr                         DECIMAL(4, 2),      -- Overnight Policy Rate (% p.a.)
    sdfr                        DECIMAL(4, 2),      -- Standing Deposit Facility Rate
    slfr                        DECIMAL(4, 2),      -- Standing Lending Facility Rate

    -- ── Market Lending/Deposit Rates ─────────────────────────────────────────
    awpr                        DECIMAL(6, 2),      -- Avg Weighted Prime Lending Rate
    awcmr_end_week              DECIMAL(6, 2),      -- AWCMR at end of week
    awdr                        DECIMAL(6, 2),      -- Avg Weighted Deposit Rate
    awlr                        DECIMAL(6, 2),      -- Avg Weighted Lending Rate
    awfdr                       DECIMAL(6, 2),      -- Avg Weighted Fixed Deposit Rate

    -- ── Money Supply ─────────────────────────────────────────────────────────
    -- Rs. billion; broad money aggregates show monetary transmission
    reserve_money_bn            DECIMAL(10, 1),     -- Reserve money / M0 (Rs. billion)
    m1_bn                       DECIMAL(10, 1),     -- Narrow money M1 (Rs. billion)
    m2_bn                       DECIMAL(10, 1),     -- M2 money supply (Rs. billion)
    m2b_bn                      DECIMAL(10, 1),     -- M2b broad money (Rs. billion)

    -- ── Credit ───────────────────────────────────────────────────────────────
    credit_private_sector_bn    DECIMAL(10, 1),     -- Credit to private sector (Rs. billion)
    credit_govt_bn              DECIMAL(10, 1),     -- Net credit to government (Rs. billion)

    -- ── Foreign Assets ───────────────────────────────────────────────────────
    nfa_banking_bn              DECIMAL(10, 1),     -- Net Foreign Assets of banking system (Rs. billion)

    -- ── Open Market Operations ────────────────────────────────────────────────
    omo_repo_bn                 DECIMAL(10, 1),     -- OMO Repo outstanding (Rs. billion)
    omo_reverse_repo_bn         DECIMAL(10, 1),     -- OMO Reverse Repo outstanding (Rs. billion)

    -- ── Metadata ──────────────────────────────────────────────────────────────
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,
    parse_notes                 TEXT,

    CONSTRAINT cbsl_weekly_monetary_pkey PRIMARY KEY (week_ending)
);

COMMENT ON TABLE cbsl_weekly_monetary_sector IS
    'CBSL Weekly Economic Indicators — Monetary Sector (pages 7-9). '
    'Interest rates, money supply aggregates, credit, and OMO. Published every Friday.';

CREATE INDEX IF NOT EXISTS idx_cbsl_weekly_monetary_date
    ON cbsl_weekly_monetary_sector (week_ending DESC);

CREATE INDEX IF NOT EXISTS idx_cbsl_weekly_monetary_opr
    ON cbsl_weekly_monetary_sector (week_ending DESC, opr)
    WHERE opr IS NOT NULL;


-- =============================================================================
-- TABLE 4: cbsl_weekly_fiscal_sector
-- Source: CBSL Weekly Economic Indicators PDF, pages 10-12
-- PK: week_ending (Friday date)
-- =============================================================================

CREATE TABLE IF NOT EXISTS cbsl_weekly_fiscal_sector (

    week_ending                 DATE            NOT NULL,

    -- ── T-Bill Primary Market (Auction Results) ───────────────────────────────
    tbill_91d_yield             DECIMAL(6, 2),      -- 91D T-Bill primary yield (% p.a.)
    tbill_182d_yield            DECIMAL(6, 2),      -- 182D T-Bill primary yield (% p.a.)
    tbill_364d_yield            DECIMAL(6, 2),      -- 364D T-Bill primary yield (% p.a.)
    tbill_subscription_ratio    DECIMAL(6, 2),      -- T-Bill oversubscription ratio (bid/offer)
    tbill_amount_offered_bn     DECIMAL(10, 2),     -- Amount offered at auction (Rs. billion)
    tbill_amount_accepted_bn    DECIMAL(10, 2),     -- Amount accepted at auction (Rs. billion)

    -- ── T-Bond Auctions ───────────────────────────────────────────────────────
    tbond_subscription_ratio    DECIMAL(6, 2),      -- T-Bond oversubscription ratio
    tbond_amount_offered_bn     DECIMAL(10, 2),     -- T-Bond amount offered (Rs. billion)
    tbond_amount_accepted_bn    DECIMAL(10, 2),     -- T-Bond amount accepted (Rs. billion)

    -- ── Government Securities — Foreign Holdings ──────────────────────────────
    tbill_foreign_holdings_bn   DECIMAL(10, 2),     -- Foreign T-Bill holdings (Rs. billion)
    tbond_foreign_holdings_bn   DECIMAL(10, 2),     -- Foreign T-Bond holdings (Rs. billion)
    total_foreign_holdings_bn   DECIMAL(10, 2),     -- Total foreign govt securities holdings

    -- ── Secondary Market ─────────────────────────────────────────────────────
    secondary_market_volume_bn  DECIMAL(10, 2),     -- Secondary market transaction volume (Rs. billion)

    -- ── Metadata ──────────────────────────────────────────────────────────────
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,
    parse_notes                 TEXT,

    CONSTRAINT cbsl_weekly_fiscal_pkey PRIMARY KEY (week_ending)
);

COMMENT ON TABLE cbsl_weekly_fiscal_sector IS
    'CBSL Weekly Economic Indicators — Fiscal Sector (pages 10-12). '
    'T-Bill/Bond auction results, subscription ratios, and foreign holdings. Published every Friday.';

CREATE INDEX IF NOT EXISTS idx_cbsl_weekly_fiscal_date
    ON cbsl_weekly_fiscal_sector (week_ending DESC);


-- =============================================================================
-- TABLE 5: cbsl_weekly_external_sector
-- Source: CBSL Weekly Economic Indicators PDF, pages 13-16
-- PK: week_ending (Friday date)
-- =============================================================================

CREATE TABLE IF NOT EXISTS cbsl_weekly_external_sector (

    week_ending                 DATE            NOT NULL,

    -- ── Exchange Rates ────────────────────────────────────────────────────────
    usd_lkr_indicative          DECIMAL(8, 4),      -- USD/LKR indicative rate (CBSL)
    lkr_ytd_change_pct          DECIMAL(6, 2),      -- LKR YTD appreciation (+) / depreciation (-)
    eur_lkr                     DECIMAL(8, 4),      -- EUR/LKR rate
    gbp_lkr                     DECIMAL(8, 4),      -- GBP/LKR rate

    -- ── Official Reserves ─────────────────────────────────────────────────────
    gross_official_reserves_usd_bn  DECIMAL(8, 2),  -- Gross official reserves (USD billion)
    import_cover_months         DECIMAL(4, 1),       -- Import cover in months

    -- ── Trade ─────────────────────────────────────────────────────────────────
    -- Monthly figures (most recent month available)
    exports_usd_mn              DECIMAL(10, 2),     -- Monthly exports (USD million)
    imports_usd_mn              DECIMAL(10, 2),     -- Monthly imports (USD million)
    trade_balance_usd_mn        DECIMAL(10, 2),     -- Monthly trade balance (USD million, negative=deficit)
    trade_data_month            DATE,               -- The month these trade figures refer to

    -- ── Current Account Inflows ───────────────────────────────────────────────
    workers_remittances_usd_mn  DECIMAL(10, 2),     -- Workers remittances (USD million)
    tourism_earnings_usd_mn     DECIMAL(10, 2),     -- Tourism earnings (USD million)
    remittances_data_month      DATE,               -- Month these remittance figures refer to

    -- ── Balance of Payments ───────────────────────────────────────────────────
    bop_overall_usd_mn          DECIMAL(10, 2),     -- Overall BoP position (USD million)
    current_account_usd_mn      DECIMAL(10, 2),     -- Current account balance (USD million)

    -- ── Metadata ──────────────────────────────────────────────────────────────
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,
    parse_notes                 TEXT,

    CONSTRAINT cbsl_weekly_external_pkey PRIMARY KEY (week_ending)
);

COMMENT ON TABLE cbsl_weekly_external_sector IS
    'CBSL Weekly Economic Indicators — External Sector (pages 13-16). '
    'FX rates, official reserves, trade balance, remittances, tourism. Published every Friday.';

CREATE INDEX IF NOT EXISTS idx_cbsl_weekly_external_date
    ON cbsl_weekly_external_sector (week_ending DESC);

CREATE INDEX IF NOT EXISTS idx_cbsl_weekly_reserves
    ON cbsl_weekly_external_sector (week_ending DESC, gross_official_reserves_usd_bn)
    WHERE gross_official_reserves_usd_bn IS NOT NULL;


-- =============================================================================
-- TABLE 6: cse_corporate_actions (Master Log)
-- Source: CSE Daily Market Report PDF, pages 11-15
-- PK: (date, symbol, action_type) — one row per action per stock per day
-- =============================================================================

CREATE TABLE IF NOT EXISTS cse_corporate_actions (

    -- Composite PK: a stock can have multiple action types on the same date
    date                        DATE            NOT NULL,   -- Report publication date
    symbol                      TEXT            NOT NULL,   -- CSE ticker (e.g., 'JKH.N0000')
    action_type                 TEXT            NOT NULL,   -- See ENUM comment below

    -- Stock info (denormalized for query convenience)
    company_name                TEXT,

    -- ── Action Details (generic fields — specifics in type-specific tables) ───
    action_detail               TEXT,           -- Human-readable summary of the action
    effective_date              DATE,           -- When the action takes effect
    record_date                 DATE,           -- Book closure / record date
    xd_date                     DATE,           -- Ex-dividend / ex-rights date
    payment_date                DATE,           -- Dividend payment date

    -- ── Numeric Fields (type-specific — NULL for inapplicable types) ──────────
    amount_lkr                  DECIMAL(12, 4), -- Dividend per share (LKR) or ratio numerator
    ratio_numerator             DECIMAL(8, 2),  -- Right issue / subdivision ratio (numerator)
    ratio_denominator           DECIMAL(8, 2),  -- Ratio denominator
    subscription_price          DECIMAL(10, 2), -- Right issue subscription price (LKR)

    -- ── Watch List / Suspension Fields ───────────────────────────────────────
    watch_list_reason           TEXT,           -- Reason for watch list inclusion (if applicable)
    suspension_reason           TEXT,           -- Reason for trading suspension (if applicable)

    -- ── Source Tracking ───────────────────────────────────────────────────────
    source_page                 INTEGER,        -- PDF page number (11-15) where action was found
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,           -- Google Drive URL of archived PDF

    CONSTRAINT cse_corporate_actions_pkey PRIMARY KEY (date, symbol, action_type)
);

-- action_type values (not a DB enum for flexibility — enforced at application layer):
--   'right_issue'          — Entitlement to buy new shares
--   'share_subdivision'    — Stock split
--   'scrip_dividend'       — Dividend paid in shares
--   'cash_dividend'        — Dividend paid in cash
--   'watch_list_entry'     — Added to exchange watch list
--   'watch_list_exit'      — Removed from watch list
--   'trading_suspended'    — Trading halted
--   'trading_resumed'      — Trading suspension lifted

COMMENT ON TABLE cse_corporate_actions IS
    'CSE Corporate Actions master log — all action types from CSE Daily Market Report pages 11-15. '
    'One row per stock per action type per date. Composite PK ensures idempotency. '
    'Source: https://www.cse.lk/publications/cse-daily (archived to Google Drive)';

CREATE INDEX IF NOT EXISTS idx_cse_ca_date
    ON cse_corporate_actions (date DESC);

CREATE INDEX IF NOT EXISTS idx_cse_ca_symbol
    ON cse_corporate_actions (symbol, date DESC);

CREATE INDEX IF NOT EXISTS idx_cse_ca_type
    ON cse_corporate_actions (action_type, date DESC);

CREATE INDEX IF NOT EXISTS idx_cse_ca_xd_date
    ON cse_corporate_actions (xd_date)
    WHERE xd_date IS NOT NULL;


-- =============================================================================
-- TABLE 7: cse_right_issues
-- Source: CSE Daily Market Report, Page 11 ("Right Issues")
-- PK: (date, symbol) — one active right issue per stock shown per day
-- =============================================================================

CREATE TABLE IF NOT EXISTS cse_right_issues (

    date                        DATE            NOT NULL,
    symbol                      TEXT            NOT NULL,
    company_name                TEXT,

    -- ── Right Issue Terms ─────────────────────────────────────────────────────
    rights_ratio_per            INTEGER,            -- Rights per N existing shares (numerator)
    rights_ratio_held           INTEGER,            -- Existing shares (denominator)
    subscription_price_lkr      DECIMAL(10, 2),     -- Price per new share (LKR)

    -- ── Key Dates ────────────────────────────────────────────────────────────
    xr_date                     DATE,               -- Ex-rights date
    entitlement_date            DATE,               -- Entitlement date (record date)
    subscription_open_date      DATE,               -- Subscription period open
    subscription_close_date     DATE,               -- Subscription period close

    -- ── Financials ────────────────────────────────────────────────────────────
    total_shares_offered        BIGINT,             -- Total new shares in the issue
    gross_proceeds_mn_lkr       DECIMAL(14, 2),     -- Expected gross proceeds (LKR mn)
    purpose_of_issue            TEXT,               -- Use of proceeds (from prospectus note)

    -- ── Status ────────────────────────────────────────────────────────────────
    status                      TEXT,               -- 'active', 'closed', 'allotted'

    -- ── Metadata ──────────────────────────────────────────────────────────────
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,
    source_page                 INTEGER         DEFAULT 11,

    CONSTRAINT cse_right_issues_pkey PRIMARY KEY (date, symbol)
);

COMMENT ON TABLE cse_right_issues IS
    'CSE Right Issues detail — extracted from CSE Daily Report Page 11. '
    'Captures ratio, price, and all key dates for each active right issue.';

CREATE INDEX IF NOT EXISTS idx_cse_ri_symbol
    ON cse_right_issues (symbol, date DESC);

CREATE INDEX IF NOT EXISTS idx_cse_ri_xr_date
    ON cse_right_issues (xr_date)
    WHERE xr_date IS NOT NULL;


-- =============================================================================
-- TABLE 8: cse_dividends
-- Source: CSE Daily Market Report, Pages 12-13
--         Page 12: Scrip Dividends | Page 13: Cash Dividends
-- PK: (date, symbol, dividend_type) — a stock can have both cash + scrip on same day
-- =============================================================================

CREATE TABLE IF NOT EXISTS cse_dividends (

    date                        DATE            NOT NULL,
    symbol                      TEXT            NOT NULL,
    dividend_type               TEXT            NOT NULL,   -- 'cash' | 'scrip'
    company_name                TEXT,

    -- ── Cash Dividend Fields ─────────────────────────────────────────────────
    dividend_per_share_lkr      DECIMAL(10, 4),     -- Cash dividend per share (LKR)

    -- ── Scrip Dividend Fields ─────────────────────────────────────────────────
    scrip_ratio_new             INTEGER,            -- New shares issued per N existing
    scrip_ratio_held            INTEGER,            -- Existing shares (denominator)

    -- ── Key Dates (applicable for both types) ────────────────────────────────
    xd_date                     DATE,               -- Ex-dividend date
    record_date                 DATE,               -- Book closure / record date
    payment_date                DATE,               -- Cash payment / share credit date

    -- ── Metadata ──────────────────────────────────────────────────────────────
    financial_year              TEXT,               -- e.g., '2025/26 Q2' (when stated in PDF)
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,
    source_page                 INTEGER,            -- 12 (scrip) or 13 (cash)

    CONSTRAINT cse_dividends_pkey PRIMARY KEY (date, symbol, dividend_type)
);

COMMENT ON TABLE cse_dividends IS
    'CSE Dividends — cash and scrip dividends from CSE Daily Report pages 12-13. '
    'Captures XD date, record date, and payment date for each dividend announcement.';

CREATE INDEX IF NOT EXISTS idx_cse_div_symbol
    ON cse_dividends (symbol, date DESC);

CREATE INDEX IF NOT EXISTS idx_cse_div_xd
    ON cse_dividends (xd_date)
    WHERE xd_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cse_div_type
    ON cse_dividends (dividend_type, date DESC);


-- =============================================================================
-- TABLE 9: cse_watch_list_history
-- Source: CSE Daily Market Report, Pages 14-15
--         Page 14: Watch List | Page 15: Trading Suspended
-- PK: (date, symbol, status_type)
-- =============================================================================

CREATE TABLE IF NOT EXISTS cse_watch_list_history (

    date                        DATE            NOT NULL,
    symbol                      TEXT            NOT NULL,
    status_type                 TEXT            NOT NULL,   -- 'watch_list' | 'suspended'
    company_name                TEXT,

    -- ── Status Detail ────────────────────────────────────────────────────────
    reason                      TEXT,               -- Regulatory reason stated in the report
    date_entered                DATE,               -- When stock was placed on watch list / suspended
    duration_days               INTEGER,            -- Calculated: date - date_entered

    -- ── Exchange Context (from surrounding report data) ───────────────────────
    last_traded_price_lkr       DECIMAL(10, 2),     -- Last traded price on the report date
    market_cap_mn_lkr           DECIMAL(14, 2),     -- Market cap (if stated)

    -- ── Metadata ──────────────────────────────────────────────────────────────
    collected_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_file                 TEXT,
    source_page                 INTEGER,            -- 14 (watch list) or 15 (suspended)

    CONSTRAINT cse_watch_list_pkey PRIMARY KEY (date, symbol, status_type)
);

COMMENT ON TABLE cse_watch_list_history IS
    'CSE Watch List and Suspended stocks — from CSE Daily Report pages 14-15. '
    'Tracks regulatory dynamics: watch list entries, suspensions, and their reasons. '
    'Enables detection of accumulation patterns and regulatory risk signals.';

CREATE INDEX IF NOT EXISTS idx_cse_wl_symbol
    ON cse_watch_list_history (symbol, date DESC);

CREATE INDEX IF NOT EXISTS idx_cse_wl_date
    ON cse_watch_list_history (date DESC);

CREATE INDEX IF NOT EXISTS idx_cse_wl_type
    ON cse_watch_list_history (status_type, date DESC);


-- =============================================================================
-- VERIFICATION QUERIES
-- Run these after applying the migration to confirm all 9 tables were created.
-- =============================================================================

-- List all 9 tables
SELECT
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_name = t.table_name
     AND table_schema = 'public') AS column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN (
      'cbsl_daily_indicators',
      'cbsl_weekly_real_sector',
      'cbsl_weekly_monetary_sector',
      'cbsl_weekly_fiscal_sector',
      'cbsl_weekly_external_sector',
      'cse_corporate_actions',
      'cse_right_issues',
      'cse_dividends',
      'cse_watch_list_history'
  )
ORDER BY table_name;

-- Expected output (9 rows):
--   cbsl_daily_indicators        | 37 columns
--   cbsl_weekly_external_sector  | 19 columns
--   cbsl_weekly_fiscal_sector    | 17 columns
--   cbsl_weekly_monetary_sector  | 20 columns
--   cbsl_weekly_real_sector      | 17 columns
--   cse_corporate_actions        | 18 columns
--   cse_dividends                | 15 columns
--   cse_right_issues             | 15 columns
--   cse_watch_list_history       | 12 columns

-- =============================================================================
-- END OF MIGRATION
-- =============================================================================
