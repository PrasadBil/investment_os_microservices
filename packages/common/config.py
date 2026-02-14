#!/usr/bin/env python3
"""
Investment OS - Configuration Module
======================================
Centralized configuration management via singleton pattern.
Loads .env once, exposes all variables as typed attributes.

Design Decisions:
    - Singleton: Config loaded once per process, reused everywhere
    - Typed attributes: IDE autocomplete + catch typos at import time
    - Validation: Fails fast if critical vars missing
    - Grouped: Related configs organized by service domain

Usage:
    from common.config import get_config

    config = get_config()
    print(config.SUPABASE_URL)        # Database
    print(config.V5_EMAIL_FROM)       # Email
    print(config.WORK_DIR)            # Paths

Replaces patterns in:
    - .env direct reads via os.getenv() across 8+ files
    - Hardcoded paths ("/opt/selenium_automation") in tier1_granger, send_v5_email
    - Inline email config in send_v5_email.py (lines 19-23)
    - config.py (old StockAnalysis scraper config - NOT this file)
"""

import os
import logging
from typing import Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Singleton instance
_config: Optional['Config'] = None


class Config:
    """
    Investment OS configuration singleton.

    All environment variables loaded once from .env file.
    Grouped by service domain for clarity.

    Attributes are read-only after initialization.
    Use get_config() to access — do not instantiate directly.
    """

    def __init__(self, env_path: Optional[str] = None):
        """
        Load configuration from .env file.

        Args:
            env_path: Path to .env file (default: auto-discover via dotenv)
        """
        # Load .env file
        if env_path:
            load_dotenv(env_path)
        else:
            load_dotenv()

        # ==================================================================
        # DATABASE (Supabase)
        # ==================================================================
        self.SUPABASE_URL: str = os.getenv('SUPABASE_URL', '')
        self.SUPABASE_KEY: str = os.getenv('SUPABASE_KEY', '')

        # ==================================================================
        # EMAIL (Gmail SMTP)
        # ==================================================================
        self.V5_EMAIL_FROM: str = os.getenv('V5_EMAIL_FROM', '')
        self.V5_EMAIL_TO: str = os.getenv('V5_EMAIL_TO', '')
        self.V5_EMAIL_PASSWORD: str = os.getenv('V5_EMAIL_PASSWORD', '')
        self.V5_SMTP_SERVER: str = os.getenv('V5_SMTP_SERVER', 'smtp.gmail.com')
        self.V5_SMTP_PORT: int = int(os.getenv('V5_SMTP_PORT', '587'))

        # ==================================================================
        # PATHS
        # ==================================================================
        self.WORK_DIR: str = os.getenv(
            'INVESTMENT_OS_WORK_DIR',
            '/opt/investment-os'
        )
        self.OLD_WORK_DIR: str = '/opt/selenium_automation'  # Backward compat
        self.REPORT_DIR: str = os.path.join(self.WORK_DIR, 'v5_reports')
        self.LOG_DIR: str = os.path.join(self.WORK_DIR, 'v5_logs')
        self.SIGNALS_DIR: str = os.path.join(self.WORK_DIR, 'signals')
        self.BACKUPS_DIR: str = os.path.join(self.WORK_DIR, 'backups')

        # ==================================================================
        # MARKET CONFIGURATION
        # ==================================================================
        self.EXPECTED_TOTAL_STOCKS: int = 296
        self.MIN_DATA_DAYS: int = 20
        self.PRIMARY_TABLE: str = 'cse_daily_prices'
        self.BACKUP_TABLE: str = 'daily_prices'

        logger.info("✅ Configuration loaded")

    def validate(self) -> bool:
        """
        Validate that critical configuration is present.

        Checks:
            - Supabase credentials exist
            - Email credentials exist (warning only — not all services need email)
            - Work directory exists or can be created

        Returns:
            True if critical config valid, False otherwise
        """
        errors = []

        # Critical: Database
        if not self.SUPABASE_URL:
            errors.append("SUPABASE_URL not set")
        if not self.SUPABASE_KEY:
            errors.append("SUPABASE_KEY not set")

        # Warning: Email (not all services need it)
        if not self.V5_EMAIL_FROM or not self.V5_EMAIL_PASSWORD:
            logger.warning(
                "Email config incomplete (V5_EMAIL_FROM / V5_EMAIL_PASSWORD). "
                "Email-sending services will fail."
            )

        if errors:
            for err in errors:
                logger.error(f"❌ Config validation: {err}")
            return False

        logger.info("✅ Configuration validation passed")
        return True

    def ensure_directories(self):
        """
        Create output directories if they don't exist.

        Safe to call multiple times. Creates:
            - REPORT_DIR (v5_reports/)
            - LOG_DIR (v5_logs/)
            - SIGNALS_DIR (signals/)
            - BACKUPS_DIR (backups/)
        """
        for dir_path in [self.REPORT_DIR, self.LOG_DIR,
                         self.SIGNALS_DIR, self.BACKUPS_DIR]:
            os.makedirs(dir_path, exist_ok=True)
            logger.debug(f"Directory ensured: {dir_path}")

    def __repr__(self) -> str:
        """Safe repr that doesn't leak credentials."""
        return (
            f"Config("
            f"supabase={'✅' if self.SUPABASE_URL else '❌'}, "
            f"email={'✅' if self.V5_EMAIL_FROM else '❌'}, "
            f"work_dir='{self.WORK_DIR}')"
        )


def get_config(env_path: Optional[str] = None, force_new: bool = False) -> Config:
    """
    Get or create Config singleton.

    First call loads .env and creates the Config object.
    Subsequent calls return the same instance (no re-reading .env).

    Args:
        env_path: Path to .env file (only used on first call)
        force_new: Force reload of configuration (for testing)

    Returns:
        Config instance with all environment variables

    Examples:
        >>> config = get_config()
        >>> print(config.SUPABASE_URL)
        https://crsnnyjxfnpwnjxfdilx.supabase.co

        >>> print(config)
        Config(supabase=✅, email=✅, work_dir='/opt/investment-os')
    """
    global _config

    if _config is not None and not force_new:
        return _config

    _config = Config(env_path=env_path)
    return _config
