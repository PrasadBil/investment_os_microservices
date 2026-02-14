"""
Investment OS - Common Library
================================
Shared utilities for all Investment OS services.

Modules:
    - config: Centralized configuration (.env loading)
    - database: Supabase client management (singleton)
    - data_loader: OHLCV data loading with fallback
    - email_sender: Email notification system
    - logging_config: Standardized logging

Usage:
    from common import (
        get_config,
        get_supabase_client,
        load_stock_data
    )

    config = get_config()
    supabase = get_supabase_client()
    data = load_stock_data('CTHR.N0000', days=30)
"""

__version__ = "1.0.0"

# Export public API
from common.config import get_config
from common.database import get_supabase_client, health_check, reset_client
from common.data_loader import (
    load_stock_data,
    load_cse_data,
    validate_data_quality,
    load_from_cse_daily_prices,
    load_from_daily_prices_backup
)
from common.email_sender import EmailSender
from common.logging_config import setup_logging

# Define what's available when doing "from common import *"
__all__ = [
    # Version
    '__version__',

    # Config
    'get_config',

    # Database
    'get_supabase_client',
    'health_check',
    'reset_client',

    # Data Loader
    'load_stock_data',
    'load_cse_data',
    'validate_data_quality',
    'load_from_cse_daily_prices',
    'load_from_daily_prices_backup',

    # Email
    'EmailSender',

    # Logging
    'setup_logging',
]
