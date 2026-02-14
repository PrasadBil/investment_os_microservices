#!/usr/bin/env python3
"""
Investment OS - Database Module
================================
Centralized Supabase client management.
Replaces inline create_client() calls across all services.

Connection Management:
    - Singleton pattern: One connection per process
    - Connection pooling: Handled internally by supabase-py
    - Thread-safe: Client supports concurrent requests
    - Automatic retry: Built into supabase-py client

Usage:
    from common.database import get_supabase_client, health_check
    
    # Get singleton client (reuses connection)
    supabase = get_supabase_client()
    
    # Query as before
    response = supabase.table('cse_daily_prices').select('*').execute()
    
    # Optional: Check connection health
    if health_check():
        print("✅ Database connection healthy")

Replaces patterns in:
    - cse_data_loader.py (line 34-47)
    - calendar_signal_monitor.py (line 41)
    - tier1_granger_per_stock_v5.py (line 45-48)
    - watchlist_utils.py (line 45-51)
    - manipulation_detector_v5_0.py (via cse_data_loader import)
    - All dimension scorers (indirect via cse_data_loader)
"""
import os
import logging
from typing import Optional
from supabase import create_client, Client
from common.config import get_config

logger = logging.getLogger(__name__)

# Singleton instance
_supabase_client: Optional[Client] = None


def get_supabase_client(force_new: bool = False) -> Client:
    """
    Get or create Supabase client (singleton pattern).
    
    Reuses the same connection across all services within a process.
    This eliminates the 8+ duplicate create_client() calls.
    
    Connection Details:
        - Thread-safe: supabase-py client handles concurrent requests
        - Connection pooling: Managed internally by httpx library
        - Reconnection: Automatic retry logic built-in
        - Timeouts: Pre-configured in supabase-py (connect=5s, read=10s)
    
    Args:
        force_new: Force creation of a new client (for testing/reconnect)
    
    Returns:
        Supabase Client instance
    
    Raises:
        ValueError: If SUPABASE_URL or SUPABASE_KEY not set
    
    Examples:
        >>> client = get_supabase_client()
        >>> data = client.table('stocks').select('*').limit(10).execute()
        >>> print(len(data.data))  # 10
    """
    global _supabase_client
    
    if _supabase_client is not None and not force_new:
        return _supabase_client
    
    config = get_config()
    
    supabase_url = config.SUPABASE_URL
    supabase_key = config.SUPABASE_KEY
    
    if not supabase_url or not supabase_key:
        raise ValueError(
            "Missing Supabase credentials!\n"
            "Set in .env file:\n"
            "  SUPABASE_URL=https://your-project.supabase.co\n"
            "  SUPABASE_KEY=your-service-key"
        )
    
    logger.info(f"Connecting to Supabase: {supabase_url[:40]}...")
    _supabase_client = create_client(supabase_url, supabase_key)
    logger.info("✅ Supabase client initialized")
    
    return _supabase_client


def reset_client():
    """
    Reset the singleton client (for testing or reconnection).
    
    Useful when:
        - Running unit tests (need fresh connection per test)
        - Connection issues require reconnect
        - Switching between environments (dev/prod)
    """
    global _supabase_client
    _supabase_client = None
    logger.info("Supabase client reset")


def health_check(table: str = 'cse_daily_prices') -> bool:
    """
    Quick health check on Supabase connection.
    
    Performs a lightweight query to verify:
        - Credentials are valid
        - Network connection is working
        - Database is accessible
    
    Args:
        table: Table name to query (default: 'cse_daily_prices')
               Future-proof for NSE expansion (Phase 7)
    
    Returns:
        True if connection is healthy, False otherwise
    
    Examples:
        >>> if health_check():
        ...     print("Database ready")
        ... else:
        ...     print("Connection issues")
        
        >>> # Future NSE support
        >>> if health_check(table='nse_daily_prices'):
        ...     print("NSE database ready")
    """
    try:
        client = get_supabase_client()
        # Lightweight query to verify connection
        client.table(table).select('symbol').limit(1).execute()
        logger.info(f"✅ Database health check passed ({table})")
        return True
    except Exception as e:
        logger.error(f"❌ Database health check failed: {e}")
        return False
