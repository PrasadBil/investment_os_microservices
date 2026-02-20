
"""
WATCH LIST UTILITY - Investment OS

FILE: watchlist_utils.py
CREATED: 2026-01-03
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-01-03  Initial creation — CSE watch list checker utility
    v1.0.1  2026-02-10  Migrated to services/scoring-7d (Phase 2 microservices)
                         Replaced dotenv/load_dotenv with common.database.get_supabase_client()
    v1.0.2  2026-02-16  Added version history header (new project standard)

PURPOSE:
Utility functions for checking CSE watch list status from Supabase.

USAGE:
    from watchlist_utils import WatchListChecker

    checker = WatchListChecker()

    # Check single stock
    if checker.is_watch_list('MHDL.N0000'):
        print("This stock is on the watch list!")

    # Get all watch list stocks
    all_watch_list = checker.get_all_watch_list_stocks()
"""

from typing import List, Optional, Set

# === Investment OS Common Library (Phase 2 Migration) ===
from common.database import get_supabase_client


class WatchListChecker:
    """
    Check if stocks are on CSE watch list using Supabase
    """

    def __init__(self):
        """Initialize connection to Supabase via common library"""
        self.client = get_supabase_client()

        # Cache for performance
        self._cache: Optional[Set[str]] = None

    def get_all_watch_list_stocks(self, use_cache: bool = True) -> List[str]:
        """
        Get all active watch list stock symbols

        Args:
            use_cache: Use cached result if available (faster)

        Returns:
            List of stock symbols on watch list
        """
        # Return cache if available
        if use_cache and self._cache is not None:
            return list(self._cache)

        try:
            response = self.client.table('cse_watch_list')\
                .select('symbol')\
                .eq('status', 'ACTIVE')\
                .execute()

            symbols = [row['symbol'] for row in response.data]

            # Update cache
            self._cache = set(symbols)

            return symbols

        except Exception as e:
            print(f"Warning: Error fetching watch list from Supabase: {e}")
            print("   Falling back to hardcoded list...")
            return self._get_fallback_list()

    def is_watch_list(self, symbol: str, use_cache: bool = True) -> bool:
        """
        Check if a stock is on the watch list

        Args:
            symbol: Stock symbol (e.g., 'MHDL.N0000')
            use_cache: Use cached list (faster)

        Returns:
            True if on watch list, False otherwise
        """
        watch_list = self.get_all_watch_list_stocks(use_cache=use_cache)
        return symbol in watch_list

    def get_watch_list_info(self, symbol: str) -> Optional[dict]:
        """
        Get full watch list information for a stock

        Args:
            symbol: Stock symbol

        Returns:
            Dict with watch list info or None if not on list
        """
        try:
            response = self.client.table('cse_watch_list')\
                .select('*')\
                .eq('symbol', symbol)\
                .eq('status', 'ACTIVE')\
                .execute()

            if response.data:
                return response.data[0]
            return None

        except Exception as e:
            print(f"Warning: Error fetching info for {symbol}: {e}")
            return None

    def clear_cache(self):
        """Clear the cached watch list"""
        self._cache = None

    def _get_fallback_list(self) -> List[str]:
        """
        Fallback list if Supabase is unavailable

        Returns:
            Hardcoded list of watch list stocks (as of Jan 3, 2026)
        """
        return [
            'ACAP.N0000', 'ACME.N0000', 'ALHP.N0000', 'BBH.N0000', 'BLI.N0000',
            'BLUE.N0000', 'BLUE.X0000', 'CHOU.N0000', 'CSF.N0000', 'DOCK.N0000',
            'DOCK.R0000', 'HELA.N0000', 'KDL.N0000', 'MHDL.N0000', 'ODEL.N0000',
            'SHL.N0000', 'SHL.W0000', 'SING.N0000'
        ]


def test_watch_list_checker():
    """Test the watch list checker"""
    print("=" * 80)
    print("WATCH LIST CHECKER - TEST")
    print("=" * 80)

    # Initialize
    print("\n1. Initializing checker...")
    try:
        checker = WatchListChecker()
        print("   Initialized")
    except Exception as e:
        print(f"   Failed: {e}")
        return

    # Get all watch list stocks
    print("\n2. Fetching watch list from Supabase...")
    watch_list = checker.get_all_watch_list_stocks()

    if watch_list:
        print(f"   Found {len(watch_list)} watch list stocks:")
        for symbol in sorted(watch_list):
            print(f"      {symbol}")
    else:
        print("   No watch list stocks found")

    # Test specific stocks
    print("\n3. Testing specific stocks...")

    test_cases = [
        ('MHDL.N0000', True, 'Should be on watch list'),
        ('CTC.N0000', False, 'Should NOT be on watch list'),
        ('ODEL.N0000', True, 'Should be on watch list'),
        ('LION.N0000', False, 'Should NOT be on watch list'),
    ]

    for symbol, expected, description in test_cases:
        result = checker.is_watch_list(symbol)
        status = "PASS" if result == expected else "FAIL"
        print(f"   {status} {symbol}: {result} ({description})")

    # Get detailed info
    print("\n4. Getting detailed info for MHDL.N0000...")
    info = checker.get_watch_list_info('MHDL.N0000')

    if info:
        print("   Info retrieved:")
        print(f"      Company: {info.get('company_name')}")
        print(f"      Date Added: {info.get('date_added')}")
        print(f"      Status: {info.get('status')}")
        print(f"      Reason: {info.get('reason')}")
    else:
        print("   No info found")

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    test_watch_list_checker()
