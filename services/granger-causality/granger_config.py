#!/usr/bin/env python3
"""
Tier 1 Granger Test Configuration
Investment OS v6.0 - Test specifications and parameters

Defines which Granger tests to run for which stocks.
Organizes tests by category: primary, sector-specific, and macro.

Author: Investment OS Team
Date: February 4, 2026
"""

from typing import Dict, List

# ============================================================================
# PRIMARY TESTS (Run for ALL stocks)
# ============================================================================

PRIMARY_TESTS = [
    # Test 1: Volume predicts next-day return
    {
        'predictor': 'volume',
        'target': 'next_day_return',
        'lags': [1, 2, 3, 5, 10],
        'description': 'Does volume spike predict price movement?'
    },
    
    # Test 2: Volume change predicts next-day return
    {
        'predictor': 'volume_change',
        'target': 'next_day_return',
        'lags': [1, 2, 3, 5, 10],
        'description': 'Does volume change predict price movement?'
    },
    
    # Test 3: Returns predict volume (reverse causality)
    {
        'predictor': 'returns',
        'target': 'volume_change',
        'lags': [1, 2, 3, 5, 10],
        'description': 'Does price movement attract volume?'
    },
    
    # Test 4: Volatility predicts next-day return
    {
        'predictor': 'volatility',
        'target': 'next_day_return',
        'lags': [1, 2, 5, 10],
        'description': 'Does volatility expansion predict direction?'
    },
]


# ============================================================================
# SECTOR-SPECIFIC TESTS
# ============================================================================

SECTOR_SPECIFIC_TESTS = {
    # Tea Plantations: Weather signals
    'tea': {
        'symbols': [
            'CTC.N0000',    # Ceylon Tea Company
            'HAYA.N0000',   # Haycarb (plantation division)
            'KEL.N0000',    # Kelani Tyres (plantation)
            'WAT.N0000',    # Watawala Plantations
            'AGAL.N0000',   # Agalawatte Plantations
        ],
        'tests': [
            {
                'predictor': 'weather_signal',
                'target': 'next_day_return',
                'lags': [7, 14, 30, 60, 90],
                'description': 'Does weather (rainfall) predict tea stock returns?',
                'signal_type': 'weather'
            }
        ]
    },
    
    # Banking: Interest rate sensitivity
    'banking': {
        'symbols': [
            'LOLC.N0000',     # LOLC Holdings
            'COMB.N0000',     # Commercial Bank
            'HNB.N0000',      # Hatton National Bank
            'SAMPATH.N0000',  # Sampath Bank
            'NDB.N0000',      # NDB Bank
            'DFCC.N0000',     # DFCC Bank
            'LOFC.N0000',     # LOLC Finance
            'DIPD.N0000',     # Dipped Products (LOLC group, finance heavy)
        ],
        'tests': [
            {
                'predictor': 'awpr_change',
                'target': 'next_day_return',
                'lags': [1, 5, 10, 20],
                'description': 'Do interest rate changes predict bank stock returns?',
                'signal_type': 'macro'
            }
        ]
    },
    
    # Export-focused: FX sensitivity
    'exports': {
        'symbols': [
            'EXPO.N0000',   # Expolanka Holdings
            'TYRE.N0000',   # Kelani Tyres (export heavy)
            'LOLC.N0000',   # LOLC (export finance)
        ],
        'tests': [
            {
                'predictor': 'usd_lkr_change',
                'target': 'next_day_return',
                'lags': [1, 5, 10, 20],
                'description': 'Does FX movement predict export stock returns?',
                'signal_type': 'macro'
            }
        ]
    },
}


# ============================================================================
# MACRO TESTS (Run for ALL stocks)
# ============================================================================

MACRO_TESTS = [
    # USD/LKR exchange rate
    {
        'predictor': 'usd_lkr_change',
        'target': 'next_day_return',
        'lags': [10, 20, 30, 60],
        'description': 'Does FX movement predict stock returns (all stocks)?',
        'signal_type': 'macro'
    },
    
    # Interest rates (AWPR)
    {
        'predictor': 'awpr_change',
        'target': 'next_day_return',
        'lags': [10, 20, 30, 60],
        'description': 'Do interest rate changes predict stock returns (all stocks)?',
        'signal_type': 'macro'
    },
    
    # Money supply (M2)
    {
        'predictor': 'm2_change',
        'target': 'next_day_return',
        'lags': [10, 20, 30, 60],
        'description': 'Does M2 growth predict stock returns (all stocks)?',
        'signal_type': 'macro'
    },
]


# ============================================================================
# TEST STOCKS (For validation phase)
# ============================================================================

TEST_STOCKS = [
    'CTC.N0000',    # Ceylon Tea Company (tea sector)
    'LION.N0000',   # Lion Brewery (consumer)
    'LOLC.N0000',   # LOLC Holdings (banking)
    'LOFC.N0000',   # LOLC Finance (finance)
    'JKH.N0000',    # John Keells Holdings (conglomerate)
]


# ============================================================================
# STATISTICAL PARAMETERS
# ============================================================================

STATISTICAL_CONFIG = {
    # Data requirements
    'min_sample_size': 100,          # Minimum observations for valid test
    'optimal_sample_size': 365,      # Optimal historical days
    'max_sample_size': 1095,         # Maximum historical days (3 years)
    'min_data_completeness': 0.80,   # 80% data availability required
    
    # Statistical thresholds
    'significance_level': 0.05,      # p < 0.05 for significance
    'expected_significance_rate': 0.15,  # Expect 15-20% tests significant
    
    # Lag selection
    'max_lag_short_term': 10,        # Volume, price momentum
    'max_lag_medium_term': 30,       # Weather, credit card spending
    'max_lag_long_term': 90,         # Macro indicators
}


# ============================================================================
# V6.0 INTEGRATION THRESHOLDS
# ============================================================================

V6_CONFIDENCE_CONFIG = {
    # Minimum requirements for v6.0 pattern validation
    'min_significant_relationships': 3,      # Need at least 3 significant tests
    'min_confidence_score': 0.70,            # 70% minimum confidence
    'required_volume_causality': True,       # Must have volume → return causality
    
    # Confidence score weights
    'significance_rate_weight': 0.40,        # 40% from overall sig rate
    'strongest_predictor_weight': 0.30,      # 30% from best p-value
    'pattern_strength_weight': 0.30,         # 30% from v5.0 pattern quality
}


# ============================================================================
# EXECUTION PARAMETERS
# ============================================================================

EXECUTION_CONFIG = {
    # Processing parameters
    'batch_size': 50,                # Process stocks in batches
    'progress_interval': 10,         # Log progress every N stocks
    'retry_attempts': 3,             # Retry failed stocks
    'timeout_seconds': 300,          # 5 minutes per stock maximum
    
    # Output parameters
    'save_json_backup': True,        # Save JSON backup of results
    'save_csv_report': True,         # Save CSV report for analysis
    'generate_summary_txt': True,    # Generate human-readable summary
}


# ============================================================================
# EMPIRE STOCKS (For special tracking)
# ============================================================================

EMPIRE_STOCKS = {
    'empire_1_dhammika': [
        'SAMP.N0000', 'PABC.N0000', 'BLUE.N0000', 'YORK.N0000',
        'SLND.N0000', 'LION.N0000', 'LGL.N0000', 'SHOT.N0000',
        'TILE.N0000', 'TYRE.N0000', 'ODEL.N0000', 'BPPL.N0000'
    ],
    
    'empire_2_lolc_browns': [
        'LOLC.N0000', 'LOFC.N0000', 'COMB.N0000', 'BFL.N0000',
        'HNBF.N0000', 'DIPD.N0000', 'LFIN.N0000', 'PLC.N0000'
    ],
    
    'empire_3_jayawardena': [
        'AHPL.N0000', 'ASPH.N0000', 'AEL.N0000', 'KVAL.N0000',
        'PALM.N0000'
    ],
}


def get_all_empire_stocks() -> List[str]:
    """Get list of all empire stocks."""
    all_empire = []
    for empire_name, stocks in EMPIRE_STOCKS.items():
        all_empire.extend(stocks)
    return list(set(all_empire))  # Remove duplicates


def get_tests_for_stock(symbol: str) -> List[Dict]:
    """
    Get all applicable tests for a given stock.
    
    Args:
        symbol: Stock symbol (e.g., 'LOLC.N0000')
    
    Returns:
        List of test configuration dicts
    """
    tests = []
    
    # Add primary tests (all stocks)
    tests.extend(PRIMARY_TESTS)
    
    # Add sector-specific tests
    for sector_name, sector_config in SECTOR_SPECIFIC_TESTS.items():
        if symbol in sector_config['symbols']:
            tests.extend(sector_config['tests'])
    
    # Add macro tests (all stocks)
    tests.extend(MACRO_TESTS)
    
    return tests


def is_empire_stock(symbol: str) -> bool:
    """Check if stock is an empire stock."""
    return symbol in get_all_empire_stocks()


def get_empire_name(symbol: str) -> str:
    """Get empire name for a stock, or 'None' if not an empire stock."""
    for empire_name, stocks in EMPIRE_STOCKS.items():
        if symbol in stocks:
            return empire_name
    return 'None'


# Export all configuration
__all__ = [
    'PRIMARY_TESTS',
    'SECTOR_SPECIFIC_TESTS',
    'MACRO_TESTS',
    'TEST_STOCKS',
    'STATISTICAL_CONFIG',
    'V6_CONFIDENCE_CONFIG',
    'EXECUTION_CONFIG',
    'EMPIRE_STOCKS',
    'get_all_empire_stocks',
    'get_tests_for_stock',
    'is_empire_stock',
    'get_empire_name'
]
