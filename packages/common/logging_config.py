#!/usr/bin/env python3
"""
Investment OS - Logging Configuration Module
==============================================
Standardized logging across all Investment OS services.

Current State (what this replaces):
    - dimension1-7 scorers: logging.basicConfig(level=INFO, format='%(asctime)s...')
    - manipulation_detector: Custom WARNING level + logger suppression
    - tier1_granger: File + console handlers with hardcoded path
    - calendar_signal_monitor: No logging (uses print())
    - send_v5_email: No logging (uses print())

Target State:
    - Consistent format across all services
    - Console + optional file output
    - Noisy libraries suppressed by default
    - Service name in every log line

Usage:
    from common.logging_config import setup_logging

    # Basic setup (console only)
    logger = setup_logging('calendar-signals')
    logger.info("Signal generated")

    # With file output
    logger = setup_logging('manipulation-detector', log_to_file=True)
    logger.info("Scan complete")  # Goes to console AND v5_logs/

Replaces:
    - 7x identical logging.basicConfig() blocks in dimension scorers
    - Custom logging setup in manipulation_detector_v5_0.py (lines 40-52)
    - Hardcoded log file paths in tier1_granger_per_stock_v5.py (line 74-80)
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional

from common.config import get_config

# Track which services have been configured (avoid duplicate handlers)
_configured_loggers: set = set()


def setup_logging(
    service_name: str,
    level: int = logging.INFO,
    log_to_file: bool = False,
    suppress_noisy: bool = True
) -> logging.Logger:
    """
    Configure standardized logging for a service.

    Creates a logger with consistent formatting and optional file output.
    Safe to call multiple times — subsequent calls return existing logger.

    Format: "2026-02-09 19:00:01 | calendar-signals | INFO | Signal generated"

    Args:
        service_name: Service identifier (e.g., 'calendar-signals',
                      'manipulation-detector', 'scoring-7d')
        level: Logging level (default: INFO)
        log_to_file: Also write to LOG_DIR/{service_name}_{date}.log
        suppress_noisy: Suppress httpx, urllib3, supabase chatter
                        (default: True, matches manipulation_detector pattern)

    Returns:
        Configured logger instance

    Examples:
        >>> logger = setup_logging('scoring-7d')
        >>> logger.info("Dimension 1 complete")
        2026-02-09 18:00:01 | scoring-7d | INFO | Dimension 1 complete

        >>> logger = setup_logging('manipulation-detector', log_to_file=True)
        >>> logger.warning("Low confidence pattern")
        # Output to both console and v5_logs/manipulation-detector_2026-02-09.log
    """
    # Return existing logger if already configured
    if service_name in _configured_loggers:
        return logging.getLogger(service_name)

    logger = logging.getLogger(service_name)
    logger.setLevel(level)

    # Prevent propagation to root logger (avoids duplicate output)
    logger.propagate = False

    # Standardized format
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler (always)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_to_file:
        config = get_config()
        date_str = datetime.now().strftime('%Y%m%d')
        log_file = os.path.join(
            config.LOG_DIR,
            f'{service_name}_{date_str}.log'
        )

        # Ensure log directory exists
        os.makedirs(config.LOG_DIR, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.debug(f"Logging to file: {log_file}")

    # Suppress noisy libraries (matches manipulation_detector_v5_0.py pattern)
    if suppress_noisy:
        for noisy_logger in ['httpx', 'urllib3', 'supabase', 'httpcore',
                             'hpack', 'h2', 'postgrest']:
            logging.getLogger(noisy_logger).setLevel(logging.WARNING)

    _configured_loggers.add(service_name)

    return logger
