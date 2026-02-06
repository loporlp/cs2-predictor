"""
Logging infrastructure for CS2 Predictor.

Provides hierarchical loggers with configurable output formats and levels.
Supports both console and file output.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


# Global logging configuration
LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LEVEL = logging.INFO


def setup_logger(
    name: str,
    level: int = DEFAULT_LEVEL,
    log_file: Optional[Path] = None,
    console: bool = True
) -> logging.Logger:
    """
    Set up a logger with the specified configuration.

    Args:
        name: Logger name (typically __name__ of the module)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
        console: Whether to log to console

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with default configuration.

    Args:
        name: Logger name (typically __name__ of the module)

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)

    # If logger hasn't been set up yet, use default configuration
    if not logger.handlers:
        logger.setLevel(DEFAULT_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(DEFAULT_LEVEL)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


# Module-level loggers for different components
def get_fetch_logger() -> logging.Logger:
    """Get logger for fetch modules."""
    return get_logger("cs2predictor.fetch")


def get_parse_logger() -> logging.Logger:
    """Get logger for parse modules."""
    return get_logger("cs2predictor.parse")


def get_pipeline_logger() -> logging.Logger:
    """Get logger for pipeline modules."""
    return get_logger("cs2predictor.pipeline")


def get_utils_logger() -> logging.Logger:
    """Get logger for utility modules."""
    return get_logger("cs2predictor.utils")
