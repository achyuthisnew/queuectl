"""Utility functions for QueueCTL."""

import logging
import sys
from datetime import datetime
from typing import Optional


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Configure and return a logger for the application.
    
    Args:
        level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("queuectl")
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def get_timestamp() -> str:
    """
    Get current timestamp in ISO format.
    
    Returns:
        ISO formatted timestamp string
    """
    return datetime.utcnow().isoformat()


def parse_timestamp(timestamp: str) -> datetime:
    """
    Parse ISO formatted timestamp string.
    
    Args:
        timestamp: ISO formatted timestamp string
        
    Returns:
        datetime object
    """
    return datetime.fromisoformat(timestamp)


def calculate_backoff_delay(attempts: int, base: float = 2.0) -> float:
    """
    Calculate exponential backoff delay.
    
    Args:
        attempts: Number of retry attempts
        base: Base for exponential calculation
        
    Returns:
        Delay in seconds
    """
    return base ** attempts


logger = setup_logging()
