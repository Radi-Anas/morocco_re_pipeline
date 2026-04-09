"""
pipeline/logging_config.py
Logging configuration using loguru for structured JSON logging.
"""

import sys
import os
from loguru import logger

def setup_logging(log_file: str = None):
    """Setup logging with loguru for structured JSON output."""
    if log_file is None:
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "pipeline.log")
    
    # Remove default handler
    logger.remove()
    
    # Add console handler with simple format
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    
    # Add file handler with JSON-like format
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        serialize=True  # JSON format
    )
    
    return logger
