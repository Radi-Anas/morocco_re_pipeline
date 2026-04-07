"""
pipeline/logging_config.py
Logging configuration for the pipeline.
"""

import logging
import os
from datetime import datetime


def setup_logging(log_file: str = None):
    """Setup logging configuration."""
    if log_file is None:
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "pipeline.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)
