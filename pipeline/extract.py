"""
pipeline/extract.py
Responsible for ONE thing only: reading the raw CSV into a DataFrame.
In the future, this is where web scraping code will live.
"""

import pandas as pd
import logging

# Set up logging so we can see what's happening when the pipeline runs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def extract_from_csv(file_path: str) -> pd.DataFrame:
    """
    Load raw listings data from a CSV file.

    Args:
        file_path: Path to the CSV file.

    Returns:
        A raw, unmodified DataFrame.

    Raises:
        FileNotFoundError: If the CSV doesn't exist at the given path.
    """
    try:
        df = pd.read_csv(file_path,encoding='latin1')
        logger.info(f"Extracted {len(df)} rows from {file_path}")
        return df

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise