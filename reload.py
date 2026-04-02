"""
reload.py
Quick reload from existing CSV without re-scraping.

Usage:
    python reload.py
"""

import pandas as pd
from pipeline.transform import transform_scraped
from pipeline.load import load_to_postgres
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_PATH = "data/raw/listings.csv"
TABLE = "listings"


def main():
    """Load data from CSV and reload database."""
    df = pd.read_csv(RAW_PATH)
    logger.info(f"Loaded {len(df)} rows from {RAW_PATH}")

    clean = transform_scraped(df)
    logger.info(f"Transformed to {len(clean)} rows")

    load_to_postgres(clean, TABLE)
    logger.info(f"Loaded to PostgreSQL '{TABLE}' table")
    logger.info("Done! Refresh the dashboard at http://localhost:8501")


if __name__ == "__main__":
    main()
