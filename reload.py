"""
reload.py
Quick reload from existing scraped CSV without re-scraping.
"""

import pandas as pd
from pipeline.transform import transform_scraped
from pipeline.load import load_to_postgres
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_PATH = "debug_raw.csv"
TABLE = "listings"

df = pd.read_csv(RAW_PATH)
logger.info(f"Loaded {len(df)} rows from {RAW_PATH}")

clean = transform_scraped(df)
logger.info(f"Transformed to {len(clean)} rows")

load_to_postgres(clean, TABLE)
logger.info(f"Loaded to PostgreSQL '{TABLE}' table")
