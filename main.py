"""
main.py
Entry point for the pipeline.
Runs extract → transform → load in sequence.
This is the only file you need to run: `python main.py`
"""

import logging
from pipeline.extract import extract_from_csv
from pipeline.transform import transform, save_clean_csv
from pipeline.load import load_to_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# File paths — change these if your files live elsewhere
RAW_DATA_PATH   = "data/raw/listings.csv"
CLEAN_DATA_PATH = "data/clean/listings_clean.csv"
TABLE_NAME      = "listings"


def run_pipeline():
    logger.info("=== Pipeline started ===")

    # Step 1: Extract
    raw_df = extract_from_csv(RAW_DATA_PATH)

    # Step 2: Transform
    clean_df = transform(raw_df)
    save_clean_csv(clean_df, CLEAN_DATA_PATH)  # Checkpoint for debugging

    # Step 3: Load
    load_to_postgres(clean_df, TABLE_NAME)

    logger.info("=== Pipeline finished successfully ===")


if __name__ == "__main__":
    run_pipeline()