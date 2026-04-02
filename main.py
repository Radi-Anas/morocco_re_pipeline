"""
main.py
Pipeline entry point - ETL orchestration.

Runs: extract → transform → validate → load

Usage:
    python main.py

Configuration:
    Set USE_SCRAPER = True to scrape live from Avito.ma
    Set USE_SCRAPER = False to use existing CSV data
"""

import logging
import subprocess
import sys
from pipeline.extract import scrape_avito, extract_from_csv
from pipeline.transform import transform, transform_scraped, save_clean_csv
from pipeline.validate import validate
from pipeline.load import load_to_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

RAW_DATA_PATH = "data/raw/listings.csv"
CLEAN_DATA_PATH = "data/clean/listings_clean.csv"
TABLE_NAME = "listings"
USE_SCRAPER = False


def run_pipeline():
    """Execute the ETL pipeline."""
    logger.info("=== Pipeline started ===")

    # Extract
    if USE_SCRAPER:
        raw_df = scrape_avito(max_pages=3)
        if raw_df.empty:
            logger.error("Scraping returned no data. Aborting.")
            return
        clean_df = transform_scraped(raw_df)
    else:
        raw_df = extract_from_csv(RAW_DATA_PATH)
        clean_df = transform(raw_df)

    if clean_df.empty:
        logger.error("No clean data after transformation. Aborting.")
        return

    save_clean_csv(clean_df, CLEAN_DATA_PATH)

    # Validate
    min_rows = 20 if USE_SCRAPER else 5
    if not validate(clean_df, min_rows=min_rows, strict_url=False):
        logger.error("Validation failed. Data NOT loaded into PostgreSQL.")
        return

    # Load
    load_to_postgres(clean_df, TABLE_NAME)
    logger.info("=== Pipeline finished successfully ===")


def main():
    """Main entry point with dashboard auto-start."""
    run_pipeline()

    print("\n" + "=" * 50)
    print("Pipeline complete! Starting dashboard...")
    print("=" * 50)

    subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "dashboard.py", "--server.port", "8501"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print("Dashboard: http://localhost:8501")


if __name__ == "__main__":
    main()
