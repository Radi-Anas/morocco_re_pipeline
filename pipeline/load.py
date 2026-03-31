"""
pipeline/load.py
Loads the clean DataFrame into PostgreSQL.
Uses SQLAlchemy (the professional way) — not raw SQL strings.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import logging
from config.settings import DATABASE_URL

logger = logging.getLogger(__name__)


def get_engine():
    """
    Create and return a SQLAlchemy engine.
    The engine is the connection factory — it doesn't open a connection yet.
    """
    try:
        engine = create_engine(DATABASE_URL)
        # Test the connection immediately so we fail fast with a clear error
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful.")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def load_to_postgres(df: pd.DataFrame, table_name: str = "listings") -> None:
    """
    Write the clean DataFrame to a PostgreSQL table.

    Uses if_exists='replace' during development so you can re-run freely.
    In production you'd switch to 'append' and handle deduplication separately.

    Args:
        df:         The clean DataFrame from transform.py
        table_name: Target table name in PostgreSQL (default: 'listings')
    """
    engine = get_engine()

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",   # Drop and recreate table on each run
            index=False,           # Don't write the DataFrame index as a column
            method="multi",        # Batch inserts — faster than row-by-row
        )
        logger.info(f"Loaded {len(df)} rows into table '{table_name}'.")

    except Exception as e:
        logger.error(f"Failed to load data into PostgreSQL: {e}")
        raise

    finally:
        engine.dispose()  # Always close the connection pool when done