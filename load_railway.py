"""
load_railway.py
Load data to Railway PostgreSQL.

Usage:
    DATABASE_URL="postgresql://..." python load_railway.py
"""

import os
import pandas as pd
from pipeline.transform import transform, transform_scraped
from pipeline.load import load_to_postgres

TABLE = "listings"


def main():
    # Use DATABASE_URL from environment (Railway) or .env (local)
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("ERROR: Set DATABASE_URL environment variable")
        print("Example: DATABASE_URL='postgresql://...' python load_railway.py")
        return
    
    print(f"Loading data to: {db_url.split('@')[1] if '@' in db_url else 'database'}")
    
    # Load CSV
    df = pd.read_csv("data/raw/listings.csv")
    print(f"Loaded {len(df)} rows from CSV")
    
    # Transform
    clean = transform(df)
    print(f"Transformed to {len(clean)} rows")
    
    # Override DATABASE_URL temporarily for load
    import config.settings
    config.settings.DATABASE_URL = db_url
    
    # Load
    load_to_postgres(clean, TABLE)
    print(f"Loaded to '{TABLE}' table")
    print("Done!")


if __name__ == "__main__":
    main()
