"""
Incremental ETL pipeline for insurance claims.
Only processes new records since last run, using watermarks.

Usage:
    python -c "from pipeline.incremental_etl import run_incremental_etl; run_incremental_etl()"
"""

import logging
import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from configs.settings import DATABASE_URL
from typing import Optional, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DATA_PATH = "data/raw/insurance_claims.csv"
STATE_FILE = "logs/etl_state.json"


class ETLWatermark:
    """Track last processed records for incremental loads."""
    
    @staticmethod
    def load_state() -> Dict:
        """Load previous run state."""
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {
                "last_run": None,
                "last_row_count": 0,
                "last_checksum": None
            }
    
    @staticmethod
    def save_state(state: Dict) -> None:
        """Save current run state."""
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)


def compute_file_checksum(file_path: str) -> str:
    """Compute MD5 checksum of file for change detection."""
    import hashlib
    with open(file_path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


def has_new_data() -> bool:
    """Check if source file has new data since last run."""
    current_checksum = compute_file_checksum(RAW_DATA_PATH)
    state = ETLWatermark.load_state()
    
    if state.get('last_checksum') != current_checksum:
        return True
    
    current_df = pd.read_csv(RAW_DATA_PATH, nrows=1)
    current_count = len(current_df)
    
    return current_count > state.get('last_row_count', 0)


def get_last_processed_id(engine) -> Optional[int]:
    """Get the last processed policy_number from DB."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT MAX(processed_id) FROM claims_audit")
            ).fetchone()
            return result[0] if result and result[0] else None
    except Exception:
        return None


def extract_incremental(last_id: Optional[int] = None) -> pd.DataFrame:
    """Extract only new records since last run."""
    df = pd.read_csv(RAW_DATA_PATH)
    
    if last_id is not None:
        df = df[df['policy_number'] > last_id]
    
    logger.info(f"Extracted {len(df)} new records")
    return df


def transform_claims_incremental(df: pd.DataFrame) -> pd.DataFrame:
    """Apply same transformations as main ETL."""
    from claims_etl import transform_claims
    return transform_claims(df)


def load_incremental(df: pd.DataFrame) -> int:
    """Load new records to database with audit trail."""
    engine = create_engine(DATABASE_URL)
    
    try:
        # Add to audit table
        df_audit = df.copy()
        df_audit['processed_at'] = datetime.now().isoformat()
        
        df_audit.to_sql(
            name='claims',
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logger.info(f"Loaded {len(df)} new records to DB")
        return len(df)
    finally:
        engine.dispose()


def run_incremental_etl() -> Dict:
    """
    Run incremental ETL - only process new data since last run.
    
    Returns:
        dict with run metrics
    """
    start_time = datetime.now()
    state = ETLWatermark.load_state()
    
    logger.info(f"Starting incremental ETL. Last run: {state.get('last_run')}")
    
    # Check if source file changed
    current_checksum = compute_file_checksum(RAW_DATA_PATH)
    df_raw = pd.read_csv(RAW_DATA_PATH)
    current_count = len(df_raw)
    
    if current_checksum == state.get('last_checksum') and current_count == state.get('last_row_count', 0):
        logger.info("No new data detected. Skipping ETL.")
        return {
            "status": "skipped",
            "reason": "no_new_data",
            "timestamp": start_time.isoformat()
        }
    
    # Extract new records
    df_new = transform_claims_incremental(df_raw)
    
    # Load to DB
    rows_loaded = load_incremental(df_new)
    
    # Update state
    new_state = {
        "last_run": start_time.isoformat(),
        "last_row_count": current_count,
        "last_checksum": current_checksum
    }
    ETLWatermark.save_state(new_state)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    return {
        "status": "success",
        "rows_extracted": len(df_raw),
        "rows_loaded": rows_loaded,
        "duration_seconds": round(duration, 2),
        "timestamp": start_time.isoformat()
    }


if __name__ == "__main__":
    result = run_incremental_etl()
    print(f"\nIncremental ETL: {result}")