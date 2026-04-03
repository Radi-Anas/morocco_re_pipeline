"""
config/settings.py
Loads database credentials from environment or .env file.
Railway provides DATABASE_URL directly — no .env needed.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Use DATABASE_URL if set (Railway), otherwise use individual vars (local)
if os.getenv("DATABASE_URL"):
    DATABASE_URL = os.getenv("DATABASE_URL")
else:
    DB_CONFIG = {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "morocco_re"),
        "user":     os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }
    DATABASE_URL = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
