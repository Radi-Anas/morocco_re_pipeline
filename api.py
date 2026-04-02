"""
api.py
FastAPI endpoint for serving Moroccan real estate listings.

Run:
    uvicorn api:app --reload

Endpoints:
    GET /                    - API info
    GET /listings            - List all listings
    GET /listings/{id}       - Get single listing
    GET /stats               - Price statistics by city
    GET /health              - Health check
"""

from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from sqlalchemy import create_engine, text
import logging

from config.settings import DATABASE_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Morocco RE API",
    description="API for Moroccan Real Estate listings from Avito.ma",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_connection():
    """Create database connection."""
    try:
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database unavailable")


@app.get("/")
def root():
    """API root endpoint."""
    return {
        "name": "Morocco Real Estate API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "GET /listings",
            "GET /listings/{id}",
            "GET /stats",
            "GET /health",
        ],
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}


@app.get("/listings")
def get_listings(
    city: Optional[str] = Query(None, description="Filter by city"),
    min_price: Optional[float] = Query(None, description="Minimum price"),
    max_price: Optional[float] = Query(None, description="Maximum price"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    offset: int = Query(0, ge=0, description="Skip results"),
) -> dict:
    """
    Get listings with optional filters.
    
    Example:
        GET /listings?city=Casablanca&min_price=500000&max_price=2000000
    """
    try:
        engine = get_db_connection()
        query = "SELECT * FROM listings WHERE 1=1"
        params = {}
        
        if city:
            query += " AND city = :city"
            params["city"] = city.title()
        
        if min_price is not None:
            query += " AND price >= :min_price"
            params["min_price"] = min_price
        
        if max_price is not None:
            query += " AND price <= :max_price"
            params["max_price"] = max_price
        
        query += " ORDER BY price DESC"
        query += f" LIMIT {limit} OFFSET {offset}"
        
        df = pd.read_sql_query(text(query), engine, params=params)
        engine.dispose()
        
        return {
            "count": len(df),
            "limit": limit,
            "offset": offset,
            "data": df.to_dict(orient="records"),
        }
    
    except Exception as e:
        logger.error(f"Error fetching listings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/listings/{listing_id}")
def get_listing(listing_id: int) -> dict:
    """Get a single listing by ID."""
    try:
        engine = get_db_connection()
        df = pd.read_sql_query(
            text("SELECT * FROM listings WHERE id = :id"),
            engine,
            params={"id": listing_id},
        )
        engine.dispose()
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Listing not found")
        
        return df.to_dict(orient="records")[0]
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching listing {listing_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats(city: Optional[str] = Query(None, description="Filter by city")) -> dict:
    """
    Get price statistics.
    
    Example:
        GET /stats?city=Casablanca
    """
    try:
        engine = get_db_connection()
        
        base_query = """
            SELECT 
                city,
                COUNT(*) as listing_count,
                ROUND(AVG(price), 0) as avg_price,
                ROUND(MIN(price), 0) as min_price,
                ROUND(MAX(price), 0) as max_price,
                ROUND(AVG(price_per_m2), 0) as avg_price_per_m2
            FROM listings
        """
        
        if city:
            query = base_query + " WHERE city = :city GROUP BY city"
            params = {"city": city.title()}
        else:
            query = base_query + " GROUP BY city ORDER BY avg_price DESC"
            params = {}
        
        df = pd.read_sql_query(text(query), engine, params=params)
        engine.dispose()
        
        return {
            "count": len(df),
            "data": df.to_dict(orient="records"),
        }
    
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cities")
def get_cities() -> dict:
    """Get list of all available cities."""
    try:
        engine = get_db_connection()
        df = pd.read_sql_query(
            text("SELECT DISTINCT city FROM listings ORDER BY city"),
            engine,
        )
        engine.dispose()
        
        return {
            "count": len(df),
            "cities": df["city"].tolist(),
        }
    
    except Exception as e:
        logger.error(f"Error fetching cities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
