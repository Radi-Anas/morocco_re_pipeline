"""
api.py
FastAPI endpoint for Insurance Claims Fraud Detection.

Run:
    uvicorn api:app --reload

Endpoints:
    GET  /                    - API info
    GET  /health              - Health check
    POST /predict             - Predict fraud for a claim
    GET  /stats                - Fraud statistics
    GET  /claims               - List claims
"""

from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from sqlalchemy import create_engine, text
import logging

from config.settings import DATABASE_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Insurance Claims Fraud API",
    description="ML-powered fraud detection for insurance claims",
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


def get_model():
    """Load fraud detection model."""
    try:
        from fraud_model import load_model
        return load_model()
    except Exception as e:
        logger.error(f"Model load failed: {e}")
        return None


@app.get("/")
def root():
    """API root endpoint."""
    return {
        "name": "Insurance Claims Fraud API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "GET /health",
            "POST /predict",
            "GET /stats",
            "GET /claims",
        ],
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Check model
        model = get_model()
        model_status = "loaded" if model else "not loaded"
        
        return {"status": "healthy", "database": "connected", "model": model_status}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/predict")
def predict_fraud(claim_data: dict) -> dict:
    """
    Predict fraud for an insurance claim.
    
    Example payload:
    {
        "months_as_customer": 12,
        "age": 35,
        "policy_state": "OH",
        "policy_csl": "250/500",
        "policy_deductable": 500,
        "policy_annual_premium": 1200,
        "insured_sex": "M",
        "insured_education_level": "BS",
        "insured_occupation": "Tech",
        "incident_type": "Single Vehicle Collision",
        "incident_severity": "Major Damage",
        "total_claim_amount": 5000,
        "vehicle_claim": 3000,
        "property_claim": 1500,
        "injury_claim": 500,
        "auto_make": "Toyota"
    }
    """
    try:
        model_data = get_model()
        if model_data is None:
            raise HTTPException(status_code=503, detail="Model not available")
        
        from fraud_model import predict_fraud
        result = predict_fraud(claim_data, model_data)
        
        return {
            "prediction": result["is_fraud"],
            "fraud_probability": result["fraud_probability"],
            "confidence": result["confidence"],
            "risk_level": "HIGH" if result["fraud_probability"] > 0.7 else "MEDIUM" if result["fraud_probability"] > 0.3 else "LOW"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats() -> dict:
    """Get fraud statistics."""
    try:
        engine = get_db_connection()
        
        df = pd.read_sql_query(text("""
            SELECT 
                COUNT(*) as total_claims,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
                ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) as fraud_rate,
                ROUND(AVG(total_claim_amount::numeric), 2) as avg_claim_amount,
                ROUND(AVG(policy_annual_premium::numeric), 2) as avg_premium
            FROM claims
        """), engine)
        
        engine.dispose()
        
        return {
            "total_claims": int(df.iloc[0]["total_claims"]),
            "fraud_count": int(df.iloc[0]["fraud_count"]),
            "fraud_rate_percent": float(df.iloc[0]["fraud_rate"]),
            "avg_claim_amount": float(df.iloc[0]["avg_claim_amount"]),
            "avg_premium": float(df.iloc[0]["avg_premium"]),
        }
    
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/claims")
def get_claims(
    fraud_only: bool = Query(False, description="Filter to fraud claims only"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Get claims with optional filters."""
    try:
        engine = get_db_connection()
        query = "SELECT * FROM claims"
        
        if fraud_only:
            query += " WHERE is_fraud = 1"
        
        query += f" LIMIT {limit} OFFSET {offset}"
        
        df = pd.read_sql_query(text(query), engine)
        engine.dispose()
        
        return {
            "count": len(df),
            "limit": limit,
            "offset": offset,
            "data": df.to_dict(orient="records"),
        }
    
    except Exception as e:
        logger.error(f"Error fetching claims: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
