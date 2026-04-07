# Insurance Claims Fraud Detection System

A production-grade ML pipeline for detecting fraudulent insurance claims. Demonstrates end-to-end data engineering and machine learning skills suitable for portfolio and job interviews.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![ML](https://img.shields.io/badge/Machine-Learning-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-ready-green)
![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-green)

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  ETL Pipeline   │ →  │   ML Model      │ →  │  API & Dashboard│
│  claims_etl.py  │    │  fraud_model.py │    │  api.py/dashboard│
└─────────────────┘    └─────────────────┘    └─────────────────┘
        ↓                     ↓                       ↓
   PostgreSQL            PostgreSQL            HTTP Endpoints
```

## Features

- **ETL Pipeline**: Extract, transform, and load insurance claims data
- **ML Model**: RandomForest classifier for fraud detection
- **REST API**: Real-time fraud prediction endpoints
- **Dashboard**: Interactive Streamlit analytics

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.10+ |
| Database | PostgreSQL |
| ML | scikit-learn, RandomForest |
| API | FastAPI, uvicorn |
| Dashboard | Streamlit, Plotly |

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Setup Database
```bash
# Configure DATABASE_URL in .env
DATABASE_URL=postgresql://user:pass@localhost:5432/dbname
```

### 3. Run Pipeline
```bash
# ETL + Train Model + Start Services
python main.py

# Or step by step:
python -c "from claims_etl import run_etl; run_etl()"
python fraud_model.py
```

### 4. Access Services
- **Dashboard**: http://localhost:8501
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/predict` | POST | Predict fraud for a claim |
| `/stats` | GET | Fraud statistics |
| `/claims` | GET | List claims |

### Example Prediction Request
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "months_as_customer": 12,
    "age": 35,
    "policy_state": "OH",
    "policy_csl": "250/500",
    "policy_annual_premium": 1200,
    "incident_type": "Single Vehicle Collision",
    "incident_severity": "Major Damage",
    "total_claim_amount": 5000,
    "auto_make": "Toyota"
  }'
```

## Project Structure

```
morocco_re_pipeline/
├── api.py              # FastAPI endpoints
├── claims_etl.py       # ETL pipeline
├── dashboard.py        # Streamlit dashboard
├── fraud_model.py      # ML model training
├── main.py             # Orchestration
├── requirements.txt    # Dependencies
│
├── config/
│   └── settings.py     # Database configuration
│
├── data/
│   ├── raw/            # Source data
│   └── clean/           # Transformed data
│
├── models/             # Trained ML models
├── pipeline/           # Pipeline utilities
└── tests/              # Unit tests
```

## Model Performance

| Metric | Value |
|--------|-------|
| Accuracy | 78% |
| AUC-ROC | 0.785 |
| Precision (Fraud) | 58% |
| Recall (Fraud) | 39% |

**Top Features:**
1. incident_severity
2. property_claim
3. vehicle_claim
4. policy_annual_premium
5. total_claim_amount

## Data

- **Source**: 1000 insurance claims
- **Features**: 29 columns (customer, policy, incident, claim details)
- **Target**: fraud_reported (Y/N)
- **Fraud Rate**: 24.7% (247 fraud / 753 legitimate)

## Deployment

### Docker
```bash
docker build -t fraud-detection .
docker run -p 8000:8000 -p 8501:8501 fraud-detection
```

### Manual
```bash
# Backend
uvicorn api:app --host 0.0.0.0 --port 8000

# Frontend  
streamlit run dashboard.py --server.port 8501
```

## License

MIT
