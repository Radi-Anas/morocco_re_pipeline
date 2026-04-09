# Insurance Claims Fraud Detection System

A production-grade data pipeline that detects fraudulent insurance claims using machine learning. Built with modern data engineering practices and deployed as a complete ML platform.

---

## The Problem

Insurance fraud costs companies billions annually. When a claim comes in, investigators must manually review each one - taking hours per claim. This system helps prioritize which claims need immediate attention by scoring them for fraud probability.

**Business Impact:**
- Reduce manual review time by 70%
- Catch more fraud with consistent scoring
- Enable data-driven investigation decisions

---

## Architecture Overview

```
1. RAW DATA (CSV)
   └── Insurance claims with customer, policy, incident details

2. ETL PIPELINE (src/data/ingestion/)
   ├── Extract: Load from CSV
   ├── Transform: Clean data, handle missing values, create features
   ├── Validate: Great Expectations, schema registry
   └── Load: Insert into PostgreSQL with connection pooling

3. DATABASE (PostgreSQL)
   └── 1000 claims, indexed on fraud flag, severity, vehicle make
   └── Connection pooling (5 connections, 10 overflow)

4. ML MODEL (src/models/)
   ├── Features: 29 columns + 14 engineered features
   ├── Target: is_fraud (binary)
   └── Training: XGBoost + RandomForest ensemble

5. API (src/api/)
   ├── /predict: Single claim prediction
   ├── /stats: Aggregated fraud statistics
   ├── /health: System health check
   └── Rate limiting: 5-10 requests/minute

6. DASHBOARD (src/services/)
   ├── Overview: Fraud rate, severity charts
   ├── Claims browser with filters
   └── Model performance metrics
```

---

## Model Performance

| Metric | Value | Why It Matters |
|--------|-------|----------------|
| Accuracy | 81.5% | Overall correctness |
| AUC-ROC | 0.805 | Strong ranking ability |
| Precision | 60% | When we say fraud, 60% actually are fraud |
| Recall | 72% | We catch 72% of actual fraud |

### Feature Engineering

Created 14 domain-specific features based on fraud detection logic:

| Feature | Formula | Why It Predicts Fraud |
|---------|---------|----------------------|
| `no_witness_injury` | bodily_injuries > 0 AND witnesses = 0 | **#1 predictor!** Injuries without witnesses are suspicious |
| `claim_to_premium_ratio` | total_claim_amount / policy_annual_premium | High claim relative to premium = higher risk |
| `vehicle_property_ratio` | vehicle_claim / property_claim | Unusual damage patterns |
| `injury_ratio` | injury_claim / total_claim_amount | High injury portion may indicate exaggeration |
| `tenure_age_ratio` | months_as_customer / (age * 12) | New customer with old age = suspicious |
| `complex_no_witness` | vehicles > 1 AND witnesses = 0 | Multi-vehicle accidents without witnesses |
| `deductible_claim_ratio` | policy_deductable / total_claim_amount | Low deductible vs high claim |
| `net_capital` | capital-gains - capital-loss | Financial stress indicator |

### Why `no_witness_injury` is #1 Predictor

**Domain Logic:**
- Legitimate claims typically have witnesses (passengers, other drivers, police)
- Fraudsters prefer scenarios where no one can contradict their story
- Injuries without witnesses are 3x more likely to be fraudulent

**Business Insight:**
This feature directly answers: "Is anyone to corroborate this claim?"
If answer is NO + injuries exist -> flag for review

---

## Tech Stack

| Component | Technology |
|-----------|-------------|
| Language | Python 3.10 |
| Database | PostgreSQL |
| ML | scikit-learn, XGBoost |
| API | FastAPI |
| Dashboard | Streamlit |
| Scheduling | Prefect |
| Data Quality | Great Expectations |
| Transformations | dbt |
| Versioning | DVC |
| Testing | pytest |
| Containerization | Docker |
| CI/CD | GitHub Actions |

---

## Project Structure

```
.
├── src/
│   ├── api/                    # FastAPI application
│   │   └── app.py              # 8 endpoints, auth, rate limiting
│   ├── data/
│   │   ├── ingestion/          # ETL pipeline
│   │   │   └── claims_etl.py
│   │   ├── processing/        # dbt transformations
│   │   │   └── transformations/
│   │   └── validation/        # Great Expectations
│   │       └── data_quality/
│   ├── models/                 # ML models
│   │   ├── fraud_model.py
│   │   ├── fraud_model.pkl
│   │   └── label_encoders.pkl
│   ├── pipelines/             # Orchestration
│   │   ├── scheduler.py       # Prefect flows
│   │   ├── incremental_etl.py # Watermark-based processing
│   │   ├── schema_registry.py # Avro schemas
│   │   └── lineage.py        # Data lineage tracking
│   └── services/              # Dashboard
│       └── dashboard.py
├── configs/                   # Configuration
│   ├── settings.py
│   └── params.yaml
├── tests/                     # 56 unit tests
├── migrations/               # Alembic database migrations
├── sql/                      # SQL analysis
├── scripts/                  # Backup/restore utilities
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL (or use Docker)

### Setup

```bash
# Clone and install
git clone https://github.com/Radi-Anas/Insurance_Data_Piepline-ML.git
cd morocco_re_pipeline
pip install -r requirements.txt

# Configure database
cp .env.development .env
# Edit .env with your DB credentials

# Run ETL and start services
python main.py
```

### Access
- Dashboard: http://localhost:8501
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Database & model status |
| `/predict` | POST | Score a claim (10/min limit) |
| `/predict/batch` | POST | Batch score claims (5/min limit) |
| `/stats` | GET | Fraud statistics (cached) |
| `/claims` | GET | List claims with filters |
| `/model/metrics` | GET | Model performance (API key required) |
| `/model/train` | POST | Retrain model (API key required) |

### Example Usage

```python
import requests

response = requests.post("http://localhost:8000/predict", json={
    "months_as_customer": 12,
    "age": 35,
    "policy_state": "OH",
    "policy_annual_premium": 1200,
    "incident_type": "Single Vehicle Collision",
    "incident_severity": "Major Damage",
    "total_claim_amount": 5000,
    "auto_make": "Toyota"
})

print(response.json())
# {"prediction": 1, "fraud_probability": 0.54, "confidence": "medium", "risk_level": "MEDIUM"}
```

---

## Data Engineering Features

### ETL Pipeline
- Incremental processing with watermark-based approach
- Connection pooling (5 connections, 10 overflow)
- Database indexes for performance

### Data Quality
- Great Expectations for validation
- Schema registry with Avro
- Data lineage tracking
- Automated backup/restore

### Transformations
- dbt for SQL-based transformations
- Staging views for clean data
- Mart tables for analytics

### Versioning
- DVC for data and model versioning
- Parameters in configs/params.yaml

---

## Machine Learning Features

- Ensemble model (XGBoost + RandomForest + LogisticRegression)
- 14 engineered features
- Optimized threshold (0.35) for better recall
- Model persistence with joblib
- Feature importance analysis
- Decision logging

---

## Automation & Monitoring

### Prefect Pipeline
- Daily ETL at 2 AM
- Weekly model retraining on Sundays
- Health checks after each run

### Monitoring
- Prometheus metrics endpoint
- Health check endpoints
- Database connection monitoring

### Database
- Alembic migrations
- Connection pooling
- Automated PostgreSQL backups

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

**56 tests passing** (100%)

---

## Docker

```bash
# Start all services
docker-compose up -d

# Services:
# - postgres:5432
# - api:8000
# - dashboard:8501
```

### Manual Docker Build

```bash
# Build image
docker build -t fraud-detection .

# Run container
docker run -p 8000:8000 -p 8501:8501 fraud-detection
```

---

## Environment Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `ENV` | Environment (development/staging/production) | development |
| `DATABASE_URL` | PostgreSQL connection string | postgresql://... |
| `API_KEY` | API key for protected endpoints | (none) |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING) | INFO |

### Production Checklist

- [ ] Set `ENV=production` in environment
- [ ] Use strong `API_KEY` (generate with `openssl rand -hex 32`)
- [ ] Configure `DATABASE_URL` to production PostgreSQL
- [ ] Set `LOG_LEVEL=WARNING` to reduce log volume
- [ ] Use reverse proxy (nginx) for SSL termination
- [ ] Set up monitoring (Prometheus/Grafana)
- [ ] Configure automated backups

---

## License

MIT