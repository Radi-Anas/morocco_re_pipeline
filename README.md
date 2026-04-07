# Insurance Claims Fraud Detection System

A production-grade data engineering pipeline for detecting fraudulent insurance claims. Demonstrates end-to-end data engineering skills suitable for senior data engineer roles and technical interviews.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-ready-green)
![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-green)
![Docker](https://img.shields.io/badge/Docker-ready-blue)
![CI/CD](https://img.shields.io/badge/CI/CD-GitHub_Actions-green)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        INSURANCE FRAUD PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │     ETL      │───▶│  ML Model    │───▶│  API Server  │              │
│  │  Pipeline    │    │  Training    │    │  + Dashboard │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│         │                   │                   │                      │
│         ▼                   ▼                   ▼                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    PRODUCTION INFRASTRUCTURE                     │   │
│  │  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌──────────┐           │   │
│  │  │PostgreSQL│  │ Prefect  │  │Prometheus│  │ Alembic  │           │   │
│  │  │   DB    │  │Scheduler │  │ Metrics │  │Migrations│           │   │
│  │  └─────────┘  └──────────┘  └─────────┘  └──────────┘           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Features

### Data Engineering
- **ETL Pipeline**: Extract, transform, validate, and load insurance claims data
- **Data Validation**: Schema enforcement, range checks, quality metrics
- **Database Migrations**: Alembic for version-controlled schema changes
- **Connection Pooling**: Production-ready PostgreSQL connection pool
- **Data Lineage**: Track data sources, transformations, and quality at each stage

### Machine Learning
- **Model Training**: RandomForest classifier with scikit-learn
- **Model Persistence**: Joblib for model serialization
- **Feature Engineering**: Automated encoding, derived features
- **Model Validation**: Accuracy/AUC thresholds, automated validation

### API & Services
- **REST API**: FastAPI with 6 endpoints
- **Authentication**: API key protection for sensitive endpoints
- **Rate Limiting**: Per-endpoint rate limits (slowapi)
- **Request Caching**: In-memory TTL cache for frequently accessed data
- **Prometheus Metrics**: `/metrics` endpoint for observability

### Automation & Monitoring
- **Pipeline Scheduling**: Prefect for ETL and model training automation
- **Health Monitoring**: Success rate tracking, execution time metrics
- **Alerting**: Slack/email notifications on pipeline failures
- **Structured Logging**: JSON-formatted logs for production

### DevOps
- **Docker**: Multi-service containerization (API + Dashboard + PostgreSQL)
- **Environment Config**: Development, staging, production configs
- **CI/CD**: GitHub Actions with pytest, coverage, linting

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.10+ |
| Database | PostgreSQL |
| ORM | SQLAlchemy |
| ML | scikit-learn, RandomForest |
| API | FastAPI, uvicorn, slowapi |
| Dashboard | Streamlit, Plotly |
| Scheduling | Prefect |
| Migrations | Alembic |
| Metrics | Prometheus |
| Testing | pytest, pytest-cov |
| CI/CD | GitHub Actions |

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
# Copy environment file
cp .env.development .env

# Edit .env with your database credentials
```

### 3. Run Pipeline
```bash
# Full pipeline (ETL + Model + API + Dashboard)
python main.py

# Or step by step:
python -c "from claims_etl import run_etl; run_etl()"
python fraud_model.py
```

### 4. Access Services
- **Dashboard**: http://localhost:8501
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Prometheus Metrics**: http://localhost:8000/metrics

## API Endpoints

| Endpoint | Method | Description | Auth | Rate Limit |
|----------|--------|-------------|------|------------|
| `/health` | GET | Health check | No | Unlimited |
| `/predict` | POST | Predict fraud | No | 10/min |
| `/predict/batch` | POST | Batch predictions | No | 5/min |
| `/model/metrics` | GET | Model metrics | Yes | - |
| `/predictions` | GET | Prediction history | Yes | - |
| `/stats` | GET | Fraud statistics | No | Cached |
| `/claims` | GET | List claims | No | - |
| `/metrics` | GET | Prometheus metrics | No | - |

### Example Prediction
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
├── api.py                      # FastAPI application
├── claims_etl.py               # ETL pipeline
├── fraud_model.py              # ML model training
├── dashboard.py                # Streamlit dashboard
├── main.py                     # Orchestration entry point
├── requirements.txt            # Dependencies
│
├── config/
│   ├── __init__.py             # Pydantic configuration
│   └── settings.py             # Environment-aware settings
│
├── data/
│   ├── raw/                    # Source data
│   └── clean/                  # Transformed data
│
├── models/                     # Trained models
│   ├── fraud_model.pkl
│   └── label_encoders.pkl
│
├── pipeline/                   # Pipeline utilities
│   ├── scheduler.py            # Prefect scheduling
│   ├── monitoring.py           # Health monitoring
│   ├── metrics.py              # Prometheus metrics
│   ├── lineage.py              # Data lineage tracking
│   └── logging_config.py
│
├── migrations/                  # Alembic migrations
│   └── env.py
│
├── tests/                      # Unit tests
│   ├── test_claims_etl.py
│   └── test_fraud_model.py
│
├── sql/                        # SQL scripts
│   └── optimize.sql
│
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI/CD
│
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # Container image
└── alembic.ini                 # Alembic configuration
```

## Environment Configuration

| Environment | Pool Size | Log Level | Use |
|-------------|-----------|-----------|-----|
| development | 5/10 | DEBUG | Local dev |
| staging | 10/20 | INFO | Testing |
| production | 20/40 | WARNING | Live |

```bash
# Run in different environments
ENV=development python main.py
ENV=staging python main.py
ENV=production python main.py
```

## Model Performance

| Metric | Value |
|--------|-------|
| Accuracy | 78% |
| AUC-ROC | 0.785 |
| Precision | 58% |
| Recall | 39% |

**Top Features:**
1. incident_severity
2. property_claim
3. vehicle_claim
4. policy_annual_premium
5. total_claim_amount

## Data

- **Source**: 1000 insurance claims
- **Features**: 29 columns
- **Target**: fraud_reported (Y/N)
- **Fraud Rate**: 24.7%

## Deployment

### Docker Compose
```bash
docker-compose up -d
```

### Manual
```bash
# API
uvicorn api:app --host 0.0.0.0 --port 8000

# Dashboard
streamlit run dashboard.py --server.port 8501
```

### Run Tests
```bash
pytest tests/ -v --cov=. --cov-report=html
```

## CI/CD Pipeline

GitHub Actions automatically:
1. Runs unit tests with pytest
2. Checks code coverage
3. Validates code formatting (Black)
4. Lints code (Ruff)

## Key Skills Demonstrated

- **Data Pipeline Design**: ETL, scheduling, orchestration
- **Database Engineering**: PostgreSQL, migrations, connection pooling
- **API Development**: FastAPI, authentication, rate limiting
- **ML Engineering**: Model training, validation, persistence
- **DevOps**: Docker, CI/CD, monitoring
- **Testing**: Unit tests, coverage reporting
- **Observability**: Prometheus metrics, logging, alerting
- **Configuration**: Environment-based settings, secrets management

---

**License**: MIT  
**Author**: Data Engineering Portfolio Project
