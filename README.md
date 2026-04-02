# Morocco Real Estate Pipeline

**ETL pipeline for Moroccan real estate listings from Avito.ma**

Scrapes property listings, cleans data, loads to PostgreSQL, and provides REST API + Dashboard.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌────────────┐
│   Avito.ma  │────▶│   Extract    │────▶│  Transform  │────▶│   Load     │
│   (Scraper) │     │   (Selenium) │     │   (pandas)  │     │ (PostgreSQL)│
└─────────────┘     └──────────────┘     └─────────────┘     └─────┬──────┘
                                                                    │
                        ┌─────────────────┬──────────────────────┘
                        │                 │
                        ▼                 ▼
                  ┌──────────┐     ┌──────────┐
                  │   API    │     │ Dashboard │
                  │ (FastAPI)│     │(Streamlit)│
                  └──────────┘     └──────────┘
```

## Features

- **Web Scraper**: Selenium-based two-pass scraping from Avito.ma
- **Data Cleaning**: Price normalization, city standardization, deduplication
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Orchestration**: Prefect for scheduled, production-grade pipelines
- **REST API**: FastAPI with filtering, pagination, and statistics
- **Dashboard**: Streamlit with Plotly visualizations
- **Testing**: 30+ pytest unit tests
- **CI/CD**: GitHub Actions for automated testing and deployment

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Create `.env` file:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=morocco_re
DB_USER=postgres
DB_PASSWORD=postgres
```

### 3. Run Pipeline

```bash
# Full pipeline (scraping + load)
python main.py

# Or use Prefect flow
python prefect_flow.py
```

### 4. View Dashboard

```bash
streamlit run dashboard.py
```

Dashboard: http://localhost:8501

### 5. Use API

```bash
uvicorn api:app --reload
```

API Docs: http://localhost:8000/docs

## Project Structure

```
morocco_re_pipeline/
├── config/              # Configuration settings
│   └── settings.py
├── pipeline/            # ETL pipeline modules
│   ├── extract.py       # Web scraping
│   ├── transform.py     # Data cleaning
│   ├── validate.py      # Data quality checks
│   └── load.py         # Database loading
├── sql/                 # SQL analysis queries
│   └── analysis.sql
├── tests/               # Unit tests
│   ├── conftest.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_validate.py
├── api.py               # FastAPI REST API
├── dashboard.py         # Streamlit dashboard
├── main.py              # Pipeline entry point
├── prefect_flow.py      # Prefect orchestration
├── deployment.py        # Prefect deployment config
├── docker-compose.yml   # Full stack deployment
├── Dockerfile           # Container image
├── requirements.txt     # Python dependencies
└── README.md
```

## Usage Modes

### CSV Mode (Development)

Uses existing data from `data/raw/listings.csv`:

```python
USE_SCRAPER = False  # In main.py
python main.py
```

### Scraper Mode (Production)

Live scrape from Avito.ma:

```python
USE_SCRAPER = True  # In main.py
python main.py
```

## Data Schema

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| title | VARCHAR | Listing title |
| price | NUMERIC | Price in MAD |
| surface_m2 | NUMERIC | Surface area in m² |
| price_per_m2 | NUMERIC | Calculated price per m² |
| price_range | VARCHAR | Budget/Mid-range/Premium/Luxury |
| city | VARCHAR | City name |
| category | VARCHAR | Property category |
| listing_type | VARCHAR | Vente (sale) or Location (rental) |
| seller_name | VARCHAR | Seller name |
| seller_type | VARCHAR | Agence or Particulier |
| url | VARCHAR | Original Avito URL |
| description | TEXT | Listing description |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API info |
| `/listings` | GET | List listings (filters: city, min_price, max_price) |
| `/listings/{id}` | GET | Get single listing |
| `/stats` | GET | Price statistics by city |
| `/cities` | GET | List available cities |
| `/health` | GET | Health check |

## Prefect Deployment

```bash
# Create deployment
python deployment.py create dev

# Start worker
prefect agent start --work-queue dev-etl

# Run deployment
prefect deployment run 'morocco-re-pipeline/morocco-re-dev'
```

## Docker Deployment

```bash
# Start full stack
docker-compose up

# Or build and run pipeline only
docker build -t morocco-re-pipeline .
docker run morocco-re-pipeline
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=pipeline --cov-report=html
```

## Technologies

- **Python 3.10+**
- **Selenium** - Web scraping
- **pandas** - Data manipulation
- **SQLAlchemy** - Database ORM
- **PostgreSQL** - Database
- **Prefect** - Workflow orchestration
- **FastAPI** - REST API
- **Streamlit** - Dashboard
- **Plotly** - Visualizations
- **Docker** - Containerization
- **GitHub Actions** - CI/CD

## License

MIT
