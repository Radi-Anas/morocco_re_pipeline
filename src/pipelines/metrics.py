"""
pipeline/metrics.py
Prometheus metrics for pipeline monitoring.

Exposes:
    - Pipeline execution time
    - ETL row counts
    - Model accuracy
    - API request counts
"""

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from datetime import datetime
import time

# ETL Metrics
etl_rows_processed = Counter('etl_rows_processed_total', 'Total rows processed by ETL')
etl_execution_time = Histogram('etl_execution_seconds', 'ETL execution time')
etl_errors = Counter('etl_errors_total', 'Total ETL errors')

# Model Metrics
model_predictions = Counter('model_predictions_total', 'Total model predictions')
model_fraud_predictions = Counter('model_fraud_predictions_total', 'Total fraud predictions')
model_accuracy = Gauge('model_accuracy_current', 'Current model accuracy')
model_auc = Gauge('model_auc_current', 'Current model AUC')

# API Metrics
api_requests = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method'])
api_request_duration = Histogram('api_request_duration_seconds', 'API request duration', ['endpoint'])
api_errors = Counter('api_errors_total', 'Total API errors', ['endpoint'])

# Database Metrics
db_connections = Gauge('db_connections_active', 'Active database connections')
db_query_duration = Histogram('db_query_duration_seconds', 'Database query duration')

# Pipeline Metrics
pipeline_runs = Counter('pipeline_runs_total', 'Total pipeline runs', ['pipeline_name', 'status'])
pipeline_duration = Histogram('pipeline_duration_seconds', 'Pipeline execution time', ['pipeline_name'])


class MetricsTimer:
    """Context manager for timing operations."""
    
    def __init__(self, histogram):
        self.histogram = histogram
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, *args):
        self.histogram.observe(time.time() - self.start_time)


def track_pipeline_run(pipeline_name: str, status: str):
    """Track a pipeline run."""
    pipeline_runs.labels(pipeline_name=pipeline_name, status=status).inc()


def track_etl_metrics(rows: int, duration: float, success: bool):
    """Track ETL execution metrics."""
    etl_rows_processed.inc(rows)
    etl_execution_time.observe(duration)
    if not success:
        etl_errors.inc()


def track_model_metrics(accuracy: float, auc: float):
    """Track model performance metrics."""
    model_accuracy.set(accuracy)
    model_auc.set(auc)


def track_api_request(endpoint: str, method: str, duration: float):
    """Track API request metrics."""
    api_requests.labels(endpoint=endpoint, method=method).inc()
    api_request_duration.labels(endpoint=endpoint).observe(duration)


def track_api_error(endpoint: str):
    """Track API errors."""
    api_errors.labels(endpoint=endpoint).inc()


# Metrics endpoint for scraping
def get_metrics():
    """Get Prometheus metrics."""
    return generate_latest()


def metrics_endpoint():
    """FastAPI endpoint for Prometheus scraping."""
    from fastapi import APIRouter
    from fastapi.responses import Response
    
    router = APIRouter()
    
    @router.get("/metrics")
    def metrics():
        return Response(content=get_metrics(), media_type=CONTENT_TYPE_LATEST)
    
    return router
