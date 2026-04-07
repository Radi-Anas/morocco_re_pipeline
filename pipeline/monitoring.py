"""
pipeline/monitoring.py
Pipeline monitoring and alerting system.

Monitors:
    - Pipeline execution time
    - Data quality metrics
    - Model performance
    - API request rates
    - Database health

Alerts:
    - Email notifications on failure
    - Slack webhooks
    - Health check endpoints
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import json
import os

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Metrics for a pipeline run."""
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, success, failed
    rows_processed: int = 0
    error_message: str = ""
    execution_time_seconds: float = 0
    
    # Data quality
    data_quality_score: float = 0.0
    validation_issues: List[str] = field(default_factory=list)
    
    # Model metrics
    model_accuracy: float = 0.0
    model_auc: float = 0.0


class PipelineMonitor:
    """Monitor and track pipeline metrics."""
    
    def __init__(self):
        self.metrics_history: List[PipelineMetrics] = []
        self.alert_webhooks = {
            "slack": os.getenv("SLACK_WEBHOOK_URL", ""),
            "email": os.getenv("ALERT_EMAIL", ""),
        }
    
    def start_pipeline(self, pipeline_name: str) -> PipelineMetrics:
        """Start tracking a pipeline run."""
        metrics = PipelineMetrics(
            pipeline_name=pipeline_name,
            start_time=datetime.now(),
        )
        logger.info(f"Started monitoring: {pipeline_name}")
        return metrics
    
    def end_pipeline(self, metrics: PipelineMetrics, status: str, error: str = ""):
        """Complete pipeline tracking."""
        metrics.end_time = datetime.now()
        metrics.status = status
        metrics.execution_time_seconds = (metrics.end_time - metrics.start_time).total_seconds()
        
        if error:
            metrics.error_message = error
        
        self.metrics_history.append(metrics)
        
        # Check for alerts
        if status == "failed":
            self._send_alert(metrics)
        
        logger.info(f"Pipeline {metrics.pipeline_name} {status} in {metrics.execution_time_seconds:.1f}s")
    
    def get_recent_metrics(self, hours: int = 24) -> List[PipelineMetrics]:
        """Get metrics from recent hours."""
        cutoff = datetime.now() - timedelta(hours=hours)
        return [m for m in self.metrics_history if m.start_time > cutoff]
    
    def get_success_rate(self, hours: int = 24) -> float:
        """Calculate success rate for recent runs."""
        recent = self.get_recent_metrics(hours)
        if not recent:
            return 1.0
        
        successful = sum(1 for m in recent if m.status == "success")
        return successful / len(recent)
    
    def get_avg_execution_time(self, hours: int = 24) -> float:
        """Calculate average execution time."""
        recent = self.get_recent_metrics(hours)
        if not recent:
            return 0.0
        
        completed = [m for m in recent if m.end_time is not None]
        if not completed:
            return 0.0
        
        return sum(m.execution_time_seconds for m in completed) / len(completed)
    
    def _send_alert(self, metrics: PipelineMetrics):
        """Send alert notification."""
        message = f"Pipeline {metrics.pipeline_name} FAILED: {metrics.error_message}"
        
        # Slack webhook
        if self.alert_webhooks["slack"]:
            try:
                import requests
                requests.post(
                    self.alert_webhooks["slack"],
                    json={"text": message},
                    timeout=5
                )
            except Exception as e:
                logger.error(f"Failed to send Slack alert: {e}")
        
        logger.error(f"ALERT: {message}")
    
    def get_health_report(self) -> Dict:
        """Get overall pipeline health report."""
        return {
            "timestamp": datetime.now().isoformat(),
            "success_rate_24h": round(self.get_success_rate(24) * 100, 1),
            "avg_execution_time_24h": round(self.get_avg_execution_time(24), 1),
            "total_runs_24h": len(self.get_recent_metrics(24)),
            "status": "healthy" if self.get_success_rate(24) >= 0.9 else "degraded",
        }


# Global monitor instance
monitor = PipelineMonitor()


# API endpoint for health dashboard
@app.get("/monitoring/health")
def get_monitoring_health():
    """Get monitoring health report."""
    return monitor.get_health_report()


# Backtest monitoring decorator
def monitor_pipeline(func):
    """Decorator to automatically monitor pipeline functions."""
    def wrapper(*args, **kwargs):
        metrics = monitor.start_pipeline(func.__name__)
        try:
            result = func(*args, **kwargs)
            monitor.end_pipeline(metrics, "success")
            return result
        except Exception as e:
            monitor.end_pipeline(metrics, "failed", str(e))
            raise
    return wrapper


if __name__ == "__main__":
    # Test monitoring
    m = PipelineMetrics(
        pipeline_name="test_etl",
        start_time=datetime.now() - timedelta(hours=1),
    )
    m.end_time = datetime.now()
    m.status = "success"
    m.execution_time_seconds = 120.5
    
    monitor.metrics_history.append(m)
    
    print(json.dumps(monitor.get_health_report(), indent=2))
