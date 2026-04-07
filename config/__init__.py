"""
config/__init__.py
Centralized configuration management using Pydantic.

Provides:
    - Type-safe configuration
    - Environment validation
    - Default values
    - Sensitive data masking
"""

import os
from typing import Optional, List
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from functools import lru_cache


class DatabaseConfig(BaseModel):
    """Database configuration."""
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    name: str = Field(default="insurance_fraud")
    user: str = Field(default="postgres")
    password: str = Field(default="")
    pool_size: int = Field(default=5)
    max_overflow: int = Field(default=10)
    
    @property
    def url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
    
    @property
    def url_masked(self) -> str:
        return f"postgresql+psycopg2://{self.user}:****@{self.host}:{self.port}/{self.name}"


class APIConfig(BaseModel):
    """API configuration."""
    title: str = Field(default="Insurance Fraud Detection API")
    version: str = Field(default="1.0.0")
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    debug: bool = Field(default=False)
    api_key: Optional[str] = Field(default=None)
    rate_limit_predict: str = Field(default="10/minute")
    rate_limit_batch: str = Field(default="5/minute")


class MLConfig(BaseModel):
    """ML model configuration."""
    model_path: str = Field(default="models/fraud_model.pkl")
    encoders_path: str = Field(default="models/label_encoders.pkl")
    min_accuracy: float = Field(default=0.70)
    min_auc: float = Field(default=0.70)
    test_size: float = Field(default=0.2)
    random_state: int = Field(default=42)


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = Field(default="INFO")
    format: str = Field(default="json")
    file_path: str = Field(default="logs/pipeline.log")


class AlertConfig(BaseModel):
    """Alert configuration."""
    slack_webhook: Optional[str] = Field(default=None)
    email: Optional[str] = Field(default=None)
    on_failure: bool = Field(default=True)
    on_success: bool = Field(default=False)


class Settings(BaseSettings):
    """Application settings."""
    
    # Environment
    env: str = Field(default="development")
    
    # Database
    db: DatabaseConfig = Field(default_factory=DatabaseConfig)
    
    # API
    api: APIConfig = Field(default_factory=APIConfig)
    
    # ML
    ml: MLConfig = Field(default_factory=MLConfig)
    
    # Logging
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    # Alerts
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    
    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"
    
    @validator("env")
    def validate_env(cls, v):
        allowed = ["development", "staging", "production"]
        if v not in allowed:
            raise ValueError(f"ENV must be one of {allowed}")
        return v


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience functions
def get_db_config() -> DatabaseConfig:
    """Get database configuration."""
    return get_settings().db


def get_api_config() -> APIConfig:
    """Get API configuration."""
    return get_settings().api


def get_ml_config() -> MLConfig:
    """Get ML configuration."""
    return get_settings().ml


def is_production() -> bool:
    """Check if running in production."""
    return get_settings().env == "production"


def is_development() -> bool:
    """Check if running in development."""
    return get_settings().env == "development"


# Pydantic models for API requests
class ClaimInput(BaseModel):
    """Schema for claim input."""
    months_as_customer: int = Field(..., ge=0, le=500)
    age: int = Field(..., ge=18, le=100)
    policy_state: str = Field(..., max_length=10)
    policy_csl: str = Field(..., max_length=20)
    policy_deductable: int = Field(..., ge=0)
    policy_annual_premium: float = Field(..., ge=0)
    insured_sex: str = Field(..., max_length=10)
    insured_education_level: str = Field(..., max_length=20)
    insured_occupation: str = Field(..., max_length=50)
    incident_type: str = Field(..., max_length=50)
    incident_severity: str = Field(..., max_length=20)
    total_claim_amount: float = Field(..., ge=0)
    vehicle_claim: float = Field(..., ge=0)
    property_claim: float = Field(..., ge=0)
    injury_claim: float = Field(..., ge=0)
    auto_make: str = Field(..., max_length=30)
    
    class Config:
        json_schema_extra = {
            "example": {
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
        }


class PredictionOutput(BaseModel):
    """Schema for prediction output."""
    prediction: int
    fraud_probability: float
    confidence: str
    risk_level: str
