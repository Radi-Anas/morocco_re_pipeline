"""
test_claims_etl.py
Unit tests for claims ETL pipeline.
"""

import pytest
import pandas as pd
import numpy as np
from src.data.ingestion.claims_etl import transform_claims, extract_from_csv


class TestExtractFromCSV:
    """Tests for CSV extraction."""

    def test_extract_from_csv_returns_dataframe(self):
        """Test that extract_from_csv returns a DataFrame."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        
        assert isinstance(df, pd.DataFrame)

    def test_extract_from_csv_has_rows(self):
        """Test that extracted DataFrame has rows."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        
        assert len(df) > 0


class TestTransformClaims:
    """Tests for claims transformation."""

    @pytest.fixture
    def sample_claims(self):
        """Create sample claims DataFrame for testing."""
        data = {
            'months_as_customer': [12, 24, 36],
            'age': [25, 35, 45],
            'policy_number': ['POL001', 'POL002', 'POL003'],
            'policy_state': ['OH', 'IN', 'IL'],
            'policy_csl': ['100/300', '250/500', '500/1000'],
            'policy_deductable': [500, 1000, 1500],
            'policy_annual_premium': [1200, 1500, 1800],
            'insured_sex': ['M', 'F', 'M'],
            'insured_education_level': ['BS', 'MS', 'PhD'],
            'insured_occupation': ['Tech', 'Doctor', 'Lawyer'],
            'capital-gains': [1000, 2000, 3000],
            'capital-loss': [0, 500, 1000],
            'incident_type': ['Single Vehicle Collision', 'Multi-vehicle Collision', 'Parked Car'],
            'collision_type': ['Front', 'Rear', 'Side'],
            'incident_severity': ['Minor Damage', 'Major Damage', 'Total Loss'],
            'incident_hour_of_the_day': [10, 14, 22],
            'number_of_vehicles_involved': [1, 2, 3],
            'property_damage': ['YES', 'NO', 'YES'],
            'bodily_injuries': [0, 1, 2],
            'witnesses': [1, 2, 0],
            'police_report_available': ['YES', 'NO', 'YES'],
            'total_claim_amount': [5000, 10000, 20000],
            'injury_claim': [1000, 2000, 5000],
            'property_claim': [2000, 5000, 10000],
            'vehicle_claim': [2000, 3000, 5000],
            'auto_make': ['Toyota', 'Honda', 'Ford'],
            'auto_year': [2020, 2019, 2018],
            'fraud_reported': ['Y', 'N', 'N']
        }
        return pd.DataFrame(data)

    def test_transform_returns_dataframe(self):
        """Test that transform returns a DataFrame."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        assert isinstance(result, pd.DataFrame)

    def test_transform_creates_is_fraud_column(self):
        """Test that transform creates is_fraud column."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        assert 'is_fraud' in result.columns

    def test_is_fraud_values(self):
        """Test that is_fraud contains only 0 and 1."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        unique_values = result['is_fraud'].unique()
        assert set(unique_values).issubset({0, 1})

    def test_transform_creates_age_group(self):
        """Test that transform creates age_group column."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        assert 'age_group' in result.columns

    def test_transform_creates_tenure_group(self):
        """Test that transform creates tenure_group column."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        assert 'tenure_group' in result.columns

    def test_transform_creates_has_claim(self):
        """Test that transform creates has_claim column."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        assert 'has_claim' in result.columns

    def test_has_claim_is_binary(self):
        """Test that has_claim contains only 0 and 1."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        unique_values = result['has_claim'].unique()
        assert set(unique_values).issubset({0, 1})

    def test_transform_removes_junk_columns(self):
        """Test that transform removes columns starting with _."""
        data = {
            '_c39': [1, 2, 3],
            'valid_col': [4, 5, 6],
            'fraud_reported': ['Y', 'N', 'N']
        }
        df = pd.DataFrame(data)
        result = transform_claims(df)
        
        assert '_c39' not in result.columns

    def test_transform_handles_missing_values(self):
        """Test that transform handles missing values."""
        data = {
            'months_as_customer': [12, None, 36],
            'age': [25, 35, None],
            'policy_number': ['POL001', 'POL002', 'POL003'],
            'policy_state': ['OH', 'IN', 'IL'],
            'policy_csl': ['100/300', '250/500', '500/1000'],
            'policy_deductable': [500, 1000, 1500],
            'policy_annual_premium': [1200, 1500, 1800],
            'insured_sex': ['M', 'F', 'M'],
            'insured_education_level': ['BS', 'MS', 'PhD'],
            'insured_occupation': ['Tech', 'Doctor', 'Lawyer'],
            'capital-gains': [1000, 2000, 3000],
            'capital-loss': [0, 500, 1000],
            'incident_type': ['Single Vehicle Collision', 'Multi-vehicle Collision', 'Parked Car'],
            'collision_type': ['Front', 'Rear', 'Side'],
            'incident_severity': ['Minor Damage', 'Major Damage', 'Total Loss'],
            'incident_hour_of_the_day': [10, 14, 22],
            'number_of_vehicles_involved': [1, 2, 3],
            'property_damage': ['YES', 'NO', 'YES'],
            'bodily_injuries': [0, 1, 2],
            'witnesses': [1, 2, 0],
            'police_report_available': ['YES', 'NO', 'YES'],
            'total_claim_amount': [5000, 10000, 20000],
            'injury_claim': [1000, 2000, 5000],
            'property_claim': [2000, 5000, 10000],
            'vehicle_claim': [2000, 3000, 5000],
            'auto_make': ['Toyota', 'Honda', 'Ford'],
            'auto_year': [2020, 2019, 2018],
            'fraud_reported': ['Y', 'N', 'N']
        }
        df = pd.DataFrame(data)
        result = transform_claims(df)
        
        assert result is not None
        assert len(result) > 0

    def test_transform_preserves_row_count(self):
        """Test that transform preserves row count."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        original_count = len(df)
        result = transform_claims(df)
        
        assert len(result) == original_count


class TestFraudRate:
    """Tests for fraud rate calculation."""

    def test_fraud_rate_is_around_25_percent(self):
        """Test that fraud rate is approximately 25%."""
        df = extract_from_csv("data/raw/insurance_claims.csv")
        result = transform_claims(df)
        
        fraud_rate = result['is_fraud'].mean()
        
        assert 0.20 <= fraud_rate <= 0.30
