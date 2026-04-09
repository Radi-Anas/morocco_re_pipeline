"""
Integration tests for data pipeline flow.

Tests the complete data flow: API → ETL → Database → Prediction.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from claims_etl import transform_claims, validate_data
from pipeline.schema_registry import validate_incoming_claim
from pipeline.incremental_etl import ETLWatermark


class TestDataFlow:
    """Test complete data pipeline flow."""
    
    def test_transform_preserves_required_columns(self):
        """Test that transform keeps required columns."""
        df = pd.DataFrame({
            'policy_number': ['POL001'],
            'age': [35],
            'months_as_customer': [12],
            'policy_state': ['OH'],
            'policy_annual_premium': [1000],
            'total_claim_amount': [5000],
            'fraud_reported': ['N']
        })
        
        result = transform_claims(df)
        
        required = ['policy_number', 'age', 'months_as_customer', 'is_fraud']
        for col in required:
            assert col in result.columns
    
    def test_schema_validation_accepts_valid_claim(self):
        """Test schema validation accepts valid claim."""
        valid_claim = {
            'policy_number': 'POL12345',
            'months_as_customer': 24,
            'age': 35,
            'policy_state': 'OH',
            'policy_csl': '250/500',
            'policy_annual_premium': 1500.0,
            'incident_type': 'Vehicle Collision',
            'incident_severity': 'Minor Damage',
            'total_claim_amount': 5000.0,
            'auto_make': 'Toyota'
        }
        
        result = validate_incoming_claim(valid_claim)
        
        assert result['valid'] is True
        assert len(result['errors']) == 0
    
    def test_schema_validation_rejects_invalid_state(self):
        """Test schema validation rejects invalid policy_state."""
        invalid_claim = {
            'policy_number': 'POL12345',
            'policy_state': 'INVALID',
            'policy_annual_premium': 1500.0,
            'total_claim_amount': 5000.0,
            'incident_severity': 'Minor Damage',
            'auto_make': 'Toyota'
        }
        
        result = validate_incoming_claim(invalid_claim)
        
        assert result['valid'] is False
        assert any('Invalid policy_state' in e for e in result['errors'])
    
    def test_schema_validation_rejects_negative_amount(self):
        """Test schema validation rejects negative amounts."""
        invalid_claim = {
            'policy_number': 'POL12345',
            'policy_state': 'OH',
            'policy_annual_premium': -100.0,
            'total_claim_amount': 5000.0,
            'incident_severity': 'Minor Damage',
            'auto_make': 'Toyota'
        }
        
        result = validate_incoming_claim(invalid_claim)
        
        assert result['valid'] is False
        assert any('non-negative' in e for e in result['errors'])


class TestETLWatermark:
    """Test ETL watermark state management."""
    
    def test_load_state_returns_dict(self):
        """Test load state returns valid dict."""
        state = ETLWatermark.load_state()
        
        assert isinstance(state, dict)
        assert 'last_run' in state
        assert 'last_row_count' in state
    
    def test_save_and_load_state(self):
        """Test save and load state cycle."""
        test_state = {
            'last_run': '2026-04-09T10:00:00',
            'last_row_count': 1000,
            'last_checksum': 'abc123'
        }
        
        ETLWatermark.save_state(test_state)
        loaded = ETLWatermark.load_state()
        
        assert loaded['last_row_count'] == 1000
        assert loaded['last_checksum'] == 'abc123'


class TestDataQuality:
    """Test data quality checks."""
    
    def test_validate_data_detects_missing_columns(self):
        """Test validation detects missing columns."""
        df = pd.DataFrame({
            'age': [35],
            'total_claim_amount': [5000]
        })
        
        issues = validate_data(df)
        
        assert 'missing_columns' in issues
    
    def test_validate_data_detects_invalid_ranges(self):
        """Test validation detects out-of-range values."""
        df = pd.DataFrame({
            'age': [150],
            'months_as_customer': [12],
            'policy_state': ['OH'],
            'policy_annual_premium': [1000],
            'total_claim_amount': [5000],
            'policy_number': ['POL001'],
            'is_fraud': [0]
        })
        
        issues = validate_data(df)
        
        assert 'age_out_of_range' in issues
    
    def test_validate_data_accepts_valid_data(self):
        """Test validation passes for valid data."""
        df = pd.DataFrame({
            'policy_number': ['POL001', 'POL002'],
            'age': [35, 42],
            'months_as_customer': [12, 24],
            'policy_state': ['OH', 'NY'],
            'policy_annual_premium': [1000, 1500],
            'total_claim_amount': [5000, 8000],
            'is_fraud': [0, 1]
        })
        
        issues = validate_data(df)
        
        assert 'missing_columns' not in issues


class TestTransform:
    """Test data transformations."""
    
    def test_transform_creates_age_groups(self):
        """Test transform creates age groups."""
        df = pd.DataFrame({
            'policy_number': ['POL001'],
            'age': [30],
            'months_as_customer': [12],
            'policy_state': ['OH'],
            'policy_annual_premium': [1000],
            'total_claim_amount': [5000],
            'fraud_reported': ['N']
        })
        
        result = transform_claims(df)
        
        assert 'age_group' in result.columns
    
    def test_transform_encodes_fraud_column(self):
        """Test transform encodes fraud as binary."""
        df = pd.DataFrame({
            'policy_number': ['POL001'],
            'age': [35],
            'months_as_customer': [12],
            'policy_state': ['OH'],
            'policy_annual_premium': [1000],
            'total_claim_amount': [5000],
            'fraud_reported': ['Y']
        })
        
        result = transform_claims(df)
        
        assert result['is_fraud'].iloc[0] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])