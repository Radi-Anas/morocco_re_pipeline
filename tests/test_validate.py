"""
test_validate.py
Unit tests for the validation module.
"""

import pytest
import pandas as pd
from pipeline.validate import (
    check_minimum_rows,
    check_no_empty_dataframe,
    check_required_columns,
    check_price_not_null,
    check_price_range,
    validate,
)


class TestCheckMinimumRows:
    """Tests for minimum rows check."""

    def test_passes_when_rows_sufficient(self):
        """Test that check passes when row count is sufficient."""
        df = pd.DataFrame({"price": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
        
        passed, level, msg = check_minimum_rows(df, minimum=10)
        
        assert passed is True

    def test_fails_when_rows_insufficient(self):
        """Test that check fails when row count is too low."""
        df = pd.DataFrame({"price": [1, 2, 3]})
        
        passed, level, msg = check_minimum_rows(df, minimum=10)
        
        assert passed is False
        assert level == "CRITICAL"


class TestCheckNoEmptyDataFrame:
    """Tests for empty DataFrame check."""

    def test_passes_for_non_empty_df(self, sample_clean_df):
        """Test that non-empty DataFrame passes."""
        passed, level, msg = check_no_empty_dataframe(sample_clean_df)
        
        assert passed is True

    def test_fails_for_empty_df(self):
        """Test that empty DataFrame fails."""
        df = pd.DataFrame()
        
        passed, level, msg = check_no_empty_dataframe(df)
        
        assert passed is False
        assert level == "CRITICAL"


class TestCheckRequiredColumns:
    """Tests for required columns check."""

    def test_passes_when_all_columns_present(self, sample_clean_df):
        """Test that check passes when all required columns exist."""
        passed, level, msg = check_required_columns(sample_clean_df)
        
        assert passed is True

    def test_fails_when_columns_missing(self):
        """Test that check fails when required columns are missing."""
        df = pd.DataFrame({"price": [1000]})
        
        passed, level, msg = check_required_columns(df)
        
        assert passed is False
        assert "title" in msg or "Missing" in msg


class TestCheckPriceNotNull:
    """Tests for price null check."""

    def test_passes_when_no_null_prices(self, sample_clean_df):
        """Test that check passes when no prices are null."""
        passed, level, msg = check_price_not_null(sample_clean_df)
        
        assert passed

    def test_fails_when_null_prices_exist(self):
        """Test that check fails when null prices are found."""
        df = pd.DataFrame({"price": [1000, None, 3000]})
        
        passed, level, msg = check_price_not_null(df)
        
        assert not passed
        assert level == "CRITICAL"


class TestCheckPriceRange:
    """Tests for price range check."""

    def test_passes_for_reasonable_prices(self):
        """Test that reasonable average prices pass."""
        df = pd.DataFrame({"price": [1000000, 2000000, 3000000]})
        
        passed, level, msg = check_price_range(df)
        
        assert passed
        assert level == "WARNING"

    def test_fails_for_suspicious_prices(self):
        """Test that suspicious average prices fail."""
        df = pd.DataFrame({"price": [100, 200, 300]})
        
        passed, level, msg = check_price_range(df)
        
        assert not passed


class TestValidate:
    """Integration tests for validate function."""

    def test_validate_returns_true_for_clean_data(self, sample_clean_df):
        """Test that validate returns True for clean data."""
        result = validate(sample_clean_df, min_rows=2, strict_url=True)
        
        assert result is True

    def test_validate_returns_false_for_empty_df(self):
        """Test that validate returns False for empty DataFrame."""
        df = pd.DataFrame()
        
        result = validate(df)
        
        assert result is False

    def test_validate_strict_mode(self, sample_clean_df):
        """Test validation in strict mode."""
        result = validate(sample_clean_df, min_rows=1, strict_url=True)
        
        assert result is True
