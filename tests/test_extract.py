"""
test_extract.py
Unit tests for the extract module.
"""

import pytest
import pandas as pd
from pipeline.extract import extract_from_csv, extract_surface_from_text


class TestExtractFromCSV:
    """Tests for CSV extraction."""

    def test_extract_from_csv_returns_dataframe(self):
        """Test that extract_from_csv returns a DataFrame."""
        df = extract_from_csv("data/raw/listings.csv")
        
        assert isinstance(df, pd.DataFrame)

    def test_extract_from_csv_has_rows(self):
        """Test that extracted DataFrame has rows."""
        df = extract_from_csv("data/raw/listings.csv")
        
        assert len(df) > 0


class TestExtractSurfaceFromText:
    """Tests for surface extraction from text."""

    def test_extracts_m2_format(self):
        """Test extraction from '85m²' format."""
        result = extract_surface_from_text("Superficie de 85m²")
        
        assert result == 85

    def test_extracts_m2_with_space(self):
        """Test extraction from '85 m2' format."""
        result = extract_surface_from_text("Surface 85 m2")
        
        assert result == 85

    def test_extracts_metres_carrés(self):
        """Test extraction from 'mètres carrés' format."""
        result = extract_surface_from_text("85 mètres carrés")
        
        assert result == 85

    def test_returns_none_for_invalid_text(self):
        """Test that invalid text returns None."""
        result = extract_surface_from_text("No surface here")
        
        assert result is None

    def test_rejects_unrealistic_values(self):
        """Test that unrealistic surface values are rejected."""
        result = extract_surface_from_text("Superficie de 5m²")
        
        assert result is None
        
        result = extract_surface_from_text("Superficie de 50000m²")
        
        assert result is None
