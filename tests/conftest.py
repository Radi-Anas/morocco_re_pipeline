"""
conftest.py
Pytest fixtures for pipeline tests.
"""

import pandas as pd
import pytest


@pytest.fixture
def sample_raw_df():
    """Sample raw DataFrame mimicking scraper output."""
    return pd.DataFrame({
        "title": [
            "Appartement à Casablanca",
            "Villa à Marrakech",
            "Studio à Rabat",
        ],
        "description": ["Desc 1", "Desc 2", "Desc 3"],
        "price": ["1500000", "2500000", "500000"],
        "currency": ["DH", "DH", "DH"],
        "surface_m2": [85, 200, 35],
        "listing_type": ["Vente", "Vente", "Location"],
        "seller_name": ["Particulier", "Agence", "Particulier"],
        "city": ["casablanca", "marrakech", "rabat"],
        "category": ["Appartements", "Villas", "Studios"],
        "url": [
            "https://avito.ma/casablanca/appart1",
            "https://avito.ma/marrakech/villa1",
            "https://avito.ma/rabat/studio1",
        ],
    })


@pytest.fixture
def sample_clean_df():
    """Sample cleaned DataFrame ready for PostgreSQL."""
    return pd.DataFrame({
        "title": ["Appartement À Casablanca", "Villa À Marrakech"],
        "price": [1500000.0, 2500000.0],
        "city": ["Casablanca", "Marrakech"],
        "surface_m2": [85, 200],
        "url": [
            "https://avito.ma/casablanca/appart1",
            "https://avito.ma/marrakech/villa1",
        ],
        "price_per_m2": [17647.06, 12500.0],
        "price_range": ["Mid-range", "Premium"],
    })


@pytest.fixture
def dirty_df():
    """DataFrame with invalid/missing data."""
    return pd.DataFrame({
        "title": ["Valid", "No Price", "Zero Surface", None],
        "price": [1000000, "invalid", 500000, 750000],
        "city": ["Casablanca", "Rabat", "Fes", "Agadir"],
        "surface_m2": [80, 50, 0, 100],
        "url": ["url1", "url2", "url3", "url4"],
    })
