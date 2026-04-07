"""
test_fraud_model.py
Unit tests for fraud detection model.
"""

import pytest
import pandas as pd
import numpy as np
import os
from fraud_model import (
    load_data, 
    prepare_features, 
    train_model, 
    save_model, 
    load_model,
    predict_fraud
)


class TestLoadData:
    """Tests for data loading."""

    def test_load_data_returns_dataframe(self):
        """Test that load_data returns a DataFrame."""
        df = load_data()
        
        assert isinstance(df, pd.DataFrame)

    def test_load_data_has_rows(self):
        """Test that load_data has rows."""
        df = load_data()
        
        assert len(df) > 0

    def test_load_data_has_is_fraud_column(self):
        """Test that data has is_fraud column."""
        df = load_data()
        
        assert 'is_fraud' in df.columns


class TestPrepareFeatures:
    """Tests for feature preparation."""

    def test_prepare_features_returns_tuples(self):
        """Test that prepare_features returns correct tuple."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        
        assert isinstance(X, pd.DataFrame)
        assert isinstance(y, np.ndarray)
        assert isinstance(encoders, dict)
        assert isinstance(feature_names, list)

    def test_prepare_features_removes_target(self):
        """Test that target is removed from features."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        
        assert 'is_fraud' not in X.columns

    def test_prepare_features_removes_policy_number(self):
        """Test that policy_number is removed from features."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        
        assert 'policy_number' not in X.columns

    def test_prepare_features_encodes_categorical(self):
        """Test that categorical columns are encoded."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        
        cat_cols = X.select_dtypes(include=['object']).columns
        assert len(cat_cols) == 0, "All categorical columns should be encoded to numeric"

    def test_prepare_features_preserves_row_count(self):
        """Test that row count is preserved."""
        df = load_data()
        original_count = len(df)
        X, y, encoders, feature_names = prepare_features(df)
        
        assert len(X) == original_count
        assert len(y) == original_count

    def test_prepare_features_y_is_binary(self):
        """Test that y contains only 0 and 1."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        
        unique_values = np.unique(y)
        assert set(unique_values).issubset({0, 1})


class TestTrainModel:
    """Tests for model training."""

    def test_train_model_returns_dict(self):
        """Test that train_model returns a dictionary."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        assert isinstance(results, dict)

    def test_train_model_contains_model(self):
        """Test that results contain model object."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        assert 'model' in results

    def test_train_model_contains_metrics(self):
        """Test that results contain metrics."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        assert 'accuracy' in results
        assert 'auc_score' in results

    def test_train_model_accuracy_in_range(self):
        """Test that accuracy is between 0 and 1."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        assert 0 <= results['accuracy'] <= 1

    def test_train_model_auc_in_range(self):
        """Test that AUC is between 0 and 1."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        assert 0 <= results['auc_score'] <= 1


class TestSaveAndLoadModel:
    """Tests for model persistence."""

    def test_save_model_creates_files(self):
        """Test that save_model creates model files."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        save_model(results['model'], encoders, feature_names)
        
        assert os.path.exists("models/fraud_model.pkl")
        assert os.path.exists("models/label_encoders.pkl")

    def test_load_model_returns_dict(self):
        """Test that load_model returns dictionary."""
        model_data = load_model()
        
        assert isinstance(model_data, dict)
        assert 'model' in model_data
        assert 'encoders' in model_data
        assert 'features' in model_data


class TestPredictFraud:
    """Tests for fraud prediction."""

    @pytest.fixture
    def sample_claim(self):
        """Create sample claim data."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        return X.iloc[0].to_dict()

    def test_predict_fraud_returns_dict(self, sample_claim):
        """Test that predict_fraud returns a dictionary."""
        result = predict_fraud(sample_claim)
        
        assert isinstance(result, dict)

    def test_predict_fraud_contains_prediction(self, sample_claim):
        """Test that result contains prediction."""
        result = predict_fraud(sample_claim)
        
        assert 'is_fraud' in result
        assert result['is_fraud'] in [0, 1]

    def test_predict_fraud_contains_probability(self, sample_claim):
        """Test that result contains probability."""
        result = predict_fraud(sample_claim)
        
        assert 'fraud_probability' in result
        assert 0 <= result['fraud_probability'] <= 1

    def test_predict_fraud_contains_confidence(self, sample_claim):
        """Test that result contains confidence."""
        result = predict_fraud(sample_claim)
        
        assert 'confidence' in result
        assert result['confidence'] in ['high', 'medium', 'low']

    def test_predict_fraud_with_model_data(self, sample_claim):
        """Test prediction with explicit model data."""
        model_data = load_model()
        result = predict_fraud(sample_claim, model_data)
        
        assert isinstance(result, dict)
        assert 'is_fraud' in result


class TestModelPerformance:
    """Tests for model performance expectations."""

    def test_model_meets_minimum_accuracy(self):
        """Test that model accuracy is at least 70%."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        assert results['accuracy'] >= 0.70, f"Expected accuracy >= 0.70, got {results['accuracy']}"

    def test_model_meets_minimum_auc(self):
        """Test that model AUC is at least 0.70."""
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        assert results['auc_score'] >= 0.70, f"Expected AUC >= 0.70, got {results['auc_score']}"
