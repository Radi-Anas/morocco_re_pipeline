"""
Great Expectations suite for insurance claims data validation.

Run: python data_quality/gx_suite.py run
"""

import great_expectations as gx
from great_expectations.dataset import PandasDataset
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_PATH = "data/raw/insurance_claims.csv"


def create_expectations_suite() -> gx.DataContext:
    """Create and configure Great Expectations context."""
    context = gx.get_context()
    
    suite = context.suites.add(
        gx.expectations.ExpectationSuite(name="claims_data_quality")
    )
    
    return context, suite


def add_claim_expectations(dataset: PandasDataset) -> None:
    """Add data quality expectations for insurance claims."""
    
    # --- Column presence expectations ---
    dataset.expect_table_columns_to_match_ordered_list(
        column_list=[
            'months_as_customer', 'age', 'policy_number', 'policy_state',
            'policy_csl', 'policy_deductable', 'policy_annual_premium',
            'insured_sex', 'fraud_reported', 'incident_type', 'incident_severity',
            'total_claim_amount', 'auto_make'
        ]
    )
    
    # --- Numeric column expectations ---
    dataset.expect_column_values_to_be_between(
        column="age", min_value=18, max_value=100
    )
    
    dataset.expect_column_values_to_be_between(
        column="months_as_customer", min_value=0, max_value=500
    )
    
    dataset.expect_column_values_to_be_between(
        column="policy_annual_premium", min_value=0, max_value=100000
    )
    
    dataset.expect_column_values_to_be_between(
        column="total_claim_amount", min_value=0, max_value=1000000
    )
    
    # --- Categorical value expectations ---
    dataset.expect_column_values_to_be_in_set(
        column="policy_state", value_set=["OH", "IN", "IL", "PA", "NY"]
    )
    
    dataset.expect_column_values_to_be_in_set(
        column="insured_sex", value_set=["M", "F"]
    )
    
    dataset.expect_column_values_to_be_in_set(
        column="incident_severity", 
        value_set=["Trivial Damage", "Minor Damage", "Major Damage", "Total Loss"]
    )
    
    # --- Null expectations ---
    dataset.expect_column_values_to_not_be_null(column="policy_number")
    dataset.expect_column_values_to_not_be_null(column="age")
    dataset.expect_column_values_to_not_be_null(column="total_claim_amount")
    
    # --- Uniqueness expectations ---
    dataset.expect_column_values_to_be_unique(column="policy_number")
    
    # --- Value ranges for derived features ---
    dataset.expect_column_values_to_be_between(
        column="capital-gains", min_value=0, max_value=100000
    )
    
    dataset.expect_column_values_to_be_between(
        column="bodily_injuries", min_value=0, max_value=10
    )
    
    dataset.expect_column_values_to_be_between(
        column="witnesses", min_value=0, max_value=5
    )


def run_data_quality_check(data_path: str = DATA_PATH) -> dict:
    """
    Run Great Expectations data quality checks.
    
    Returns:
        dict with validation results
    """
    logger.info(f"Running data quality checks on {data_path}")
    
    df = pd.read_csv(data_path)
    dataset = PandasDataset(df)
    
    context = gx.get_context()
    
    suite = context.suites.create("claims_data_quality")
    add_expectations(dataset, suite)
    
    results = dataset.validate(
        expectation_suite_name="claims_data_quality",
        run_id="claim_validation_run"
    )
    
    return {
        "success": results.success,
        "statistics": results.statistics,
        "results": [
            {"expectation": r.expectation_config.expectation_type, "success": r.success}
            for r in results.results
        ]
    }


def add_expectations(dataset: PandasDataset, suite) -> None:
    """Add all expectations to dataset."""
    
    # Numeric ranges
    dataset.expect_column_values_to_be_between(
        column="age", min_value=18, max_value=100, expectation_kwargs={"suite": suite}
    )
    dataset.expect_column_values_to_be_between(
        column="months_as_customer", min_value=0, max_value=500, expectation_kwargs={"suite": suite}
    )
    dataset.expect_column_values_to_be_between(
        column="policy_annual_premium", min_value=0, max_value=100000, expectation_kwargs={"suite": suite}
    )
    dataset.expect_column_values_to_be_between(
        column="total_claim_amount", min_value=0, max_value=1000000, expectation_kwargs={"suite": suite}
    )
    
    # Categorical
    dataset.expect_column_values_to_be_in_set(
        column="policy_state", value_set=["OH", "IN", "IL", "PA", "NY"], expectation_kwargs={"suite": suite}
    )
    dataset.expect_column_values_to_be_in_set(
        column="incident_severity",
        value_set=["Trivial Damage", "Minor Damage", "Major Damage", "Total Loss"],
        expectation_kwargs={"suite": suite}
    )
    
    # Null checks
    dataset.expect_column_values_to_not_be_null(column="policy_number", expectation_kwargs={"suite": suite})
    dataset.expect_column_values_to_not_be_null(column="age", expectation_kwargs={"suite": suite})
    dataset.expect_column_values_to_not_be_null(column="total_claim_amount", expectation_kwargs={"suite": suite})


if __name__ == "__main__":
    result = run_data_quality_check()
    print(f"\nData Quality Check: {'PASSED' if result['success'] else 'FAILED'}")
    print(f"Statistics: {result['statistics']}")