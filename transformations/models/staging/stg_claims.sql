-- staging: clean and standardize raw claims data
-- Usage: dbt run --select stg_claims

{{ config(materialized='view') }}

select
    policy_number,
    months_as_customer,
    age,
    policy_state,
    policy_csl,
    policy_deductable,
    policy_annual_premium,
    insured_sex,
    insured_education_level,
    insured_occupation,
    "capital-gains" as capital_gains,
    "capital-loss" as capital_loss,
    incident_type,
    collision_type,
    incident_severity,
    incident_hour_of_the_day,
    number_of_vehicles_involved,
    property_damage,
    bodily_injuries,
    witnesses,
    police_report_available,
    total_claim_amount,
    injury_claim,
    property_claim,
    vehicle_claim,
    auto_make,
    auto_year,
    has_claim,
    age_group,
    tenure_group,
    is_fraud,
    -- Normalize string columns
    trim(lower(policy_state)) as policy_state_clean,
    trim(lower(incident_severity)) as incident_severity_clean,
    -- Calculate derived metrics
    total_claim_amount / nullif(policy_annual_premium, 0) as claim_to_premium_ratio,
    vehicle_claim / nullif(total_claim_amount, 0) as vehicle_claim_ratio,
    injury_claim / nullif(total_claim_amount, 0) as injury_claim_ratio
from {{ ref('claims') }}