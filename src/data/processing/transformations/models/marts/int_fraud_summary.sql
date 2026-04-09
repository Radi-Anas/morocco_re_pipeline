-- marts: fraud analytics aggregates
-- Usage: dbt run --select int_fraud_summary

{{ config(materialized='table') }}

with fraud_stats as (
    select
        auto_make,
        incident_severity_clean as incident_severity,
        age_group,
        tenure_group,
        count(*) as total_claims,
        sum(is_fraud) as fraud_count,
        round(sum(is_fraud)::numeric / count(*), 3) as fraud_rate,
        avg(total_claim_amount) as avg_claim_amount,
        avg(policy_annual_premium) as avg_premium,
        sum(total_claim_amount) as total_claim_value
    from {{ ref('stg_claims') }}
    group by 
        auto_make, 
        incident_severity_clean, 
        age_group, 
        tenure_group
)
select
    *,
    case 
        when fraud_rate >= 0.2 then 'HIGH_RISK'
        when fraud_rate >= 0.1 then 'MEDIUM_RISK'
        else 'LOW_RISK'
    end as risk_category,
    row_number() over (order by fraud_rate desc) as fraud_rank
from fraud_stats
order by fraud_rate desc