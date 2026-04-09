"""
Schema registry for insurance claims using Avro.

Provides schema validation and evolution for claim data contracts.
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_REGISTRY = {}

CLAIM_SCHEMA_V1 = {
    "type": "record",
    "name": "InsuranceClaim",
    "namespace": "com.insurance.fraud",
    "version": 1,
    "fields": [
        {"name": "policy_number", "type": "string"},
        {"name": "months_as_customer", "type": ["int", "null"]},
        {"name": "age", "type": ["int", "null"]},
        {"name": "policy_state", "type": {"type": "enum", "symbols": ["OH", "IN", "IL", "PA", "NY"]}},
        {"name": "policy_csl", "type": "string"},
        {"name": "policy_annual_premium", "type": "double"},
        {"name": "policy_deductable", "type": ["int", "null"]},
        {"name": "insured_sex", "type": {"type": "enum", "symbols": ["M", "F"]}},
        {"name": "incident_type", "type": "string"},
        {"name": "incident_severity", "type": {"type": "enum", "symbols": ["Trivial Damage", "Minor Damage", "Major Damage", "Total Loss"]}},
        {"name": "incident_hour_of_the_day", "type": ["int", "null"]},
        {"name": "number_of_vehicles_involved", "type": ["int", "null"]},
        {"name": "bodily_injuries", "type": ["int", "null"]},
        {"name": "witnesses", "type": ["int", "null"]},
        {"name": "police_report_available", "type": {"type": "enum", "symbols": ["YES", "NO"]}},
        {"name": "total_claim_amount", "type": "double"},
        {"name": "injury_claim", "type": ["double", "null"]},
        {"name": "property_claim", "type": ["double", "null"]},
        {"name": "vehicle_claim", "type": ["double", "null"]},
        {"name": "auto_make", "type": "string"},
        {"name": "auto_year", "type": ["int", "null"]},
        {"name": "timestamp", "type": "string"}
    ]
}

SCHEMA_REGISTRY["insurance_claim_v1"] = CLAIM_SCHEMA_V1


class SchemaRegistry:
    """Manage schema versions and validation."""
    
    @staticmethod
    def get_schema(version: str = "v1") -> Dict:
        """Get schema by version."""
        return SCHEMA_REGISTRY.get(f"insurance_claim_{version}")
    
    @staticmethod
    def validate_claim(data: Dict[str, Any], version: str = "v1") -> tuple[bool, Optional[str]]:
        """
        Validate claim data against schema.
        
        Returns:
            (is_valid, error_message)
        """
        schema = SchemaRegistry.get_schema(version)
        if not schema:
            return False, f"Unknown schema version: {version}"
        
        required_fields = ["policy_number", "policy_state", "policy_annual_premium", "total_claim_amount"]
        
        for field in required_fields:
            if field not in data:
                return False, f"Missing required field: {field}"
        
        valid_states = {"OH", "IN", "IL", "PA", "NY"}
        if data.get("policy_state") and data["policy_state"] not in valid_states:
            return False, f"Invalid policy_state: {data['policy_state']}"
        
        valid_severities = {"Trivial Damage", "Minor Damage", "Major Damage", "Total Loss"}
        if data.get("incident_severity") and data["incident_severity"] not in valid_severities:
            return False, f"Invalid incident_severity: {data['incident_severity']}"
        
        numeric_fields = ["months_as_customer", "age", "policy_annual_premium", "total_claim_amount"]
        for field in numeric_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], (int, float)):
                    return False, f"Field {field} must be numeric"
                if data[field] < 0:
                    return False, f"Field {field} must be non-negative"
        
        return True, None
    
    @staticmethod
    def register_schema(schema_name: str, schema: Dict) -> bool:
        """Register a new schema version."""
        SCHEMA_REGISTRY[schema_name] = schema
        logger.info(f"Registered schema: {schema_name}")
        return True
    
    @staticmethod
    def list_schemas() -> list:
        """List all registered schemas."""
        return list(SCHEMA_REGISTRY.keys())


def validate_incoming_claim(claim_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and enrich claim data with metadata.
    
    Returns:
        dict with validation status and enriched data
    """
    is_valid, error = SchemaRegistry.validate_claim(claim_data)
    
    result = {
        "valid": is_valid,
        "errors": [error] if error else [],
        "data": claim_data,
        "validated_at": datetime.now().isoformat()
    }
    
    if is_valid:
        result["data"]["timestamp"] = datetime.now().isoformat()
    
    return result


if __name__ == "__main__":
    test_claim = {
        "policy_number": "POL12345",
        "months_as_customer": 24,
        "age": 35,
        "policy_state": "OH",
        "policy_csl": "250/500",
        "policy_annual_premium": 1500.0,
        "incident_type": "Vehicle Collision",
        "incident_severity": "Minor Damage",
        "total_claim_amount": 5000.0,
        "auto_make": "Toyota"
    }
    
    result = validate_incoming_claim(test_claim)
    print(f"Validation result: {result['valid']}")
    if not result['valid']:
        print(f"Errors: {result['errors']}")