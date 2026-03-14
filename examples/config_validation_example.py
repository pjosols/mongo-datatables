"""
Example demonstrating the configuration validation system in mongo-datatables 1.14.1

This example shows how the new ConfigValidator provides helpful warnings and error
messages for common configuration issues while maintaining backward compatibility.
"""

from mongo_datatables import DataTables, DataField
from pymongo import MongoClient

# Sample data setup
client = MongoClient('mongodb://localhost:27017/')
db = client.test_db
collection = db.users

# Define data fields
data_fields = [
    DataField("name", "string"),
    DataField("email", "string"), 
    DataField("age", "number"),
    DataField("created_at", "date"),
    DataField("department", "string")
]

# Example 1: Valid configuration (no warnings)
print("=== Example 1: Valid Configuration ===")
request_args = {
    "draw": 1,
    "start": 0,
    "length": 10,
    "fixedColumns": {"left": 1, "right": 1},
    "colReorder": {"enabled": True}
}

dt = DataTables(client, 'test_db.users', request_args, data_fields)
response = dt.get_rows()
print("✓ Configuration validated successfully")

# Example 2: Invalid FixedColumns configuration
print("\n=== Example 2: Invalid FixedColumns Configuration ===")
request_args_invalid = {
    "draw": 1,
    "start": 0,
    "length": 10,
    "fixedColumns": {"left": 3, "right": 3}  # Too many fixed columns
}

dt_invalid = DataTables(client, 'test_db.users', request_args_invalid, data_fields)
# This will log warnings about the invalid configuration
response_invalid = dt_invalid.get_rows()
print("⚠️  Configuration warnings logged (check logs)")

# Example 3: Performance warnings
print("\n=== Example 3: Performance Warnings ===")
request_args_perf = {
    "draw": 1,
    "start": 0,
    "length": 10,
    "search": {"value": "john"},  # Global search without text index
    "searchBuilder": {"enabled": True}  # SearchBuilder on unindexed fields
}

dt_perf = DataTables(client, 'test_db.users', request_args_perf, data_fields)
response_perf = dt_perf.get_rows()
print("⚠️  Performance warnings logged (check logs)")

# Example 4: Direct validation usage
print("\n=== Example 4: Direct Validation Usage ===")
from mongo_datatables.config_validator import ConfigValidator

validator = ConfigValidator(collection, data_fields)

# Validate ColReorder configuration
colreorder_config = {"order": [0, 1]}  # Invalid: too few columns
result = validator.validate_colreorder_config(colreorder_config)

print(f"Valid: {result.is_valid}")
print(f"Errors: {result.errors}")
print(f"Warnings: {result.warnings}")

# Validate performance
perf_result = validator.validate_performance({"search": {"value": "test"}})
print(f"Performance warnings: {perf_result.warnings}")

print("\n=== Validation System Benefits ===")
print("✓ Provides helpful error messages for configuration issues")
print("✓ Warns about performance problems (missing indexes, large datasets)")
print("✓ Maintains full backward compatibility")
print("✓ Enhances developer experience without breaking existing code")
print("✓ Structured validation results with technical details")