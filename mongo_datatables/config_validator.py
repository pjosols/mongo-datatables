"""Configuration validation system for mongo-datatables extensions."""

import logging
from typing import Dict, List, Any, Optional, Tuple
from pymongo.collection import Collection

logger = logging.getLogger(__name__)


class ValidationResult:
    """Structured validation result with user-friendly messages."""
    
    def __init__(self, is_valid: bool = True):
        self.is_valid = is_valid
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.technical_details: List[str] = []
    
    def add_error(self, message: str, technical_detail: str = None):
        """Add validation error."""
        self.is_valid = False
        self.errors.append(message)
        if technical_detail:
            self.technical_details.append(technical_detail)
    
    def add_warning(self, message: str, technical_detail: str = None):
        """Add validation warning."""
        self.warnings.append(message)
        if technical_detail:
            self.technical_details.append(technical_detail)


class ConfigValidator:
    """Validates DataTables extension configurations."""
    
    def __init__(self, collection: Collection, data_fields: List[Any]):
        self.collection = collection
        self.data_fields = data_fields
        self._field_names = {field.name for field in data_fields}
    
    def validate_colreorder_config(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate ColReorder extension configuration."""
        result = ValidationResult()
        
        if not config:
            return result
            
        # Validate order array
        if "order" in config:
            order = config["order"]
            if not isinstance(order, list):
                result.add_error("ColReorder order must be a list", f"Got {type(order)}")
            elif len(order) != len(self.data_fields):
                result.add_error(
                    f"ColReorder order length ({len(order)}) doesn't match columns ({len(self.data_fields)})",
                    "Order array must contain all column indices"
                )
        
        return result
    
    def validate_searchbuilder_config(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate SearchBuilder extension configuration."""
        result = ValidationResult()
        
        if not config:
            return result
            
        # Check for indexed fields for performance
        indexed_fields = self._get_indexed_fields()
        searchable_fields = [f.name for f in self.data_fields if f.data_type in ["string", "number", "date"]]
        
        unindexed_searchable = set(searchable_fields) - indexed_fields
        if unindexed_searchable:
            result.add_warning(
                f"SearchBuilder may be slow on unindexed fields: {', '.join(unindexed_searchable)}",
                "Consider adding indexes for better performance"
            )
        
        return result
    
    def validate_fixedcolumns_config(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate FixedColumns extension configuration."""
        result = ValidationResult()
        
        if not config:
            return result
            
        left = config.get("left", 0)
        right = config.get("right", 0)
        total_cols = len(self.data_fields)
        
        if left + right >= total_cols:
            result.add_error(
                f"Fixed columns ({left} left + {right} right) exceed total columns ({total_cols})",
                "Leave at least one column unfixed"
            )
        
        return result
    
    def validate_responsive_config(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate Responsive extension configuration."""
        result = ValidationResult()
        
        if not config:
            return result
            
        # Check for potential conflicts with FixedColumns
        if "breakpoints" in config and len(self.data_fields) > 10:
            result.add_warning(
                f"Responsive with {len(self.data_fields)} columns may cause layout issues",
                "Consider reducing columns or using column priorities"
            )
        
        return result
    
    def validate_performance(self, request_args: Dict[str, Any]) -> ValidationResult:
        """Validate configuration for performance issues."""
        result = ValidationResult()
        
        # Check dataset size
        try:
            doc_count = self.collection.estimated_document_count()
            if doc_count > 100000:
                result.add_warning(
                    f"Large dataset ({doc_count:,} documents) detected",
                    "Consider using indexes and limiting result sets"
                )
        except Exception as e:
            logger.debug(f"Could not estimate document count: {e}")
        
        # Check for missing text index on searchable fields
        if request_args.get("search", {}).get("value"):
            if not self._has_text_index():
                result.add_warning(
                    "Global search without text index may be slow",
                    "Consider creating a text index for better search performance"
                )
        
        return result
    
    def _get_indexed_fields(self) -> set:
        """Get set of indexed field names."""
        try:
            indexes = self.collection.list_indexes()
            indexed_fields = set()
            for index in indexes:
                for field_name in index.get("key", {}):
                    indexed_fields.add(field_name)
            return indexed_fields
        except Exception as e:
            logger.debug(f"Could not get index info: {e}")
            return set()
    
    def _has_text_index(self) -> bool:
        """Check if collection has a text index."""
        try:
            indexes = self.collection.list_indexes()
            for index in indexes:
                if any(v == "text" for v in index.get("key", {}).values()):
                    return True
        except Exception as e:
            logger.debug(f"Could not check text index: {e}")
        return False