"""Configuration parser for DataTables extensions."""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ConfigParser:
    """Centralized configuration parser for DataTables extensions.
    
    This class consolidates the parsing logic for various DataTables extensions
    to eliminate code duplication and provide consistent configuration handling.
    """
    
    def __init__(self, request_args: Dict[str, Any], config_validator: Any = None):
        """Initialize the ConfigParser.
        
        Args:
            request_args: DataTables request parameters
            config_validator: Optional ConfigValidator instance for validation
        """
        self.request_args = request_args
        self.config_validator = config_validator
    
    def parse_boolean_or_object_config(self, param_name: str, default_config: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Parse configuration that can be boolean or object.
        
        Args:
            param_name: Parameter name in request_args
            default_config: Default configuration when boolean true is provided
            
        Returns:
            Parsed configuration or None if not requested
        """
        params = self.request_args.get(param_name)
        if not params:
            return None
            
        # Handle boolean false case
        if isinstance(params, bool) and not params:
            return None
            
        config = {}
        
        # Handle boolean true case
        if isinstance(params, bool) and params:
            return default_config or {"enabled": True}
            
        # Handle object configuration
        if isinstance(params, dict):
            config = params.copy()
            
        return config if config else None
    
    def parse_fixed_columns_config(self) -> Optional[Dict[str, Any]]:
        """Parse FixedColumns configuration from request parameters."""
        fixed_columns = self.request_args.get("fixedColumns")
        if not fixed_columns:
            return None
            
        config = {}
        
        # Parse left fixed columns
        if "left" in fixed_columns:
            try:
                config["left"] = int(fixed_columns["left"])
            except (ValueError, TypeError):
                config["left"] = 0
                
        # Parse right fixed columns  
        if "right" in fixed_columns:
            try:
                config["right"] = int(fixed_columns["right"])
            except (ValueError, TypeError):
                config["right"] = 0
        
        # Validate configuration
        if self.config_validator:
            validation = self.config_validator.validate_fixedcolumns_config(config)
            if not validation.is_valid:
                logger.warning(f"FixedColumns validation errors: {validation.errors}")
            if validation.warnings:
                logger.info(f"FixedColumns warnings: {validation.warnings}")
                
        return config if config else None

    def parse_fixed_header_config(self) -> Optional[Dict[str, Any]]:
        """Parse FixedHeader extension configuration from request parameters."""
        return self.parse_boolean_or_object_config("fixedHeader", {"header": True, "footer": False})

    def parse_responsive_config(self) -> Optional[Dict[str, Any]]:
        """Parse Responsive extension configuration from request parameters."""
        config = self.parse_boolean_or_object_config("responsive")
        if config and self.config_validator:
            validation = self.config_validator.validate_responsive_config(config)
            if validation.warnings:
                logger.info(f"Responsive warnings: {validation.warnings}")
        return config

    def parse_buttons_config(self) -> Optional[Dict[str, Any]]:
        """Parse Buttons extension configuration from request parameters."""
        return self.parse_boolean_or_object_config("buttons")

    def parse_select_config(self) -> Optional[Dict[str, Any]]:
        """Parse Select extension configuration from request parameters."""
        return self.parse_boolean_or_object_config("select", {"style": "os"})

    def parse_searchpanes_config(self) -> Optional[Dict[str, Any]]:
        """Parse SearchPanes configuration from request parameters."""
        config = self.parse_boolean_or_object_config("searchPanes", {})
        if config and self.config_validator:
            validation = self.config_validator.validate_searchpanes_config(config)
            if not validation.is_valid:
                logger.warning(f"SearchPanes validation errors: {validation.errors}")
            if validation.warnings:
                logger.info(f"SearchPanes warnings: {validation.warnings}")
        return config

    def parse_rowgroup_config(self) -> Optional[Dict[str, Any]]:
        """Parse RowGroup extension configuration from request parameters."""
        return self.parse_boolean_or_object_config("rowGroup")

    def parse_colreorder_config(self) -> Optional[Dict[str, Any]]:
        """Parse ColReorder extension configuration from request parameters."""
        config = self.parse_boolean_or_object_config("colReorder")
        if config and self.config_validator:
            validation = self.config_validator.validate_colreorder_config(config)
            if not validation.is_valid:
                logger.warning(f"ColReorder validation errors: {validation.errors}")
            if validation.warnings:
                logger.info(f"ColReorder warnings: {validation.warnings}")
        return config