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
        fixed_header_params = self.request_args.get("fixedHeader")
        if fixed_header_params is None:
            return None
        
        # Handle boolean false case
        if isinstance(fixed_header_params, bool) and not fixed_header_params:
            return None
            
        config = {}
        
        # Handle boolean configuration (fixedHeader: true)
        if isinstance(fixed_header_params, bool):
            if fixed_header_params:
                config = {"header": True, "footer": False}
        # Handle object configuration (fixedHeader: {header: true, footer: false})
        elif isinstance(fixed_header_params, dict):
            if "header" in fixed_header_params:
                config["header"] = bool(fixed_header_params["header"])
            if "footer" in fixed_header_params:
                config["footer"] = bool(fixed_header_params["footer"])
                
        return config if (config or isinstance(fixed_header_params, dict)) else None

    def parse_responsive_config(self) -> Optional[Dict[str, Any]]:
        """Parse Responsive extension configuration from request parameters."""
        responsive_params = self.request_args.get("responsive")
        if not responsive_params:
            return None
            
        config = {}
        
        # Handle boolean configuration (responsive: true)
        if isinstance(responsive_params, bool):
            if responsive_params:
                config = {"enabled": True}
            else:
                return None
        # Handle object configuration
        elif isinstance(responsive_params, dict):
            # Parse breakpoints configuration
            if "breakpoints" in responsive_params:
                breakpoints = responsive_params["breakpoints"]
                if isinstance(breakpoints, dict):
                    config["breakpoints"] = breakpoints
                    
            # Parse display configuration
            if "display" in responsive_params:
                display = responsive_params["display"]
                if isinstance(display, dict):
                    config["display"] = display
                    
            # Parse column priorities
            if "priorities" in responsive_params:
                priorities = responsive_params["priorities"]
                if isinstance(priorities, dict):
                    config["priorities"] = priorities
        
        # Validate configuration
        if self.config_validator:
            validation = self.config_validator.validate_responsive_config(config)
            if validation.warnings:
                logger.info(f"Responsive warnings: {validation.warnings}")
                
        return config if config else None

    def parse_buttons_config(self) -> Optional[Dict[str, Any]]:
        """Parse Buttons extension configuration from request parameters."""
        buttons_params = self.request_args.get("buttons")
        if not buttons_params:
            return None
            
        config = {}
        
        # Handle boolean configuration (buttons: true)
        if isinstance(buttons_params, bool):
            if buttons_params:
                config = {"enabled": True}
            else:
                return None
        # Handle object configuration
        elif isinstance(buttons_params, dict):
            # Parse export configuration
            if "export" in buttons_params:
                export_config = buttons_params["export"]
                if isinstance(export_config, dict):
                    config["export"] = export_config
                    
            # Parse column visibility configuration
            if "colvis" in buttons_params:
                colvis_config = buttons_params["colvis"]
                if isinstance(colvis_config, dict):
                    config["colvis"] = colvis_config
                    
            # Parse print configuration
            if "print" in buttons_params:
                print_config = buttons_params["print"]
                if isinstance(print_config, dict):
                    config["print"] = print_config
                    
            # Parse copy configuration
            if "copy" in buttons_params:
                copy_config = buttons_params["copy"]
                if isinstance(copy_config, dict):
                    config["copy"] = copy_config
                
        return config if config else None

    def parse_select_config(self) -> Optional[Dict[str, Any]]:
        """Parse Select extension configuration from request parameters."""
        select_params = self.request_args.get("select")
        if not select_params:
            return None
            
        # Handle boolean true case (default configuration)
        if select_params is True:
            return {"style": "os"}
            
        config = {}
        
        # Parse selection style
        if isinstance(select_params, dict):
            style = select_params.get("style", "os")
            if style in ["os", "single", "multi", "multi+shift"]:
                config["style"] = style
            else:
                config["style"] = "os"
                
        return config if config else None

    def parse_searchpanes_config(self) -> Optional[Dict[str, Any]]:
        """Parse SearchPanes configuration from request parameters."""
        searchpanes = self.request_args.get("searchPanes")
        if not searchpanes:
            return None
            
        # Handle boolean true case (default configuration)
        if searchpanes is True:
            return {}
            
        config = {}
        
        # Parse columns configuration
        if isinstance(searchpanes, dict):
            if "columns" in searchpanes:
                columns = searchpanes["columns"]
                if isinstance(columns, list):
                    config["columns"] = columns
                    
            # Parse threshold
            if "threshold" in searchpanes:
                try:
                    threshold = float(searchpanes["threshold"])
                    if 0 <= threshold <= 1:
                        config["threshold"] = threshold
                except (ValueError, TypeError):
                    pass
                    
            # Parse other options
            for key in ["cascadePanes", "clear", "container", "dtOpts", "emptyMessage", "hideCount", "i18n", "layout", "orderable", "preSelect", "viewCount"]:
                if key in searchpanes:
                    config[key] = searchpanes[key]
        
        # Validate configuration
        if self.config_validator:
            validation = self.config_validator.validate_searchpanes_config(config)
            if not validation.is_valid:
                logger.warning(f"SearchPanes validation errors: {validation.errors}")
            if validation.warnings:
                logger.info(f"SearchPanes warnings: {validation.warnings}")
                
        return config if config else None

    def parse_rowgroup_config(self) -> Optional[Dict[str, Any]]:
        """Parse RowGroup extension configuration from request parameters."""
        rowgroup_params = self.request_args.get("rowGroup")
        if not rowgroup_params:
            return None
            
        config = {}
        
        # Parse data source for grouping
        if "dataSrc" in rowgroup_params:
            data_src = rowgroup_params["dataSrc"]
            if isinstance(data_src, (str, int)):
                config["dataSrc"] = data_src
                
        # Parse start render function indicator
        if "startRender" in rowgroup_params:
            config["startRender"] = bool(rowgroup_params["startRender"])
            
        # Parse end render function indicator  
        if "endRender" in rowgroup_params:
            config["endRender"] = bool(rowgroup_params["endRender"])
            
        return config if config else None

    def parse_colreorder_config(self) -> Optional[Dict[str, Any]]:
        """Parse ColReorder extension configuration from request parameters."""
        colreorder_params = self.request_args.get("colReorder")
        if not colreorder_params:
            return None
            
        config = {}
        
        # Handle boolean configuration (colReorder: true)
        if isinstance(colreorder_params, bool):
            if colreorder_params:
                config = {"enabled": True}
            else:
                return None
        # Handle object configuration
        elif isinstance(colreorder_params, dict):
            # Parse column order if provided
            if "order" in colreorder_params:
                order = colreorder_params["order"]
                if isinstance(order, list):
                    config["order"] = order
                    
            # Parse realtime configuration
            if "realtime" in colreorder_params:
                config["realtime"] = bool(colreorder_params["realtime"])
        
        # Validate configuration
        if self.config_validator:
            validation = self.config_validator.validate_colreorder_config(config)
            if not validation.is_valid:
                logger.warning(f"ColReorder validation errors: {validation.errors}")
            if validation.warnings:
                logger.info(f"ColReorder warnings: {validation.warnings}")
                
        return config if config else None