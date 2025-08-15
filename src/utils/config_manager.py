"""Configuration management utilities using functional patterns"""
from typing import Dict, Any, Optional
import os
import yaml
from functools import reduce
from .logging_utils import get_logger
from .constants import ERR_MISSING_CONFIG

logger = get_logger(__name__)

def load_yaml_file(file_path: str) -> Dict[str, Any]:
    """
    Pure function to load YAML content from a file
    
    Args:
        file_path: Path to YAML file
        
    Returns:
        Dict containing loaded YAML data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        yaml.YAMLError: If YAML parsing fails
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        return yaml.safe_load(content)
    except Exception as e:
        logger.error(f"Failed to load YAML file {file_path}: {str(e)}")
        raise

def get_env_config_path(env: str) -> str:
    """
    Pure function to construct environment config file path
    
    Args:
        env: Environment name (dev, prod, etc)
        
    Returns:
        Absolute path to config file
        
    Raises:
        ValueError: If environment is invalid
    """
    if not env:
        raise ValueError(ERR_MISSING_CONFIG.format("environment"))
        
    config_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'configs')
    return os.path.join(config_dir, f"{env}.yaml")

def merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure function to merge two dictionaries recursively
    
    Args:
        base: Base dictionary
        override: Override dictionary taking precedence
        
    Returns:
        New merged dictionary without modifying inputs
    """
    merged = base.copy()
    
    for key, value in override.items():
        if (
            key in merged and 
            isinstance(merged[key], dict) and 
            isinstance(value, dict)
        ):
            merged[key] = merge_dict(merged[key], value)
        else:
            merged[key] = value
            
    return merged

def load_config(env: str, override_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration with environment and optional override
    
    Pure function combining base config, environment config,
    and optional override config.
    
    Args:
        env: Environment name (dev, prod, etc)
        override_path: Optional path to override config file
        
    Returns:
        Final merged configuration dictionary
        
    Raises:
        FileNotFoundError: If any required config file is missing
        ValueError: If environment is invalid
    """
    try:
        # Load base config
        base_path = get_env_config_path('base')
        if not os.path.exists(base_path):
            raise FileNotFoundError(ERR_MISSING_CONFIG.format("base config"))
        base_config = load_yaml_file(base_path)
        
        # Load environment config
        env_path = get_env_config_path(env)
        if not os.path.exists(env_path):
            raise FileNotFoundError(ERR_MISSING_CONFIG.format(f"{env} config"))
        env_config = load_yaml_file(env_path)
        
        # Merge configs
        config = merge_dict(base_config, env_config)
        
        # Apply override if provided
        if override_path:
            if not os.path.exists(override_path):
                raise FileNotFoundError(ERR_MISSING_CONFIG.format("override config"))
            override_config = load_yaml_file(override_path)
            config = merge_dict(config, override_config)
            
        return config
        
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise
    """Get complete configuration for specified environment"""
    # Load base configuration
    base_config = load_yaml_config(
        os.path.join(
            os.path.dirname(__file__), 
            '..', '..', 
            'configs', 
            'base.yaml'
        )
    )
    
    # Load environment config
    env_config = get_env_config(env)
    
    # Merge configurations
    config = merge_configs(base_config, env_config)
    
    # Apply additional overrides if specified
    if override_path:
        override_config = load_yaml_config(override_path)
        config = merge_configs(config, override_config)
    
    return config

def validate_config(config: Dict[str, Any], schema: Dict[str, Any]) -> bool:
    """Validate configuration against schema"""
    def _validate_type(value: Any, expected_type: Any) -> bool:
        if expected_type == "list":
            return isinstance(value, list)
        elif expected_type == "dict":
            return isinstance(value, dict)
        elif expected_type == "number":
            return isinstance(value, (int, float))
        elif expected_type == "string":
            return isinstance(value, str)
        elif expected_type == "boolean":
            return isinstance(value, bool)
        return True
    
    for key, spec in schema.items():
        if key not in config:
            if spec.get("required", False):
                logger.error(f"Missing required config key: {key}")
                return False
            continue
        
        if not _validate_type(config[key], spec.get("type")):
            logger.error(f"Invalid type for config key {key}")
            return False
    
    return True
