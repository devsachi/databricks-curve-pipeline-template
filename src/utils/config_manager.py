"""Configuration management utilities"""
from typing import Dict, Any, Optional
import os
import yaml
from .logging_utils import get_logger

logger = get_logger(__name__)

def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config from {file_path}: {str(e)}")
        raise

def get_env_config(env: str) -> Dict[str, Any]:
    """Get environment-specific configuration"""
    config_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'configs')
    config_file = os.path.join(config_dir, f"{env}.yaml")
    
    if not os.path.exists(config_file):
        raise ValueError(f"Configuration file not found for environment: {env}")
    
    return load_yaml_config(config_file)

def merge_configs(
    base_config: Dict[str, Any],
    override_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge two configurations with override taking precedence"""
    merged = base_config.copy()
    
    for key, value in override_config.items():
        if (
            key in merged and 
            isinstance(merged[key], dict) and 
            isinstance(value, dict)
        ):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    
    return merged

def get_config(
    env: str,
    override_path: Optional[str] = None
) -> Dict[str, Any]:
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
