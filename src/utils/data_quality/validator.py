"""Data validation functions for curve formation"""
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..logging_utils import get_logger
from . import checks

logger = get_logger(__name__)

def validate_schema(
    df: DataFrame,
    required_columns: List[str],
    column_types: Dict[str, str]
) -> bool:
    """Validate DataFrame schema against requirements"""
    logger.info(f"Validating schema for {len(required_columns)} required columns")
    
    if not checks.check_missing_values(df, required_columns):
        logger.error("Missing required columns")
        return False
        
    schema_dict = {f.name: f.dataType.typeName() for f in df.schema.fields}
    for col, expected_type in column_types.items():
        if col in df.columns and schema_dict[col] != expected_type:
            logger.error(f"Invalid type for column {col}")
            return False
            
    return True

def validate_date_range(
    df: DataFrame,
    date_col: str,
    min_date: Optional[datetime] = None,
    max_date: Optional[datetime] = None
) -> bool:
    """Validate date range in DataFrame"""
    logger.info(f"Validating date range for column {date_col}")
    
    if date_col not in df.columns:
        logger.error(f"Date column {date_col} not found")
        return False
        
    date_stats = df.agg(
        F.min(date_col).alias("min_date"),
        F.max(date_col).alias("max_date")
    ).collect()[0]
    
    if min_date and date_stats.min_date < min_date:
        logger.error(f"Found dates before minimum date {min_date}")
        return False
    if max_date and date_stats.max_date > max_date:
        logger.error(f"Found dates after maximum date {max_date}")
        return False
        
    return True

def validate_data_quality(df: DataFrame, config: Dict[str, Any]) -> bool:
    """
    Validate DataFrame against all configured data quality checks

    Args:
        df: Spark DataFrame to validate
        config: Dictionary containing validation configuration:
            - required_columns: List of required column names
            - key_columns: List of columns that should be unique
            - range_checks: List of dicts with {column, min, max} for value range validation
            - monotonicity_checks: List of dicts with {time_column, value_column, ascending}
            - anomaly_checks: List of dicts with {column, n_std} for outlier detection
            - continuity_checks: List of dicts with {date_column, interval} for date gaps
            
    Returns:
        bool: True if all validations pass, False otherwise
    """
    try:
        logger.info("Starting data quality validation")
        
        # Required columns check
        if not checks.check_missing_values(df, config["required_columns"]):
            logger.error("Missing required columns")
            return False
                
        # Duplicate check
        if not checks.check_duplicates(df, config["key_columns"]):
            logger.error("Duplicate records found")
            return False
            
        # Value range checks
        for check_config in config.get("range_checks", []):
            if not checks.check_value_ranges(
                df, 
                check_config["column"],
                check_config["min"],
                check_config["max"]
            ):
                logger.error(f"Values out of range for {check_config['column']}")
                return False
            
        # Monotonicity checks  
        for check_config in config.get("monotonicity_checks", []):
            if not checks.check_monotonicity(
                df,
                check_config["time_column"],
                check_config["value_column"],
                check_config.get("ascending", True)
            ):
                logger.error(
                    f"Non-monotonic values in {check_config['value_column']}"
                )
                return False
                
        # Statistical anomaly checks
        for check_config in config.get("anomaly_checks", []):
            if not checks.check_statistical_anomalies(
                df,
                check_config["column"],
                check_config.get("n_std", 3)
            ):
                logger.error(
                    f"Statistical anomalies found in {check_config['column']}"
                )
                return False
                
        # Date continuity checks
        for check_config in config.get("continuity_checks", []):
            if not checks.check_date_continuity(
                df,
                check_config["date_column"],
                check_config.get("interval", "1 day")
            ):
                logger.error(
                    f"Date gaps found in {check_config['date_column']}"
                )
                return False
            
        logger.info("All data quality validations passed")
        return True
            
    except Exception as e:
        logger.error(f"Error during data quality validation: {str(e)}")
        raise

def validate_market_data(df: DataFrame, config: Dict[str, Any]) -> bool:
    """
    Specialized validation for market data inputs
    
    Combines schema, date range, and data quality validations
    """
    logger.info("Starting market data validation")
    
    # Schema validation
    if not validate_schema(
        df,
        config.get('required_columns', []),
        config.get('column_types', {})
    ):
        return False
    
    # Date range validation if configured
    if 'date_column' in config:
        if not validate_date_range(
            df,
            config['date_column'],
            config.get('min_date'),
            config.get('max_date')
        ):
            return False
    
    # Data quality validation
    if not validate_data_quality(df, config):
        return False
    
    # Custom validations from config
    for validation in config.get('custom_validations', []):
        if not validation(df):
            logger.error(f"Custom validation failed: {validation.__name__}")
            return False
    
    logger.info("Market data validation successful")
    return True
