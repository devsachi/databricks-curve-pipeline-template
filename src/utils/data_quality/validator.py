"""Data validation functions for curve formation"""
from typing import Dict, Any, List, Callable
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..logging_utils import get_logger

logger = get_logger(__name__)

def validate_schema(
    df: DataFrame,
    required_columns: List[str],
    column_types: Dict[str, str]
) -> bool:
    """Validate DataFrame schema against requirements"""
    logger.info(f"Validating schema for {len(required_columns)} required columns")
    
    # Check required columns
    if not all(col in df.columns for col in required_columns):
        logger.error("Missing required columns")
        return False
        
    # Check column types
    schema_dict = {f.name: f.dataType.typeName() for f in df.schema.fields}
    for col, expected_type in column_types.items():
        if col in df.columns and schema_dict[col] != expected_type:
            logger.error(f"Invalid type for column {col}")
            return False
            
    return True

def validate_date_range(
    df: DataFrame,
    date_col: str,
    min_date: datetime = None,
    max_date: datetime = None
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

def validate_market_data(df: DataFrame, config: Dict[str, Any]) -> bool:
    """Validate market data against configuration"""
    logger.info("Starting market data validation")
    
    # Schema validation
    if not validate_schema(
        df,
        config['required_columns'],
        config.get('column_types', {})
    ):
        return False
    
    # Date range validation
    if not validate_date_range(
        df,
        config['date_column'],
        config.get('min_date'),
        config.get('max_date')
    ):
        return False
    
    # Custom validations from config
    if 'custom_validations' in config:
        for validation in config['custom_validations']:
            if not validation(df):
                logger.error(f"Custom validation failed: {validation.__name__}")
                return False
    
    logger.info("Market data validation successful")
    return True
        self.checks = DataQualityChecks()
        
    def validate_market_data(self, df, config: Dict[str, Any]) -> bool:
        """
        Validate market data against configured quality checks
        
        Args:
            df: Spark DataFrame containing market data
            config: Dictionary containing validation rules
            
        Returns:
            bool: True if all validations pass, False otherwise
        """
        try:
            logger.info("Starting market data validation")
            
            # Required columns check
            if not self.checks.check_missing_values(df, config["required_columns"]):
                logger.error("Missing required columns")
                return False
                
            # Duplicate check
            if not self.checks.check_duplicates(df, config["key_columns"]):
                logger.error("Duplicate records found")
                return False
            
            # Value range checks
            for range_check in config["range_checks"]:
                if not self.checks.check_value_ranges(
                    df, 
                    range_check["column"],
                    range_check["min"],
                    range_check["max"]
                ):
                    logger.error(f"Values out of range for {range_check['column']}")
                    return False
            
            # Monotonicity checks
            for monotonic_check in config["monotonicity_checks"]:
                if not self.checks.check_monotonicity(
                    df,
                    monotonic_check["time_column"],
                    monotonic_check["value_column"],
                    monotonic_check.get("ascending", True)
                ):
                    logger.error(
                        f"Non-monotonic values in {monotonic_check['value_column']}"
                    )
                    return False
            
            # Statistical anomaly checks
            for anomaly_check in config["anomaly_checks"]:
                if not self.checks.check_statistical_anomalies(
                    df,
                    anomaly_check["column"],
                    anomaly_check.get("n_std", 3)
                ):
                    logger.error(
                        f"Statistical anomalies found in {anomaly_check['column']}"
                    )
                    return False
            
            # Date continuity checks
            for continuity_check in config["continuity_checks"]:
                if not self.checks.check_date_continuity(
                    df,
                    continuity_check["date_column"],
                    continuity_check.get("interval", "1 day")
                ):
                    logger.error(
                        f"Date gaps found in {continuity_check['date_column']}"
                    )
                    return False
            
            logger.info("All market data validations passed")
            return True
            
        except Exception as e:
            logger.error(f"Error during market data validation: {str(e)}")
            raise
