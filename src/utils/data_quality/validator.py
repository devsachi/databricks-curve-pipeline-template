from typing import List, Dict, Any
from .checks import DataQualityChecks
from ..logging_utils import get_logger

logger = get_logger(__name__)

class DataValidator:
    """Data validation orchestrator"""
    
    def __init__(self):
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
