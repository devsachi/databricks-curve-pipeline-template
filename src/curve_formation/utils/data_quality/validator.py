"""Data quality validation for curve formation pipeline"""
from typing import Dict, List, Optional, Union
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, DateType

from ..models import CurveData, CurvePoint

class DataQualityValidator:
    """Data quality validator for market data and curve inputs"""
    
    def __init__(self):
        self.validation_results = {}
        self.error_messages = []
    
    def validate_market_data(self, df: DataFrame) -> bool:
        """
        Validate market data DataFrame structure and content
        
        Args:
            df: Market data DataFrame with required columns
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        # Required columns
        required_cols = {
            'curve_type': StringType(),
            'tenor': DoubleType(),
            'rate': DoubleType(),
            'quote_date': DateType()
        }
        
        # Check column presence and types
        for col, dtype in required_cols.items():
            if col not in df.columns:
                self.error_messages.append(f"Missing required column: {col}")
                return False
            
            actual_type = str(df.schema[col].dataType)
            expected_type = str(dtype)
            if actual_type != expected_type:
                self.error_messages.append(
                    f"Invalid type for {col}: expected {expected_type}, got {actual_type}"
                )
                return False
        
        # Check for nulls in critical columns
        null_checks = df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in required_cols.keys()
        ]).collect()[0]
        
        for col in required_cols:
            if null_checks[col] > 0:
                self.error_messages.append(
                    f"Found {null_checks[col]} null values in column: {col}"
                )
                return False
        
        # Check rate values are reasonable
        rate_stats = df.select(
            F.min('rate').alias('min_rate'),
            F.max('rate').alias('max_rate')
        ).collect()[0]
        
        if rate_stats.min_rate < -100 or rate_stats.max_rate > 100:
            self.error_messages.append(
                f"Rate values outside reasonable range: [{rate_stats.min_rate}, {rate_stats.max_rate}]"
            )
            return False
        
        # All checks passed
        self.validation_results['market_data'] = "PASS"
        return True
    
    def validate_curve_data(self, curve: CurveData) -> bool:
        """
        Validate curve data structure and content
        
        Args:
            curve: CurveData object to validate
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        # Check curve type
        valid_types = {'credit', 'fx', 'inflation', 'interest_rate', 'volatility'}
        if curve.curve_type not in valid_types:
            self.error_messages.append(
                f"Invalid curve type: {curve.curve_type}. Must be one of {valid_types}"
            )
            return False
        
        # Check curve date
        if curve.curve_date > datetime.now():
            self.error_messages.append(
                f"Curve date {curve.curve_date} is in the future"
            )
            return False
        
        # Check curve points
        if not curve.points:
            self.error_messages.append("Curve must have at least one point")
            return False
        
        # Check tenor ordering
        tenors = [p.tenor for p in curve.points]
        if not all(tenors[i] <= tenors[i+1] for i in range(len(tenors)-1)):
            self.error_messages.append("Tenors must be in ascending order")
            return False
        
        # Check for duplicate tenors
        if len(set(tenors)) != len(tenors):
            self.error_messages.append("Found duplicate tenors")
            return False
        
        # Check value ranges based on curve type
        for point in curve.points:
            if not self._validate_curve_point(point, curve.curve_type):
                return False
        
        # All checks passed
        self.validation_results[f'curve_{curve.curve_type}'] = "PASS"
        return True
    
    def _validate_curve_point(self, point: CurvePoint, curve_type: str) -> bool:
        """Validate individual curve point based on curve type"""
        if point.tenor < 0:
            self.error_messages.append(f"Invalid negative tenor: {point.tenor}")
            return False
            
        if point.weight is not None and (point.weight < 0 or point.weight > 1):
            self.error_messages.append(
                f"Invalid weight {point.weight} for tenor {point.tenor}. Must be between 0 and 1"
            )
            return False
        
        # Value range checks by curve type
        if curve_type == 'interest_rate':
            if point.value < -10 or point.value > 30:  # Reasonable IR range in %
                self.error_messages.append(
                    f"Interest rate {point.value} outside reasonable range for tenor {point.tenor}"
                )
                return False
                
        elif curve_type == 'fx':
            if point.value <= 0:  # FX rates must be positive
                self.error_messages.append(
                    f"Invalid FX rate {point.value} for tenor {point.tenor}"
                )
                return False
                
        elif curve_type == 'volatility':
            if point.value < 0 or point.value > 200:  # Vol in % terms
                self.error_messages.append(
                    f"Volatility {point.value} outside reasonable range for tenor {point.tenor}"
                )
                return False
        
        return True
    
    def get_validation_report(self) -> Dict[str, Union[Dict[str, str], List[str]]]:
        """Get validation results and error messages"""
        return {
            'results': self.validation_results,
            'errors': self.error_messages
        }
    
    def clear_validation_state(self):
        """Clear previous validation results and errors"""
        self.validation_results = {}
        self.error_messages = []
