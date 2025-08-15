from pyspark.sql import DataFrame
from typing import Dict, Any
from src.utils.data_quality.validator import validate_data_quality
from src.curve_formation.business_logic import calendar, day_count

def prepare_curve_input(input_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Common input preparation logic for curves"""
    validate_data_quality(input_df, config)
    return input_df.dropDuplicates()

def calculate_base_metrics(df: DataFrame) -> Dict[str, Any]:
    """Calculate common base metrics for curves"""
    return {
        'count': df.count(),
        'min_date': df.agg({'date': 'min'}).collect()[0][0],
        'max_date': df.agg({'date': 'max'}).collect()[0][0]
    }
