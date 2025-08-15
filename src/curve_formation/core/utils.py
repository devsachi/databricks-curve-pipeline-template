"""Common utility functions for curve formation"""
from typing import Dict, Any
from datetime import datetime
from pyspark.sql import DataFrame, functions as F

def validate_date_inputs(df: DataFrame, required_columns: list) -> bool:
    """Validate date-related input columns"""
    return all(col in df.columns for col in required_columns)

def calculate_curve_metrics(df: DataFrame, curve_name: str) -> Dict[str, Any]:
    """Calculate standard curve metrics"""
    metrics = {
        'curve_name': curve_name,
        'points_count': df.count(),
        'calculation_time': datetime.now().isoformat()
    }
    
    for col in ['value', 'tenor']:
        if col in df.columns:
            aggs = df.agg({col: 'min', col: 'max'}).collect()[0]
            metrics[f'min_{col}'] = aggs[0]
            metrics[f'max_{col}'] = aggs[1]
    
    return metrics

def standardize_curve_output(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Apply standard curve output formatting"""
    output_cols = config.get('output_columns', [
        'curve_name', 'tenor', 'value', 'currency', 'trade_date'
    ])
    
    return (
        df.select(*output_cols)
        .orderBy('trade_date', 'tenor')
        .withColumn('last_updated', F.current_timestamp())
    )
