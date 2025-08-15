from typing import Dict, List, Any
from pyspark.sql import DataFrame, SparkSession
from ..curves import interest_rate_curve, fx_curve, credit_curve, inflation_curve, volatility_curve
from .utils import calculate_curve_metrics, standardize_curve_output
from curve_formation.utils.data_quality.validator import validate_input_data

def generate_curve(
    spark: SparkSession,
    input_df: DataFrame,
    curve_type: str,
    config: Dict[str, Any]
) -> DataFrame:
    """Generate a specific type of curve"""
    curve_generators = {
        'rates': interest_rate_curve.generate,
        'fx': fx_curve.generate,
        'credit': credit_curve.generate,
        'inflation': inflation_curve.generate,
        'volatility': volatility_curve.generate
    }
    
    if curve_type not in curve_generators:
        raise ValueError(f"Unsupported curve type: {curve_type}")
        
    curve_df = curve_generators[curve_type](spark, input_df, config)
    metrics = calculate_curve_metrics(curve_df, curve_type)
    
    return standardize_curve_output(curve_df, config), metrics

def process_all_curves(
    spark: SparkSession,
    input_dfs: Dict[str, DataFrame],
    config: Dict[str, any]
) -> Dict[str, DataFrame]:
    """Process all curve types from input data"""
    # Validate inputs
    validate_input_data(input_dfs, config)
    
    # Process each curve type
    processed_curves = {}
    all_metrics = []
    
    for curve_type, input_df in input_dfs.items():
        curve_df, metrics = generate_curve(spark, input_df, curve_type, config)
        processed_curves[curve_type] = curve_df
        all_metrics.append(metrics)
    
    # Store metrics if configured
    if config.get('store_metrics', False):
        store_curve_metrics(spark, all_metrics, config)
    
    return processed_curves

def store_curve_metrics(
    spark: SparkSession,
    metrics: List[Dict[str, any]],
    config: Dict[str, any]
) -> None:
    """Store curve generation metrics"""
    if metrics:
        metrics_df = spark.createDataFrame(metrics)
        metrics_df.write.format('delta').mode('append').saveAsTable(
            f"{config['catalog']}.{config['schemas']['monitoring']}.curve_metrics"
        )
