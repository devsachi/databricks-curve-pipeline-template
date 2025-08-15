from pyspark.sql import DataFrame
from ..curves import (
    interest_rate_curve,
    fx_curve,
    credit_curve,
    inflation_curve,
    volatility_curve
)
from .combiner import combine_curves
from ..utils.data_quality.validator import validate_input_data

def process_all_curves(spark, input_dfs, config):
    """Process all curve types from input data"""
    # Validate inputs
    validate_input_data(input_dfs, config)
    
    # Generate each curve
    curves = [
        interest_rate_curve.generate(spark, input_dfs["rates"], config),
        fx_curve.generate(spark, input_dfs["fx"], config),
        credit_curve.generate(spark, input_dfs["credit"], config),
        inflation_curve.generate(spark, input_dfs["inflation"], config),
        volatility_curve.generate(spark, input_dfs["volatility"], config)
    ]
    
    # Combine curves by monthly date
    return combine_curves(spark, curves, "monthlydate")
