"""Inflation curve calculation functions"""
from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession, functions as F
from ..business_logic import interpolation
from .base_curve import prepare_curve_input

def get_standard_forward_years() -> List[int]:
    """Get standard forward periods in years"""
    return [1, 2, 3, 5, 7, 10]

def calculate_yoy_rates(df: DataFrame) -> DataFrame:
    """Calculate year-on-year inflation rates"""
    return (
        df.withColumn(
            "yoy_rate",
            (F.col("index_value") / F.lag("index_value", 1).over(
                F.Window.partitionBy("series_id").orderBy("date")
            ) - 1) * 100
        )
        .filter(F.col("yoy_rate").isNotNull())
    )

def format_curve_output(df: DataFrame) -> DataFrame:
    """Format curve output with pivot and aggregation"""
    return (
        df.withColumn("monthlydate", F.date_trunc("month", F.col("valuation_date")))
        .groupBy("monthlydate", "index_type")
        .pivot("forward_years")
        .agg(F.first("interpolated_yoy_rate").alias("rate"))
    )

def calculate_inflation_curve(
    df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Calculate inflation rate curve"""
    # Prepare input data
    clean_df = prepare_curve_input(df, config)
    
    # Calculate year-on-year rates
    yoy_rates = calculate_yoy_rates(clean_df)
    
    # Interpolate at standard forward periods
    interpolated = interpolation.cubic_spline(
        df.sparkSession,
        yoy_rates,
        "forward_years",
        "yoy_rate",
        get_standard_forward_years()
    )
    
    # Format output
    return format_curve_output(interpolated)

def generate(
    spark: SparkSession,
    input_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Generate inflation curve from input data"""
    return calculate_inflation_curve(input_df, config)
