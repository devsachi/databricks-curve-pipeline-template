"""FX volatility curve calculation functions"""
from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession, functions as F
from ..business_logic import interpolation, smoothing
from .base_curve import prepare_curve_input

def get_standard_deltas() -> List[int]:
    """Get standard delta points for FX volatility"""
    return [-25, -10, 0, 10, 25]

def prepare_vol_data(df: DataFrame) -> DataFrame:
    """Prepare volatility data for curve calculation"""
    return df.withColumn(
        "delta_point",
        F.when(F.col("option_type") == "PUT", -F.col("delta"))
         .otherwise(F.col("delta"))
    )

def format_vol_curve(df: DataFrame) -> DataFrame:
    """Format volatility curve output"""
    return (
        df.withColumn("monthlydate", F.date_trunc("month", F.col("valuation_date")))
        .withColumn("tenor", F.col("tenor_months"))
        .groupBy("monthlydate", "tenor")
        .pivot("delta")
        .agg(F.first("interpolated_smoothed_vol").alias("volatility"))
    )

def calculate_fx_vol_curve(
    df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Calculate FX volatility curve"""
    # Prepare input data
    clean_df = prepare_curve_input(df, config)
    prepared_df = prepare_vol_data(clean_df)
    
    # Apply volatility smoothing
    smoothed = smoothing.apply_smoothing(
        prepared_df,
        method=config.get("smoothing_method", "savgol"),
        value_col="volatility",
        params=config.get("smoothing_params", {})
    )
    
    # Interpolate at standard deltas
    interpolated = interpolation.cubic_spline(
        df.sparkSession,
        smoothed,
        "delta",
        "smoothed_volatility",
        get_standard_deltas()
    )
    
    # Format output
    return format_vol_curve(interpolated)

def generate(
    spark: SparkSession,
    input_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Generate FX volatility curve from input data"""
    return calculate_fx_vol_curve(input_df, config)
