from pyspark.sql import functions as F, DataFrame
from typing import Dict, Any, List
from ..business_logic import interpolation, day_count
from .base_curve import prepare_curve_input

def get_standard_tenors() -> List[float]:
    """Get standard tenor points in years"""
    return [1.0, 2.0, 3.0, 5.0, 7.0, 10.0]

def apply_rating_grouping(df: DataFrame) -> DataFrame:
    """Apply credit rating grouping logic"""
    return df.withColumn(
        "rating_group",
        F.when(F.col("rating").isin(["AAA", "AA+", "AA", "AA-"]), "AA+")
         .when(F.col("rating").isin(["A+", "A", "A-"]), "A")
         .when(F.col("rating").isin(["BBB+", "BBB", "BBB-"]), "BBB")
         .otherwise("Other")
    )

def calculate_credit_curve(
    input_df: DataFrame,
    config: Dict[str, Dict[str, str]]
) -> DataFrame:
    """Calculate credit spread curve
    
    Args:
        input_df: Input DataFrame containing raw credit spread data
        config: Configuration dictionary containing:
            curve_parameters: Dict with:
                default_day_count: Day count convention to use
    
    Returns:
        DataFrame containing interpolated credit spread curve"""
    # Prepare input data
    clean_df = prepare_curve_input(input_df, config)
    
    # Apply day count convention
    df_with_dcf = day_count.apply_convention(
        clean_df,
        config["curve_parameters"]["default_day_count"],
        "start_date",
        "end_date"
    )
    
    # Interpolate at standard tenors
    interpolated = interpolation.cubic_spline(
        input_df.sparkSession,
        df_with_dcf,
        "term_years",
        "spread_bps",
        get_standard_tenors()
    )
    
    # Add credit rating grouping and format output
    return (
        apply_rating_grouping(interpolated)
        .withColumn("monthlydate", F.date_trunc("month", F.col("valuation_date")))
        .groupBy("monthlydate", "rating_group")
        .pivot("term_years")
        .agg(F.first("interpolated_spread_bps").alias("spread"))
    )

def generate(spark: DataFrame, input_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Generate credit spread curve from input data
    
    Args:
        spark: Spark session
        input_df: Input DataFrame containing raw credit spread data
        config: Configuration dictionary containing:
            curve_parameters: Dict with:
                default_day_count: Day count convention to use
    
    Returns:
        DataFrame containing the calculated credit spread curve"""
    return calculate_credit_curve(input_df, config)
