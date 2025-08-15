from pyspark.sql import functions as F, DataFrame
from typing import Dict, Any, List
from ..business_logic import interpolation, day_count
from .base_curve import prepare_curve_input

def get_standard_tenors() -> List[int]:
    """Get standard tenor points in years"""
    return [1, 2, 3, 5, 7, 10]

def apply_rating_grouping(df: DataFrame) -> DataFrame:
    """Apply credit rating grouping logic"""
    return df.withColumn(
        "rating_group",
        F.when(F.col("rating").isin(["AAA", "AA+", "AA", "AA-"]), "AA+")
         .when(F.col("rating").isin(["A+", "A", "A-"]), "A")
         .when(F.col("rating").isin(["BBB+", "BBB", "BBB-"]), "BBB")
         .otherwise("Other")
    )

def calculate_credit_curve(input_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Calculate credit spread curve"""
    # Prepare input data
    clean_df = prepare_curve_input(input_df, config)
    
    # Apply day count convention
    df_with_dcf = day_count.apply_convention(
        clean_df,
        config.curve_parameters.default_day_count,
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
    """Generate credit spread curve from input data"""
    curve = CreditCurve(config)
    return curve.calculate(input_df)
