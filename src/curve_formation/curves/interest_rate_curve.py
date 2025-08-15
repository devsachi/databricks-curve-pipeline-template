from pyspark.sql import functions as F
from ..business_logic import interpolation, day_count

def generate(spark, input_df, config):
    """Generate interest rate curve from input data"""
    params = config["curve_parameters"]
    
    # Apply day count convention
    df_with_dcf = day_count.apply_convention(
        input_df,
        params["default_day_count"],
        "start_date",
        "end_date"
    )
    
    # Interpolate rates at standard tenors
    tenors = [1, 2, 3, 6, 12]  # months
    interpolated = interpolation.cubic_spline(
        spark,
        df_with_dcf,
        "term",
        "rate",
        tenors
    )
    
    # Format output
    return (
        interpolated
        .withColumn("monthlydate", F.date_trunc("month", F.col("valuation_date")))
        .groupBy("monthlydate")
        .pivot("term")
        .agg(F.first("interpolated_rate").alias("rate"))
    )
