from pyspark.sql import functions as F
from ..business_logic import interpolation
from .base_curve import BaseCurve

class InflationCurve(BaseCurve):
    """Inflation Rate Curve Implementation"""
    
    def calculate(self, input_df):
        """Calculate inflation rate curve"""
        # Prepare input data
        clean_df = self._prepare_input(input_df)
        
        # Calculate year-on-year inflation rates
        yoy_rates = (
            clean_df
            .withColumn(
                "yoy_rate",
                (F.col("index_value") / F.lag("index_value", 1).over(
                    F.Window.partitionBy("series_id").orderBy("date")
                ) - 1) * 100
            )
            .filter(F.col("yoy_rate").isNotNull())
        )
        
        # Interpolate at standard forward periods
        forward_years = [1, 2, 3, 5, 7, 10]  # Standard forward periods
        interpolated = interpolation.cubic_spline(
            input_df.sparkSession,
            yoy_rates,
            "forward_years",
            "yoy_rate",
            forward_years
        )
        
        # Format output
        return (
            interpolated
            .withColumn("monthlydate", F.date_trunc("month", F.col("valuation_date")))
            .groupBy("monthlydate", "index_type")
            .pivot("forward_years")
            .agg(F.first("interpolated_yoy_rate").alias("rate"))
        )

def generate(spark, input_df, config):
    """Generate inflation curve from input data"""
    curve = InflationCurve(config)
    return curve.calculate(input_df)
