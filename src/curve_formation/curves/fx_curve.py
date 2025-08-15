from pyspark.sql import functions as F
from ..business_logic import interpolation, smoothing
from .base_curve import BaseCurve

class FXVolatilityCurve(BaseCurve):
    """FX Volatility Curve Implementation"""
    
    def calculate(self, input_df):
        """Calculate FX volatility curve"""
        # Prepare input data
        clean_df = self._prepare_input(input_df)
        
        # Apply volatility smoothing
        smoothed = smoothing.smooth_volatilities(
            clean_df,
            "strike",
            "volatility",
            self.config.curve_parameters.smoothing_params
        )
        
        # Interpolate at standard deltas
        standard_deltas = [-25, -10, 0, 10, 25]  # Standard delta points
        interpolated = interpolation.cubic_spline(
            input_df.sparkSession,
            smoothed,
            "delta",
            "smoothed_vol",
            standard_deltas
        )
        
        # Format output
        return (
            interpolated
            .withColumn("monthlydate", F.date_trunc("month", F.col("valuation_date")))
            .withColumn("tenor", F.col("tenor_months"))
            .groupBy("monthlydate", "tenor")
            .pivot("delta")
            .agg(F.first("interpolated_smoothed_vol").alias("volatility"))
        )

def generate(spark, input_df, config):
    """Generate FX volatility curve from input data"""
    curve = FXVolatilityCurve(config)
    return curve.calculate(input_df)
