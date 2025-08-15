from pyspark.sql import functions as F
from ..business_logic import interpolation, day_count
from .base_curve import BaseCurve

class CreditCurve(BaseCurve):
    """Credit Spread Curve Implementation"""
    
    def calculate(self, input_df):
        """Calculate credit spread curve"""
        # Prepare input data
        clean_df = self._prepare_input(input_df)
        
        # Apply day count convention
        df_with_dcf = day_count.apply_convention(
            clean_df,
            self.config.curve_parameters.default_day_count,
            "start_date",
            "end_date"
        )
        
        # Interpolate at standard tenors
        standard_tenors = [1, 2, 3, 5, 7, 10]  # Years
        interpolated = interpolation.cubic_spline(
            input_df.sparkSession,
            df_with_dcf,
            "term_years",
            "spread_bps",
            standard_tenors
        )
        
        # Add credit rating grouping
        with_rating = interpolated.withColumn(
            "rating_group",
            F.when(F.col("rating").isin(["AAA", "AA+", "AA", "AA-"]), "AA+")
             .when(F.col("rating").isin(["A+", "A", "A-"]), "A")
             .when(F.col("rating").isin(["BBB+", "BBB", "BBB-"]), "BBB")
             .otherwise("Other")
        )
        
        # Format output
        return (
            with_rating
            .withColumn("monthlydate", F.date_trunc("month", F.col("valuation_date")))
            .groupBy("monthlydate", "rating_group")
            .pivot("term_years")
            .agg(F.first("interpolated_spread_bps").alias("spread"))
        )

def generate(spark, input_df, config):
    """Generate credit spread curve from input data"""
    curve = CreditCurve(config)
    return curve.calculate(input_df)
