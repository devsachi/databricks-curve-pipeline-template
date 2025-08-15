from pyspark.sql import functions as F
from pyspark.sql.types import *
import numpy as np

class DataQualityChecks:
    """Data quality check implementations"""
    
    @staticmethod
    def check_missing_values(df, columns):
        """Check for missing values in specified columns"""
        return all(
            df.filter(F.col(col).isNull()).count() == 0
            for col in columns
        )
    
    @staticmethod
    def check_value_ranges(df, column, min_value, max_value):
        """Check if values are within specified range"""
        return df.filter(
            (F.col(column) < min_value) | (F.col(column) > max_value)
        ).count() == 0
    
    @staticmethod
    def check_duplicates(df, key_columns):
        """Check for duplicate records"""
        return df.count() == df.dropDuplicates(key_columns).count()
    
    @staticmethod
    def check_monotonicity(df, time_column, value_column, ascending=True):
        """Check if values are monotonic"""
        window = F.Window.orderBy(time_column)
        check = df.withColumn(
            "is_monotonic",
            F.when(ascending,
                  F.col(value_column) >= F.lag(value_column).over(window))
            .otherwise(F.col(value_column) <= F.lag(value_column).over(window))
        )
        return check.filter(~F.col("is_monotonic")).count() == 0
    
    @staticmethod
    def check_statistical_anomalies(df, column, n_std=3):
        """Check for statistical anomalies using z-score"""
        stats = df.select(
            F.mean(column).alias("mean"),
            F.stddev(column).alias("stddev")
        ).collect()[0]
        
        zscore_df = df.withColumn(
            "zscore",
            F.abs((F.col(column) - stats.mean) / stats.stddev)
        )
        return zscore_df.filter(F.col("zscore") > n_std).count() == 0
    
    @staticmethod
    def check_date_continuity(df, date_column, expected_interval="1 day"):
        """Check for gaps in date sequence"""
        return df.select(
            F.count(F.when(
                F.datediff(
                    F.lead(date_column).over(F.Window.orderBy(date_column)),
                    F.col(date_column)
                ) > F.expr(f"interval {expected_interval}"),
                1
            )).alias("gaps")
        ).collect()[0].gaps == 0
