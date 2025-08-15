"""Data quality check functions"""
from typing import List, Union, Dict, Any
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *
import numpy as np

def check_missing_values(df: DataFrame, columns: List[str]) -> bool:
    """Check for missing values in specified columns"""
    return all(
        df.filter(F.col(col).isNull()).count() == 0
        for col in columns
    )

def check_value_ranges(
    df: DataFrame,
    column: str,
    min_value: Union[int, float],
    max_value: Union[int, float]
) -> bool:
    """Check if values are within specified range"""
    return df.filter(
        (F.col(column) < min_value) | (F.col(column) > max_value)
    ).count() == 0

def check_duplicates(df: DataFrame, key_columns: List[str]) -> bool:
    """Check for duplicate records"""
    return df.count() == df.dropDuplicates(key_columns).count()

def check_monotonicity(
    df: DataFrame,
    time_column: str,
    value_column: str,
    ascending: bool = True
) -> bool:
    """Check if values are monotonic"""
    window = F.Window.orderBy(time_column)
    check = df.withColumn(
        "is_monotonic",
        F.when(ascending,
              F.col(value_column) >= F.lag(value_column).over(window))
        .otherwise(F.col(value_column) <= F.lag(value_column).over(window))
    )
    return check.filter(~F.col("is_monotonic")).count() == 0

def calculate_zscore(
    df: DataFrame,
    column: str
) -> DataFrame:
    """Calculate z-score for a column"""
    stats = df.select(
        F.mean(column).alias("mean"),
        F.stddev(column).alias("stddev")
    ).collect()[0]
    
    return df.withColumn(
        "zscore",
        F.abs((F.col(column) - stats.mean) / stats.stddev)
    )

def check_statistical_anomalies(
    df: DataFrame,
    column: str,
    n_std: float = 3
) -> bool:
    """Check for statistical anomalies using z-score"""
    zscore_df = calculate_zscore(df, column)
    return zscore_df.filter(F.col("zscore") > n_std).count() == 0

def check_data_quality(
    df: DataFrame,
    config: Dict[str, Any]
) -> Dict[str, bool]:
    """Run all configured data quality checks"""
    results = {}
    
    # Check missing values
    if "required_columns" in config:
        results["missing_values"] = check_missing_values(
            df, config["required_columns"]
        )
    
    # Check value ranges
    for col, range_config in config.get("value_ranges", {}).items():
        results[f"{col}_range"] = check_value_ranges(
            df,
            col,
            range_config["min"],
            range_config["max"]
        )
    
    # Check duplicates
    if "unique_keys" in config:
        results["duplicates"] = check_duplicates(
            df, config["unique_keys"]
        )
    
    # Check monotonicity
    for check in config.get("monotonicity_checks", []):
        results[f"{check['value_col']}_monotonic"] = check_monotonicity(
            df,
            check["time_col"],
            check["value_col"],
            check.get("ascending", True)
        )
    
    # Check anomalies
    for col, anomaly_config in config.get("anomaly_checks", {}).items():
        results[f"{col}_anomalies"] = check_statistical_anomalies(
            df,
            col,
            anomaly_config.get("n_std", 3)
        )
    
    return results
        )
        return zscore_df.filter(F.col("zscore") > n_std).count() == 0
    
def check_date_continuity(
    df: DataFrame,
    date_column: str,
    expected_interval: str = "1 day"
) -> bool:
    """
    Pure function to check for gaps in date sequence

    Args:
        df: DataFrame containing date column
        date_column: Name of column containing dates
        expected_interval: Expected interval between dates (e.g., "1 day", "1 month")

    Returns:
        bool: True if no gaps found, False otherwise
    """
    return df.select(
        F.count(F.when(
            F.datediff(
                F.lead(date_column).over(F.Window.orderBy(date_column)),
                F.col(date_column)
            ) > F.expr(f"interval {expected_interval}"),
            1
        )).alias("gaps")
    ).collect()[0].gaps == 0
