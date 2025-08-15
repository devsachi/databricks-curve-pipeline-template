"""Interpolation functions for curve formation"""
from typing import List, Union
import numpy as np
from scipy.interpolate import CubicSpline
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def prepare_interpolation_input(
    df: DataFrame,
    x_col: str,
    y_col: str
) -> tuple[np.ndarray, np.ndarray]:
    """Prepare input data for interpolation"""
    points = df.select(x_col, y_col).collect()
    x = np.array([p[0] for p in points])
    y = np.array([p[1] for p in points])
    return x, y

def cubic_spline(
    spark: SparkSession,
    df: DataFrame,
    x_col: str,
    y_col: str,
    target_points: List[Union[int, float]]
) -> DataFrame:
    """Perform cubic spline interpolation"""
    # Prepare input data
    x, y = prepare_interpolation_input(df, x_col, y_col)
    
    # Create interpolation function
    cs = CubicSpline(x, y)
    
    # Create target points DataFrame
    target_df = spark.createDataFrame(
        [(float(x),) for x in target_points],
        [x_col]
    )
    
    # Define UDF for interpolation
    @F.udf(returnType=DoubleType())
    def interpolate_udf(x):
        return float(cs(x))
    
    # Apply interpolation
    return target_df.withColumn(
        f"interpolated_{y_col}",
        interpolate_udf(F.col(x_col))
    )

def linear_interpolation(
    spark: SparkSession,
    df: DataFrame,
    x_col: str,
    y_col: str,
    target_points: List[Union[int, float]]
) -> DataFrame:
    """Perform linear interpolation"""
    # Prepare input data
    x, y = prepare_interpolation_input(df, x_col, y_col)
    
    # Create target points DataFrame
    target_df = spark.createDataFrame(
        [(float(x),) for x in target_points],
        [x_col]
    )
    
    # Define UDF for interpolation
    @F.udf(returnType=DoubleType())
    def interpolate_udf(x):
        return float(np.interp(x, x, y))
    
    # Apply interpolation
    return target_df.withColumn(
        f"interpolated_{y_col}",
        interpolate_udf(F.col(x_col))
    )
