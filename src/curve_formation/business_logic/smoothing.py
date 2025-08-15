"""Smoothing functions for curve formation"""
from typing import List, Union, Optional
import numpy as np
from scipy.signal import savgol_filter
from scipy.interpolate import UnivariateSpline
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, ArrayType

def moving_average(
    df: DataFrame,
    value_col: str,
    window_size: int = 3
) -> DataFrame:
    """Apply simple moving average smoothing"""
    return df.withColumn(
        f"smoothed_{value_col}",
        F.avg(value_col).over(
            F.Window.orderBy("tenor").rowsBetween(-(window_size//2), window_size//2)
        )
    )

def exponential_smoothing(
    df: DataFrame,
    value_col: str,
    alpha: float = 0.3
) -> DataFrame:
    """Apply exponential smoothing"""
    @F.pandas_udf(DoubleType())
    def exp_smooth(values):
        result = np.zeros_like(values)
        result[0] = values[0]
        for i in range(1, len(values)):
            result[i] = alpha * values[i] + (1 - alpha) * result[i-1]
        return result
    
    return df.withColumn(
        f"smoothed_{value_col}",
        exp_smooth(F.col(value_col))
    )

def savitzky_golay(
    df: DataFrame,
    value_col: str,
    window_length: int = 5,
    polyorder: int = 2
) -> DataFrame:
    """Apply Savitzky-Golay smoothing"""
    @F.pandas_udf(DoubleType())
    def savgol_smooth(values):
        return savgol_filter(values, window_length, polyorder)
    
    return df.withColumn(
        f"smoothed_{value_col}",
        savgol_smooth(F.col(value_col))
    )

def spline_smoothing(
    df: DataFrame,
    x_col: str,
    y_col: str,
    smoothing_factor: float = 0.5
) -> DataFrame:
    """Apply spline smoothing"""
    @F.pandas_udf(DoubleType())
    def spline_smooth(x, y):
        spl = UnivariateSpline(x, y, s=smoothing_factor)
        return spl(x)
    
    return df.withColumn(
        f"smoothed_{y_col}",
        spline_smooth(F.col(x_col), F.col(y_col))
    )

def kalman_smoothing(
    df: DataFrame,
    value_col: str,
    measurement_noise: float = 0.1,
    process_noise: float = 0.001
) -> DataFrame:
    """Apply Kalman filter smoothing"""
    @F.pandas_udf(DoubleType())
    def kalman_smooth(values):
        n = len(values)
        # State estimates
        x_hat = np.zeros(n)
        p = np.zeros(n)  # Error estimate
        
        # Initialize
        x_hat[0] = values[0]
        p[0] = 1
        
        # Forward pass
        for i in range(1, n):
            # Predict
            x_hat_minus = x_hat[i-1]
            p_minus = p[i-1] + process_noise
            
            # Update
            k = p_minus / (p_minus + measurement_noise)  # Kalman gain
            x_hat[i] = x_hat_minus + k * (values[i] - x_hat_minus)
            p[i] = (1 - k) * p_minus
            
        return x_hat
    
    return df.withColumn(
        f"smoothed_{value_col}",
        kalman_smooth(F.col(value_col))
    )

def apply_smoothing(
    df: DataFrame,
    method: str,
    value_col: str,
    params: Optional[dict] = None
) -> DataFrame:
    """Apply specified smoothing method with parameters"""
    if params is None:
        params = {}
    
    smoothing_methods = {
        'moving_average': lambda: moving_average(df, value_col, params.get('window_size', 3)),
        'exponential': lambda: exponential_smoothing(df, value_col, params.get('alpha', 0.3)),
        'savgol': lambda: savitzky_golay(
            df, value_col,
            params.get('window_length', 5),
            params.get('polyorder', 2)
        ),
        'spline': lambda: spline_smoothing(
            df,
            params.get('x_col', 'tenor'),
            value_col,
            params.get('smoothing_factor', 0.5)
        ),
        'kalman': lambda: kalman_smoothing(
            df,
            value_col,
            params.get('measurement_noise', 0.1),
            params.get('process_noise', 0.001)
        )
    }
    
    if method not in smoothing_methods:
        raise ValueError(f"Unsupported smoothing method: {method}")
    
    return smoothing_methods[method]()
