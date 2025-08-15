"""Volatility curve calculation functions"""
from typing import Dict, Any, List, TYPE_CHECKING, Union
from datetime import datetime

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    import pyspark.sql.functions as F
else:
    try:
        from pyspark.sql import DataFrame, SparkSession
        import pyspark.sql.functions as F
    except ImportError:
        DataFrame = SparkSession = F = None

from ..business_logic import interpolation, smoothing
from ..core.utils import standardize_curve_output

def prepare_vol_data(df: DataFrame) -> DataFrame:
    """Prepare volatility data for curve calculation"""
    return (df
        .withColumn("moneyness", 
            F.col("strike_price") / F.col("forward_price"))
        .withColumn("time_to_expiry", 
            F.datediff(F.col("expiry_date"), F.col("trade_date")) / 365.0)
    )

def calculate_implied_vol(
    df: DataFrame,
    method: str = "black_scholes"
) -> DataFrame:
    """Calculate implied volatility using specified method"""
    if method == "black_scholes":
        return df.withColumn("implied_vol",
            F.sqrt(F.pow(F.log(F.col("moneyness")), 2) / 
                  (2 * F.col("time_to_expiry"))))
    else:
        raise ValueError(f"Unsupported volatility calculation method: {method}")

def build_vol_surface(
    df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Build volatility surface from market data
    
    Args:
        df: Input DataFrame containing volatility data
        config: Configuration dictionary
        
    Returns:
        DataFrame with interpolated volatility surface
    """
    # Define standard moneyness and tenor points
    moneyness_points = config.get("moneyness_points", [0.8, 0.9, 1.0, 1.1, 1.2])
    tenor_points = config.get("tenor_points", [0.25, 0.5, 1.0, 2.0, 3.0])
    
    # Interpolate across moneyness
    moneyness_interpolated = interpolation.cubic_spline(
        df.sparkSession,
        df,
        "moneyness",
        "implied_vol",
        moneyness_points
    )
    
    # Interpolate across tenors
    surface = interpolation.cubic_spline(
        df.sparkSession, 
        moneyness_interpolated, 
        "time_to_expiry", 
        "interpolated_implied_vol", 
        tenor_points
    )
    
    return surface

def generate(spark: SparkSession, input_df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Generate volatility curve from market data
    
    Args:
        spark: Spark session
        input_df: Input DataFrame containing option market data
        config: Configuration dictionary containing curve parameters
        
    Returns:
        DataFrame containing the calculated volatility surface
    """
    # Prepare volatility data
    prepared_data = prepare_vol_data(input_df)
    
    # Calculate implied volatilities
    vol_data = calculate_implied_vol(
        prepared_data,
        method=config.get("vol_method", "black_scholes")
    )
    
    # Build volatility surface
    vol_surface = build_vol_surface(vol_data, config)
    
    # Apply any smoothing if configured
    if config.get("apply_smoothing", False):
        vol_surface = smoothing.apply_smoothing(
            vol_surface,
            method=config.get("smoothing_method", "savgol"),
            value_col="interpolated_implied_vol",
            params=config.get("smoothing_params", {})
        )
    
    return vol_surface
        "time_to_expiry",
        "implied_vol",
        tenor_points
    )
    
    # Apply smoothing if configured
    if config.get("apply_smoothing", False):
        tenor_interpolated = smoothing.apply_smoothing(
            tenor_interpolated,
            method=config.get("smoothing_method", "savgol"),
            value_col="implied_vol",
            params=config.get("smoothing_params", {})
        )
    
    return tenor_interpolated

def generate_vol_curve(
    spark: SparkSession,
    input_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Generate volatility curve from input data"""
    # Prepare data
    prepared_df = prepare_vol_data(input_df)
    
    # Calculate implied volatilities
    with_vol = calculate_implied_vol(
        prepared_df,
        method=config.get("vol_calculation_method", "black_scholes")
    )
    
    # Build volatility surface
    vol_surface = build_vol_surface(with_vol, config)
    
    # Format output
    return standardize_curve_output(
        vol_surface,
        config.get("output_config", {})
    )
