"""
Databricks Curve Formation Pipeline

A functional programming approach to financial curve construction and analytics.
This package provides tools for constructing, analyzing, and managing financial
market curves using Apache Spark and Databricks.

Main Components:
- Curve Formation: Core curve construction and processing
- Business Logic: Market conventions and calculations
- Data Quality: Validation and quality checks
- Utilities: Configuration, logging, and Spark helpers
"""

from curve_formation.core import (
    process_all_curves,
    generate_curve,
    combine_curves,
    write_output
)

from curve_formation.curves import (
    BaseCurve,
    CreditCurve,
    FXCurve,
    InflationCurve,
    InterestRateCurve,
    VolatilityCurve
)

from curve_formation.business_logic.interpolation import (
    cubic_spline as interpolate_curve,
    linear_interpolation,
    log_linear_interpolation
)

from curve_formation.business_logic.smoothing import (
    savitzky_golay as smooth_curve,
    moving_average,
    exponential_smoothing
)

from curve_formation.business_logic.day_count import (
    calculate_accrual_factor as calculate_day_count_fraction,
    ACT360,
    ACT365,
    THIRTY360
)

from curve_formation.business_logic.calendar import (
    add_business_days as adjust_date,
    is_business_day,
    next_business_day,
    previous_business_day
)

from curve_formation.utils.config_manager import ConfigManager as load_config
from curve_formation.utils.logging_utils import LoggerFactory as get_logger
from curve_formation.utils.data_quality.validator import DataQualityValidator as validate_market_data
from curve_formation.utils.exception_handler import handle_exception
from curve_formation.models import Config, CurveData, Metrics

__version__ = "1.0.0"

__all__ = [
    # Core processing
    'process_all_curves',
    'generate_curve',
    'combine_curves',
    'write_output',
    
    # Curve Types
    'BaseCurve',
    'CreditCurve', 
    'FXCurve',
    'InflationCurve',
    'InterestRateCurve',
    'VolatilityCurve',
    
    # Business logic - Interpolation
    'interpolate_curve',      # alias for cubic_spline
    'linear_interpolation',
    'log_linear_interpolation',

    # Business logic - Smoothing
    'smooth_curve',          # alias for savitzky_golay
    'moving_average',
    'exponential_smoothing',

    # Business logic - Day Count
    'calculate_day_count_fraction',  # alias for calculate_accrual_factor
    'ACT360',
    'ACT365',
    'THIRTY360',

    # Business logic - Calendar
    'adjust_date',           # alias for add_business_days
    'is_business_day',
    'next_business_day',
    'previous_business_day',
    
    # Utilities
    'load_config',           # alias for ConfigManager
    'get_logger',            # alias for LoggerFactory
    'validate_market_data',  # alias for DataQualityValidator
    'handle_exception',
    
    # Type definitions
    'Config',
    'CurveData',
    'Metrics',
    
    # Version
    '__version__'
]
