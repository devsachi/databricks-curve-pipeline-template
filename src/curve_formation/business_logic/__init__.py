"""Business logic functions for curve formation"""
from .calendar import (
    adjust_date,
    calculate_date_range,
    generate_schedule
)
from .day_count import (
    calculate_day_count_fraction,
    calculate_accrued_days,
    apply_day_count_convention
)
from .interpolation import (
    interpolate_curve,
    extrapolate_curve,
    apply_interpolation_method
)
from .smoothing import (
    smooth_curve,
    apply_smoothing_method,
    calculate_smoothing_parameters
)

__all__ = [
    # Calendar functions
    'adjust_date',
    'calculate_date_range',
    'generate_schedule',
    
    # Day count functions
    'calculate_day_count_fraction',
    'calculate_accrued_days',
    'apply_day_count_convention',
    
    # Interpolation functions
    'interpolate_curve',
    'extrapolate_curve',
    'apply_interpolation_method',
    
    # Smoothing functions
    'smooth_curve',
    'apply_smoothing_method',
    'calculate_smoothing_parameters'
]
