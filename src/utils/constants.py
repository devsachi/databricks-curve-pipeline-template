"""Common constants for curve formation pipeline"""
from datetime import datetime
from typing import Dict, Any

# Date formats
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Standard periods
STANDARD_TENORS = [1, 2, 3, 5, 7, 10]  # Years
STANDARD_DELTAS = [-25, -10, 0, 10, 25]  # Delta points
STANDARD_STRIKES = [0.8, 0.9, 1.0, 1.1, 1.2]  # Strike ratios

# Default configurations
DEFAULT_CURVE_CONFIG: Dict[str, Any] = {
    "smoothing_method": "savgol",
    "smoothing_params": {
        "window_length": 5,
        "polyorder": 2
    },
    "interpolation_method": "cubic_spline"
}

# Error messages
ERR_MISSING_CONFIG = "Missing required configuration: {}"
ERR_INVALID_DATA = "Invalid data format: {}"
ERR_CALCULATION = "Error in calculation: {}"
