"""Common type definitions for curve formation pipeline"""
from typing import TypeVar, Dict, Any, Union, List
from datetime import datetime
from pyspark.sql import DataFrame

# Type aliases
Config = Dict[str, Any]
CurveData = Dict[str, DataFrame]
Metrics = Dict[str, Any]
TimePoint = Union[datetime, str]
NumericValue = Union[int, float]
MetricsList = List[Metrics]

# Generic types
T = TypeVar('T')
U = TypeVar('U')
