"""Data models for curve formation"""
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field
from datetime import datetime

class Config(BaseModel):
    """Configuration model"""
    data_path: str = Field(description="Path to market data files")
    output_path: str = Field(description="Path to save processed curves")
    curve_types: List[str] = Field(description="List of curve types to process")
    date_range: Optional[tuple[datetime, datetime]] = Field(
        None, description="Optional date range for processing"
    )
    environment: str = Field("dev", description="Environment configuration to use")

class CurvePoint(BaseModel):
    """Single point on a curve"""
    tenor: Union[int, float] = Field(description="Time point on curve")
    value: float = Field(description="Rate/price at this tenor")
    weight: Optional[float] = Field(1.0, description="Optional weight for smoothing")

class CurveData(BaseModel):
    """Curve data container"""
    curve_type: str = Field(description="Type of financial curve")
    curve_date: datetime = Field(description="Reference date for curve")
    points: List[CurvePoint] = Field(description="List of curve points")
    metadata: Dict = Field(default_factory=dict, description="Additional curve metadata")

class Metrics(BaseModel):
    """Quality metrics for curve fitting"""
    mse: float = Field(description="Mean squared error of fit")
    max_error: float = Field(description="Maximum absolute error")
    r_squared: float = Field(description="R-squared value of fit")
    validation_checks: List[str] = Field(
        default_factory=list,
        description="List of validation checks passed"
    )
