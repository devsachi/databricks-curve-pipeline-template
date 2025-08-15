"""Day count convention functions for curve calculations"""
from datetime import datetime
from typing import Union
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from .conventions import DayCountConvention

# Export day count conventions
ACT360 = DayCountConvention.ACT360
ACT365 = DayCountConvention.ACT365
THIRTY360 = DayCountConvention.THIRTY360

def get_days_between(date1: datetime, date2: datetime) -> int:
    """Calculate number of days between two dates"""
    return (date2 - date1).days

def calculate_accrual_factor(
    days: int,
    convention: Union[str, DayCountConvention] = ACT365
) -> float:
    """Calculate day count fraction based on convention"""
    conv_str = str(convention) if isinstance(convention, DayCountConvention) else convention
    if conv_str == ACT365:
        return days / 365.0
    elif conv_str == ACT360:
        return days / 360.0
    elif conv_str == THIRTY360:
        return days / 360.0
    else:
        raise ValueError(f"Unsupported day count convention: {convention}")

def apply_convention(
    df: DataFrame,
    convention: Union[str, DayCountConvention],
    start_col: str,
    end_col: str
) -> DataFrame:
    """Apply day count convention to a DataFrame"""
    
    @F.udf(returnType=DoubleType())
    def accrual_factor_udf(start_date: datetime, end_date: datetime) -> float:
        days = get_days_between(start_date, end_date)
        return calculate_accrual_factor(days, convention)
    
    return df.withColumn(
        "day_count_factor",
        accrual_factor_udf(F.col(start_col), F.col(end_col))
    )

def get_year_fraction(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    convention: Union[str, DayCountConvention] = ACT365
) -> float:
    """Calculate year fraction between two dates"""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    days = get_days_between(start_date, end_date)
    return calculate_accrual_factor(days, convention)
