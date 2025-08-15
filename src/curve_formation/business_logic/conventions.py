"""Day count conventions for financial calculations"""
from enum import Enum

class DayCountConvention(str, Enum):
    """Day count conventions"""
    ACT360 = "ACT/360"
    ACT365 = "ACT/365"
    THIRTY360 = "30/360"

    def __str__(self):
        return self.value
