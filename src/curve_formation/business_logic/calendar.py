# Business calendar utilities
from datetime import datetime, timedelta
from typing import List, Set

def create_holiday_set(holidays: List[datetime] = None) -> Set[datetime]:
    """Create a set of holiday dates"""
    return set(holidays) if holidays else set()

def is_business_day(date: datetime, holidays: Set[datetime]) -> bool:
    """Check if date is a business day"""
    # Weekend check
    if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Holiday check
    if date.date() in {h.date() for h in holidays}:
        return False
    
    return True

def add_business_days(start_date: datetime, days: int, holidays: Set[datetime]) -> datetime:
    """Add business days to a date"""
    current_date = start_date
    days_added = 0
    
    while days_added < days:
        current_date += timedelta(days=1)
        if is_business_day(current_date, holidays):
            days_added += 1
    
    return current_date
