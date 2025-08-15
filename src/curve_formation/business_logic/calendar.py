"""Business calendar utilities"""
from datetime import datetime, timedelta
from typing import List, Set, Union

def create_holiday_set(holidays: List[datetime] = None) -> Set[datetime]:
    """Create a set of holiday dates"""
    return set(holidays) if holidays else set()

def is_business_day(date: Union[datetime, str], holidays: Set[datetime] = None) -> bool:
    """Check if date is a business day"""
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")
    
    holidays = holidays or set()
    
    # Weekend check
    if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Holiday check
    if date.date() in {h.date() for h in holidays}:
        return False
    
    return True

def next_business_day(date: Union[datetime, str], holidays: Set[datetime] = None) -> datetime:
    """Get the next business day"""
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")
    
    current_date = date + timedelta(days=1)
    while not is_business_day(current_date, holidays):
        current_date += timedelta(days=1)
    return current_date

def previous_business_day(date: Union[datetime, str], holidays: Set[datetime] = None) -> datetime:
    """Get the previous business day"""
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")
    
    current_date = date - timedelta(days=1)
    while not is_business_day(current_date, holidays):
        current_date -= timedelta(days=1)
    return current_date

def add_business_days(start_date: Union[datetime, str], days: int, holidays: Set[datetime] = None) -> datetime:
    """Add business days to a date"""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    
    holidays = holidays or set()
    current_date = start_date
    days_added = 0
    
    while days_added < days:
        current_date += timedelta(days=1)
        if is_business_day(current_date, holidays):
            days_added += 1
    
    return current_date
