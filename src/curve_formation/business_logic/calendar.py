# Business calendar utilities
from datetime import datetime, timedelta
from typing import List, Set

class BusinessCalendar:
    """Business calendar for date calculations"""
    
    def __init__(self, holidays: List[datetime] = None):
        self.holidays = set(holidays) if holidays else set()
    
    def is_business_day(self, date: datetime) -> bool:
        """Check if date is a business day"""
        # Weekend check
        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Holiday check
        if date.date() in {h.date() for h in self.holidays}:
            return False
        
        return True
    
    def add_business_days(self, start_date: datetime, days: int) -> datetime:
        """Add business days to a date"""
        current_date = start_date
        days_added = 0
        
        while days_added < days:
            current_date += timedelta(days=1)
            if self.is_business_day(current_date):
                days_added += 1
        
        return current_date
