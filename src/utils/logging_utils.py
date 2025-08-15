"""Logging utilities for the curve formation pipeline"""
import logging
from typing import Optional
from datetime import datetime

def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_format: Optional[str] = None
) -> logging.Logger:
    """Set up a logger with the specified configuration"""
    if log_format is None:
        log_format = '[%(asctime)s] %(levelname)s - %(name)s - %(message)s'
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(handler)
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """Get or create a logger for the specified name"""
    return setup_logger(name)

def log_execution_time(logger: logging.Logger):
    """Decorator to log function execution time"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            logger.info(f"Starting {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = datetime.now() - start_time
                logger.info(f"Completed {func.__name__} in {execution_time}")
                return result
            except Exception as e:
                execution_time = datetime.now() - start_time
                logger.error(f"Failed {func.__name__} after {execution_time}: {str(e)}")
                raise
        return wrapper
    return decorator
