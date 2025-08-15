"""Exception handling utilities for the curve formation pipeline"""
import functools
from typing import Callable, Optional, Any
from .logging_utils import get_logger

logger = get_logger(__name__)

def handle_exception(error_message: Optional[str] = None) -> Callable:
    """Decorator for handling exceptions in functions"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                msg = error_message or f"Error in {func.__name__}: {str(e)}"
                logger.error(msg, exc_info=True)
                raise type(e)(msg) from e
        return wrapper
    return decorator

def safe_execute(func: Callable, *args, default_value: Any = None, **kwargs) -> Any:
    """Execute a function safely and return default value on error"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Error executing {func.__name__}: {str(e)}", exc_info=True)
        return default_value

def log_and_raise(error: Exception, message: str) -> None:
    """Log an error and raise it with a custom message"""
    logger.error(message, exc_info=True)
    raise type(error)(message) from error
