"""Core functionality module for curve formation using functional patterns"""
from .processor import process_all_curves, generate_curve
from .combiner import combine_curves
from .utils import (
    standardize_curve_output,
    calculate_curve_metrics,
    validate_date_inputs
)
from .writer import write_output

__all__ = [
    # Processor functions
    'process_all_curves',
    'generate_curve',
    
    # Combiner functions
    'combine_curves',
    
    # Utility functions
    'standardize_curve_output',
    'calculate_curve_metrics',
    'validate_date_inputs',
    
    # Output functions
    'write_output'
]
