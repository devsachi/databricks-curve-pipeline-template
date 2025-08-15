"""Unit tests for curve formation pipeline using functional patterns"""
import os
import sys
from pathlib import Path

# Add project root to Python path for test imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import test utilities
from ..test_utils import (
    create_test_spark_session,
    generate_test_data,
    validate_test_output,
    cleanup_test_data
)

__all__ = [
    'create_test_spark_session',
    'generate_test_data',
    'validate_test_output',
    'cleanup_test_data'
]
