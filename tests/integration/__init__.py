"""Integration tests for curve formation pipeline using functional patterns"""
import os
import sys
from pathlib import Path

# Add project root to Python path for test imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import test configuration
from ..conftest import (
    spark_session,
    load_test_config,
    get_test_data_path,
    setup_test_environment,
    teardown_test_environment
)

__all__ = [
    'spark_session',
    'load_test_config',
    'get_test_data_path',
    'setup_test_environment',
    'teardown_test_environment'
]
