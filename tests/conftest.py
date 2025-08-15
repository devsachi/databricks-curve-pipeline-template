"""Test configuration and fixtures"""
import os
import pytest
from pathlib import Path
from typing import Dict, Any

import sys
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils import load_config
from .test_utils import create_test_spark_session, generate_test_data

# Test Configuration
TEST_DATA_PATH = Path(__file__).parent / 'data'
TEST_CONFIGS_PATH = Path(__file__).parent / 'test_configs'

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = create_test_spark_session()
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def load_test_config() -> Dict[str, Any]:
    """Load test configuration"""
    return load_config('test')

@pytest.fixture(scope="session")
def get_test_data_path() -> Path:
    """Get test data directory path"""
    return TEST_DATA_PATH

@pytest.fixture(autouse=True)
def setup_test_environment(spark_session, load_test_config):
    """Setup test environment before each test"""
    # Create test directories if needed
    TEST_DATA_PATH.mkdir(exist_ok=True)
    TEST_CONFIGS_PATH.mkdir(exist_ok=True)
    
    # Generate test data
    test_data = generate_test_data(spark_session)
    
    yield test_data
    
    # Cleanup after tests
    for p in [TEST_DATA_PATH, TEST_CONFIGS_PATH]:
        if p.exists():
            import shutil
            shutil.rmtree(p)
            
@pytest.fixture(autouse=True)
def teardown_test_environment():
    """Cleanup after tests"""
    yield
    # Additional cleanup if needed
