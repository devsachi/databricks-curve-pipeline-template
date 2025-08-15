"""Test utilities for curve formation pipeline"""
from typing import Dict, Any
import os
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame

def create_test_spark_session() -> SparkSession:
    """Create a Spark session for testing"""
    return (SparkSession.builder
            .master("local[2]")
            .appName("curve_formation_tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def generate_test_data(spark: SparkSession) -> Dict[str, DataFrame]:
    """Generate test market data"""
    from datetime import datetime
    trade_date = datetime.now().strftime("%Y-%m-%d")
    
    # Interest rate data
    ir_data = [
        (trade_date, "SWAP", "1M", 0.02, "TRADER"),
        (trade_date, "SWAP", "3M", 0.025, "TRADER"),
        (trade_date, "SWAP", "6M", 0.03, "TRADER"),
        (trade_date, "SWAP", "1Y", 0.035, "TRADER")
    ]
    ir_df = spark.createDataFrame(
        ir_data,
        ["trade_date", "instrument_type", "tenor", "value", "source"]
    )
    
    # Add more test data generators as needed
    return {"IR": ir_df}

def validate_test_output(df: DataFrame, config: Dict[str, Any]) -> bool:
    """Validate test output data"""
    from src.utils.data_quality import validate_data_quality
    return validate_data_quality(df, config)

def cleanup_test_data(test_path: Path) -> None:
    """Clean up test data files"""
    import shutil
    if test_path.exists():
        shutil.rmtree(test_path)
