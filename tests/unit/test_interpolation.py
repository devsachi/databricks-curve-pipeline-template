import pytest
from src.curve_formation.business_logic.interpolation import linear, cubic_spline
from pyspark.sql import SparkSession

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_linear_interpolation(spark):
    test_data = [(1, 0.1), (2, 0.2), (3, 0.3)]
    df = spark.createDataFrame(test_data, ["x", "y"])
    
    result = linear(spark, df, "x", "y", [1.5, 2.5])
    assert result.count() == 2
    assert "interpolated_y" in result.columns

def test_cubic_spline_interpolation(spark):
    test_data = [(1, 0.1), (2, 0.2), (3, 0.3)]
    df = spark.createDataFrame(test_data, ["x", "y"])
    
    result = cubic_spline(spark, df, "x", "y", [1.5, 2.5])
    assert result.count() == 2
    assert "interpolated_y" in result.columns
