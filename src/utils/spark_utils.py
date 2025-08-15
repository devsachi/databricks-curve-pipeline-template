from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "Curve Formation") -> SparkSession:
    """Create and configure a Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
