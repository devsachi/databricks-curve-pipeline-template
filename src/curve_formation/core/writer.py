from pyspark.sql import DataFrame
from src.utils.logging_utils import get_logger
from src.utils.data_quality.validator import validate_output_data

logger = get_logger(__name__)

def write_output(spark, df: DataFrame, table_name: str, mode: str = "overwrite"):
    """Write DataFrame to Unity Catalog table with validation"""
    logger.info(f"Writing to table: {table_name}")
    
    # Validate output before writing
    validate_output_data(df)
    
    try:
        (df.write
           .format("delta")
           .mode(mode)
           .option("overwriteSchema", "true")
           .saveAsTable(table_name))
        
        logger.info(f"Successfully wrote to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to {table_name}: {str(e)}")
        raise
