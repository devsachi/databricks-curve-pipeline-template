from pyspark.sql import DataFrame, functions as F
from functools import reduce
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def combine_curves(spark, curves: list[DataFrame], join_key: str = "monthlydate") -> DataFrame:
    """Combine multiple curve DataFrames using the specified join key"""
    logger.info(f"Combining {len(curves)} curves using join key: {join_key}")
    
    if not curves:
        raise ValueError("No curves provided for combining")
        
    # Validate all DataFrames have the join key
    for idx, df in enumerate(curves):
        if join_key not in df.columns:
            raise ValueError(f"DataFrame at index {idx} missing join key '{join_key}'")
    
    # Perform sequential outer joins
    return reduce(
        lambda df1, df2: df1.join(df2, on=join_key, how="outer"),
        curves
    )
