"""Functions for combining multiple curves"""
from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from functools import reduce
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def validate_join_key(curves: List[DataFrame], join_key: str) -> None:
    """Validate that all curves have the required join key"""
    for idx, df in enumerate(curves):
        if join_key not in df.columns:
            raise ValueError(f"DataFrame at index {idx} missing join key '{join_key}'")

def combine_curves(
    spark: SparkSession,
    curves: List[DataFrame],
    join_key: str = "monthlydate"
) -> DataFrame:
    """Combine multiple curve DataFrames using the specified join key"""
    logger.info(f"Combining {len(curves)} curves using join key: {join_key}")
    
    if not curves:
        raise ValueError("No curves provided for combining")
    
    # Validate join key presence
    validate_join_key(curves, join_key)
    
    # Perform sequential outer joins
    return reduce(
        lambda df1, df2: df1.join(df2, on=join_key, how="outer"),
        curves
    ).orderBy(join_key)

def merge_curve_outputs(
    dfs: List[DataFrame],
    join_key: str,
    value_cols: List[str]
) -> DataFrame:
    """Merge multiple curve outputs with specific value columns"""
    if not dfs:
        return None
        
    base_df = dfs[0]
    for df in dfs[1:]:
        base_df = base_df.join(df.select(join_key, *value_cols), on=join_key, how='outer')
    
    return base_df.orderBy(join_key)
