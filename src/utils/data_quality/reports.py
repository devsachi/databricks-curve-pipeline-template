"""Data quality reporting functions"""
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from ..logging_utils import get_logger

logger = get_logger(__name__)

def generate_column_stats(df: DataFrame, column: str) -> Dict[str, Any]:
    """Generate statistics for a single column"""
    col_stats = df.select(
        F.count(column).alias("count"),
        F.count(F.when(F.col(column).isNull(), True)).alias("nulls"),
        F.mean(column).alias("mean"),
        F.stddev(column).alias("stddev"),
        F.min(column).alias("min"),
        F.max(column).alias("max")
    ).collect()[0]
    
    return {
        "count": col_stats["count"],
        "null_count": col_stats["nulls"],
        "null_percentage": (col_stats["nulls"] / col_stats["count"]) * 100,
        "mean": col_stats["mean"],
        "stddev": col_stats["stddev"],
        "min": col_stats["min"],
        "max": col_stats["max"]
    }

def generate_summary_stats(df: DataFrame, columns: List[str]) -> Dict[str, Dict[str, Any]]:
    """Generate summary statistics for specified columns"""
    return {
        col: generate_column_stats(df, col)
        for col in columns
    }

def generate_distribution_stats(
    df: DataFrame,
    column: str,
    bins: int = 10
) -> Dict[str, Any]:
    """Generate distribution statistics for a column"""
    hist = df.select(column).rdd.flatMap(lambda x: x).histogram(bins)
    
    return {
        "bin_edges": hist[0],
        "bin_counts": hist[1].tolist()
    }

def generate_data_quality_report(
    df: DataFrame,
    numeric_columns: List[str],
    categorical_columns: Optional[List[str]] = None,
    output_path: Optional[str] = None
) -> Dict[str, Any]:
    """Generate comprehensive data quality report"""
    report = {
        "timestamp": datetime.now().isoformat(),
        "row_count": df.count(),
        "column_count": len(df.columns),
        "numeric_stats": generate_summary_stats(df, numeric_columns),
        "distributions": {
            col: generate_distribution_stats(df, col)
            for col in numeric_columns
        }
    }
    
    if categorical_columns:
        report["categorical_stats"] = {
            col: df.groupBy(col).count().collect()
            for col in categorical_columns
        }
    
    if output_path:
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
    
    return report
def generate_quality_report(
    df: DataFrame,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate comprehensive data quality report
    
    Args:
        df: Input DataFrame to analyze
        config: Configuration containing:
            - columns_to_analyze: List of columns to include in summary stats
            - distribution_columns: Optional list of columns for distribution analysis
            - distribution_bins: Optional number of bins for distributions
            - correlation_columns: Optional list of columns for correlation analysis
            
    Returns:
        Dict containing the quality report with timestamp, statistics and distributions
    """
    try:
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_records": df.count(),
            "summary_statistics": generate_summary_stats(
                df, config["columns_to_analyze"]
            ),
            "distributions": {
                col: generate_distribution_stats(
                    df, col, config.get("distribution_bins", 10)
                )
                for col in config.get("distribution_columns", [])
            }
            
        
        # Add correlation matrix if specified
        if config.get("compute_correlations", False):
            correlation_columns = config.get("correlation_columns", [])
            if correlation_columns:
                correlations = df.select(correlation_columns).toPandas().corr()
                report["correlations"] = correlations.to_dict()
            else:
                logger.warning("No correlation columns specified in config")
        else:
            logger.debug("Correlation computation not requested in config")
        
        return report
        
    except Exception as e:
        logger.error(f"Error generating quality report: {str(e)}")
        raise
def save_quality_report(report: Dict[str, Any], output_path: str) -> None:
    """Save data quality report to JSON file
    
    Args:
        report: The quality report dictionary to save
        output_path: Directory path where to save the report
    """
    try:
        report_path = f"{output_path}/data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"Data quality report generated: {report_path}")
        
    except Exception as e:
        logger.error(f"Error saving data quality report: {str(e)}")
        raise
