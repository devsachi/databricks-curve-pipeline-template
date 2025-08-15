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
        
    def generate_report(self, df: DataFrame, config: Dict[str, Any]) -> None:
        """Generate comprehensive data quality report"""
        try:
            report = {
                "timestamp": datetime.now().isoformat(),
                "total_records": df.count(),
                "summary_statistics": self.generate_summary_stats(
                    df, config["columns_to_analyze"]
                ),
                "distributions": {}
            }
            
            # Generate distributions for numeric columns
            for col in config.get("distribution_columns", []):
                report["distributions"][col] = self.generate_distribution_stats(
                    df, col, config.get("distribution_bins", 10)
                )
            
            # Add correlation matrix if specified
            if config.get("compute_correlations", False):
                numeric_cols = [
                    f.name for f in df.schema.fields 
                    if isinstance(f.dataType, (IntegerType, DoubleType, FloatType))
                ]
                
                if numeric_cols:
                    correlation_matrix = {}
                    for col1 in numeric_cols:
                        correlation_matrix[col1] = {}
                        for col2 in numeric_cols:
                            correlation = df.stat.corr(col1, col2)
                            correlation_matrix[col1][col2] = correlation
                            
                    report["correlations"] = correlation_matrix
            
            # Save report
            report_path = f"{self.output_path}/data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
                
            logger.info(f"Data quality report generated: {report_path}")
            
        except Exception as e:
            logger.error(f"Error generating data quality report: {str(e)}")
            raise
