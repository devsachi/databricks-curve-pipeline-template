from typing import Dict, List, Any
from datetime import datetime
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from ..logging_utils import get_logger

logger = get_logger(__name__)

class DataQualityReporter:
    """Generates data quality reports"""
    
    def __init__(self, output_path: str):
        self.output_path = output_path
        
    def generate_summary_stats(self, df: DataFrame, columns: List[str]) -> Dict[str, Any]:
        """Generate summary statistics for specified columns"""
        stats = {}
        
        for col in columns:
            col_stats = df.select(
                F.count(col).alias("count"),
                F.count(F.when(F.col(col).isNull(), True)).alias("nulls"),
                F.mean(col).alias("mean"),
                F.stddev(col).alias("stddev"),
                F.min(col).alias("min"),
                F.max(col).alias("max")
            ).collect()[0]
            
            stats[col] = {
                "count": col_stats["count"],
                "null_count": col_stats["nulls"],
                "null_percentage": (col_stats["nulls"] / col_stats["count"]) * 100,
                "mean": col_stats["mean"],
                "stddev": col_stats["stddev"],
                "min": col_stats["min"],
                "max": col_stats["max"]
            }
            
        return stats
    
    def generate_distribution_stats(self, df: DataFrame, column: str, bins: int = 10) -> Dict[str, Any]:
        """Generate distribution statistics for a column"""
        hist = df.select(column).rdd.flatMap(lambda x: x).histogram(bins)
        
        return {
            "bin_edges": hist[0],
            "bin_counts": hist[1].tolist()
        }
        
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
