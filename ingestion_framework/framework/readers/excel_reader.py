"""
Excel File Reader

Reads Excel files (.xlsx, .xls) from cloud storage using Spark Structured Streaming.
Uses com.crealytics.spark-excel format for Excel file reading.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from framework.readers.base_file_reader import BaseFileReader
from framework.core.registry import register_reader


@register_reader('excel')
class ExcelReader(BaseFileReader):
    """
    Reader for Excel files using Spark Structured Streaming.
    
    Requires the spark-excel library to be installed:
    Maven: com.crealytics:spark-excel_2.12:3.3.1_0.18.5
    
    Supports Excel-specific configuration:
    - header: First row contains header (default: 'true')
    - inferSchema: Automatic schema inference (default: 'true')
    - dataAddress: Excel sheet and range (e.g., "'Sheet1'!A1")
    - treatEmptyValuesAsNulls: Treat empty cells as nulls (default: 'true')
    - maxRowsInMemory: Number of rows to keep in memory (default: 20)
    - excerptSize: Number of rows to read for schema inference (default: 10)
    - workbookPassword: Password for encrypted Excel files
    
    Example configuration in YAML:
    source_config:
      type: file
      file_type: excel
      location: /Volumes/demo/raw/spreadsheets/
      spark_config:
        header: 'true'
        inferSchema: 'true'
        dataAddress: "'Sheet1'!A1"
        treatEmptyValuesAsNulls: 'true'
        maxRowsInMemory: 20
        excerptSize: 10
    
    Installation:
    Add spark-excel library to your cluster configuration or pipeline:
    - Maven coordinates: com.crealytics:spark-excel_2.12:3.3.1_0.18.5
    - Or via cluster UI: Libraries > Install New > Maven
    
    Note: For very large Excel files (> 100MB), consider converting to 
    CSV/Parquet first for better performance.
    """
    
    FILE_FORMAT = 'com.crealytics.spark.excel'
    
    def __init__(self, config: Dict[str, Any], ingestion_id: int = None):
        """
        Initialize Excel reader with configuration.
        
        Args:
            config: Full configuration dictionary from YAML
            ingestion_id: Optional ingestion ID for archival tracking
        """
        super().__init__(config, ingestion_id)
