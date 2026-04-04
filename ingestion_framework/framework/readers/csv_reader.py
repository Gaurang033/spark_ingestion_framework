"""
CSV File Reader

Reads CSV files from cloud storage using Spark Structured Streaming.
Supports CSV-specific options like delimiter, header, encoding, etc.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from framework.readers.base_file_reader import BaseFileReader
from framework.core.registry import register_reader


@register_reader('csv')
class CSVReader(BaseFileReader):
    """
    Reader for CSV files using Spark Structured Streaming.
    
    Supports CSV-specific configuration:
    - delimiter: Field separator (default: ',')
    - header: First row contains header (default: 'true')
    - encoding: File encoding (default: 'UTF-8')
    - quote: Quote character (default: '"')
    - escape: Escape character (default: '\')
    - mode: PERMISSIVE, DROPMALFORMED, or FAILFAST
    - inferSchema: Automatic schema inference
    
    Example configuration in YAML:
    source_config:
      type: file
      file_type: csv
      location: /Volumes/demo/raw/data/
      spark_config:
        mode: PERMISSIVE
        header: 'true'
        delimiter: ','
        encoding: UTF-8
        quote: '"'
        escape: \
    """
    
    FILE_FORMAT = 'csv'
    
    def __init__(self, config: Dict[str, Any], ingestion_id: int = None):
        """
        Initialize CSV reader with configuration.
        
        Args:
            config: Full configuration dictionary from YAML
            ingestion_id: Optional ingestion ID for archival tracking
        """
        super().__init__(config, ingestion_id)
