"""
Audit Columns Utility

Adds audit metadata columns to DataFrames.
All audit column names are prefixed with __.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from typing import Dict, Any, Optional


class AuditColumnsManager:
    """Manages addition of audit columns to DataFrames."""
    
    @staticmethod
    def get_next_ingestion_id(config: Dict[str, Any]) -> int:
        """
        Get the next ingestion ID by querying max from target table.
        
        Args:
            config: Configuration dictionary containing target_config
            
        Returns:
            Next ingestion ID (max + 1, or 0 if table doesn't exist)
        """
        spark = SparkSession.builder.getOrCreate()
        
        # Get target table info
        target_config = config.get('target_config', {})
        target_catalog = target_config.get('catalog', '')
        target_schema = target_config.get('schema', '')
        target_table = target_config.get('table', '')
        
        # Get audit column name
        audit_config = target_config.get('audit_columns', {})
        ingestion_id_col = audit_config.get('ingestion_id', '__ingestion_id')
        
        # Build fully qualified table name
        if target_catalog and target_schema:
            full_table_name = f"{target_catalog}.{target_schema}.{target_table}"
        elif target_schema:
            full_table_name = f"{target_schema}.{target_table}"
        else:
            full_table_name = target_table
        
        try:
            # Try to get max ingestion_id from target table
            max_id = spark.sql(f"SELECT COALESCE(MAX({ingestion_id_col}), -1) as max_id FROM {full_table_name}").first()['max_id']
            next_id = max_id + 1
            print(f"✓ Found existing table {full_table_name}, next ingestion_id: {next_id}")
            return next_id
        except Exception as e:
            # Table doesn't exist or column doesn't exist, start with 0
            print(f"✓ Table {full_table_name} not found or no data, starting with ingestion_id: 0")
            return 0
    
    @staticmethod
    def add_audit_columns(df: DataFrame, config: Dict[str, Any] = None, ingestion_id: Optional[int] = None) -> DataFrame:
        """
        Add audit columns to a DataFrame.
        
        Audit columns added:
        - __ingestion_id: Sequential integer ID for the batch
        - __processed_timestamp: Timestamp when record was processed
        
        Args:
            df: Input DataFrame
            config: Optional configuration dictionary containing audit_columns settings
            ingestion_id: The ingestion ID to use for this batch
            
        Returns:
            DataFrame with audit columns added
        """
        # Get audit column names from config or use defaults
        if config:
            audit_config = config.get('target_config', {}).get('audit_columns', {})
        else:
            audit_config = {}
        
        ingestion_id_col = audit_config.get('ingestion_id', '__ingestion_id')
        processed_timestamp_col = audit_config.get('processed_timestamp', '__processed_timestamp')
        
        # Add ingestion ID as literal value (same for all rows in this batch)
        if ingestion_id is not None:
            df = df.withColumn(ingestion_id_col, lit(ingestion_id))
        else:
            # Fallback to 0 if not provided
            df = df.withColumn(ingestion_id_col, lit(0))
        
        # Add processed timestamp
        df = df.withColumn(processed_timestamp_col, current_timestamp())
        
        return df
    
    @staticmethod
    def get_audit_column_names(config: Dict[str, Any] = None) -> list:
        """
        Get list of audit column names.
        
        Args:
            config: Optional configuration dictionary containing audit_columns settings
            
        Returns:
            List of audit column names
        """
        # Get audit column names from config or use defaults
        if config:
            audit_config = config.get('target_config', {}).get('audit_columns', {})
        else:
            audit_config = {}
        
        return [
            audit_config.get('ingestion_id', '__ingestion_id'),
            audit_config.get('processed_timestamp', '__processed_timestamp')
        ]
