"""
Audit Columns Utility

Adds audit metadata columns to DataFrames.
All audit column names are prefixed with __.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, count, when, col as spark_col
from typing import Dict, Any, Optional


class AuditColumnsManager:
    """Manages addition of audit columns to DataFrames."""
    
    @staticmethod
    def get_audit_table_name(config: Dict[str, Any]) -> str:
        """
        Get the fully qualified audit metadata table name.
        
        Uses hardcoded name 'ingestion_audit_info' in the same catalog/schema as target.
        
        Args:
            config: Configuration dictionary containing target_config
            
        Returns:
            Fully qualified audit table name (e.g., "catalog.schema.ingestion_audit_info")
        """
        target_config = config.get('target_config', {})
        target_catalog = target_config.get('catalog', '')
        target_schema = target_config.get('schema', '')
        
        # Hardcoded audit table name - one per schema
        audit_table = "ingestion_audit_info"
        
        if target_catalog and target_schema:
            return f"{target_catalog}.{target_schema}.{audit_table}"
        elif target_schema:
            return f"{target_schema}.{audit_table}"
        else:
            return audit_table
    
    @staticmethod
    def get_next_ingestion_id(config: Dict[str, Any]) -> int:
        """
        Get the next ingestion ID from the audit metadata table.
        
        Queries the ingestion_audit_info table to get the max ingestion_id
        for this specific target table, then returns max + 1.
        If no entry exists for the table, returns 0.
        
        NOTE: The ingestion_audit_info table must be created manually outside 
        the pipeline before first run. Use create_audit_table_ddl() to get the DDL.
        
        Args:
            config: Configuration dictionary containing target_config
            
        Returns:
            Next ingestion ID (max + 1, or 0 if table doesn't exist or no records for this table)
        """
        spark = SparkSession.builder.getOrCreate()
        
        # Get target table name
        target_config = config.get('target_config', {})
        target_table = target_config.get('table', '')
        
        # Get audit table name
        audit_table = AuditColumnsManager.get_audit_table_name(config)
        
        try:
            # Query the audit table for this target table's max ingestion_id
            query = f"""
                SELECT COALESCE(MAX(ingestion_id), -1) as max_id 
                FROM {audit_table}
                WHERE table_name = '{target_table}'
                AND status = 'SUCCESS'
            """
            result = spark.sql(query)
            max_id = result.first()['max_id']
            next_id = max_id + 1
            
            if max_id == -1:
                print(f"✓ No entries found for '{target_table}' in {audit_table}, starting with ingestion_id: 0")
            else:
                print(f"✓ Found max ingestion_id {max_id} for '{target_table}' in {audit_table}, using next_id: {next_id}")
            
            return next_id
        except Exception as e:
            # Audit table doesn't exist or error querying
            error_msg = str(e)
            print(f"⚠ WARNING: Could not query audit table {audit_table}")
            print(f"  Error: {error_msg}")
            print(f"  Defaulting to ingestion_id: 0")
            print(f"  ACTION REQUIRED: Create the audit table using the DDL from create_audit_table_ddl()")
            return 0
    
    @staticmethod
    def create_audit_table_ddl(config: Dict[str, Any]) -> str:
        """
        Generate DDL to create the ingestion_audit_info table.
        
        This table must be created manually outside the pipeline before first run.
        SDP does not allow CREATE TABLE statements inside dataset definitions.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            SQL DDL statement to create audit table
        """
        audit_table = AuditColumnsManager.get_audit_table_name(config)
        
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
            table_name STRING COMMENT 'Name of the target table',
            status STRING COMMENT 'Status: SUCCESS or FAILED',
            ingestion_id BIGINT COMMENT 'Sequential ingestion ID per table',
            records_inserted BIGINT COMMENT 'Number of valid records inserted',
            records_rejected BIGINT COMMENT 'Number of corrupt/rejected records',
            pipeline_update_id STRING COMMENT 'Pipeline update ID from event log',
            start_timestamp TIMESTAMP COMMENT 'Ingestion start time',
            end_timestamp TIMESTAMP COMMENT 'Ingestion end time',
            error_message STRING COMMENT 'Error message if failed'
        )
        COMMENT 'Audit metadata for all pipeline ingestions in this schema'
        """
        return ddl.strip()
    
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
