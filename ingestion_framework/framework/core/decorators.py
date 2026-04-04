"""
Metadata-Driven Table Decorator

Main decorator that orchestrates the entire framework.
Takes YAML configuration and creates streaming tables with audit columns.
"""

from functools import wraps
from typing import Callable
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from framework.core.config import ConfigManager
from framework.core.factory import SourceReaderFactory
from framework.utils.audit import AuditColumnsManager
from framework.utils.archiver import FileArchiver


def metadata_driven_table(
    yaml_config_path: str,
    create_corrupt_table: bool = True,
    archive_files: bool = True
) -> Callable:
    """
    Decorator that creates streaming tables from YAML configuration.
    
    This decorator:
    1. Gets next ingestion_id from target table (max + 1, or 0 if not exists)
    2. Archives files to ingestion_id-specific folder before processing
    3. Uses factory pattern to create appropriate source reader
    4. Creates target streaming table with audit columns
    5. Creates corrupt records table (if enabled)
    
    The @dp.table decorator handles all checkpoint management automatically.
    
    Args:
        yaml_config_path: Path to YAML configuration file
        create_corrupt_table: Whether to create corrupt records table (default: True)
        archive_files: Whether to enable file archiving (default: True)
    
    Example YAML configuration:
        source_config:
          type: file
          file_type: csv
          location: /Volumes/catalog/schema/volume/data/
          spark_config:
            mode: PERMISSIVE
            header: 'true'
        target_config:
          catalog: rbc
          schema: bronze
          table: customer
          bad_records:
            table_suffix: _corrupt_records
          audit_columns:
            ingestion_id: __ingestion_id
            processed_timestamp: __processed_timestamp
        archive:
          enabled: true
          folder_name: archive
    
    Usage:
        @metadata_driven_table('data_sources/customers.yaml')
        def customers_table():
            pass  # Implementation generated automatically
    """
    
    def decorator(func: Callable) -> Callable:
        # Load configuration
        config = ConfigManager.load_source_config(yaml_config_path)
        
        # Extract target info
        target_config = config.get('target_config', {})
        target_catalog = target_config.get('catalog', '')
        target_schema = target_config.get('schema', '')
        target_table = target_config.get('table', '')
        
        # Build table name without catalog (pipeline settings define the catalog)
        if target_schema:
            full_table_name = f"{target_schema}.{target_table}"
        else:
            full_table_name = target_table
        
        # Get bad records configuration
        bad_records_config = ConfigManager.get_bad_records_config(config)
        corrupt_table_suffix = bad_records_config.get('table_suffix', '_corrupt_records')
        corrupt_table_name = f"{target_table}{corrupt_table_suffix}"
        
        if target_schema:
            full_corrupt_table_name = f"{target_schema}.{corrupt_table_name}"
        else:
            full_corrupt_table_name = corrupt_table_name

        # Common function to get next ingestion_id, read files, and add audit columns
        def get_base_df() -> DataFrame:
            # Step 1: Get next ingestion_id from target table
            ingestion_id = AuditColumnsManager.get_next_ingestion_id(config)
            print(f"✓ Using ingestion_id: {ingestion_id}")
            
            # Step 2: Create reader with ingestion_id (will archive files before reading if configured)
            reader = SourceReaderFactory.create_reader(config, ingestion_id=ingestion_id)
            
            # Step 3: Read data from source (files already archived at this point)
            df = reader.read()
            
            # Step 4: Add audit columns with the ingestion_id
            df = AuditColumnsManager.add_audit_columns(df, config, ingestion_id=ingestion_id)
            
            return df
        
        # Create the target table function
        @dp.table(
            name=full_table_name,
            comment=f"Metadata-driven table from {yaml_config_path}"
        )
        @wraps(func)
        def target_table_func():
            df = get_base_df()
            spark_options = ConfigManager.get_spark_options(config)
            corrupt_col = spark_options.get('columnNameOfCorruptRecord', '_corrupt_record')
            if corrupt_col in df.columns:
                df = df.filter(col(corrupt_col).isNull()).drop(corrupt_col)
            return df
        
        # Create the corrupt records table function if enabled
        if create_corrupt_table:
            @dp.table(
                name=full_corrupt_table_name,
                comment=f"Corrupt records from {yaml_config_path}"
            )
            def corrupt_records_table_func():
                df = get_base_df()
                spark_options = ConfigManager.get_spark_options(config)
                corrupt_col = spark_options.get('columnNameOfCorruptRecord', '_corrupt_record')
                if corrupt_col in df.columns:
                    df = df.filter(col(corrupt_col).isNotNull())
                else:
                    df = df.limit(0)
                return df
            
            # Store corrupt table function as attribute for access
            target_table_func._corrupt_records_table = corrupt_records_table_func
        
        return target_table_func
    
    return decorator


def get_table_names_from_config(yaml_config_path: str) -> dict:
    """
    Helper function to get target and corrupt table names from YAML config.
    
    Args:
        yaml_config_path: Path to YAML configuration file
        
    Returns:
        Dictionary with 'target_table' and 'corrupt_table' keys
    """
    config = ConfigManager.load_source_config(yaml_config_path)
    target_config = config.get('target_config', {})
    
    target_catalog = target_config.get('catalog', '')
    target_schema = target_config.get('schema', '')
    target_table = target_config.get('table', '')
    
    bad_records_config = ConfigManager.get_bad_records_config(config)
    corrupt_table_suffix = bad_records_config.get('table_suffix', '_corrupt_records')
    corrupt_table = f"{target_table}{corrupt_table_suffix}"
    
    return {
        'target_table': target_table,
        'corrupt_table': corrupt_table,
        'catalog': target_catalog,
        'schema': target_schema
    }
