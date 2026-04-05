"""
Base File Reader using Streaming

Base class for file-based readers that use Spark Structured Streaming.
Provides common functionality for archival, column mapping, and streaming reads.
Specific file format readers (CSV, JSON, Parquet) inherit from this class.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
from framework.core.source_reader import SourceReader
from framework.core.config import ConfigManager


class BaseFileReader(SourceReader):
    """
    Base class for file-based readers using Spark Structured Streaming.
    
    Provides common functionality:
    - File archival before processing
    - Streaming read configuration
    - Column mapping/renaming
    - Schema inference (automatic)
    
    Subclasses must specify the file format.
    """
    
    # Override in subclass to specify file format
    FILE_FORMAT = None
    
    def __init__(self, config: Dict[str, Any], ingestion_id: int = None):
        """
        Initialize file reader with configuration.
        
        Args:
            config: Full configuration dictionary from YAML
            ingestion_id: Optional ingestion ID for archival tracking
        """
        super().__init__(config, ingestion_id)
        self.source_config = ConfigManager.get_source_config(config)
    
    def _archive_files_before_processing(self, source_location: str, archive_base: str, ingestion_id: int) -> str:
        """
        Archive files to ingestion_id-specific folder before processing.
        
        Args:
            source_location: Source directory containing files to archive
            archive_base: Base archive directory path
            ingestion_id: Ingestion ID to use in folder name
            
        Returns:
            Archive location path where files were moved, or source location if no files to archive
        """
        archive_location = f"{archive_base}/ingestion_id={ingestion_id}"
        
        try:
            # Use dbutils to list and move files
            files = dbutils.fs.ls(source_location)
            
            # Filter to only actual files (not directories)
            actual_files = [f for f in files if not f.isDir()]
            
            if len(actual_files) == 0:
                print(f"⚠ No files found in {source_location}")
                return source_location
            
            # Create archive directory
            dbutils.fs.mkdirs(archive_location)
            print(f"✓ Created archive directory: {archive_location}")
            
            # Move each file to archive location
            for file_info in actual_files:
                source_file = file_info.path
                file_name = os.path.basename(source_file)
                dest_file = f"{archive_location}/{file_name}"
                
                # Move file
                dbutils.fs.mv(source_file, dest_file)
                print(f"✓ Archived: {file_name} -> {archive_location}")
            
            print(f"✓ Archived {len(actual_files)} file(s) to {archive_location}")
            return archive_location
            
        except Exception as e:
            print(f"⚠ Warning during archival: {e}")
            # If archival fails, read from source location
            return source_location
    
    def _apply_column_mapping(self, df: DataFrame) -> DataFrame:
        """
        Apply column name mapping if specified in configuration.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with renamed columns
        """
        column_mapping = self.source_config.get('column_mapping', {})
        if column_mapping:
            # Rename columns based on mapping
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)
                else:
                    print(f"⚠ Warning: Column '{old_name}' not found in DataFrame, skipping rename")
        
        return df
    
    def _get_read_location(self) -> str:
        """
        Get the location to read from, handling archival if configured.
        
        Returns:
            Path to read files from (original or archive location)
        """
        location = self.source_config.get('location')
        if not location:
            raise ValueError("Source location is required for file reader")
        
        # Handle archival if configured
        archive_config = ConfigManager.get_archive_config(self.config)
        if archive_config.get('enabled', False) and self.ingestion_id is not None:
            archive_folder = archive_config.get('folder_name', 'archive')
            
            # Build archive base path (sibling folder within source location)
            archive_base = f"{location.rstrip('/')}/{archive_folder}"
            
            # Archive files and get new location
            location = self._archive_files_before_processing(location, archive_base, self.ingestion_id)
            print(f"✓ Reading from location: {location}")
        
        return location
    
    def _build_reader(self) -> 'DataStreamReader':
        """
        Build streaming reader with configuration.
        
        Returns:
            Configured DataStreamReader
        """
        spark = SparkSession.builder.getOrCreate()
        
        # Set the file format (must be specified by subclass)
        if self.FILE_FORMAT is None:
            raise NotImplementedError("Subclass must specify FILE_FORMAT")
        
        # Start with basic readStream format
        reader = spark.readStream.format(self.FILE_FORMAT)
        
        # Apply schema if provided
        schema_hints = self.source_config.get('schema_hints')
        if schema_hints:
            reader = reader.schema(schema_hints)
            print(f"✓ Applied schema: {schema_hints}")
        
        # Apply Spark options from config
        spark_options = ConfigManager.get_spark_options(self.config)
        for key, value in spark_options.items():
            reader = reader.option(key, value)
        
        # Handle recursive option if specified
        if self.source_config.get('recursive', True):
            reader = reader.option("recursiveFileLookup", "true")
        
        # Apply any additional custom options from source config
        custom_options = self.source_config.get('options', {})
        for key, value in custom_options.items():
            reader = reader.option(key, value)
        
        return reader
    
    def read(self) -> DataFrame:
        """
        Read files using Spark Structured Streaming.
        Archives files before processing if configured.
        
        Returns:
            Streaming DataFrame with data from files
        """
        # Get location (with archival if configured)
        location = self._get_read_location()
        
        # Build configured reader
        reader = self._build_reader()
        
        # Load the data
        df = reader.load(location)
        
        # Apply column mapping
        df = self._apply_column_mapping(df)
        
        return df
