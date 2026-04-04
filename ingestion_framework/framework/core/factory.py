"""
Source Reader Factory Pattern

This module implements a factory pattern for creating source readers.
It uses file type and source type to instantiate the appropriate reader.
"""

from typing import Dict, Any, Optional
from framework.core.registry import SourceReaderRegistry


class SourceReaderFactory:
    """Factory for creating source reader instances based on configuration."""
    
    # Registry mapping file types to reader types for 'file' sources
    _file_type_readers = {
        'csv': 'csv',
        'excel': 'excel'
    }
    
    @staticmethod
    def create_reader(config: Dict[str, Any], ingestion_id: Optional[int] = None) -> Any:
        """
        Create a reader instance based on complete configuration.
        
        Uses the 'type' field from source_config to determine reader category:
        - type: 'file' -> Uses file_type to determine specific reader (CSV, Excel)
        - type: 'rdbms' -> Uses RDBMSReader (to be implemented)
        
        Args:
            config: Complete configuration dictionary from YAML including
                   source_config, target_config, and archive sections
            ingestion_id: Optional ingestion ID for archival folder naming
            
        Returns:
            An instance of the appropriate reader class
            
        Raises:
            ValueError: If config is invalid, type is missing, or file_type is unsupported
        """
        if not isinstance(config, dict):
            raise ValueError("config must be a dictionary")
        
        source_config = config.get('source_config', {})
        source_type = source_config.get('type')
        
        if not source_type:
            raise ValueError(
                "source_config must contain 'type' key (e.g., 'file' or 'rdbms')"
            )
        
        # For 'file' type, use file_type to determine specific reader
        if source_type.lower() == 'file':
            file_type = source_config.get('file_type')
            if not file_type:
                raise ValueError(
                    "source_config with type='file' must contain 'file_type' key "
                    "(e.g., 'csv', 'excel')"
                )
            
            # Map file_type to reader type in registry
            file_type_lower = file_type.lower()
            reader_type = SourceReaderFactory._file_type_readers.get(file_type_lower)
            
            if not reader_type:
                raise ValueError(
                    f"Unsupported file type: '{file_type}'. "
                    f"Supported types: {list(SourceReaderFactory._file_type_readers.keys())}"
                )
            
            # Get the reader class from registry
            reader_class = SourceReaderRegistry.get(reader_type)
        else:
            # For non-file types (e.g., 'rdbms'), get reader directly from registry
            reader_class = SourceReaderRegistry.get(source_type)
        
        # Instantiate and return the reader with complete config and ingestion_id
        return reader_class(config, ingestion_id=ingestion_id)
    
    @classmethod
    def register_file_type(cls, file_type: str, reader_type: str) -> None:
        """
        Register a new file type mapping.
        
        Args:
            file_type: File type identifier (e.g., 'avro', 'orc')
            reader_type: Reader type in registry (e.g., 'avro')
        """
        cls._file_type_readers[file_type.lower()] = reader_type.lower()
    
    @classmethod
    def list_supported_file_types(cls) -> list:
        """
        Get list of supported file types.
        
        Returns:
            List of supported file type identifiers
        """
        return list(cls._file_type_readers.keys())
