"""
Source Reader Registry Pattern

This module implements a registry pattern for managing different source reader types.
It allows dynamic registration and retrieval of reader classes based on source type.
"""

from typing import Dict, Callable, Any


class SourceReaderRegistry:
    """Registry for storing and retrieving source reader factories."""
    
    _readers: Dict[str, Callable] = {}
    
    @classmethod
    def register(cls, source_type: str, reader_class: Callable) -> None:
        """
        Register a reader class for a specific source type.
        
        Args:
            source_type: Type identifier (e.g., 'csv', 'excel', 'rdbms')
            reader_class: The reader class to register
        """
        cls._readers[source_type.lower()] = reader_class
        
    @classmethod
    def get(cls, source_type: str) -> Callable:
        """
        Retrieve a reader class by source type.
        
        Args:
            source_type: Type identifier (e.g., 'csv', 'excel', 'rdbms')
            
        Returns:
            The registered reader class
            
        Raises:
            ValueError: If source type is not registered
        """
        reader = cls._readers.get(source_type.lower())
        if reader is None:
            raise ValueError(
                f"No reader registered for source type: {source_type}. "
                f"Available types: {list(cls._readers.keys())}"
            )
        return reader
    
    @classmethod
    def list_types(cls) -> list:
        """Return list of registered source types."""
        return list(cls._readers.keys())


def register_reader(reader_type: str):
    """
    Decorator to automatically register a reader class.
    
    Usage:
        @register_reader('csv')
        class CSVReader(BaseFileReader):
            FILE_FORMAT = 'csv'
    
    Args:
        reader_type: Type identifier for the reader (e.g., 'csv', 'excel')
        
    Returns:
        Decorator function that registers the class and returns it unchanged
    """
    def decorator(reader_class: Callable) -> Callable:
        SourceReaderRegistry.register(reader_type, reader_class)
        return reader_class
    return decorator
