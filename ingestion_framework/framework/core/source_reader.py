"""
Abstract Source Reader Base Class

Defines the interface that all source reader implementations must follow.
This abstract base class ensures consistent behavior across different
data source types (files, databases, APIs, etc.).
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any


class SourceReader(ABC):
    """
    Abstract base class for all source readers.
    
    All reader implementations must inherit from this class
    and implement the read() method to return a DataFrame.
    
    The base class provides common initialization for configuration
    and ingestion tracking.
    """
    
    def __init__(self, config: Dict[str, Any], ingestion_id: int = None):
        """
        Initialize source reader with configuration.
        
        Args:
            config: Full configuration dictionary from YAML
                Expected structure:
                - source_config: Source-specific settings
                - target_config: Target table settings
                - archive: Archive configuration (optional)
                - notification: Notification settings (optional)
            ingestion_id: Optional ingestion ID for tracking and archival
        """
        self.config = config
        self.ingestion_id = ingestion_id
    
    @abstractmethod
    def read(self) -> DataFrame:
        """
        Read data from the source and return a DataFrame.
        
        Returns:
            DataFrame containing the data from the source.
            Can be either batch or streaming DataFrame depending on the source.
        
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        pass
