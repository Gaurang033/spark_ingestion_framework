"""
Metadata-Driven Framework for Spark Declarative Pipelines

This framework enables YAML-based configuration for data ingestion pipelines
using factory and registry patterns.

Supports CSV and Excel file formats.
Readers are automatically registered via @register_reader decorator.
"""

from framework.core.registry import SourceReaderRegistry, register_reader
from framework.core.factory import SourceReaderFactory
from framework.core.config import ConfigManager
from framework.core.decorators import metadata_driven_table, get_table_names_from_config
from framework.utils.audit import AuditColumnsManager
from framework.utils.archiver import FileArchiver

# Import readers to trigger automatic registration via decorators
from framework.readers import CSVReader, ExcelReader


# Public API exports
__all__ = [
    'metadata_driven_table',
    'get_table_names_from_config',
    'SourceReaderRegistry',
    'SourceReaderFactory',
    'ConfigManager',
    'AuditColumnsManager',
    'FileArchiver',
    'CSVReader',
    'ExcelReader',
    'register_reader'
]
