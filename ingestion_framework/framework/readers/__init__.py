"""
Readers Package

Provides source reader implementations for CSV and Excel file formats.
All readers inherit from the SourceReader abstract base class.

Usage:
    from framework.readers import CSVReader, ExcelReader
    
    # Create reader based on file type
    reader = CSVReader(config, ingestion_id=0)
    df = reader.read()
"""

from framework.readers.base_file_reader import BaseFileReader
from framework.readers.csv_reader import CSVReader
from framework.readers.excel_reader import ExcelReader

__all__ = [
    'BaseFileReader',
    'CSVReader',
    'ExcelReader'
]
