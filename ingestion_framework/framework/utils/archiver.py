"""
File Archiver Utility

Handles moving processed files to archive folder.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any


class FileArchiver:
    """Manages archiving of processed files."""
    
    @staticmethod
    def get_archive_path(source_path: str, config: Dict[str, Any] = None) -> Optional[str]:
        """
        Get the archive path for a given source path.
        
        Args:
            source_path: Original source file/folder path
            config: Optional configuration dictionary containing archive settings
            
        Returns:
            Archive path or None if archiving is disabled
        """
        # Get archive config from provided config or use defaults
        if config:
            archive_config = config.get('archive', {})
        else:
            archive_config = {}
        
        if not archive_config.get('enabled', True):
            return None
        
        archive_folder_name = archive_config.get('folder_name', 'archive')
        
        # Parse the source path
        source_path_obj = Path(source_path)
        parent_dir = source_path_obj.parent
        
        # Create archive path in the parent directory
        archive_path = parent_dir / archive_folder_name
        
        return str(archive_path)
    
    @staticmethod
    def setup_archive_trigger(source_location: str, table_name: str, config: Dict[str, Any] = None) -> dict:
        """
        Setup trigger configuration for archiving files after processing.
        
        This returns configuration that can be used with Auto Loader's
        file notification mode to move files after successful processing.
        
        Args:
            source_location: Source folder path
            table_name: Target table name (used for checkpoint location)
            config: Optional configuration dictionary containing archive settings
            
        Returns:
            Dictionary with trigger and archive configuration
        """
        # Get archive config from provided config or use defaults
        if config:
            archive_config = config.get('archive', {})
        else:
            archive_config = {}
        
        if not archive_config.get('enabled', True):
            return {}
        
        archive_path = FileArchiver.get_archive_path(source_location, config)
        
        if not archive_path:
            return {}
        
        # Configuration for Auto Loader file archiving
        # Note: Actual file movement happens via Auto Loader options or
        # external orchestration (Databricks Jobs, workflows, etc.)
        return {
            'archive_enabled': True,
            'archive_path': archive_path,
            'source_location': source_location,
            # These would be applied as Auto Loader options
            'options': {
                # Auto Loader can track processed files via checkpoint
            }
        }
