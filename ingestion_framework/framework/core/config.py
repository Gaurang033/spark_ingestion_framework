"""
Configuration Management Module

This module handles loading and parsing of YAML configuration files.
Supports data source configurations with the new format:

source_config:
  type: file  # Required: 'file' or 'rdbms'
  # For type='file':
  location: /Volumes/...
  file_type: csv  # csv, json, excel, xml, parquet, etc.
  spark_config: {...}
  column_mapping: {...}
  schema_hints: "..."
  # For type='rdbms' (to be implemented):
  # connection_string: ...
  # query: ...
  # Additional RDBMS-specific options
target_config:
  catalog: rbc
  schema: bronze
  table: customer
  bad_records:
    table_suffix: _corrupt_records
  audit_columns:  # OPTIONAL - defaults shown below
    ingestion_id: __ingestion_id
    processed_timestamp: __processed_timestamp
archive:
  enabled: true
  folder_name: archive
notification:  # OPTIONAL
  type: email  # email, slack, or teams
  enabled: true
  notify_on_success: false  # Set to true to receive success emails
  notify_on_error: true
  # For type='email':
  smtp_host: smtp.gmail.com
  smtp_port: 587
  credential_scope: email-credentials
  credential_key_username: smtp-username
  credential_key_password: smtp-password
  from_email: noreply@example.com
  to_emails: [admin@example.com]
  subject_prefix: '[Pipeline Alert]'
"""

import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigManager:
    """Manages configuration loading and access."""
    
    @classmethod
    def load_source_config(cls, source_yaml_path: str) -> Dict[str, Any]:
        """
        Load data source configuration from YAML file.
        
        Args:
            source_yaml_path: Path to data source YAML file
            
        Returns:
            Dictionary containing source configuration
            
        Raises:
            FileNotFoundError: If YAML file doesn't exist
            ValueError: If YAML is invalid or missing required fields
        """
        yaml_path = Path(source_yaml_path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Source config file not found: {source_yaml_path}")
        
        try:
            with open(yaml_path, 'r') as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {source_yaml_path}: {e}")
        
        # Validate required fields
        if not config:
            raise ValueError(f"Empty configuration in {source_yaml_path}")
        
        required_fields = ['source_config', 'target_config']
        missing_fields = [f for f in required_fields if f not in config]
        if missing_fields:
            raise ValueError(
                f"Missing required fields in {source_yaml_path}: {missing_fields}"
            )
        
        return config
    
    @classmethod
    def get_source_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get source configuration section.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Source configuration dictionary
        """
        return config.get('source_config', {})
    
    @classmethod
    def get_target_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get target configuration section.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Target configuration dictionary
        """
        return config.get('target_config', {})
    
    @classmethod
    def get_spark_options(cls, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Get Spark options from source configuration.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Dictionary of Spark options
        """
        source_config = cls.get_source_config(config)
        return source_config.get('spark_config', {})
    
    @classmethod
    def get_bad_records_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get bad records configuration from target config.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Bad records configuration dictionary
        """
        target_config = cls.get_target_config(config)
        return target_config.get('bad_records', {'table_suffix': '_corrupt_records'})
    
    @classmethod
    def get_archive_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get archive configuration.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Archive configuration dictionary
        """
        return config.get('archive', {'enabled': True, 'folder_name': 'archive'})
    
    @classmethod
    def get_audit_columns_config(cls, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Get audit columns configuration from target config.
        
        If audit_columns is not specified in the YAML, returns default column names:
        - ingestion_id: __ingestion_id
        - processed_timestamp: __processed_timestamp
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Audit columns configuration dictionary with keys 'ingestion_id' 
            and 'processed_timestamp'
        """
        target_config = cls.get_target_config(config)
        return target_config.get('audit_columns', {
            'ingestion_id': '__ingestion_id',
            'processed_timestamp': '__processed_timestamp'
        })
    
    @classmethod
    def get_notification_config(cls, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get notification configuration.
        
        Notification configuration is optional. If not specified, returns None.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Notification configuration dictionary or None if not configured
        """
        return config.get('notification', None)
