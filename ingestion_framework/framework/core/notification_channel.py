"""
Abstract Notification Channel Base Class

Defines the interface that all notification implementations must follow.
This abstract base class ensures consistent behavior across different
notification channels (email, slack, teams, etc.).
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class NotificationChannel(ABC):
    """
    Abstract base class for notification channels.
    
    All notification implementations must inherit from this class
    and implement notify_success and notify_error methods.
    
    The base class provides common functionality for checking whether
    notifications should be sent based on configuration.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize notification channel with configuration.
        
        Args:
            config: Notification configuration dictionary from YAML
                Expected keys:
                - enabled: bool (default: False)
                - notify_on_success: bool (default: False)
                - notify_on_error: bool (default: True)
        """
        self.config = config
        self.enabled = config.get('enabled', False)
        self.notify_on_success = config.get('notify_on_success', False)
        self.notify_on_error = config.get('notify_on_error', True)
    
    @abstractmethod
    def notify_success(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send notification on successful pipeline execution.
        
        Args:
            message: Success message to send
            metadata: Optional metadata (table name, record count, duration, etc.)
        
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        pass
    
    @abstractmethod
    def notify_error(self, error_message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send notification on pipeline failure.
        
        Args:
            error_message: Error message to send
            metadata: Optional metadata (table name, stack trace, error details, etc.)
        
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        pass
    
    def should_notify_success(self) -> bool:
        """
        Check if success notifications are enabled.
        
        Returns:
            True if notifications are globally enabled AND success notifications are enabled
        """
        return self.enabled and self.notify_on_success
    
    def should_notify_error(self) -> bool:
        """
        Check if error notifications are enabled.
        
        Returns:
            True if notifications are globally enabled AND error notifications are enabled
        """
        return self.enabled and self.notify_on_error
