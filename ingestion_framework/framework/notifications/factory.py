"""
Notification Factory

Factory pattern for creating notification channel instances based on configuration.
Supports registration of custom notification types.
"""

from typing import Dict, Any, Optional
from framework.core.notification_channel import NotificationChannel
from framework.notifications.email import EmailNotification
from framework.notifications.slack import SlackNotification
from framework.notifications.teams import TeamsNotification


class NotificationFactory:
    """
    Factory for creating notification channel instances based on configuration.
    
    Uses a registry pattern to map notification types to their implementation classes.
    Supports dynamic registration of custom notification types.
    """
    
    # Registry of notification types
    _notification_types = {
        'email': EmailNotification,
        'slack': SlackNotification,
        'teams': TeamsNotification
    }
    
    @classmethod
    def create_notification(cls, config: Dict[str, Any]) -> Optional[NotificationChannel]:
        """
        Create a notification channel instance based on configuration.
        
        Args:
            config: Notification configuration dictionary from YAML.
                   Must contain 'type' and 'enabled' keys.
            
        Returns:
            NotificationChannel instance or None if disabled
            
        Raises:
            ValueError: If notification type is not supported or config is invalid
        
        Examples:
            >>> config = {'type': 'email', 'enabled': True, ...}
            >>> notifier = NotificationFactory.create_notification(config)
            >>> notifier.notify_success("Pipeline completed successfully")
        """
        # Return None if notification is not configured or disabled
        if not config or not config.get('enabled', False):
            return None
        
        notification_type = config.get('type', '').lower()
        
        if not notification_type:
            raise ValueError("Notification config must contain 'type' field")
        
        notification_class = cls._notification_types.get(notification_type)
        
        if not notification_class:
            raise ValueError(
                f"Unsupported notification type: '{notification_type}'. "
                f"Available types: {list(cls._notification_types.keys())}"
            )
        
        # Instantiate and return the notification channel
        return notification_class(config)
    
    @classmethod
    def register_notification_type(cls, notification_type: str, notification_class: type) -> None:
        """
        Register a custom notification type.
        
        Allows extending the factory with new notification implementations
        without modifying the core factory code.
        
        Args:
            notification_type: Type identifier (e.g., 'pagerduty', 'webhook')
            notification_class: Class implementing NotificationChannel interface
        
        Raises:
            TypeError: If notification_class doesn't inherit from NotificationChannel
        
        Examples:
            >>> class CustomNotification(NotificationChannel):
            ...     def notify_success(self, message, metadata): pass
            ...     def notify_error(self, error_message, metadata): pass
            >>> 
            >>> NotificationFactory.register_notification_type('custom', CustomNotification)
        """
        if not issubclass(notification_class, NotificationChannel):
            raise TypeError(
                f"{notification_class.__name__} must inherit from NotificationChannel"
            )
        
        cls._notification_types[notification_type.lower()] = notification_class
    
    @classmethod
    def list_notification_types(cls) -> list:
        """
        Get list of registered notification types.
        
        Returns:
            List of available notification type identifiers
        """
        return list(cls._notification_types.keys())
