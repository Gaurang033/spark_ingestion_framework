"""
Microsoft Teams Notification Implementation

Sends notifications to Microsoft Teams via webhooks with credentials from Databricks secrets.
"""

from typing import Dict, Any, Optional
from framework.core.notification_channel import NotificationChannel


class TeamsNotification(NotificationChannel):
    """
    Microsoft Teams notification implementation using webhooks.
    
    Expected configuration structure:
    {
        'type': 'teams',
        'enabled': true,
        'notify_on_success': false,
        'notify_on_error': true,
        'credential_scope': 'teams-credentials',
        'credential_key_webhook': 'webhook-url',
        'theme_color': '00FF00'  # Hex color for success (optional)
    }
    
    Implementation Notes:
    - Webhook URL should be retrieved using: dbutils.secrets.get(scope, key)
    - Use Teams Incoming Webhooks for notifications
    - Format messages using Teams MessageCard format or Adaptive Cards
    - Color codes: Green (00FF00) for success, Red (FF0000) for errors
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Teams notification channel.
        
        Args:
            config: Teams notification configuration dictionary
        """
        super().__init__(config)
        
        # Extract Teams-specific configuration
        self.credential_scope = config.get('credential_scope')
        self.credential_key_webhook = config.get('credential_key_webhook', 'webhook-url')
        self.success_color = config.get('success_color', '00FF00')  # Green
        self.error_color = config.get('error_color', 'FF0000')      # Red
        
        # Validate required fields
        self._validate_config()
    
    def _validate_config(self) -> None:
        """
        Validate that all required Teams configuration fields are present.
        
        Raises:
            ValueError: If any required fields are missing
        """
        required_fields = ['credential_scope']
        missing = [f for f in required_fields if not self.config.get(f)]
        
        if missing:
            raise ValueError(
                f"Teams notification config missing required fields: {missing}"
            )
    
    def notify_success(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send success notification to Teams.
        
        Args:
            message: Success message
            metadata: Optional metadata (table_name, records_processed, etc.)
        """
        if not self.should_notify_success():
            return
        
        print(f"✓ Teams notification prepared (success): {message}")
        print(f"  Color: {self.success_color}")
        
        if metadata:
            print(f"  Metadata: {metadata}")
        
        # TODO: Implement Teams webhook call
        # webhook_url = dbutils.secrets.get(self.credential_scope, self.credential_key_webhook)
        # Send formatted MessageCard with success color
    
    def notify_error(self, error_message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send error notification to Teams.
        
        Args:
            error_message: Error message
            metadata: Optional metadata (table_name, error_type, etc.)
        """
        if not self.should_notify_error():
            return
        
        print(f"✗ Teams notification prepared (error): {error_message}")
        print(f"  Color: {self.error_color}")
        
        if metadata:
            print(f"  Metadata: {metadata}")
        
        # TODO: Implement Teams webhook call
        # webhook_url = dbutils.secrets.get(self.credential_scope, self.credential_key_webhook)
        # Send formatted MessageCard with error color
