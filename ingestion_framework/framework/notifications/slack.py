"""
Slack Notification Implementation

Sends notifications to Slack via webhooks with credentials from Databricks secrets.
"""

from typing import Dict, Any, Optional
from framework.core.notification_channel import NotificationChannel


class SlackNotification(NotificationChannel):
    """
    Slack notification implementation using webhooks.
    
    Expected configuration structure:
    {
        'type': 'slack',
        'enabled': true,
        'notify_on_success': false,
        'notify_on_error': true,
        'credential_scope': 'slack-credentials',
        'credential_key_webhook': 'webhook-url',
        'channel': '#alerts',
        'username': 'Pipeline Bot',
        'icon_emoji': ':robot_face:'
    }
    
    Implementation Notes:
    - Webhook URL should be retrieved using: dbutils.secrets.get(scope, key)
    - Use Slack Incoming Webhooks for simple notifications
    - Consider using Slack SDK for more advanced features
    - Format messages using Slack Block Kit for rich formatting
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Slack notification channel.
        
        Args:
            config: Slack notification configuration dictionary
        """
        super().__init__(config)
        
        # Extract Slack-specific configuration
        self.credential_scope = config.get('credential_scope')
        self.credential_key_webhook = config.get('credential_key_webhook', 'webhook-url')
        self.channel = config.get('channel', '#alerts')
        self.username = config.get('username', 'Pipeline Bot')
        self.icon_emoji = config.get('icon_emoji', ':robot_face:')
        
        # Validate required fields
        self._validate_config()
    
    def _validate_config(self) -> None:
        """
        Validate that all required Slack configuration fields are present.
        
        Raises:
            ValueError: If any required fields are missing
        """
        required_fields = ['credential_scope']
        missing = [f for f in required_fields if not self.config.get(f)]
        
        if missing:
            raise ValueError(
                f"Slack notification config missing required fields: {missing}"
            )
    
    def notify_success(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send success notification to Slack.
        
        Args:
            message: Success message
            metadata: Optional metadata (table_name, records_processed, etc.)
        """
        if not self.should_notify_success():
            return
        
        print(f"✓ Slack notification prepared (success): {message}")
        print(f"  Channel: {self.channel}")
        
        if metadata:
            print(f"  Metadata: {metadata}")
        
        # TODO: Implement Slack webhook call
        # webhook_url = dbutils.secrets.get(self.credential_scope, self.credential_key_webhook)
        # Send formatted message to webhook
    
    def notify_error(self, error_message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send error notification to Slack.
        
        Args:
            error_message: Error message
            metadata: Optional metadata (table_name, error_type, etc.)
        """
        if not self.should_notify_error():
            return
        
        print(f"✗ Slack notification prepared (error): {error_message}")
        print(f"  Channel: {self.channel}")
        
        if metadata:
            print(f"  Metadata: {metadata}")
        
        # TODO: Implement Slack webhook call
        # webhook_url = dbutils.secrets.get(self.credential_scope, self.credential_key_webhook)
        # Send formatted error message to webhook
