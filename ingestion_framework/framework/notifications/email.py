"""
Email Notification Implementation

Sends notifications via SMTP with credentials from Databricks secrets.
Supports configurable success/error notifications with formatted messages.
"""

from typing import Dict, Any, Optional
from framework.core.notification_channel import NotificationChannel


class EmailNotification(NotificationChannel):
    """
    Email notification implementation using SMTP.
    
    Expected configuration structure:
    {
        'type': 'email',
        'enabled': true,
        'notify_on_success': false,
        'notify_on_error': true,
        'smtp_host': 'smtp.company.com',
        'smtp_port': 587,
        'credential_scope': 'email-credentials',
        'credential_key_username': 'smtp-username',
        'credential_key_password': 'smtp-password',
        'from_email': 'noreply@example.com',
        'to_emails': ['admin@example.com', 'team@example.com'],
        'subject_prefix': '[Pipeline Alert]'
    }
    
    Implementation Notes:
    - Credentials should be retrieved using: dbutils.secrets.get(scope, key)
    - SMTP connection should use TLS for security
    - Consider using Databricks Jobs email notifications as alternative
    - Actual SMTP sending requires external implementation
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize email notification channel.
        
        Args:
            config: Email notification configuration dictionary
            
        Raises:
            ValueError: If required configuration fields are missing
        """
        super().__init__(config)
        
        # Extract email-specific configuration
        self.smtp_host = config.get('smtp_host')
        self.smtp_port = config.get('smtp_port', 587)
        self.credential_scope = config.get('credential_scope')
        self.credential_key_username = config.get('credential_key_username', 'username')
        self.credential_key_password = config.get('credential_key_password', 'password')
        self.from_email = config.get('from_email')
        self.to_emails = config.get('to_emails', [])
        self.subject_prefix = config.get('subject_prefix', '[Pipeline Alert]')
        
        # Validate required fields
        self._validate_config()
    
    def _validate_config(self) -> None:
        """
        Validate that all required email configuration fields are present.
        
        Raises:
            ValueError: If any required fields are missing
        """
        required_fields = ['smtp_host', 'credential_scope', 'from_email', 'to_emails']
        missing = [f for f in required_fields if not self.config.get(f)]
        
        if missing:
            raise ValueError(
                f"Email notification config missing required fields: {missing}"
            )
        
        if not isinstance(self.to_emails, list) or len(self.to_emails) == 0:
            raise ValueError("to_emails must be a non-empty list")
    
    def notify_success(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send success notification via email.
        
        Args:
            message: Success message
            metadata: Optional metadata (table_name, records_processed, duration_seconds, etc.)
        """
        if not self.should_notify_success():
            return
        
        table_name = metadata.get('table_name', 'Pipeline') if metadata else 'Pipeline'
        subject = f"{self.subject_prefix} Success: {table_name}"
        body = self._format_success_message(message, metadata)
        
        # Log notification (actual email sending requires external implementation)
        print(f"✓ Email notification prepared (success): {subject}")
        print(f"  To: {', '.join(self.to_emails)}")
        print(f"  Message: {message}")
        
        if metadata:
            print(f"  Metadata: {metadata}")
        
        # TODO: Implement actual email sending using:
        # 1. Databricks Jobs email notifications (recommended)
        # 2. External email service integration
        # 3. Custom SMTP implementation in a separate task
    
    def notify_error(self, error_message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send error notification via email.
        
        Args:
            error_message: Error message
            metadata: Optional metadata (table_name, error_type, stack_trace, etc.)
        """
        if not self.should_notify_error():
            return
        
        table_name = metadata.get('table_name', 'Pipeline') if metadata else 'Pipeline'
        subject = f"{self.subject_prefix} Error: {table_name}"
        body = self._format_error_message(error_message, metadata)
        
        # Log notification (actual email sending requires external implementation)
        print(f"✗ Email notification prepared (error): {subject}")
        print(f"  To: {', '.join(self.to_emails)}")
        print(f"  Error: {error_message}")
        
        if metadata:
            print(f"  Metadata: {metadata}")
        
        # TODO: Implement actual email sending using:
        # 1. Databricks Jobs email notifications (recommended)
        # 2. External email service integration
        # 3. Custom SMTP implementation in a separate task
    
    def _format_success_message(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Format success message with metadata.
        
        Args:
            message: Success message
            metadata: Optional metadata dictionary
            
        Returns:
            Formatted message string
        """
        lines = [
            "Pipeline Execution Successful",
            "=" * 50,
            f"\nMessage: {message}\n"
        ]
        
        if metadata:
            lines.append("\nDetails:")
            for key, value in metadata.items():
                lines.append(f"  {key}: {value}")
        
        lines.append("\n" + "=" * 50)
        return "\n".join(lines)
    
    def _format_error_message(self, error_message: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Format error message with metadata.
        
        Args:
            error_message: Error message
            metadata: Optional metadata dictionary
            
        Returns:
            Formatted message string
        """
        lines = [
            "Pipeline Execution Failed",
            "=" * 50,
            f"\nError: {error_message}\n"
        ]
        
        if metadata:
            lines.append("\nDetails:")
            for key, value in metadata.items():
                lines.append(f"  {key}: {value}")
        
        lines.append("\n" + "=" * 50)
        return "\n".join(lines)
    
    def get_email_subject(self, is_success: bool, table_name: str = 'Pipeline') -> str:
        """
        Generate email subject line.
        
        Args:
            is_success: True for success notification, False for error
            table_name: Name of the table/pipeline
            
        Returns:
            Formatted subject line
        """
        status = "Success" if is_success else "Error"
        return f"{self.subject_prefix} {status}: {table_name}"
