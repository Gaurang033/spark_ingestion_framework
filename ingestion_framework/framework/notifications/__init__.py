"""
Notifications Package

Provides notification implementations for various channels (email, slack, teams).
All notifications inherit from the NotificationChannel abstract base class.

Usage:
    from framework.notifications import NotificationFactory
    
    notifier = NotificationFactory.create_notification(config)
    if notifier:
        notifier.notify_success("Pipeline completed", metadata={'table': 'customers'})
"""

from framework.notifications.factory import NotificationFactory
from framework.notifications.email import EmailNotification
from framework.notifications.slack import SlackNotification
from framework.notifications.teams import TeamsNotification

__all__ = [
    'NotificationFactory',
    'EmailNotification',
    'SlackNotification',
    'TeamsNotification'
]
