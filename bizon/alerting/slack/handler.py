import os
from typing import Dict, List

import requests
from loguru import logger

from bizon.alerting.alerts import AbstractAlert, AlertMethod
from bizon.alerting.models import LogLevel
from bizon.alerting.slack.config import SlackConfig


class SlackHandler(AbstractAlert):
    def __init__(self, config: SlackConfig, log_levels: List[LogLevel] = [LogLevel.ERROR]):
        super().__init__(type=AlertMethod.SLACK, config=config, log_levels=log_levels)
        self.webhook_url = config.webhook_url

    def handler(self, message: Dict) -> None:
        """
        Custom handler to send error logs to Slack, with additional context.
        """
        log_entry = message.record
        error_message = (
            f"*Sync*: `{os.environ.get('BIZON_SYNC_NAME', 'N/A')}`\n"
            f"*Source*: `{os.environ.get('BIZON_SOURCE_NAME', 'N/A')}` - `{os.environ.get('BIZON_SOURCE_STREAM', 'N/A')}`\n"  # noqa
            f"*Destination*: `{os.environ.get('BIZON_DESTINATION_NAME', 'N/A')}`\n\n"
            f"*Message:*\n```{log_entry['message']}```\n"
            f"*File:* `{log_entry['file'].path}:{log_entry['line']}`\n"
            f"*Function:* `{log_entry['function']}`\n"
            f"*Level:* `{log_entry['level'].name}`\n"
        )

        payload = {"text": f":rotating_light: *Bizon Pipeline Alert* :rotating_light:\n\n{error_message}"}

        try:
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send log to Slack: {e}")
        return None
