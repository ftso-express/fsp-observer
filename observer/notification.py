from typing import Any

import requests

from configuration.types import (
    NotificationDiscord,
    NotificationGeneric,
    NotificationSlack,
    NotificationTelegram,
)

from .message import Message


def notify(
    url: str, method: str, headers: dict[str, str], json: dict[str, Any]
) -> requests.Response | None:
    try:
        return requests.request(
            url=url,
            method=method,
            headers=headers,
            json=json,
        )
    except Exception:
        pass


def notify_discord(
    config: NotificationDiscord, message: str
) -> requests.Response | None:
    return notify(
        config.webhook_url,
        "POST",
        headers={"Content-Type": "application/json"},
        json={"content": message},
    )


def notify_slack(config: NotificationSlack, message: str) -> requests.Response | None:
    return notify(
        config.webhook_url,
        "POST",
        headers={"Content-Type": "application/json"},
        json={"text": message},
    )


def notify_telegram(
    config: NotificationTelegram, message: str
) -> requests.Response | None:
    return notify(
        f"https://api.telegram.org/bot{config.bot_token}/sendMessage",
        "POST",
        headers={"Content-Type": "application/json"},
        json={"chat_id": config.chat_id, "text": message},
    )


def notify_generic(
    config: NotificationGeneric, issue: "Message"
) -> requests.Response | None:
    return notify(
        config.webhook_url,
        "POST",
        headers={"Content-Type": "application/json"},
        json={"level": issue.level.value, "message": issue.message},
    )
