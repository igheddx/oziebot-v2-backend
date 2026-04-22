from __future__ import annotations

import logging
from typing import Protocol

import httpx

log = logging.getLogger("alerts-worker.adapters")


class NotificationAdapter(Protocol):
    channel: str

    def send(self, destination: str, message: str, payload: dict) -> None: ...


class SmsAdapter:
    channel = "sms"

    def __init__(self, webhook_url: str | None) -> None:
        self._webhook_url = webhook_url

    def send(self, destination: str, message: str, payload: dict) -> None:
        if not self._webhook_url:
            log.info("sms noop destination=%s message=%s", destination, message)
            return
        with httpx.Client(timeout=10) as client:
            r = client.post(
                self._webhook_url,
                json={"to": destination, "message": message, "payload": payload},
            )
            r.raise_for_status()


class SlackAdapter:
    channel = "slack"

    def __init__(self, webhook_url: str | None) -> None:
        self._webhook_url = webhook_url

    @staticmethod
    def _is_webhook_url(value: str) -> bool:
        normalized = value.strip().lower()
        return normalized.startswith(
            "https://hooks.slack.com/"
        ) or normalized.startswith("https://hooks.slack-gov.com/")

    def send(self, destination: str, message: str, payload: dict) -> None:
        destination = destination.strip()
        webhook_url = (
            destination if self._is_webhook_url(destination) else self._webhook_url
        )
        if not webhook_url:
            log.info("slack noop destination=%s message=%s", destination, message)
            return
        text = (
            message
            if self._is_webhook_url(destination)
            else f"{message}\nDestination: {destination}"
        )
        with httpx.Client(timeout=10) as client:
            r = client.post(webhook_url, json={"text": text, "metadata": payload})
            r.raise_for_status()


class TelegramAdapter:
    channel = "telegram"

    def __init__(self, bot_token: str | None) -> None:
        self._bot_token = bot_token

    def send(self, destination: str, message: str, payload: dict) -> None:
        if not self._bot_token:
            log.info("telegram noop destination=%s message=%s", destination, message)
            return
        url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
        with httpx.Client(timeout=10) as client:
            r = client.post(url, json={"chat_id": destination, "text": message})
            r.raise_for_status()
