"""Dreo WebSocket control helpers."""

from __future__ import annotations

import json
import logging
import time
from typing import Any
from urllib.parse import urlencode

import websocket
from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

WS_TIMEOUT = 10
WS_REPLY_TIMEOUT = 5
WS_HEADERS = {
    "ua": "dreo/2.8.1 (iPhone; iOS 18.0.0; Scale/3.00)",
    "lang": "en",
    "accept-encoding": "gzip",
    "user-agent": "okhttp/4.9.1",
}


class DreoWebSocketControlError(Exception):
    """Raised when a Dreo WebSocket command cannot be sent."""


def _servers_from_client(client: Any) -> list[str]:
    """Infer candidate Dreo WebSocket server regions from the pydreo client."""
    endpoint = str(getattr(client, "endpoint", "") or "").lower()
    if "app-api-eu" in endpoint:
        return ["eu", "us"]
    return ["us", "eu"]


def _access_token_from_client(client: Any) -> str:
    """Fetch the access token from the pydreo client."""
    access_token = str(getattr(client, "access_token", "") or "")
    if not access_token:
        msg = "Dreo client does not expose an access token"
        raise DreoWebSocketControlError(msg)
    return access_token


async def async_send_control(
    hass: HomeAssistant,
    client: Any,
    device_id: str,
    params: dict[str, Any],
) -> None:
    """Send raw Dreo control params over the app WebSocket channel."""
    await hass.async_add_executor_job(_send_control, client, device_id, params)


def _send_control(
    client: Any,
    device_id: str,
    params: dict[str, Any],
) -> None:
    """Send raw Dreo control params over the app WebSocket channel."""
    access_token = _access_token_from_client(client)
    payload = {
        "deviceSn": device_id,
        "method": "control",
        "params": params,
        "timestamp": int(time.time() * 1000),
    }

    last_error: Exception | None = None
    for server in _servers_from_client(client):
        query = urlencode(
            {
                "accessToken": access_token,
                "timestamp": int(time.time() * 1000),
            }
        )
        url = f"wss://wsb-{server}.dreo-tech.com/websocket?{query}"
        try:
            ws = websocket.create_connection(
                url,
                timeout=WS_TIMEOUT,
                header=[f"{key}: {value}" for key, value in WS_HEADERS.items()],
            )
            try:
                ws.settimeout(WS_REPLY_TIMEOUT)
                ws.send(json.dumps(payload))
                reply = ws.recv()
                _LOGGER.debug(
                    "Sent Dreo WebSocket control for %s via %s: %s",
                    device_id,
                    server,
                    params,
                )
                _LOGGER.debug(
                    "Dreo WebSocket control reply for %s: %s",
                    device_id,
                    reply,
                )
                return
            finally:
                ws.close()
        except (
            TimeoutError,
            OSError,
            websocket.WebSocketException,
        ) as ex:
            last_error = ex
            _LOGGER.debug(
                "Dreo WebSocket control failed for %s via %s",
                device_id,
                server,
                exc_info=ex,
            )

    msg = f"Failed to send Dreo WebSocket control for {device_id}"
    raise DreoWebSocketControlError(msg) from last_error
