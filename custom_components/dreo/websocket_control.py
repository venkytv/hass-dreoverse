"""Dreo WebSocket control helpers."""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import urlencode

from aiohttp import ClientError
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

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
    access_token = _access_token_from_client(client)
    payload = {
        "deviceSn": device_id,
        "method": "control",
        "params": params,
        "timestamp": int(time.time() * 1000),
    }

    session = async_get_clientsession(hass)
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
            async with session.ws_connect(
                url,
                timeout=WS_TIMEOUT,
                headers=WS_HEADERS,
                heartbeat=15,
            ) as ws:
                await ws.send_json(payload)
                reply = await ws.receive(timeout=WS_REPLY_TIMEOUT)
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
        except (ClientError, TimeoutError) as ex:
            last_error = ex
            _LOGGER.debug(
                "Dreo WebSocket control failed for %s via %s",
                device_id,
                server,
                exc_info=ex,
            )

    msg = f"Failed to send Dreo WebSocket control for {device_id}"
    raise DreoWebSocketControlError(msg) from last_error
