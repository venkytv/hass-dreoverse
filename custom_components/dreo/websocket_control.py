"""Dreo WebSocket control helpers."""

from __future__ import annotations

import json
import logging
import time
from typing import Any
from urllib.parse import urlencode

import requests
import websocket
from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

CLIENT_ID = "d8a56a73d93b427cad801116dc4d3188"
CLIENT_SECRET = "2ac9b179f7e84be58bb901d6ed8bf374"
HIMEI = "463299817f794e52a228868167df3f34"
HTTP_TIMEOUT = 20
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


def _app_login(server: str, username: str, password_hash: str) -> dict[str, Any]:
    """Authenticate with the same app OAuth flow used by the Dreo mobile app."""
    response = requests.post(
        f"https://app-api-{server}.dreo-tech.com/api/oauth/login",
        params={"timestamp": int(time.time() * 1000)},
        headers=WS_HEADERS | {"content-type": "application/json; charset=UTF-8"},
        json={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "email": username,
            "encrypt": "ciphertext",
            "grant_type": "email-password",
            "himei": HIMEI,
            "password": password_hash,
            "scope": "all",
        },
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data")
    if not isinstance(data, dict) or not data.get("access_token"):
        msg = f"Dreo app login failed: {payload.get('msg', 'unknown error')}"
        raise DreoWebSocketControlError(msg)
    return data


def _app_access_token(
    client: Any,
    username: str | None,
    password_hash: str | None,
) -> tuple[str, list[str]]:
    """Fetch a Dreo app token and ordered candidate WebSocket regions."""
    if not username or not password_hash:
        return _access_token_from_client(client), _servers_from_client(client)

    first_server = _servers_from_client(client)[0]
    auth = _app_login(first_server, username, password_hash)
    region = auth.get("region")
    if region == "EU" and first_server != "eu":
        auth = _app_login("eu", username, password_hash)
        return str(auth["access_token"]), ["eu", "us"]
    if region == "NA" and first_server != "us":
        auth = _app_login("us", username, password_hash)
        return str(auth["access_token"]), ["us", "eu"]
    return str(auth["access_token"]), _servers_from_client(client)


async def async_send_control(
    hass: HomeAssistant,
    client: Any,
    username: str | None,
    password_hash: str | None,
    device_id: str,
    params: dict[str, Any],
) -> None:
    """Send raw Dreo control params over the app WebSocket channel."""
    await hass.async_add_executor_job(
        _send_control, client, username, password_hash, device_id, params
    )


def _send_control(
    client: Any,
    username: str | None,
    password_hash: str | None,
    device_id: str,
    params: dict[str, Any],
) -> None:
    """Send raw Dreo control params over the app WebSocket channel."""
    access_token, servers = _app_access_token(client, username, password_hash)
    payload = {
        "deviceSn": device_id,
        "method": "control",
        "params": params,
        "timestamp": int(time.time() * 1000),
    }

    last_error: Exception | None = None
    for server in servers:
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
