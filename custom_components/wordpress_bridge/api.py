"""WordPress Bridge API client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aiohttp import ClientError, ClientResponseError, ClientSession
from .const import API_NAMESPACE, DEFAULT_COMMAND_LIMIT


class WordPressBridgeApiError(Exception):
    """Base API error for WordPress Bridge."""


class WordPressBridgeAuthError(WordPressBridgeApiError):
    """Raised when WordPress rejects the bridge token."""


@dataclass(slots=True)
class WordPressBridgeApi:
    """Small async client for the WordPress bridge endpoints."""

    session: ClientSession
    site_url: str
    api_token: str

    def __post_init__(self) -> None:
        self.site_url = self.site_url.rstrip("/")

    def _url(self, path: str) -> str:
        """Build a WordPress REST endpoint URL."""
        return f"{self.site_url}/wp-json/{API_NAMESPACE}/{path.lstrip('/')}"

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Make an API request and return decoded JSON."""
        try:
            async with self.session.request(
                method,
                self._url(path),
                headers=self._headers,
                json=json,
                params=params,
                timeout=15,
            ) as response:
                if response.status in (401, 403):
                    raise WordPressBridgeAuthError("WordPress rejected the bridge token")

                response.raise_for_status()

                if response.content_type == "application/json":
                    return await response.json()

                return {}
        except WordPressBridgeAuthError:
            raise
        except ClientResponseError as err:
            raise WordPressBridgeApiError(
                f"WordPress returned HTTP {err.status}"
            ) from err
        except ClientError as err:
            raise WordPressBridgeApiError(f"Could not reach WordPress: {err}") from err

    async def async_ping(self) -> dict[str, Any]:
        """Check whether the WordPress bridge endpoint is reachable."""
        data = await self._request("GET", "status")
        return data if isinstance(data, dict) else {}

    async def async_push_states(self, states: list[dict[str, Any]]) -> dict[str, Any]:
        """Push one or more Home Assistant entity states to WordPress."""
        data = await self._request("POST", "states", json={"states": states})
        return data if isinstance(data, dict) else {}

    async def async_get_pending_commands(
        self, limit: int = DEFAULT_COMMAND_LIMIT
    ) -> list[dict[str, Any]]:
        """Fetch pending commands created by WordPress."""
        data = await self._request("GET", "commands/pending", params={"limit": limit})

        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict) and isinstance(data.get("commands"), list):
            return [item for item in data["commands"] if isinstance(item, dict)]

        return []

    async def async_ack_command(
        self,
        command_id: int | str,
        *,
        status: str,
        message: str = "",
        state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Acknowledge a command result back to WordPress."""
        payload: dict[str, Any] = {"status": status, "message": message}
        if state is not None:
            payload["state"] = state

        data = await self._request(
            "POST",
            f"commands/{command_id}/ack",
            json=payload,
        )
        return data if isinstance(data, dict) else {}
