"""The WordPress Bridge integration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant, State, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.typing import ConfigType

from .api import (
    WordPressBridgeApi,
    WordPressBridgeApiError,
    WordPressBridgeAuthError,
)
from .const import (
    ALLOWED_SERVICE_COMMANDS,
    CONF_API_TOKEN,
    CONF_ENTITY_IDS,
    CONF_POLL_INTERVAL,
    CONF_PUSH_ON_START,
    CONF_SITE_URL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_PUSH_ON_START,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)


@dataclass
class WordPressBridgeRuntime:
    """Runtime objects for one bridge config entry."""

    api: WordPressBridgeApi
    entity_ids: set[str]
    poll_interval: int
    stop_event: asyncio.Event
    poll_task: asyncio.Task[None]
    unsubscribe_state_listener: Callable[[], None]
    push_tasks: set[asyncio.Task[None]]


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the integration package."""
    return True


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Set up WordPress Bridge from a config entry."""
    config = {**entry.data, **entry.options}
    api = WordPressBridgeApi(
        async_get_clientsession(hass),
        config[CONF_SITE_URL],
        config[CONF_API_TOKEN],
    )

    try:
        await api.async_ping()
    except WordPressBridgeAuthError as err:
        raise ConfigEntryAuthFailed from err
    except WordPressBridgeApiError as err:
        raise ConfigEntryNotReady from err

    entity_ids = set(config.get(CONF_ENTITY_IDS, []))
    poll_interval = int(config.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL))
    push_tasks: set[asyncio.Task[None]] = set()

    async def async_push_state(state: State) -> None:
        """Push a single state to WordPress."""
        try:
            await api.async_push_states([_serialize_state(state)])
        except WordPressBridgeAuthError:
            _LOGGER.error("WordPress rejected the bridge token while pushing state")
        except WordPressBridgeApiError as err:
            _LOGGER.warning("Could not push state for %s: %s", state.entity_id, err)

    @callback
    def state_changed(event: Event) -> None:
        """Handle a tracked state change."""
        new_state: State | None = event.data.get("new_state")
        if new_state is None:
            return

        task = hass.async_create_task(async_push_state(new_state))
        push_tasks.add(task)
        task.add_done_callback(push_tasks.discard)

    unsubscribe_state_listener = async_track_state_change_event(
        hass,
        list(entity_ids),
        state_changed,
    )

    stop_event = asyncio.Event()
    poll_task = hass.async_create_task(
        _async_command_poll_loop(hass, api, entity_ids, poll_interval, stop_event)
    )

    runtime = WordPressBridgeRuntime(
        api=api,
        entity_ids=entity_ids,
        poll_interval=poll_interval,
        stop_event=stop_event,
        poll_task=poll_task,
        unsubscribe_state_listener=unsubscribe_state_listener,
        push_tasks=push_tasks,
    )
    entry.runtime_data = runtime

    if config.get(CONF_PUSH_ON_START, DEFAULT_PUSH_ON_START):
        states = [
            _serialize_state(state)
            for entity_id in entity_ids
            if (state := hass.states.get(entity_id)) is not None
        ]
        if states:
            try:
                await api.async_push_states(states)
            except WordPressBridgeApiError as err:
                _LOGGER.warning("Initial state push failed: %s", err)

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _stop_runtime(runtime))
    )

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Unload a config entry."""
    runtime = entry.runtime_data
    await _async_stop_runtime(runtime)
    return True


async def _async_update_listener(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Reload the integration when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


def _stop_runtime(runtime: WordPressBridgeRuntime):
    """Return an event callback that stops runtime tasks."""

    async def _async_stop(_: Event) -> None:
        await _async_stop_runtime(runtime)

    return _async_stop


async def _async_stop_runtime(runtime: WordPressBridgeRuntime) -> None:
    """Stop listeners and background tasks."""
    runtime.unsubscribe_state_listener()
    runtime.stop_event.set()
    runtime.poll_task.cancel()

    tasks = {runtime.poll_task, *runtime.push_tasks}
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _async_command_poll_loop(
    hass: HomeAssistant,
    api: WordPressBridgeApi,
    entity_ids: set[str],
    poll_interval: int,
    stop_event: asyncio.Event,
) -> None:
    """Poll WordPress for pending commands until stopped."""
    while not stop_event.is_set():
        try:
            commands = await api.async_get_pending_commands()
            for command in commands:
                await _async_handle_command(hass, api, entity_ids, command)
        except WordPressBridgeAuthError:
            _LOGGER.error("WordPress rejected the bridge token while polling commands")
        except WordPressBridgeApiError as err:
            _LOGGER.warning("Could not fetch WordPress commands: %s", err)
        except Exception:
            _LOGGER.exception("Unexpected error while polling WordPress commands")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
        except TimeoutError:
            continue


async def _async_handle_command(
    hass: HomeAssistant,
    api: WordPressBridgeApi,
    entity_ids: set[str],
    command: dict[str, Any],
) -> None:
    """Execute one WordPress command locally in Home Assistant."""
    command_id = command.get("id")
    entity_id = command.get("entity_id")
    service = command.get("command")
    payload = command.get("payload") or {}

    if command_id is None:
        _LOGGER.warning("Ignoring command without an id: %s", command)
        return

    if not isinstance(entity_id, str) or entity_id not in entity_ids:
        await api.async_ack_command(
            command_id,
            status="failed",
            message=f"Entity {entity_id!r} is not exposed by this bridge",
        )
        return

    if "." not in entity_id:
        await api.async_ack_command(
            command_id,
            status="failed",
            message=f"Invalid entity_id {entity_id!r}",
        )
        return

    domain = entity_id.split(".", 1)[0]
    if not isinstance(service, str) or service not in ALLOWED_SERVICE_COMMANDS.get(domain, set()):
        await api.async_ack_command(
            command_id,
            status="failed",
            message=f"Command {service!r} is not allowed for {domain}",
        )
        return

    service_data = {"entity_id": entity_id}
    if isinstance(payload, dict):
        service_data.update(payload)

    try:
        await hass.services.async_call(
            domain,
            service,
            service_data,
            blocking=True,
        )
    except Exception as err:
        _LOGGER.exception("Command %s failed for %s", service, entity_id)
        await api.async_ack_command(
            command_id,
            status="failed",
            message=str(err),
        )
        return

    state = hass.states.get(entity_id)
    await api.async_ack_command(
        command_id,
        status="done",
        message="Command executed",
        state=_serialize_state(state) if state is not None else None,
    )


def _serialize_state(state: State) -> dict[str, Any]:
    """Convert a Home Assistant state object to WordPress JSON."""
    return {
        "entity_id": state.entity_id,
        "state": state.state,
        "attributes": dict(state.attributes),
        "last_changed": _format_datetime(state.last_changed),
        "last_updated": _format_datetime(state.last_updated),
        "context_id": state.context.id,
    }


def _format_datetime(value: datetime) -> str:
    """Format a datetime for transport."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
