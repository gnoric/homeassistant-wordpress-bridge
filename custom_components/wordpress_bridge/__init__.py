"""The WordPress Bridge integration."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, HomeAssistant, State, callback
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
    hass.data.setdefault(DOMAIN, {})

    async def async_poll_now(_: Any) -> None:
        """Poll all configured WordPress bridges immediately."""
        _LOGGER.warning("Manual WordPress Bridge poll requested")
        for runtime in list(hass.data.get(DOMAIN, {}).values()):
            try:
                commands = await runtime.api.async_get_pending_commands()
                _LOGGER.warning("Manual WordPress Bridge poll returned %d commands", len(commands))
                for command in commands:
                    await _async_handle_command(hass, runtime.api, runtime.entity_ids, command)
            except WordPressBridgeAuthError:
                _LOGGER.error("WordPress rejected the bridge token during manual poll")
            except WordPressBridgeApiError as err:
                _LOGGER.warning("Manual WordPress Bridge poll failed: %s", err)
            except Exception:
                _LOGGER.exception("Unexpected error during manual WordPress Bridge poll")

    if not hass.services.has_service(DOMAIN, "poll_now"):
        hass.services.async_register(DOMAIN, "poll_now", async_poll_now)

    return True


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Set up WordPress Bridge from a config entry."""
    config = {**entry.data, **entry.options}
    if not str(config.get(CONF_API_TOKEN, "")).strip() and entry.data.get(CONF_API_TOKEN):
        config[CONF_API_TOKEN] = entry.data[CONF_API_TOKEN]

    api = WordPressBridgeApi(
        async_get_clientsession(hass),
        config[CONF_SITE_URL],
        str(config[CONF_API_TOKEN]).strip(),
    )

    entity_ids = set(config.get(CONF_ENTITY_IDS, []))
    poll_interval = int(config.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL))
    push_tasks: set[asyncio.Task[None]] = set()

    _LOGGER.info(
        "WordPress Bridge loaded for %s with %d exposed entities; polling every %d seconds",
        config[CONF_SITE_URL],
        len(entity_ids),
        poll_interval,
    )

    async def async_push_state(state: State) -> None:
        """Push a single state to WordPress."""
        try:
            await api.async_push_states([_serialize_state(state)])
        except WordPressBridgeAuthError:
            _LOGGER.error("WordPress rejected the bridge token while pushing state")
        except WordPressBridgeApiError as err:
            _LOGGER.warning("Could not push state for %s: %s", state.entity_id, err)

    async def async_push_startup_states() -> None:
        """Push startup states without blocking Home Assistant startup."""
        states = [
            _serialize_state(state)
            for entity_id in entity_ids
            if (state := hass.states.get(entity_id)) is not None
        ]
        _LOGGER.debug("Pushing %d startup states to WordPress", len(states))
        if not states:
            return

        try:
            await api.async_push_states(states)
        except WordPressBridgeAuthError:
            _LOGGER.error("WordPress rejected the bridge token while pushing startup states")
        except WordPressBridgeApiError as err:
            _LOGGER.warning("Initial state push failed: %s", err)
        except Exception:
            _LOGGER.exception("Unexpected error while pushing startup states")

    @callback
    def state_changed(event: Event) -> None:
        """Handle a tracked state change."""
        new_state: State | None = event.data.get("new_state")
        if new_state is None:
            return

        task = _create_entry_background_task(
            hass,
            entry,
            async_push_state(new_state),
            f"{DOMAIN} push state {new_state.entity_id}",
        )
        push_tasks.add(task)
        task.add_done_callback(push_tasks.discard)

    unsubscribe_state_listener = async_track_state_change_event(
        hass,
        list(entity_ids),
        state_changed,
    )

    stop_event = asyncio.Event()
    poll_task = _create_entry_background_task(
        hass,
        entry,
        _async_command_poll_loop(hass, api, entity_ids, poll_interval, stop_event),
        f"{DOMAIN} command poll loop",
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
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = runtime

    if config.get(CONF_PUSH_ON_START, DEFAULT_PUSH_ON_START):
        startup_push_task = _create_entry_background_task(
            hass,
            entry,
            async_push_startup_states(),
            f"{DOMAIN} startup state push",
        )
        push_tasks.add(startup_push_task)
        startup_push_task.add_done_callback(push_tasks.discard)

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Unload a config entry."""
    runtime = entry.runtime_data
    await _async_stop_runtime(runtime)
    hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    return True


async def _async_update_listener(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Reload the integration when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def _async_stop_runtime(runtime: WordPressBridgeRuntime) -> None:
    """Stop listeners and background tasks."""
    runtime.unsubscribe_state_listener()
    runtime.stop_event.set()
    runtime.poll_task.cancel()
    for task in runtime.push_tasks:
        task.cancel()

    tasks = {runtime.poll_task, *runtime.push_tasks}
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _create_entry_background_task(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coro: Awaitable[None],
    name: str,
) -> asyncio.Task[None]:
    """Create a background task tied to the config entry lifecycle."""
    create_background_task = getattr(entry, "async_create_background_task", None)
    if create_background_task is not None:
        return create_background_task(hass, coro, name)

    return hass.async_create_task(coro)


async def _async_command_poll_loop(
    hass: HomeAssistant,
    api: WordPressBridgeApi,
    entity_ids: set[str],
    poll_interval: int,
    stop_event: asyncio.Event,
) -> None:
    """Poll WordPress for pending commands until stopped."""
    _LOGGER.debug("WordPress command poll loop started")
    while not stop_event.is_set():
        try:
            _LOGGER.debug("Polling WordPress for pending commands")
            commands = await api.async_get_pending_commands()
            _LOGGER.debug("WordPress returned %d pending commands", len(commands))
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

    _LOGGER.debug(
        "Handling WordPress command id=%s entity=%s service=%s payload=%s",
        command_id,
        entity_id,
        service,
        payload,
    )

    if command_id is None:
        _LOGGER.warning("Ignoring command without an id: %s", command)
        return

    if not isinstance(entity_id, str) or entity_id not in entity_ids:
        _LOGGER.warning("Rejecting command %s: entity %r is not exposed", command_id, entity_id)
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
        _LOGGER.info("Executing WordPress command %s: %s.%s", command_id, domain, service)
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
    _LOGGER.debug("Command %s completed; latest state for %s is %s", command_id, entity_id, state.state if state else None)
    await api.async_ack_command(
        command_id,
        status="done",
        message="Command executed",
        state=_serialize_state(state) if state is not None else None,
    )
    if state is not None:
        await _async_push_latest_state(api, state)


async def _async_push_latest_state(api: WordPressBridgeApi, state: State) -> None:
    """Push the latest state after a command result."""
    try:
        await api.async_push_states([_serialize_state(state)])
    except WordPressBridgeApiError as err:
        _LOGGER.warning("Could not push command result state for %s: %s", state.entity_id, err)


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
