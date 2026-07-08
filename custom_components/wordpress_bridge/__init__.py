"""The WordPress Bridge integration."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any

from homeassistant.components import persistent_notification
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
COMMAND_STATE_SETTLE_TIMEOUT = 10
BRIGHTNESS_TOLERANCE = 2
RGB_TOLERANCE = 2


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
        runtimes = list(hass.data.get(DOMAIN, {}).values())
        commands_seen = 0
        commands_handled = 0
        messages: list[str] = [
            f"Active bridge entries: {len(runtimes)}",
        ]

        if not runtimes:
            message = "\n".join(
                [
                    *messages,
                    "No active WordPress Bridge config entries are registered.",
                    "The domain service exists, but async_setup_entry is not active.",
                ]
            )
            persistent_notification.async_create(
                hass,
                message,
                title="WordPress Bridge manual poll",
                notification_id="wordpress_bridge_manual_poll",
            )
            _LOGGER.error(message)
            return

        for runtime in runtimes:
            messages.append(
                f"Polling {len(runtime.entity_ids)} exposed entities at interval {runtime.poll_interval}s"
            )
            try:
                commands = await runtime.api.async_get_pending_commands(entity_ids=runtime.entity_ids)
                commands_seen += len(commands)
                _LOGGER.warning("Manual WordPress Bridge poll returned %d commands", len(commands))
                for command in commands:
                    await _async_handle_command(hass, runtime.api, runtime.entity_ids, command)
                    commands_handled += 1
            except WordPressBridgeAuthError:
                messages.append("WordPress rejected the bridge token during manual poll.")
                _LOGGER.error("WordPress rejected the bridge token during manual poll")
            except WordPressBridgeApiError as err:
                messages.append(f"Manual poll failed: {err}")
                _LOGGER.warning("Manual WordPress Bridge poll failed: %s", err)
            except Exception:
                messages.append("Unexpected error during manual poll. Check the Home Assistant log.")
                _LOGGER.exception("Unexpected error during manual WordPress Bridge poll")

        messages.append(f"Commands fetched: {commands_seen}")
        messages.append(f"Commands handled: {commands_handled}")
        persistent_notification.async_create(
            hass,
            "\n".join(messages),
            title="WordPress Bridge manual poll",
            notification_id="wordpress_bridge_manual_poll",
        )

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
            commands = await api.async_get_pending_commands(entity_ids=entity_ids)
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

    before_state = hass.states.get(entity_id)

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

    state = await _async_wait_for_command_state(hass, entity_id, before_state, service, payload)
    _LOGGER.debug("Command %s completed; latest state for %s is %s", command_id, entity_id, state.state if state else None)
    await api.async_ack_command(
        command_id,
        status="done",
        message="Command executed",
        state=_serialize_state(state) if state is not None else None,
    )
    if state is not None:
        await _async_push_latest_state(api, state)


async def _async_wait_for_command_state(
    hass: HomeAssistant,
    entity_id: str,
    before_state: State | None,
    service: str,
    payload: Any,
) -> State | None:
    """Wait briefly for the service call to produce the expected entity state."""
    current = hass.states.get(entity_id)
    if _is_acceptable_command_state(before_state, current, service, payload):
        return current

    future: asyncio.Future[State] = hass.loop.create_future()

    @callback
    def state_changed(event: Event) -> None:
        new_state: State | None = event.data.get("new_state")
        if _is_acceptable_command_state(before_state, new_state, service, payload) and not future.done():
            future.set_result(new_state)

    unsubscribe = async_track_state_change_event(hass, [entity_id], state_changed)
    try:
        current = hass.states.get(entity_id)
        if _is_acceptable_command_state(before_state, current, service, payload):
            return current

        return await asyncio.wait_for(future, timeout=COMMAND_STATE_SETTLE_TIMEOUT)
    except TimeoutError:
        _LOGGER.debug("Timed out waiting for %s to settle after command", entity_id)
        return hass.states.get(entity_id)
    finally:
        unsubscribe()


def _is_acceptable_command_state(
    before_state: State | None,
    candidate: State | None,
    service: str,
    payload: Any,
) -> bool:
    """Return true when candidate is a good command result state."""
    if not _is_newer_state(before_state, candidate):
        return False

    target_match = _command_target_matches(before_state, candidate, service, payload)
    return target_match is not False


def _command_target_matches(
    before_state: State | None,
    candidate: State | None,
    service: str,
    payload: Any,
) -> bool | None:
    """Match command-specific target state, or return None when no target is known."""
    if candidate is None:
        return False

    expected_state = _expected_state_after_command(before_state, service)
    if expected_state is not None and candidate.state != expected_state:
        return False

    if not isinstance(payload, dict):
        return None if expected_state is None else True

    brightness_pct = payload.get("brightness_pct")
    if brightness_pct is not None:
        try:
            expected_brightness = round(max(0, min(100, float(brightness_pct))) / 100 * 255)
            actual_brightness = int(candidate.attributes.get("brightness"))
        except (TypeError, ValueError):
            return False

        if abs(actual_brightness - expected_brightness) > BRIGHTNESS_TOLERANCE:
            return False

    rgb_color = payload.get("rgb_color")
    if rgb_color is not None:
        actual_rgb = candidate.attributes.get("rgb_color")
        if not _rgb_values_close(rgb_color, actual_rgb):
            return False

    if expected_state is None and brightness_pct is None and rgb_color is None:
        return None

    return True


def _expected_state_after_command(before_state: State | None, service: str) -> str | None:
    """Return the expected state for simple power commands."""
    if service == "turn_on":
        return "on"
    if service == "turn_off":
        return "off"
    if service == "toggle" and before_state is not None:
        return "off" if before_state.state == "on" else "on"
    return None


def _rgb_values_close(expected: Any, actual: Any) -> bool:
    """Return true when two RGB triplets are effectively the same."""
    if not isinstance(expected, (list, tuple)) or not isinstance(actual, (list, tuple)):
        return False
    if len(expected) < 3 or len(actual) < 3:
        return False

    try:
        return all(abs(int(actual[index]) - int(expected[index])) <= RGB_TOLERANCE for index in range(3))
    except (TypeError, ValueError):
        return False


def _is_newer_state(before_state: State | None, candidate: State | None) -> bool:
    """Return true when candidate appears newer than the pre-command state."""
    if candidate is None:
        return False

    if before_state is None:
        return True

    return (
        candidate.state != before_state.state
        or candidate.last_updated != before_state.last_updated
        or candidate.context.id != before_state.context.id
    )


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
