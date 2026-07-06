"""Constants for the WordPress Bridge integration."""

from __future__ import annotations

DOMAIN = "wordpress_bridge"

CONF_SITE_URL = "site_url"
CONF_API_TOKEN = "api_token"
CONF_ENTITY_IDS = "entity_ids"
CONF_POLL_INTERVAL = "poll_interval"
CONF_PUSH_ON_START = "push_on_start"

DEFAULT_POLL_INTERVAL = 5
DEFAULT_PUSH_ON_START = True

MIN_POLL_INTERVAL = 2
MAX_POLL_INTERVAL = 300

API_NAMESPACE = "ha-bridge/v1"
DEFAULT_COMMAND_LIMIT = 10

ALLOWED_SERVICE_COMMANDS: dict[str, set[str]] = {
    "switch": {"turn_on", "turn_off", "toggle"},
    "light": {"turn_on", "turn_off", "toggle"},
    "input_boolean": {"turn_on", "turn_off", "toggle"},
    "fan": {"turn_on", "turn_off", "toggle"},
    "cover": {"open_cover", "close_cover", "stop_cover"},
}
