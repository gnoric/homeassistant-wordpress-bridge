"""Config flow for the WordPress Bridge integration."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import EntitySelector, EntitySelectorConfig

from .api import (
    WordPressBridgeApi,
    WordPressBridgeApiError,
    WordPressBridgeAuthError,
)
from .const import (
    CONF_API_TOKEN,
    CONF_ENTITY_IDS,
    CONF_POLL_INTERVAL,
    CONF_PUSH_ON_START,
    CONF_SITE_URL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_PUSH_ON_START,
    DOMAIN,
    MAX_POLL_INTERVAL,
    MIN_POLL_INTERVAL,
)


def _parse_entity_ids(raw_value: str | list[str]) -> list[str]:
    """Normalize entity IDs entered in the form."""
    if isinstance(raw_value, list):
        values = raw_value
    else:
        values = raw_value.replace("\n", ",").split(",")

    return sorted({value.strip() for value in values if value.strip()})


def _schema(defaults: dict[str, Any] | None = None) -> vol.Schema:
    """Build the config/options schema."""
    defaults = defaults or {}
    return vol.Schema(
        {
            vol.Required(
                CONF_SITE_URL,
                default=defaults.get(CONF_SITE_URL, "https://example.com"),
            ): str,
            vol.Required(CONF_API_TOKEN): str,
            vol.Required(
                CONF_ENTITY_IDS,
                default=defaults.get(CONF_ENTITY_IDS, []),
            ): EntitySelector(EntitySelectorConfig(multiple=True)),
            vol.Required(
                CONF_POLL_INTERVAL,
                default=defaults.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL),
            ): vol.All(vol.Coerce(int), vol.Range(min=MIN_POLL_INTERVAL, max=MAX_POLL_INTERVAL)),
            vol.Required(
                CONF_PUSH_ON_START,
                default=defaults.get(CONF_PUSH_ON_START, DEFAULT_PUSH_ON_START),
            ): bool,
        }
    )


class WordPressBridgeConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for WordPress Bridge."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            user_input = dict(user_input)
            user_input[CONF_SITE_URL] = user_input[CONF_SITE_URL].rstrip("/")
            user_input[CONF_ENTITY_IDS] = _parse_entity_ids(user_input[CONF_ENTITY_IDS])

            session = async_get_clientsession(self.hass)
            api = WordPressBridgeApi(
                session,
                user_input[CONF_SITE_URL],
                user_input[CONF_API_TOKEN],
            )

            try:
                await api.async_ping()
            except WordPressBridgeAuthError:
                errors["base"] = "invalid_auth"
            except WordPressBridgeApiError:
                errors["base"] = "cannot_connect"
            else:
                await self.async_set_unique_id(user_input[CONF_SITE_URL])
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=user_input[CONF_SITE_URL],
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=_schema(),
            errors=errors,
        )

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Create the options flow."""
        return WordPressBridgeOptionsFlow(config_entry)


class WordPressBridgeOptionsFlow(config_entries.OptionsFlow):
    """Handle WordPress Bridge options."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage options."""
        if user_input is not None:
            user_input = dict(user_input)
            user_input[CONF_SITE_URL] = user_input[CONF_SITE_URL].rstrip("/")
            user_input[CONF_ENTITY_IDS] = _parse_entity_ids(user_input[CONF_ENTITY_IDS])
            return self.async_create_entry(title="", data=user_input)

        defaults = {**self.config_entry.data, **self.config_entry.options}
        return self.async_show_form(
            step_id="init",
            data_schema=_schema(defaults),
        )
