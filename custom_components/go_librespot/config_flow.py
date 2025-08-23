"""Config flow for Go Librespot integration."""
import logging
from typing import Any
import voluptuous as vol
import aiohttp
import asyncio

from homeassistant import config_entries
from homeassistant.const import CONF_HOST, CONF_PORT, CONF_NAME
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import DOMAIN, DEFAULT_PORT, DEFAULT_NAME

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema({
    vol.Required(CONF_HOST): str,
    vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
    vol.Optional(CONF_NAME, default=DEFAULT_NAME): str,
})


class GoLibrespotConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Go Librespot."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors = {}

        if user_input is not None:
            host = user_input[CONF_HOST]
            port = user_input[CONF_PORT]
            name = user_input[CONF_NAME]

            # Test connection
            session = async_get_clientsession(self.hass)
            try:
                async with asyncio.timeout(10):
                    async with session.get(f"http://{host}:{port}/") as response:
                        if response.status == 200:
                            # Create unique ID based on host:port
                            unique_id = f"{host}_{port}"
                            await self.async_set_unique_id(unique_id)
                            self._abort_if_unique_id_configured()

                            return self.async_create_entry(
                                title=name,
                                data={
                                    CONF_HOST: host,
                                    CONF_PORT: port,
                                    CONF_NAME: name,
                                },
                            )
                        else:
                            errors["base"] = "cannot_connect"
            except (aiohttp.ClientError, asyncio.TimeoutError):
                errors["base"] = "cannot_connect"
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors
        )