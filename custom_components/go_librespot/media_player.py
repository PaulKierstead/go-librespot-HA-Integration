"""Media player platform for Go Librespot integration."""

import logging
from typing import Any, Callable
import aiohttp
import asyncio
import json

from homeassistant.components.media_player import (
    MediaPlayerEntity,
    MediaPlayerEntityFeature,
    MediaPlayerState,
    MediaType,
)
from datetime import datetime
import time
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_PORT, CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.util import dt as dt_util  # ADDED: For proper timestamp handling

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class GoLibrespotWebSocketClient:
    """WebSocket client for Go Librespot."""

    def __init__(
        self, hass: HomeAssistant, host: str, port: int, update_callback: Callable
    ) -> None:
        """Initialize the WebSocket client."""
        self.hass = hass
        self.host = host
        self.port = port
        self.update_callback = update_callback
        self.session = async_get_clientsession(hass)
        self.ws_url = f"ws://{host}:{port}/events"
        self.base_url = f"http://{host}:{port}"
        self._ws = None
        self._listen_task = None
        self._reconnect_task = None
        self._refresh_task = None
        self._data = None
        self._connected = False
        self._last_volume_update = None
        self._position_updated_at = None  # ADDED: Store position timestamp

    @property
    def data(self):
        """Return the current data."""
        return self._data

    @property
    def connected(self) -> bool:
        """Return connection status."""
        return self._connected
    
    @property
    def position_updated_at(self) -> datetime | None:
        """Return when position was last updated."""
        return self._position_updated_at

    async def connect(self) -> bool:
        """Connect to the WebSocket."""
        try:
            # First get initial status via HTTP
            await self._fetch_initial_status()

            # Then connect to WebSocket for real-time updates
            _LOGGER.debug("Connecting to WebSocket at %s", self.ws_url)
            self._ws = await self.session.ws_connect(self.ws_url)
            self._connected = True

            # Start listening for messages
            self._listen_task = asyncio.create_task(self._listen())

            # Start periodic status refresh to ensure we don't lose track metadata
            self._refresh_task = asyncio.create_task(self._periodic_refresh())

            _LOGGER.info("Connected to Go Librespot WebSocket")
            return True

        except Exception as err:
            _LOGGER.error("Failed to connect to WebSocket: %s", err)
            self._connected = False
            # Schedule reconnection
            self._schedule_reconnect()
            return False

    async def _fetch_initial_status(self) -> None:
        """Fetch initial status via HTTP API."""
        try:
            async with asyncio.timeout(10):
                async with self.session.get(f"{self.base_url}/status") as response:
                    if response.status == 200:
                        new_data = await response.json()
                        _LOGGER.debug("Initial status response: %s", new_data)

                        # If we have existing data, preserve recent volume updates
                        if self._data and self._last_volume_update:
                            current_time = time.time()
                            if current_time - self._last_volume_update < 10:
                                # Keep our optimistic volume, but update everything else
                                _LOGGER.debug(
                                    "Preserving recent volume update during status refresh"
                                )
                                old_volume = self._data.get("volume")
                                old_volume_steps = self._data.get("volume_steps")
                                self._data = new_data
                                if old_volume is not None:
                                    self._data["volume"] = old_volume
                                if old_volume_steps is not None:
                                    self._data["volume_steps"] = old_volume_steps
                            else:
                                self._data = new_data
                        else:
                            self._data = new_data
                        
                        # ADDED: Update position timestamp when fetching status
                        # Position can be at root or inside track object
                        if self._data:
                            has_position = "position" in self._data
                            if not has_position and "track" in self._data:
                                has_position = "position" in self._data["track"]
                            if has_position:
                                self._position_updated_at = dt_util.utcnow()
                                _LOGGER.debug("Updated position timestamp from status fetch")

                        # Debug album cover URL from initial status
                        if self._data and "track" in self._data and self._data["track"]:
                            album_cover_url = self._data["track"].get("album_cover_url")
                            _LOGGER.debug(
                                "Initial status - album_cover_url: %s", album_cover_url
                            )
                        else:
                            _LOGGER.debug("No track data in initial status")
                        self.update_callback()
        except Exception as err:
            _LOGGER.warning("Failed to fetch initial status: %s", err)

    async def _listen(self) -> None:
        """Listen for WebSocket messages."""
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        event = json.loads(msg.data)
                        await self._handle_event(event)
                    except json.JSONDecodeError as err:
                        _LOGGER.warning("Failed to decode WebSocket message: %s", err)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    _LOGGER.error("WebSocket error: %s", self._ws.exception())
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                    _LOGGER.info("WebSocket connection closed")
                    break
        except Exception as err:
            _LOGGER.error("WebSocket listen error: %s", err)
        finally:
            self._connected = False
            self._schedule_reconnect()

    async def _handle_event(self, event: dict) -> None:
        """Handle incoming WebSocket events."""
        event_type = event.get("type")

        if not event_type:
            return

        _LOGGER.debug("Received event: %s with data: %s", event_type, event)

        # Initialize data if not present
        if self._data is None:
            await self._fetch_initial_status()
            if self._data is None:
                return

        # Handle different event types
        if event_type == "active":
            self._data["stopped"] = False

        elif event_type == "inactive":
            self._data["stopped"] = True

        elif event_type == "metadata":
            # Update track information
            album_cover_url = event.get("album_cover_url")
            _LOGGER.debug("Metadata event - album_cover_url: %s", album_cover_url)

            track_data = {
                "uri": event.get("uri"),
                "name": event.get("name"),
                "artist_names": event.get("artist_names", []),
                "album_name": event.get("album_name"),
                "album_cover_url": album_cover_url,
                "duration": event.get("duration"),
            }
            self._data["track"] = track_data
            
            # ADDED: Reset position to 0 for new track
            self._data["position"] = 0
            self._position_updated_at = dt_util.utcnow()
            _LOGGER.debug("New track metadata - reset position to 0")

        elif event_type in ["playing", "will_play"]:
            self._data["paused"] = False
            self._data["stopped"] = False
            if "uri" in event:
                # Only update URI if we don't have track data, or if it's a different track
                if "track" not in self._data:
                    self._data["track"] = {}
                current_uri = self._data["track"].get("uri")
                if current_uri != event["uri"]:
                    # New track - we might need to fetch full metadata
                    self._data["track"]["uri"] = event["uri"]
                    # Keep existing metadata if available, WebSocket metadata event will update it
            if "play_origin" in event:
                self._data["play_origin"] = event["play_origin"]
            
            # ADDED: Update position timestamp when playback starts
            if "position" in event:
                self._data["position"] = event["position"]
                self._position_updated_at = dt_util.utcnow()
                _LOGGER.debug("Updated position from playing event")

        elif event_type == "paused":
            self._data["paused"] = True
            if "uri" in event:
                # Don't overwrite existing track data, just update URI if needed
                if "track" not in self._data:
                    self._data["track"] = {}
                if self._data["track"].get("uri") != event["uri"]:
                    self._data["track"]["uri"] = event["uri"]
            if "play_origin" in event:
                self._data["play_origin"] = event["play_origin"]
            
            # ADDED: Update position timestamp when paused
            if "position" in event:
                self._data["position"] = event["position"]
                self._position_updated_at = dt_util.utcnow()
                _LOGGER.debug("Updated position from paused event")

        elif event_type == "not_playing":
            self._data["paused"] = True
            if "play_origin" in event:
                self._data["play_origin"] = event["play_origin"]

        elif event_type == "stopped":
            self._data["stopped"] = True
            self._data["paused"] = False
            if "play_origin" in event:
                self._data["play_origin"] = event["play_origin"]
            
            # ADDED: Clear position when stopped
            self._data["position"] = 0
            self._position_updated_at = None
            _LOGGER.debug("Stopped - cleared position")

        elif event_type == "seek":
            # MODIFIED: Update position information with proper timestamp
            if "position" in event:
                self._data["position"] = event["position"]
                self._position_updated_at = dt_util.utcnow()  # ADDED
                _LOGGER.debug("Updated position from seek event: %s", event["position"])
            # Update track data if we have it, but don't overwrite existing metadata
            if "track" in self._data and self._data["track"]:
                if "duration" in event:
                    self._data["track"]["duration"] = event["duration"]
                if "uri" in event and self._data["track"].get("uri") != event["uri"]:
                    self._data["track"]["uri"] = event["uri"]
            elif "uri" in event:
                # Create minimal track data if none exists
                self._data["track"] = {"uri": event["uri"]}
                if "duration" in event:
                    self._data["track"]["duration"] = event["duration"]
            if "play_origin" in event:
                self._data["play_origin"] = event["play_origin"]

        elif event_type == "volume":
            # Only update volume if we haven't set it recently (within 10 seconds)
            current_time = time.time()
            if (
                self._last_volume_update is None
                or current_time - self._last_volume_update > 10
            ):
                self._data["volume"] = event.get("value", 0)
                self._data["volume_steps"] = event.get("max", 100)
            else:
                _LOGGER.debug("Ignoring WebSocket volume event - recent local update")
                # Still update volume_steps in case it changed
                self._data["volume_steps"] = event.get("max", 100)

        elif event_type == "shuffle_context":
            self._data["shuffle_context"] = event.get("value", False)

        elif event_type == "repeat_context":
            self._data["repeat_context"] = event.get("value", False)

        elif event_type == "repeat_track":
            self._data["repeat_track"] = event.get("value", False)

        # Trigger UI update
        self.update_callback()

    def _schedule_reconnect(self) -> None:
        """Schedule a reconnection attempt."""
        if self._reconnect_task and not self._reconnect_task.done():
            return

        async def reconnect():
            await asyncio.sleep(5)  # Wait 5 seconds before reconnecting
            if not self._connected:
                _LOGGER.info("Attempting to reconnect to WebSocket")
                await self.connect()

        self._reconnect_task = asyncio.create_task(reconnect())

    async def disconnect(self) -> None:
        """Disconnect from the WebSocket."""
        self._connected = False

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()

        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()

        if self._ws and not self._ws.closed:
            await self._ws.close()

    async def make_request(
        self, endpoint: str, method: str = "POST", data: dict = None
    ) -> bool:
        """Make a request to the Go Librespot API."""
        try:
            url = f"{self.base_url}{endpoint}"
            _LOGGER.debug("Making %s request to %s with data: %s", method, url, data)
            async with asyncio.timeout(10):
                if method == "POST":
                    async with self.session.post(url, json=data) as response:
                        _LOGGER.debug("Response status: %s", response.status)
                        if response.status != 200:
                            response_text = await response.text()
                            _LOGGER.warning(
                                "API request failed: %s - %s",
                                response.status,
                                response_text,
                            )
                        return response.status == 200
                else:
                    async with self.session.get(url) as response:
                        _LOGGER.debug("Response status: %s", response.status)
                        return response.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            _LOGGER.error("Error communicating with Go Librespot API: %s", err)
            return False

    async def refresh_status(self) -> None:
        """Manually refresh status via HTTP (for debugging)."""
        await self._fetch_initial_status()

    async def _periodic_refresh(self) -> None:
        """Periodically refresh status to ensure we have complete track metadata."""
        while self._connected:
            try:
                await asyncio.sleep(30)  # Refresh every 30 seconds
                if self._connected:
                    await self._fetch_initial_status()
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.warning("Error in periodic refresh: %s", err)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Go Librespot media player platform."""
    host = config_entry.data[CONF_HOST]
    port = config_entry.data[CONF_PORT]
    name = config_entry.data[CONF_NAME]

    # Create and add the media player entity
    media_player = GoLibrespotMediaPlayer(hass, host, port, name)
    async_add_entities([media_player])

    # Start WebSocket connection
    await media_player.async_added_to_hass()


class GoLibrespotMediaPlayer(MediaPlayerEntity):
    """Representation of a Go Librespot media player."""

    _attr_has_entity_name = True
    _attr_name = None

    def __init__(self, hass: HomeAssistant, host: str, port: int, name: str) -> None:
        """Initialize the media player."""
        self.hass = hass
        self._attr_unique_id = f"{host}_{port}"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, self._attr_unique_id)},
            "name": name,
            "manufacturer": "Go Librespot",
            "model": "Media Player",
        }

        # Initialize WebSocket client
        self._ws_client = GoLibrespotWebSocketClient(
            hass, host, port, self._handle_update
        )

    async def async_added_to_hass(self) -> None:
        """When entity is added to hass."""
        await super().async_added_to_hass()
        await self._ws_client.connect()

    async def async_will_remove_from_hass(self) -> None:
        """When entity will be removed from hass."""
        await self._ws_client.disconnect()
        await super().async_will_remove_from_hass()

    @callback
    def _handle_update(self) -> None:
        """Handle WebSocket data updates."""
        self.async_write_ha_state()

    @property
    def supported_features(self) -> MediaPlayerEntityFeature:
        """Flag media player features that are supported."""
        return (
            MediaPlayerEntityFeature.PLAY
            | MediaPlayerEntityFeature.PAUSE
            | MediaPlayerEntityFeature.STOP
            | MediaPlayerEntityFeature.NEXT_TRACK
            | MediaPlayerEntityFeature.PREVIOUS_TRACK
            | MediaPlayerEntityFeature.VOLUME_SET
            | MediaPlayerEntityFeature.SEEK
            | MediaPlayerEntityFeature.SHUFFLE_SET
            | MediaPlayerEntityFeature.REPEAT_SET
        )

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        return self._ws_client.connected

    @property
    def state(self) -> MediaPlayerState:
        """Return the state of the media player."""
        if not self._ws_client.data:
            return MediaPlayerState.OFF

        data = self._ws_client.data
        if data.get("stopped", True):
            return MediaPlayerState.OFF
        elif data.get("buffering", False):
            return MediaPlayerState.BUFFERING
        elif data.get("paused", False):
            return MediaPlayerState.PAUSED
        else:
            return MediaPlayerState.PLAYING

    @property
    def volume_level(self) -> float | None:
        """Volume level of the media player (0..1)."""
        if not self._ws_client.data:
            return None

        volume = self._ws_client.data.get("volume", 0)
        volume_steps = self._ws_client.data.get("volume_steps", 100)
        level = volume / volume_steps if volume_steps > 0 else 0

        # Debug volume level calculation
        if (
            hasattr(self._ws_client, "_last_volume_update")
            and self._ws_client._last_volume_update
        ):
            time_since_update = time.time() - self._ws_client._last_volume_update
            _LOGGER.debug(
                "Volume level: %.2f (%s/%s), time since update: %.1fs",
                level,
                volume,
                volume_steps,
                time_since_update,
            )

        return level

    @property
    def media_content_type(self) -> str:
        """Content type of current playing media."""
        return MediaType.MUSIC

    @property
    def media_title(self) -> str | None:
        """Title of current playing media."""
        if not self._ws_client.data or not self._ws_client.data.get("track"):
            return None
        return self._ws_client.data["track"].get("name")

    @property
    def media_artist(self) -> str | None:
        """Artist of current playing media."""
        if not self._ws_client.data or not self._ws_client.data.get("track"):
            return None
        artists = self._ws_client.data["track"].get("artist_names", [])
        return ", ".join(artists) if artists else None

    @property
    def media_album_name(self) -> str | None:
        """Album name of current playing media."""
        if not self._ws_client.data or not self._ws_client.data.get("track"):
            return None
        return self._ws_client.data["track"].get("album_name")

    @property
    def media_image_url(self) -> str | None:
        """Image url of current playing media."""
        if not self._ws_client.data or not self._ws_client.data.get("track"):
            _LOGGER.debug("No data or track available for media image")
            return None

        album_cover_url = self._ws_client.data["track"].get("album_cover_url")
        _LOGGER.debug("Album cover URL: %s", album_cover_url)
        return album_cover_url

    @property
    def media_duration(self) -> int | None:
        """Duration of current playing media in seconds."""
        if not self._ws_client.data or not self._ws_client.data.get("track"):
            return None
        duration_ms = self._ws_client.data["track"].get("duration")
        return duration_ms // 1000 if duration_ms else None

    @property
    def media_position(self) -> int | None:
        """Position of current playing media in seconds."""
        if not self._ws_client.data:
            return None
        # Position can be at root level or inside track object
        position_ms = self._ws_client.data.get("position")
        if position_ms is None and "track" in self._ws_client.data:
            position_ms = self._ws_client.data["track"].get("position")
        return position_ms // 1000 if position_ms else None

    @property
    def media_position_updated_at(self) -> datetime | None:
        """When was the position of the current playing media valid."""
        # FIXED: Return the stored timestamp instead of datetime.now()
        return self._ws_client.position_updated_at

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return additional state attributes."""
        attrs = {}

        if self._ws_client.data:
            # Add debug info about track data
            if "track" in self._ws_client.data and self._ws_client.data["track"]:
                track = self._ws_client.data["track"]
                attrs["track_uri"] = track.get("uri")
                attrs["album_cover_url_debug"] = track.get("album_cover_url")

            # Add other useful info
            attrs["device_id"] = self._ws_client.data.get("device_id")
            attrs["device_name"] = self._ws_client.data.get("device_name")
            attrs["play_origin"] = self._ws_client.data.get("play_origin")
            attrs["websocket_connected"] = self._ws_client.connected

        return attrs

    @property
    def entity_picture(self) -> str | None:
        """Return the entity picture to use in the frontend."""
        return self.media_image_url

    @property
    def shuffle(self) -> bool | None:
        """Boolean if shuffle is enabled."""
        if not self._ws_client.data:
            return None
        return self._ws_client.data.get("shuffle_context", False)

    @property
    def repeat(self) -> str | None:
        """Return current repeat mode."""
        if not self._ws_client.data:
            return None

        if self._ws_client.data.get("repeat_track", False):
            return "one"
        elif self._ws_client.data.get("repeat_context", False):
            return "all"
        else:
            return "off"

    async def async_media_play(self) -> None:
        """Send play command."""
        await self._ws_client.make_request("/player/resume")

    async def async_media_pause(self) -> None:
        """Send pause command."""
        await self._ws_client.make_request("/player/pause")

    async def async_media_stop(self) -> None:
        """Send stop command."""
        await self._ws_client.make_request("/player/pause")

    async def async_media_next_track(self) -> None:
        """Send next track command."""
        # Send empty JSON body as the API expects a request body
        _LOGGER.debug("Sending next track command")
        success = await self._ws_client.make_request("/player/next", data={})
        _LOGGER.debug("Next track command result: %s", success)

    async def async_media_previous_track(self) -> None:
        """Send previous track command."""
        await self._ws_client.make_request("/player/prev")

    async def async_set_volume_level(self, volume: float) -> None:
        """Set volume level, range 0..1."""
        if not self._ws_client.data:
            return

        volume_steps = self._ws_client.data.get("volume_steps", 100)
        target_volume = int(volume * volume_steps)

        # Record the time of this volume update
        self._ws_client._last_volume_update = time.time()

        # Optimistically update the volume in our local data to prevent UI jumping
        old_volume = self._ws_client.data.get("volume", 0)
        self._ws_client.data["volume"] = target_volume

        _LOGGER.debug(
            "Setting volume to %s/%s (%.2f)", target_volume, volume_steps, volume
        )

        # Send the volume command
        success = await self._ws_client.make_request(
            "/player/volume", data={"volume": target_volume}
        )

        # If the request failed, revert the optimistic update
        if not success:
            self._ws_client.data["volume"] = old_volume
            self._ws_client._last_volume_update = None

        # Trigger UI update immediately to prevent jumping
        self._handle_update()

    async def async_media_seek(self, position: float) -> None:
        """Send seek command."""
        position_ms = int(position * 1000)
        
        # ADDED: Update position immediately for responsive UI
        if self._ws_client.data:
            self._ws_client.data["position"] = position_ms
            self._ws_client._position_updated_at = dt_util.utcnow()
            self._handle_update()
        
        # Send the seek command
        await self._ws_client.make_request(
            "/player/seek", data={"position": position_ms}
        )

    async def async_set_shuffle(self, shuffle: bool) -> None:
        """Enable/disable shuffle mode."""
        await self._ws_client.make_request(
            "/player/shuffle_context", data={"shuffle_context": shuffle}
        )

    async def async_set_repeat(self, repeat: str) -> None:
        """Set repeat mode."""
        if repeat == "one":
            await self._ws_client.make_request(
                "/player/repeat_track", data={"repeat_track": True}
            )
            await self._ws_client.make_request(
                "/player/repeat_context", data={"repeat_context": False}
            )
        elif repeat == "all":
            await self._ws_client.make_request(
                "/player/repeat_track", data={"repeat_track": False}
            )
            await self._ws_client.make_request(
                "/player/repeat_context", data={"repeat_context": True}
            )
        else:  # off
            await self._ws_client.make_request(
                "/player/repeat_track", data={"repeat_track": False}
            )
            await self._ws_client.make_request(
                "/player/repeat_context", data={"repeat_context": False}
            )

    async def async_update(self) -> None:
        """Update the entity state (for debugging)."""
        await self._ws_client.refresh_status()
