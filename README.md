# Go Librespot Home Assistant Integration

A custom Home Assistant integration for controlling [go-librespot](https://github.com/devgianlu/go-librespot) media players.

## Features

This integration provides full media player control for your go-librespot daemon:

- **Playback Control**: Play, pause, stop, next/previous track
- **Volume Control**: Set volume levels with full range support
- **Seek Control**: Jump to any position in the current track
- **Repeat Modes**: Support for repeat off/all/one
- **Shuffle Control**: Enable/disable shuffle mode
- **Media Information**: Display current track, artist, album, and cover art
- **Real-time Status**: Instant WebSocket updates of playback state and track information

## Installation

### HACS (Recommended)

1. Add this repository to HACS as a custom repository
2. Install "Go Librespot" from HACS
3. Restart Home Assistant

### Manual Installation

1. Copy the `custom_components/go_librespot` folder to your Home Assistant `custom_components` directory
2. Restart Home Assistant

## Configuration

1. Go to **Settings** → **Devices & Services**
2. Click **Add Integration**
3. Search for "Go Librespot"
4. Enter your go-librespot daemon details:
   - **Host**: IP address or hostname of your go-librespot daemon
   - **Port**: Port number (default: 24879)
   - **Name**: Friendly name for the media player

## API Endpoints Used

The integration uses the following go-librespot API endpoints:

### WebSocket Connection
- `WS /events` - Real-time status updates and events

### HTTP API Endpoints
- `GET /status` - Get initial player status and track information
- `POST /player/play` - Start playback with new content
- `POST /player/resume` - Resume playback
- `POST /player/pause` - Pause playback
- `POST /player/next` - Skip to next track
- `POST /player/prev` - Skip to previous track
- `POST /player/seek` - Seek to position
- `POST /player/volume` - Set volume level
- `POST /player/repeat_context` - Toggle context repeat
- `POST /player/repeat_track` - Toggle track repeat
- `POST /player/shuffle_context` - Toggle shuffle mode

## Real-time Updates

The integration uses WebSockets for instant status updates instead of polling. This provides:

- **Instant Response**: Changes are reflected immediately in Home Assistant
- **Efficient**: No unnecessary network traffic from polling
- **Reliable**: Automatic reconnection if the connection is lost
- **Battery Friendly**: Reduces power consumption on mobile devices

### WebSocket Events Handled

The integration listens for these real-time events from go-librespot:

- `active/inactive` - Device activation status
- `metadata` - Track information updates (title, artist, album, cover art)
- `playing/paused/stopped` - Playback state changes
- `seek` - Track position changes
- `volume` - Volume level changes
- `shuffle_context/repeat_context/repeat_track` - Playback mode changes

## Requirements

- Home Assistant 2023.1 or later
- go-librespot daemon running and accessible on your network
- Python aiohttp library (automatically installed)

## Troubleshooting

### Connection Issues

- Ensure your go-librespot daemon is running and accessible
- Check that the host and port are correct
- Verify there are no firewall restrictions
- Test the API directly: `curl http://your-host:24879/status`

### Integration Not Appearing

- Restart Home Assistant after installation
- Check the logs for any error messages
- Ensure the integration files are in the correct directory structure

## Contributing

Feel free to submit issues and enhancement requests!

## License

This integration is provided as-is under the MIT license.