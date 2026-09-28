# facemode-integrations

Official integration plugins for [FaceMode](https://facemode.io) - real-time AI avatar video for voice agents.

## Packages

| Package | Registry | Install | Description |
| ------- | -------- | ------- | ----------- |
| `@facemode/agents-plugin-facemode` | npm | `npm install @facemode/agents-plugin-facemode` | FaceMode avatar output for LiveKit Agents JavaScript apps. |
| `livekit-plugins-facemode` | PyPI | `pip install livekit-plugins-facemode` | FaceMode avatar output for LiveKit Agents Python apps. |
| `pipecat-facemode` | PyPI | `pip install pipecat-facemode` | FaceMode avatar output as a Pipecat `FrameProcessor` (`FaceModeVideoService`). |

## How it works

- The plugin creates a FaceMode session via `POST /api/sessions` with a server-side LiveKit room object.
- It connects to the FaceMode worker over WebSocket using a one-time `facemode.<wsToken>` subprotocol.
- Your app streams canonical 16-bit PCM TTS audio to the worker.
- The worker publishes avatar audio/video into your LiveKit room (BYOLR - Bring Your Own LiveKit Room).
- WebSocket reconnects are handled automatically.

## Documentation

- [agents-plugin-facemode](agents-plugin-facemode/README.md)
- [livekit-plugins-facemode](livekit-plugins-facemode/README.md)
- [pipecat-facemode](pipecat-facemode/README.md)
- [facemode.io](https://facemode.io)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0 - see [LICENSE](LICENSE).
