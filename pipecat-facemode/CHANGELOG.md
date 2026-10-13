# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-10-13

### Fixed

- Bounded teardown: every wait inside shutdown is now time-limited so an `EndFrame` or `CancelFrame` can never stall the pipeline forever. `end_utterance`/`end_session` still go out first (5s bound), then background-task joins (3s), track-stream closes (2s each), the WebSocket close (2s), and `room.disconnect()` (3s). Straggler tasks are logged and abandoned, with one last 3s drain window after the room disconnects. The lifecycle lock acquire is also bounded (10s) and the stop flags are set before acquiring it.
- Added stage-level teardown logging (`signaling end session`, `cancelling background tasks`, `closing track streams`, `disconnecting room`, `teardown complete`) so hangs are visible in logs.
- `examples/basic_pipeline.py`: `PipelineWorker(start_timeout_secs=300)` so the StartFrame watchdog does not kill the pipeline while a cold FaceMode worker is still starting.

### Notes

- Pipelines that queue `EndFrame` on participant disconnect with a TTS adapter that enables `pause_frame_processing` (Pipecat's ElevenLabs and Deepgram services default it on; Cartesia does not) should call `tts.resume_processing_frames()` before queueing the frame. The latched pause waits for a `BotStoppedSpeakingFrame` that may never arrive after the subscriber leaves, which would trap the `EndFrame` upstream of `FaceModeVideoService`. This is upstream Pipecat behavior, not something this plugin can change.

## [0.2.0] - 2026-09-28

### Changed

- Package metadata for public release: `authors`, `keywords`, `classifiers`, and `[project.urls]` (Homepage, Repository).
- `LICENSE` now contains the full Apache-2.0 text.
- No functional code changes since 0.1.0 (auto-reconnect already shipped in the published 0.1.0 wheel).

## [0.1.0] - 2026-09-09

### Added

- Initial public release.
- FaceMode avatar integration for Pipecat pipelines (FaceModeVideoService FrameProcessor).
- Canonical WebSocket PCM ingestion protocol.
- BYOLR (Bring Your Own LiveKit Room) support.
- Automatic worker WebSocket reconnect with fresh one-time tokens.
- Apache-2.0 license.