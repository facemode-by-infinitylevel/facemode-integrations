# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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