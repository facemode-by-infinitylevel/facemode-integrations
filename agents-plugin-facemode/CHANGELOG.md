# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-28

### Added

- Automatic worker WebSocket reconnect: on transport drop after `started`, the plugin fetches a fresh one-time `wsToken`, opens a new socket, re-sends `start`, and resumes (max 2 attempts, single-flight, sends during the gap delivered once on the new socket).

### Changed

- Package metadata for public release: `license`, `repository`, `homepage`, `keywords`, `author`, `publishConfig.access: public`.
- `LICENSE` now contains the full Apache-2.0 text.

## [0.1.0] - 2026-09-09

### Added

- Initial public release.
- FaceMode avatar integration for LiveKit Agents (JavaScript).
- Canonical WebSocket PCM ingestion protocol.
- BYOLR (Bring Your Own LiveKit Room) support.
- Automatic worker WebSocket reconnect with fresh one-time tokens.
- Apache-2.0 license.
