"""Exceptions raised by the Pipecat FaceMode integration."""


class FaceModeError(RuntimeError):
    """Base error for FaceMode integration failures."""


class FaceModeConfigurationError(FaceModeError):
    """Raised when required integration configuration is missing or invalid."""


class FaceModeAPIError(FaceModeError):
    """Raised when the FaceMode REST API rejects or cannot fulfill a request."""


class FaceModeProtocolError(FaceModeError):
    """Raised when the FaceMode WebSocket protocol fails."""


class FaceModeNotReadyError(FaceModeError):
    """Raised when the LiveKit avatar media stream is not ready."""
