"""FaceMode integration for Pipecat."""

from .exceptions import (
    FaceModeAPIError,
    FaceModeConfigurationError,
    FaceModeError,
    FaceModeNotReadyError,
    FaceModeProtocolError,
)
from .models import IngestionDetails, LiveKitRoom, SessionDetails, SessionRequest
from .service import FaceModeProcessor, FaceModeService, FaceModeVideoService
from .version import __version__

__all__ = [
    "FaceModeAPIError",
    "FaceModeConfigurationError",
    "FaceModeError",
    "FaceModeNotReadyError",
    "FaceModeProcessor",
    "FaceModeProtocolError",
    "FaceModeService",
    "FaceModeVideoService",
    "IngestionDetails",
    "LiveKitRoom",
    "SessionDetails",
    "SessionRequest",
    "__version__",
]
