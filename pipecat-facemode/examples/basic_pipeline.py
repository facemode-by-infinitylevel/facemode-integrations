"""Minimal Pipecat pipeline using FaceMode as the avatar output stage.

This example uses Pipecat's LiveKit transport for input and output. The FaceMode
worker and this service use the customer LiveKit room configured by the environment
variables below. Keep all LiveKit tokens server-side.

Required environment variables:
    FACEMODE_API_KEY
    FACEMODE_AVATAR_ID (optional)
    LIVEKIT_URL
    LIVEKIT_ROOM_NAME
    LIVEKIT_WORKER_TOKEN
    LIVEKIT_SUBSCRIBER_TOKEN
    FACEMODE_AVATAR_PARTICIPANT_IDENTITY (optional, defaults to facemode-avatar)
    LIVEKIT_TRANSPORT_TOKEN
    CARTESIA_API_KEY

Install the optional Pipecat LiveKit transport and Cartesia service before running:

    pip install "pipecat-ai[livekit,cartesia]"
"""

from __future__ import annotations

import asyncio
import logging
import os

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.transports.livekit.transport import LiveKitParams, LiveKitTransport
from pipecat.workers.runner import WorkerRunner
from pipecat.frames.frames import EndFrame, TTSSpeakFrame

from pipecat_facemode import FaceModeVideoService

logging.basicConfig(level=logging.INFO)


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


async def main() -> None:
    transport = LiveKitTransport(
        url=required("LIVEKIT_URL"),
        token=required("LIVEKIT_TRANSPORT_TOKEN"),
        room_name=required("LIVEKIT_ROOM_NAME"),
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=False,
            video_out_enabled=False,
            video_out_is_live=True,
        ),
    )

    tts = CartesiaTTSService(
        api_key=required("CARTESIA_API_KEY"),
        settings=CartesiaTTSService.Settings(
            voice="71a7ad14-091c-4e8e-a314-022ece01c121",
        ),
    )

    facemode = FaceModeVideoService(
        api_key=required("FACEMODE_API_KEY"),
        avatar_id=os.environ.get("FACEMODE_AVATAR_ID", ""),
        api_url=os.environ.get("FACEMODE_API_URL", "https://api.facemode.io/api"),
        room={
            "type": "livekit",
            "url": required("LIVEKIT_URL"),
            "token": os.environ.get("FACEMODE_ROOM_TOKEN") or required("LIVEKIT_WORKER_TOKEN"),
        },
        livekit_subscriber_token=required("LIVEKIT_SUBSCRIBER_TOKEN"),
        avatar_participant_identity=os.environ.get(
            "FACEMODE_AVATAR_PARTICIPANT_IDENTITY", "facemode-avatar"
        ),
        room_name=required("LIVEKIT_ROOM_NAME"),
    )

    # FaceMode consumes TTSAudioRawFrame and emits OutputAudioRawFrame plus an
    # output video frame. Do not put another audio output before this service or
    # the source TTS and avatar audio will both be played.
    pipeline = Pipeline(
        [
            transport.input(),
            tts,
            facemode,
            transport.output(),
        ]
    )
    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(audio_out_sample_rate=24_000),
    )

    @transport.event_handler("on_first_participant_joined")
    async def on_first_participant_joined(_transport, _participant_id) -> None:
        await worker.queue_frames(
            [TTSSpeakFrame("Hello from a Pipecat LiveKit pipeline connected to FaceMode.")]
        )

    @transport.event_handler("on_participant_disconnected")
    async def on_participant_disconnected(_transport, _participant_id) -> None:
        await worker.queue_frame(EndFrame())

    await WorkerRunner().run(worker)


if __name__ == "__main__":
    asyncio.run(main())
