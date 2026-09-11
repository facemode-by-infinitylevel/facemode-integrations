import os

from livekit.agents import Agent, AgentSession, AgentServer, JobContext, cli, inference, room_io

from livekit_plugins_facemode import AvatarSession

server = AgentServer()


@server.rtc_session(agent_name="facemode-basic")
async def entrypoint(ctx: JobContext):
    session = AgentSession(
        stt=inference.STT(model="deepgram/nova-3", language="multi"),
        llm=inference.LLM(model="openai/gpt-5.4-mini"),
        tts=inference.TTS(model="cartesia/sonic-3", voice="9626c31c-bec5-4cca-baa8-f8ba9e84c8bc"),
    )
    avatar = AvatarSession(
        api_key=os.environ["FACEMODE_API_KEY"],
        avatar_id=os.environ.get("FACEMODE_AVATAR_ID", ""),
        api_url=os.environ.get("FACEMODE_API_URL", "https://api.facemode.io/api"),
        # Optional: pin the upstream input provider for the session.
        # One of deepgram|gemini|gnani|elevenlabs|openai|cartesia|sarvam|custom.
        input_provider=os.environ.get("FACEMODE_INPUT_PROVIDER") or None,
    )
    await ctx.connect()
    await avatar.start(
        session,
        ctx.room,
        room={
            "type": "livekit",
            "url": os.environ["LIVEKIT_URL"],
            "token": os.environ.get("FACEMODE_ROOM_TOKEN") or os.environ.get("LIVEKIT_WORKER_TOKEN") or os.environ.get("LIVEKIT_TOKEN", ""),
        },
    )
    room_options = room_io.RoomOptions(audio_output=False)
    user_identity = os.environ.get("LIVEKIT_USER_IDENTITY")
    if user_identity:
        room_options.participant_identity = user_identity
    await session.start(
        agent=Agent(instructions="Greet the user and ask how you can help."),
        room=ctx.room,
        room_options=room_options,
    )
    await session.generate_reply(instructions="Greet the user and ask how you can help in 2-3 sentence.")
    await avatar.wait_for_join()


if __name__ == "__main__":
    cli.run_app(server)
