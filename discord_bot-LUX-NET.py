from __future__ import annotations

import asyncio
import ctypes.util
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Thread

import discord
import edge_tts
from discord import app_commands
from discord.ext import commands
from flask import Flask
from gtts import gTTS

# --- Keep-Alive Web Server for Render ---
app = Flask("")


@app.route("/")
def home():
    return "Bot is alive!"


def keep_alive():
    port = int(os.environ.get("PORT", 8080))
    t = Thread(target=lambda: app.run(host="0.0.0.0", port=port))
    t.daemon = True
    t.start()


# --- Main Bot Code ---
ROOT = Path(__file__).resolve().parent
BRIDGE_STATE_FILE = ROOT / "bridge_state.json"
MAX_TTS_CHARACTERS = 400
SEND_COOLDOWN_SECONDS = 2.0
MAX_QUEUED_SPEECHES = 25
MAX_LIST_MESSAGE_LENGTH = 1800
PLAYBACK_SPEED = 1.2
DEFAULT_TTS_VOICE = "en-US-AriaNeural"
LEGACY_TTS_VOICE = "google"

TTS_VOICE_CHOICES = [
    app_commands.Choice(name="Aria — US English (female)", value="en-US-AriaNeural"),
    app_commands.Choice(name="Guy — US English (male)", value="en-US-GuyNeural"),
    app_commands.Choice(name="Jenny — US English (female)", value="en-US-JennyNeural"),
    app_commands.Choice(name="Christopher — US English (male)", value="en-US-ChristopherNeural"),
    app_commands.Choice(name="Sonia — UK English (female)", value="en-GB-SoniaNeural"),
    app_commands.Choice(name="Ryan — UK English (male)", value="en-GB-RyanNeural"),
    app_commands.Choice(name="Natasha — Australian English (female)", value="en-AU-NatashaNeural"),
    app_commands.Choice(name="Clara — Canadian English (female)", value="en-CA-ClaraNeural"),
    app_commands.Choice(name="Neerja — Indian English (female)", value="en-IN-NeerjaNeural"),
    app_commands.Choice(name="Google TTS — classic voice", value=LEGACY_TTS_VOICE),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("discord-voice-bridge")

if not discord.opus.is_loaded():
    opus_library = ctypes.util.find_library("opus") or "libopus.so.0"
    try:
        discord.opus.load_opus(opus_library)
    except OSError as error:
        raise RuntimeError(
            f"Could not load the Opus voice encoder ({opus_library}); install libopus."
        ) from error


def load_bridges() -> list[set[int]]:
    """Load multi-server bridge groups, migrating legacy one-to-one pairings."""
    if not BRIDGE_STATE_FILE.exists():
        return []

    try:
        data = json.loads(BRIDGE_STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("bridge state must be a JSON object")

        if "groups" in data:
            raw_groups = data["groups"]
            if not isinstance(raw_groups, list):
                raise ValueError("bridge groups must be a list")

            groups: list[set[int]] = []
            seen_channel_ids: set[int] = set()
            for raw_group in raw_groups:
                if not isinstance(raw_group, list):
                    raise ValueError("each bridge group must be a list")
                channel_ids = [int(channel_id) for channel_id in raw_group]
                group = set(channel_ids)
                if len(group) < 2 or len(group) != len(channel_ids) or any(
                    channel_id <= 0 for channel_id in group
                ):
                    raise ValueError("bridge groups must contain unique, positive channel IDs")
                if seen_channel_ids.intersection(group):
                    raise ValueError("a voice channel cannot belong to multiple bridge groups")
                seen_channel_ids.update(group)
                groups.append(group)
            return groups

        bridges = {int(source): int(target) for source, target in data.items()}
        if any(
            source <= 0 or target <= 0 or source == target or bridges.get(target) != source
            for source, target in bridges.items()
        ):
            raise ValueError("legacy bridge pairings are invalid or not symmetric")
        return [
            {source, target}
            for source, target in sorted(bridges.items())
            if source < target
        ]
    except (OSError, ValueError, TypeError) as error:
        raise RuntimeError(
            f"Could not load {BRIDGE_STATE_FILE.name}; fix or remove the invalid "
            "bridge state file before starting the bot."
        ) from error


def save_bridges(groups: list[set[int]]) -> None:
    """Write bridge groups atomically so an interrupted write cannot corrupt the file."""
    temp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=BRIDGE_STATE_FILE.parent,
            prefix=".bridge-state-",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = temp_file.name
            json.dump(
                {
                    "version": 2,
                    "groups": sorted(
                        (sorted(group) for group in groups),
                        key=lambda group: group[0],
                    ),
                },
                temp_file,
                indent=2,
            )
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, BRIDGE_STATE_FILE)
    except OSError:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


intents = discord.Intents.default()
intents.voice_states = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents, help_command=None)
bridge_groups = load_bridges()
synced_guild_ids: set[int] = set()
send_history: dict[int, float] = {}


@dataclass(frozen=True)
class PreparedSpeech:
    target_channel_ids: tuple[int, ...]
    audio_path: str | None
    error: Exception | None


speech_queue: asyncio.Queue[tuple[str, tuple[int, ...], str]] = asyncio.Queue(
    maxsize=MAX_QUEUED_SPEECHES
)
prepared_speech_queue: asyncio.Queue[PreparedSpeech] = asyncio.Queue(maxsize=1)
speech_queue_preparer: asyncio.Task[None] | None = None
speech_queue_worker: asyncio.Task[None] | None = None


async def get_voice_channel(channel_id: int) -> discord.VoiceChannel | None:
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
    return channel if isinstance(channel, discord.VoiceChannel) else None


async def create_tts_file(text: str, voice: str) -> str:
    descriptor, path = tempfile.mkstemp(prefix="discord-bridge-", suffix=".mp3")
    os.close(descriptor)
    try:
        if voice == LEGACY_TTS_VOICE:
            await asyncio.to_thread(
                gTTS(text=text, lang="en", timeout=(10, 30)).save,
                path,
            )
        else:
            await edge_tts.Communicate(text=text, voice=voice).save(path)
        return path
    except BaseException:
        Path(path).unlink(missing_ok=True)
        raise


def find_bridge_group(channel_id: int) -> set[int] | None:
    return next((group for group in bridge_groups if channel_id in group), None)


def merge_bridge_groups(
    source_channel_id: int,
    target_channel_id: int,
) -> tuple[list[set[int]], set[int]]:
    source_group = find_bridge_group(source_channel_id)
    target_group = find_bridge_group(target_channel_id)
    merged_group = {source_channel_id, target_channel_id}
    if source_group is not None:
        merged_group.update(source_group)
    if target_group is not None:
        merged_group.update(target_group)

    remaining_groups = [
        group.copy()
        for group in bridge_groups
        if group is not source_group and group is not target_group
    ]
    remaining_groups.append(merged_group)
    return remaining_groups, merged_group


def remove_channel_from_groups(channel_id: int) -> tuple[list[set[int]], set[int] | None]:
    current_group = find_bridge_group(channel_id)
    if current_group is None:
        return [group.copy() for group in bridge_groups], None

    remaining_channels = current_group - {channel_id}
    remaining_groups = [
        group.copy() for group in bridge_groups if group is not current_group
    ]
    if len(remaining_channels) >= 2:
        remaining_groups.append(remaining_channels)
    return remaining_groups, remaining_channels


async def respond(interaction: discord.Interaction, message: str) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def chunk_message_lines(lines: list[str]) -> list[str]:
    chunks: list[str] = []
    current_lines: list[str] = []
    current_length = 0
    for line in lines:
        added_length = len(line) + (1 if current_lines else 0)
        if current_lines and current_length + added_length > MAX_LIST_MESSAGE_LENGTH:
            chunks.append("\n".join(current_lines))
            current_lines = []
            current_length = 0
            added_length = len(line)
        current_lines.append(line)
        current_length += added_length
    if current_lines:
        chunks.append("\n".join(current_lines))
    return chunks


async def sync_guild_commands(guild: discord.Guild) -> None:
    if guild.id in synced_guild_ids:
        return
    try:
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        synced_guild_ids.add(guild.id)
        logger.info("Synced %d slash commands to %s (%s)", len(synced), guild.name, guild.id)
    except discord.HTTPException:
        logger.exception(
            "Could not sync slash commands to %s; check the bot's application.commands install scope",
            guild.name,
        )


@bot.event
async def on_ready() -> None:
    global speech_queue_preparer, speech_queue_worker

    if not bot.guilds:
        logger.warning("Connected, but the bot is not installed in any servers; slash commands cannot sync yet.")
    for guild in bot.guilds:
        await sync_guild_commands(guild)
    if speech_queue_preparer is None or speech_queue_preparer.done():
        speech_queue_preparer = asyncio.create_task(
            prepare_speech_queue(),
            name="discord-voice-bridge-speech-preparer",
        )
    if speech_queue_worker is None or speech_queue_worker.done():
        speech_queue_worker = asyncio.create_task(
            process_speech_queue(),
            name="discord-voice-bridge-speech-queue",
        )
    logger.info(
        "Connected to Discord as %s (%s) in %d servers",
        bot.user,
        bot.user.id if bot.user else "unknown",
        len(bot.guilds),
    )


@bot.event
async def on_guild_join(guild: discord.Guild) -> None:
    await sync_guild_commands(guild)


@bot.tree.command(name="link-vc", description="Add a voice channel to a multi-server bridge.")
@app_commands.guild_only()
@app_commands.describe(target_channel_id="Voice channel ID to add to or merge with this bridge")
async def link_vc(interaction: discord.Interaction, target_channel_id: str) -> None:
    """Add the current voice channel to a bridge group, merging groups if needed."""
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or not member.guild_permissions.manage_guild:
        await respond(interaction, "You need the **Manage Server** permission to change voice pairings.")
        return
    if member.voice is None or not isinstance(member.voice.channel, discord.VoiceChannel):
        await respond(interaction, "Join the source voice channel before linking it.")
        return

    channel_id_text = target_channel_id.strip()
    if channel_id_text.startswith("<#") and channel_id_text.endswith(">"):
        channel_id_text = channel_id_text[2:-1]
    try:
        parsed_channel_id = int(channel_id_text)
    except ValueError:
        await respond(interaction, "Enter a valid voice-channel ID from the other server.")
        return

    source_channel = member.voice.channel
    current_group = find_bridge_group(source_channel.id)
    if current_group is not None and parsed_channel_id in current_group:
        await respond(interaction, "These voice channels are already in the same bridge group.")
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    target_channel = await get_voice_channel(parsed_channel_id)
    if target_channel is None:
        await respond(interaction, "I couldn't find that voice channel. Check its ID and my access to that server.")
        return
    if target_channel.guild.id == source_channel.guild.id:
        await respond(interaction, "The target voice channel must be in a different server.")
        return

    updated_groups, merged_group = merge_bridge_groups(source_channel.id, target_channel.id)
    channels_by_id = {
        source_channel.id: source_channel,
        target_channel.id: target_channel,
    }
    guild_ids: set[int] = set()
    for channel_id in merged_group:
        channel = channels_by_id.get(channel_id)
        if channel is None:
            channel = await get_voice_channel(channel_id)
        if channel is None:
            await respond(
                interaction,
                "I couldn't verify an existing channel in that bridge group. Check the bot's access or remove the stale link.",
            )
            return
        if channel.guild.id in guild_ids:
            await respond(
                interaction,
                "A bridge group can have only one voice channel per server. Unlink that server's current channel first.",
            )
            return
        guild_ids.add(channel.guild.id)

    previous_groups = [group.copy() for group in bridge_groups]
    bridge_groups[:] = updated_groups
    try:
        save_bridges(bridge_groups)
    except OSError:
        logger.exception("Failed to save voice bridge group")
        bridge_groups[:] = previous_groups
        await respond(interaction, "I couldn't save that bridge group. Check the bot's file permissions.")
        return

    await respond(
        interaction,
        f"Added **{source_channel.name}** in **{source_channel.guild.name}** to the bridge. "
        f"It now links {len(merged_group)} servers.",
    )


@bot.tree.command(name="unlink-vc", description="Remove your current voice channel from its bridge.")
@app_commands.guild_only()
async def unlink_vc(interaction: discord.Interaction) -> None:
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or not member.guild_permissions.manage_guild:
        await respond(interaction, "You need the **Manage Server** permission to change voice pairings.")
        return
    if member.voice is None or not isinstance(member.voice.channel, discord.VoiceChannel):
        await respond(interaction, "Join the voice channel you want to unlink first.")
        return

    source_channel = member.voice.channel
    current_group = find_bridge_group(source_channel.id)
    if current_group is None:
        await respond(interaction, "This voice channel isn't in a bridge group.")
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    updated_groups, remaining_channels = remove_channel_from_groups(source_channel.id)
    previous_groups = [group.copy() for group in bridge_groups]
    bridge_groups[:] = updated_groups
    try:
        save_bridges(bridge_groups)
    except OSError:
        logger.exception("Failed to save voice bridge channel removal")
        bridge_groups[:] = previous_groups
        await respond(interaction, "I couldn't save that change. The existing bridge group is still active.")
        return

    voice_client = discord.utils.get(bot.voice_clients, guild=source_channel.guild)
    if (
        voice_client is not None
        and voice_client.channel is not None
        and voice_client.channel.id == source_channel.id
    ):
        try:
            await voice_client.disconnect(force=True)
        except discord.DiscordException:
            logger.exception("Could not disconnect from unlinked voice channel %s", source_channel.id)

    if remaining_channels is not None and len(remaining_channels) >= 2:
        await respond(
            interaction,
            f"Removed **{source_channel.name}** from the bridge. "
            f"{len(remaining_channels)} servers remain linked.",
        )
    else:
        await respond(
            interaction,
            "Voice channel removed. The remaining channel in that group is no longer linked.",
        )


@bot.tree.command(name="list-bridges", description="Show all linked servers and voice channels.")
@app_commands.guild_only()
async def list_bridges(interaction: discord.Interaction) -> None:
    """Show every configured bridge group privately to the requester."""
    if not bridge_groups:
        await respond(interaction, "There are no linked voice channels yet.")
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    lines = ["**Linked voice bridge groups**"]
    ordered_groups = sorted(bridge_groups, key=lambda group: min(group))
    for group_number, group in enumerate(ordered_groups, start=1):
        lines.append(f"**Group {group_number} — {len(group)} servers**")
        for channel_id in sorted(group):
            channel = await get_voice_channel(channel_id)
            if channel is None:
                lines.append(f"• Unavailable voice channel (ID {channel_id})")
                continue
            server_name = discord.utils.escape_markdown(channel.guild.name)
            channel_name = discord.utils.escape_markdown(channel.name)
            lines.append(f"• **{server_name}** — #{channel_name}")

    chunks = chunk_message_lines(lines)
    await respond(interaction, chunks[0])
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, ephemeral=True)


def take_send_slot(user_id: int) -> float:
    """Return seconds remaining if the user is rate limited; otherwise 0."""
    now = time.monotonic()
    last_sent_at = send_history.get(user_id)
    if last_sent_at is not None:
        retry_after = SEND_COOLDOWN_SECONDS - (now - last_sent_at)
        if retry_after > 0:
            return retry_after
    send_history[user_id] = now
    return 0.0


async def play_audio_in_channel(channel_id: int, audio_path: str) -> None:
    target_channel = await get_voice_channel(channel_id)
    if target_channel is None:
        raise RuntimeError(f"Could not access destination voice channel {channel_id}")

    voice_client = discord.utils.get(bot.voice_clients, guild=target_channel.guild)
    if voice_client is not None and not voice_client.is_connected():
        voice_client = None

    if not channel_has_other_users(target_channel):
        if (
            voice_client is not None
            and voice_client.channel is not None
            and voice_client.channel.id == target_channel.id
        ):
            try:
                await voice_client.disconnect(force=True)
            except discord.DiscordException:
                logger.exception("Could not leave empty voice channel %s", target_channel.id)
        logger.info(
            "Skipping destination channel %s because nobody else is currently connected",
            target_channel.id,
        )
        return

    if voice_client is not None:
        while voice_client.is_playing() or voice_client.is_paused():
            await asyncio.sleep(0.1)

    try:
        if voice_client is None:
            voice_client = await target_channel.connect(timeout=20, reconnect=True)
        elif voice_client.channel is None or voice_client.channel.id != target_channel.id:
            await voice_client.move_to(target_channel)
    except (discord.ClientException, discord.Forbidden, discord.HTTPException, asyncio.TimeoutError) as error:
        raise RuntimeError(
            f"Could not connect to destination voice channel {target_channel.name} "
            f"in {target_channel.guild.name}"
        ) from error

    if not channel_has_other_users(target_channel):
        logger.info(
            "Leaving destination channel %s because its members left before playback",
            target_channel.id,
        )
        await voice_client.disconnect(force=True)
        return

    audio_source = discord.FFmpegPCMAudio(
        audio_path,
        options=f"-filter:a atempo={PLAYBACK_SPEED:.2f}",
    )
    loop = asyncio.get_running_loop()
    playback_finished: asyncio.Future[Exception | None] = loop.create_future()

    def set_playback_result(error: Exception | None) -> None:
        if not playback_finished.done():
            playback_finished.set_result(error)

    def after_playback(error: Exception | None) -> None:
        try:
            loop.call_soon_threadsafe(set_playback_result, error)
        except RuntimeError:
            logger.debug("Event loop closed before voice playback callback completed")

    try:
        voice_client.play(audio_source, after=after_playback)
    except Exception:
        audio_source.cleanup()
        raise

    playback_error = await playback_finished
    if playback_error is not None:
        raise RuntimeError(f"Voice playback failed in {target_channel.guild.name}") from playback_error


def channel_has_other_users(channel: discord.VoiceChannel) -> bool:
    """Check voice states directly so this works without the privileged members intent."""
    own_user_id = bot.user.id if bot.user is not None else None
    return any(
        user_id != own_user_id
        and voice_state.channel is not None
        and voice_state.channel.id == channel.id
        for user_id, voice_state in channel.guild.voice_states.items()
    )


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    """Disconnect from a linked destination as soon as its last other user leaves."""
    if member.bot or before.channel is None or before.channel.id == getattr(after.channel, "id", None):
        return

    previous_channel = before.channel
    if not isinstance(previous_channel, discord.VoiceChannel):
        return
    if channel_has_other_users(previous_channel):
        return

    voice_client = discord.utils.get(bot.voice_clients, guild=previous_channel.guild)
    if (
        voice_client is None
        or voice_client.channel is None
        or voice_client.channel.id != previous_channel.id
    ):
        return

    logger.info("Leaving voice channel %s because its last user disconnected", previous_channel.id)
    if voice_client.is_playing() or voice_client.is_paused():
        voice_client.stop()
    try:
        await voice_client.disconnect(force=True)
    except discord.DiscordException:
        logger.exception("Could not leave empty voice channel %s", previous_channel.id)


async def prepare_speech_queue() -> None:
    """Generate audio ahead of playback while keeping synthesis FIFO and bounded."""
    while True:
        spoken_text, target_channel_ids, voice = await speech_queue.get()
        audio_path: str | None = None
        try:
            generation_error: Exception | None = None
            try:
                audio_path = await create_tts_file(spoken_text, voice)
            except Exception as error:
                generation_error = error
                logger.exception("Could not generate queued bridge speech")

            await prepared_speech_queue.put(
                PreparedSpeech(
                    target_channel_ids=target_channel_ids,
                    audio_path=audio_path,
                    error=generation_error,
                )
            )
            audio_path = None
        finally:
            if audio_path is not None:
                try:
                    Path(audio_path).unlink(missing_ok=True)
                except OSError:
                    logger.warning("Could not remove unqueued TTS audio file %s", audio_path)
            speech_queue.task_done()


async def process_speech_queue() -> None:
    """Play prepared messages FIFO, with destinations starting concurrently."""
    while True:
        prepared = await prepared_speech_queue.get()
        try:
            if prepared.error is not None:
                continue
            if prepared.audio_path is None:
                logger.error("Queued bridge speech has no audio file")
                continue

            results = await asyncio.gather(
                *(
                    play_audio_in_channel(channel_id, prepared.audio_path)
                    for channel_id in prepared.target_channel_ids
                ),
                return_exceptions=True,
            )
            failures = [
                (channel_id, result)
                for channel_id, result in zip(prepared.target_channel_ids, results)
                if isinstance(result, BaseException)
            ]
            for channel_id, error in failures:
                logger.error("Queued speech failed in destination channel %s: %s", channel_id, error)
            if not failures:
                logger.info(
                    "Finished queued speech in %d destination servers",
                    len(prepared.target_channel_ids),
                )
        except Exception:
            logger.exception("Could not play queued bridge speech")
        finally:
            if prepared.audio_path is not None:
                try:
                    Path(prepared.audio_path).unlink(missing_ok=True)
                except OSError:
                    logger.warning("Could not remove temporary TTS audio file %s", prepared.audio_path)
            prepared_speech_queue.task_done()


@bot.tree.command(name="send-bridge", description="Queue speech for the other servers in your bridge.")
@app_commands.guild_only()
@app_commands.describe(
    message="Text to speak aloud (up to 400 characters)",
    voice="Choose a natural voice for this announcement",
)
@app_commands.choices(voice=TTS_VOICE_CHOICES)
async def send_bridge(
    interaction: discord.Interaction,
    message: str,
    voice: str = DEFAULT_TTS_VOICE,
) -> None:
    """Queue a message for parallel playback in every other bridge server."""
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or member.voice is None or not isinstance(member.voice.channel, discord.VoiceChannel):
        await respond(interaction, "Join a voice channel in a bridge group first.")
        return
    if len(message) > MAX_TTS_CHARACTERS:
        await respond(interaction, f"Keep messages to {MAX_TTS_CHARACTERS} characters or fewer.")
        return

    source_channel = member.voice.channel
    group = find_bridge_group(source_channel.id)
    if group is None:
        await respond(interaction, "This voice channel isn't in a bridge group. Ask a server manager to use `/link-vc`.")
        return
    target_channel_ids = tuple(sorted(group - {source_channel.id}))
    if not target_channel_ids:
        await respond(interaction, "This bridge group has no other linked servers.")
        return
    if speech_queue.full():
        await respond(interaction, "The speech queue is full. Wait for a few messages to finish, then try again.")
        return

    retry_after = take_send_slot(member.id)
    if retry_after > 0:
        await respond(interaction, f"Please wait {retry_after:.1f} seconds before sending another message.")
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    spoken_text = f"Message from {source_channel.guild.name}. {member.display_name} says: {message}"
    try:
        speech_queue.put_nowait((spoken_text, target_channel_ids, voice))
    except asyncio.QueueFull:
        await respond(interaction, "The speech queue is full. Wait for a few messages to finish, then try again.")
        return

    server_label = "server" if len(target_channel_ids) == 1 else "servers"
    await respond(
        interaction,
        f"Queued for {len(target_channel_ids)} other {server_label}. "
        "It will play in all of them together when it reaches the front of the queue.",
    )


def main() -> None:
    token = os.environ.get("DISCORD_TOKEN") or os.environ.get("DISCORD_BOT_TOKEN")
    
    if not token:
        raise SystemExit(
            "Missing DISCORD_TOKEN environment variable in Render."
        )
    
    keep_alive()  # Starts the Flask server for Render health checks
    bot.run(token)


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
