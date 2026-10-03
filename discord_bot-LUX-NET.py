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
import requests
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


# --- Configuration & Logging ---
ROOT = Path(__file__).resolve().parent
LOCAL_STATE_FILE = ROOT / "bridge_state.json"
MAX_TTS_CHARACTERS = 400
SEND_COOLDOWN_SECONDS = 2.0
MAX_QUEUED_SPEECHES = 25
MAX_LIST_MESSAGE_LENGTH = 1800
PLAYBACK_SPEED = 1.2
DEFAULT_TTS_VOICE = "en-US-AriaNeural"
LEGACY_TTS_VOICE = "google"

GIST_TOKEN = os.environ.get("GIST_TOKEN")
GIST_ID = os.environ.get("GIST_ID")

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


# --- Persistence Layer (Gist / Local File) ---
def load_all_data() -> tuple[list[set[int]], list[set[int]]]:
    """Load VC groups and Text Relay groups from GitHub Gist or local JSON file."""
    data = {}
    if GIST_TOKEN and GIST_ID:
        try:
            headers = {"Authorization": f"token {GIST_TOKEN}"}
            res = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers, timeout=10)
            if res.status_code == 200:
                files = res.json().get("files", {})
                if "bridge_state.json" in files:
                    content = files["bridge_state.json"]["content"]
                    data = json.loads(content)
                    logger.info("Successfully loaded state from GitHub Gist.")
        except Exception as e:
            logger.error("Failed to load from GitHub Gist, falling back to local file: %s", e)

    if not data and LOCAL_STATE_FILE.exists():
        try:
            data = json.loads(LOCAL_STATE_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error("Failed to load local state file: %s", e)

    vc_groups = parse_groups_from_json(data.get("vc_groups", data.get("groups", [])))
    relay_groups = parse_groups_from_json(data.get("relay_groups", []))
    return vc_groups, relay_groups


def parse_groups_from_json(raw_groups: list) -> list[set[int]]:
    groups: list[set[int]] = []
    if not isinstance(raw_groups, list):
        return groups
    for raw_group in raw_groups:
        if isinstance(raw_group, list):
            group = {int(cid) for cid in raw_group if int(cid) > 0}
            if len(group) >= 2:
                groups.append(group)
    return groups


def save_all_data(vc_groups: list[set[int]], relay_groups: list[set[int]]) -> None:
    """Save state data externally to GitHub Gist and locally as fallback."""
    payload = {
        "version": 3,
        "vc_groups": [sorted(list(g)) for g in vc_groups],
        "relay_groups": [sorted(list(g)) for g in relay_groups],
    }

    if GIST_TOKEN and GIST_ID:
        try:
            headers = {"Authorization": f"token {GIST_TOKEN}"}
            gist_data = {
                "files": {
                    "bridge_state.json": {
                        "content": json.dumps(payload, indent=2)
                    }
                }
            }
            res = requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=gist_data, timeout=10)
            if res.status_code == 200:
                logger.info("Saved bridge state to GitHub Gist.")
        except Exception as e:
            logger.error("Failed to save to GitHub Gist: %s", e)

    try:
        LOCAL_STATE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as e:
        logger.error("Failed to write local backup state file: %s", e)


# --- Core Discord Bot Setup ---
intents = discord.Intents.default()
intents.voice_states = True
intents.message_content = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents, help_command=None)
vc_bridge_groups, text_relay_groups = load_all_data()
synced_guild_ids: set[int] = set()
send_history: dict[int, float] = {}


@dataclass(frozen=True)
class PreparedSpeech:
    target_channel_ids: tuple[int, ...]
    audio_path: str | None
    error: Exception | None


speech_queue: asyncio.Queue[tuple[str, tuple[int, ...], str]] = asyncio.Queue(maxsize=MAX_QUEUED_SPEECHES)
prepared_speech_queue: asyncio.Queue[PreparedSpeech] = asyncio.Queue(maxsize=1)
speech_queue_preparer: asyncio.Task[None] | None = None
speech_queue_worker: asyncio.Task[None] | None = None


# --- Helper Functions ---
async def get_voice_channel(channel_id: int) -> discord.VoiceChannel | None:
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
    return channel if isinstance(channel, discord.VoiceChannel) else None


async def get_text_channel(channel_id: int) -> discord.TextChannel | None:
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
    return channel if isinstance(channel, discord.TextChannel) else None


async def get_or_create_webhook(channel: discord.TextChannel) -> discord.Webhook | None:
    """Fetch existing relay webhook or create a new one for the channel."""
    try:
        webhooks = await channel.webhooks()
        for wh in webhooks:
            if wh.name == "LUX-NET Relay":
                return wh
        return await channel.create_webhook(name="LUX-NET Relay")
    except discord.Forbidden:
        logger.error("Missing 'Manage Webhooks' permission in channel %s (%s)", channel.name, channel.id)
        return None
    except discord.HTTPException as e:
        logger.error("Failed to create webhook in channel %s: %s", channel.id, e)
        return None


async def create_tts_file(text: str, voice: str) -> str:
    descriptor, path = tempfile.mkstemp(prefix="discord-bridge-", suffix=".mp3")
    os.close(descriptor)
    try:
        if voice == LEGACY_TTS_VOICE:
            await asyncio.to_thread(gTTS(text=text, lang="en", timeout=(10, 30)).save, path)
        else:
            await edge_tts.Communicate(text=text, voice=voice).save(path)
        return path
    except BaseException:
        Path(path).unlink(missing_ok=True)
        raise


def find_group(groups: list[set[int]], channel_id: int) -> set[int] | None:
    return next((group for group in groups if channel_id in group), None)


def merge_groups(groups: list[set[int]], source_id: int, target_id: int) -> list[set[int]]:
    source_group = find_group(groups, source_id)
    target_group = find_group(groups, target_id)
    merged = {source_id, target_id}
    if source_group:
        merged.update(source_group)
    if target_group:
        merged.update(target_group)

    updated = [g.copy() for g in groups if g is not source_group and g is not target_group]
    updated.append(merged)
    return updated


def remove_from_groups(groups: list[set[int]], channel_id: int) -> list[set[int]]:
    current = find_group(groups, channel_id)
    if not current:
        return [g.copy() for g in groups]

    remaining = current - {channel_id}
    updated = [g.copy() for g in groups if g is not current]
    if len(remaining) >= 2:
        updated.append(remaining)
    return updated


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
        logger.exception("Could not sync slash commands to %s", guild.name)


@bot.event
async def on_ready() -> None:
    global speech_queue_preparer, speech_queue_worker

    for guild in bot.guilds:
        await sync_guild_commands(guild)
    if speech_queue_preparer is None or speech_queue_preparer.done():
        speech_queue_preparer = asyncio.create_task(prepare_speech_queue())
    if speech_queue_worker is None or speech_queue_worker.done():
        speech_queue_worker = asyncio.create_task(process_speech_queue())
    logger.info("Connected to Discord as %s in %d servers", bot.user, len(bot.guilds))


@bot.event
async def on_guild_join(guild: discord.Guild) -> None:
    await sync_guild_commands(guild)


# --- TEXT RELAY FUNCTIONALITY (SUPPORTING FORWARDS, BOTS, EMBEDS, WEBHOOKS) ---
@bot.event
async def on_message(message: discord.Message) -> None:
    # Ignore messages sent by THIS bot or webhooks to prevent loops
    if message.author.id == bot.user.id or message.webhook_id is not None:
        return

    # Account for thread channels by referencing parent channel ID
    channel_id = message.channel.parent_id if isinstance(message.channel, discord.Thread) else message.channel.id

    group = find_group(text_relay_groups, channel_id)
    if not group:
        return

    target_channel_ids = group - {channel_id}
    if not target_channel_ids:
        return

    # User profile data for webhook styling
    author_name = message.author.display_name
    if message.author.bot:
        author_name += " [BOT]"
    display_name = f"{author_name} ({message.guild.name})" if message.guild else author_name
    avatar_url = message.author.display_avatar.url if message.author.display_avatar else None

    for target_id in target_channel_ids:
        target_channel = await get_text_channel(target_id)
        if not target_channel:
            continue

        webhook = await get_or_create_webhook(target_channel)

        # Case A: Handle Native Discord Forwards (Snapshots)
        if hasattr(message, "message_snapshots") and message.message_snapshots:
            for snapshot in message.message_snapshots:
                snapshot_files = []
                if snapshot.attachments:
                    for att in snapshot.attachments:
                        try:
                            snapshot_files.append(await att.to_file())
                        except Exception as e:
                            logger.error("Failed to copy snapshot attachment: %s", e)

                if webhook:
                    try:
                        await webhook.send(
                            content=f"**(Forwarded)** {snapshot.content}" if snapshot.content else None,
                            username=f"{display_name} (Forward)",
                            avatar_url=avatar_url,
                            embeds=snapshot.embeds if snapshot.embeds else None,
                            files=snapshot_files if snapshot_files else None,
                        )
                    except discord.HTTPException as e:
                        logger.error("Webhook snapshot relay failed: %s", e)

        # Case B: Standard Messages, Attachments, Embeds & Other Bot Messages
        else:
            files_to_send = []
            for attachment in message.attachments:
                try:
                    files_to_send.append(await attachment.to_file())
                except Exception as e:
                    logger.error("Failed to prepare attachment for webhook: %s", e)

            if webhook:
                try:
                    await webhook.send(
                        content=message.content if message.content else None,
                        username=display_name,
                        avatar_url=avatar_url,
                        files=files_to_send if files_to_send else None,
                        embeds=message.embeds if message.embeds else None,
                    )
                except discord.HTTPException as e:
                    logger.error("Failed to send webhook message to %s: %s", target_id, e)
            else:
                # Fallback if Manage Webhooks permission is missing
                try:
                    header = f"**[{discord.utils.escape_markdown(display_name)}]**"
                    content = f"{header}: {message.content}" if message.content else header
                    await target_channel.send(content=content, files=files_to_send, embeds=message.embeds)
                except discord.HTTPException as e:
                    logger.error("Failed fallback send to channel %s: %s", target_id, e)

    await bot.process_commands(message)


@bot.tree.command(name="link-relay", description="Link this text channel to another server's text channel for live relays.")
@app_commands.guild_only()
@app_commands.describe(target_channel_id="Text channel ID from the other server to pair with")
async def link_relay(interaction: discord.Interaction, target_channel_id: str) -> None:
    global text_relay_groups
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or not member.guild_permissions.manage_guild:
        await respond(interaction, "You need **Manage Server** permissions to link text channels.")
        return

    if not isinstance(interaction.channel, discord.TextChannel):
        await respond(interaction, "This command must be run inside a standard text channel.")
        return

    channel_id_text = target_channel_id.strip()
    if channel_id_text.startswith("<#") and channel_id_text.endswith(">"):
        channel_id_text = channel_id_text[2:-1]

    try:
        parsed_id = int(channel_id_text)
    except ValueError:
        await respond(interaction, "Provide a valid numeric channel ID.")
        return

    source_channel = interaction.channel
    target_channel = await get_text_channel(parsed_id)
    if not target_channel:
        await respond(interaction, "Could not access that target text channel. Check the ID and my permissions.")
        return

    if source_channel.guild.id == target_channel.guild.id:
        await respond(interaction, "Text relay targets must be in a different server.")
        return

    text_relay_groups = merge_groups(text_relay_groups, source_channel.id, target_channel.id)
    save_all_data(vc_bridge_groups, text_relay_groups)

    await respond(
        interaction,
        f"Linked **#{source_channel.name}** to **#{target_channel.name}** in **{target_channel.guild.name}**. All messages, forwards, bot embeds, and files will now relay automatically!",
    )


@bot.tree.command(name="unlink-relay", description="Remove this text channel from text relays.")
@app_commands.guild_only()
async def unlink_relay(interaction: discord.Interaction) -> None:
    global text_relay_groups
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or not member.guild_permissions.manage_guild:
        await respond(interaction, "You need **Manage Server** permissions to unlink text channels.")
        return

    source_channel = interaction.channel
    current_group = find_group(text_relay_groups, source_channel.id)
    if not current_group:
        await respond(interaction, "This text channel is not currently part of a relay group.")
        return

    text_relay_groups = remove_from_groups(text_relay_groups, source_channel.id)
    save_all_data(vc_bridge_groups, text_relay_groups)

    await respond(interaction, f"Removed **#{source_channel.name}** from text relaying.")


# --- VOICE BRIDGE COMMANDS ---
@bot.tree.command(name="link-vc", description="Add a voice channel to a multi-server bridge.")
@app_commands.guild_only()
@app_commands.describe(target_channel_id="Voice channel ID to add to or merge with this bridge")
async def link_vc(interaction: discord.Interaction, target_channel_id: str) -> None:
    global vc_bridge_groups
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or not member.guild_permissions.manage_guild:
        await respond(interaction, "You need **Manage Server** permissions to change voice pairings.")
        return
    if member.voice is None or not isinstance(member.voice.channel, discord.VoiceChannel):
        await respond(interaction, "Join the source voice channel before linking it.")
        return

    channel_id_text = target_channel_id.strip()
    if channel_id_text.startswith("<#") and channel_id_text.endswith(">"):
        channel_id_text = channel_id_text[2:-1]

    try:
        parsed_id = int(channel_id_text)
    except ValueError:
        await respond(interaction, "Enter a valid voice-channel ID.")
        return

    source_channel = member.voice.channel
    target_channel = await get_voice_channel(parsed_id)
    if not target_channel:
        await respond(interaction, "Couldn't find that voice channel. Check its ID and my access.")
        return
    if target_channel.guild.id == source_channel.guild.id:
        await respond(interaction, "The target voice channel must be in a different server.")
        return

    vc_bridge_groups = merge_groups(vc_bridge_groups, source_channel.id, target_channel.id)
    save_all_data(vc_bridge_groups, text_relay_groups)

    await respond(
        interaction,
        f"Added **{source_channel.name}** in **{source_channel.guild.name}** to the voice bridge.",
    )


@bot.tree.command(name="unlink-vc", description="Remove your current voice channel from its bridge.")
@app_commands.guild_only()
async def unlink_vc(interaction: discord.Interaction) -> None:
    global vc_bridge_groups
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or not member.guild_permissions.manage_guild:
        await respond(interaction, "You need **Manage Server** permissions.")
        return
    if member.voice is None or not isinstance(member.voice.channel, discord.VoiceChannel):
        await respond(interaction, "Join the voice channel you want to unlink first.")
        return

    source_channel = member.voice.channel
    if not find_group(vc_bridge_groups, source_channel.id):
        await respond(interaction, "This voice channel isn't in a bridge group.")
        return

    vc_bridge_groups = remove_from_groups(vc_bridge_groups, source_channel.id)
    save_all_data(vc_bridge_groups, text_relay_groups)

    await respond(interaction, f"Removed **{source_channel.name}** from voice bridging.")


@bot.tree.command(name="list-bridges", description="Show all linked voice and text channels.")
@app_commands.guild_only()
async def list_bridges(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)
    lines = ["**=== VC BRIDGE GROUPS ===**"]

    if not vc_bridge_groups:
        lines.append("No active voice bridges.")
    else:
        for idx, group in enumerate(vc_bridge_groups, start=1):
            lines.append(f"**VC Group {idx} ({len(group)} servers):**")
            for cid in group:
                ch = await get_voice_channel(cid)
                if ch:
                    lines.append(f"• **{discord.utils.escape_markdown(ch.guild.name)}** — #{discord.utils.escape_markdown(ch.name)}")
                else:
                    lines.append(f"• Unavailable Channel ({cid})")

    lines.append("\n**=== TEXT RELAY GROUPS ===**")
    if not text_relay_groups:
        lines.append("No active text relays.")
    else:
        for idx, group in enumerate(text_relay_groups, start=1):
            lines.append(f"**Relay Group {idx} ({len(group)} servers):**")
            for cid in group:
                ch = await get_text_channel(cid)
                if ch:
                    lines.append(f"• **{discord.utils.escape_markdown(ch.guild.name)}** — #{discord.utils.escape_markdown(ch.name)}")
                else:
                    lines.append(f"• Unavailable Channel ({cid})")

    chunks = chunk_message_lines(lines)
    await respond(interaction, chunks[0])
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, ephemeral=True)


# --- TTS VOICE SPEECH PIPELINE ---
def take_send_slot(user_id: int) -> float:
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
        return

    voice_client = discord.utils.get(bot.voice_clients, guild=target_channel.guild)
    if voice_client is not None and not voice_client.is_connected():
        voice_client = None

    if voice_client is not None:
        while voice_client.is_playing() or voice_client.is_paused():
            await asyncio.sleep(0.1)

    try:
        if voice_client is None:
            voice_client = await target_channel.connect(timeout=20, reconnect=True)
        elif voice_client.channel is None or voice_client.channel.id != target_channel.id:
            await voice_client.move_to(target_channel)
    except Exception as error:
        logger.error("Could not connect to VC %s: %s", channel_id, error)
        return

    audio_source = discord.FFmpegPCMAudio(audio_path, options=f"-filter:a atempo={PLAYBACK_SPEED:.2f}")
    loop = asyncio.get_running_loop()
    playback_finished: asyncio.Future[Exception | None] = loop.create_future()

    def after_playback(error: Exception | None) -> None:
        try:
            loop.call_soon_threadsafe(
                lambda: playback_finished.set_result(error) if not playback_finished.done() else None
            )
        except RuntimeError:
            pass

    try:
        voice_client.play(audio_source, after=after_playback)
    except Exception:
        audio_source.cleanup()
        raise

    await playback_finished


async def prepare_speech_queue() -> None:
    while True:
        spoken_text, target_channel_ids, voice = await speech_queue.get()
        audio_path: str | None = None
        try:
            generation_error = None
            try:
                audio_path = await create_tts_file(spoken_text, voice)
            except Exception as error:
                generation_error = error

            await prepared_speech_queue.put(
                PreparedSpeech(
                    target_channel_ids=target_channel_ids,
                    audio_path=audio_path,
                    error=generation_error,
                )
            )
            audio_path = None
        finally:
            if audio_path and os.path.exists(audio_path):
                os.unlink(audio_path)
            speech_queue.task_done()


async def process_speech_queue() -> None:
    while True:
        prepared = await prepared_speech_queue.get()
        try:
            if prepared.error or not prepared.audio_path:
                continue
            await asyncio.gather(
                *(play_audio_in_channel(cid, prepared.audio_path) for cid in prepared.target_channel_ids),
                return_exceptions=True,
            )
        finally:
            if prepared.audio_path and os.path.exists(prepared.audio_path):
                os.unlink(prepared.audio_path)
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
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or member.voice is None or not isinstance(member.voice.channel, discord.VoiceChannel):
        await respond(interaction, "Join a voice channel in a bridge group first.")
        return
    if len(message) > MAX_TTS_CHARACTERS:
        await respond(interaction, f"Keep messages under {MAX_TTS_CHARACTERS} characters.")
        return

    source_channel = member.voice.channel
    group = find_group(vc_bridge_groups, source_channel.id)
    if not group:
        await respond(interaction, "This voice channel isn't in a bridge group. Use `/link-vc` first.")
        return

    target_channel_ids = tuple(sorted(group - {source_channel.id}))
    if not target_channel_ids:
        await respond(interaction, "This bridge group has no other linked servers.")
        return

    retry_after = take_send_slot(member.id)
    if retry_after > 0:
        await respond(interaction, f"Please wait {retry_after:.1f}s before sending another message.")
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    spoken_text = f"Message from {source_channel.guild.name}. {member.display_name} says: {message}"

    try:
        speech_queue.put_nowait((spoken_text, target_channel_ids, voice))
    except asyncio.QueueFull:
        await respond(interaction, "The speech queue is full. Try again shortly.")
        return

    await respond(interaction, f"Queued for {len(target_channel_ids)} other server(s).")


def main() -> None:
    token = os.environ.get("DISCORD_TOKEN") or os.environ.get("DISCORD_BOT_TOKEN")
    if not token:
        raise SystemExit("Missing DISCORD_TOKEN environment variable.")

    keep_alive()
    bot.run(token)


if __name__ == "__main__":
    main()
