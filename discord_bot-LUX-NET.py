import os
import sys
import asyncio
import logging
import io
import resource
import urllib.request
from collections import deque
from threading import Thread
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands
from flask import Flask

# Voice relay dependencies are optional: if they are missing the bot still runs
# all text/forum/thread features and simply disables the voice commands.
try:
    import audioop  # stdlib <=3.12, provided by `audioop-lts` on 3.13+
    from discord.ext import voice_recv
    VOICE_OK = True
except ImportError:
    audioop = None
    voice_recv = None
    VOICE_OK = False

try:
    import davey  # Discord's E2EE (DAVE) implementation; installed with discord.py[voice] 2.7+
except ImportError:
    davey = None
from supabase import create_client, Client
import yt_dlp
from duckduckgo_search import DDGS  # using duckduckgo_search package with underscore

# ------------------------------------------------------------------------------
# LOGGING SETUP
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("discord-bridge-bot")

logging.getLogger("yt_dlp").setLevel(logging.ERROR)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ------------------------------------------------------------------------------
# SUPABASE DATABASE SETUP & HELPERS
# ------------------------------------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.critical("SUPABASE_URL or SUPABASE_KEY environment variables are missing!")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Which column identifies the linked item in each relay table.
_LINK_COLS = {
    "forum_relays": "forum_channel_id",
    "text_relays": "channel_id",
    "thread_relays": "thread_id",
    "vc_code_relays": "vc_id",
}

def add_link(table: str, code: str, item_id: int):
    try:
        col = _LINK_COLS.get(table, "thread_id")
        
        supabase.table(table).upsert(
            {"network_code": code, col: item_id}, 
            on_conflict="network_code," + col
        ).execute()
        logger.info(f"Successfully added/upserted {item_id} to {table} under {code}")
    except Exception as e:
        logger.error(f"CRITICAL Supabase insert error in ({table}): {e}")

def remove_link(table: str, code: str, item_id: int):
    try:
        col = _LINK_COLS.get(table, "thread_id")
        supabase.table(table).delete().eq("network_code", code).eq(col, item_id).execute()
    except Exception as e:
        logger.error(f"Failed to remove link from Supabase ({table}): {e}")

def get_links(table: str, code: str):
    try:
        col = _LINK_COLS.get(table, "thread_id")
        
        response = supabase.table(table).select(col).eq("network_code", code).execute()
        logger.info(f"Supabase fetch response for {table} code '{code}': {response.data}")
        return [row[col] for row in response.data]
    except Exception as e:
        logger.error(f"CRITICAL Supabase fetch error in ({table}): {e}")
        return []

# Persistent Message Mapping Helpers via Supabase
def register_message_mapping(source_msg_id: int, target_channel_id: int, target_msg_id: int):
    try:
        res = supabase.table("message_mappings").select("root_message_id").or_(f"message_id.eq.{source_msg_id},message_id.eq.{target_msg_id}").limit(1).execute()
        if res.data:
            root_id = res.data[0]["root_message_id"]
        else:
            root_id = source_msg_id

        supabase.table("message_mappings").upsert([
            {"root_message_id": root_id, "channel_id": target_channel_id, "message_id": target_msg_id},
            {"root_message_id": root_id, "channel_id": source_msg_id, "message_id": source_msg_id}
        ], on_conflict="channel_id,message_id").execute()
    except Exception as e:
        logger.error(f"Failed to register message mapping in Supabase: {e}")

def get_mirrored_targets(msg_id: int):
    try:
        res = supabase.table("message_mappings").select("root_message_id").eq("message_id", msg_id).limit(1).execute()
        if not res.data:
            return []
        root_id = res.data[0]["root_message_id"]

        all_res = supabase.table("message_mappings").select("channel_id,message_id").eq("root_message_id", root_id).execute()
        return [(row["channel_id"], row["message_id"]) for row in all_res.data]
    except Exception as e:
        logger.error(f"Failed to fetch message mapping from Supabase: {e}")
        return []

# ------------------------------------------------------------------------------
# GLOBAL BOT BAN HELPERS
# ------------------------------------------------------------------------------
def is_user_banned(user_id: int) -> bool:
    try:
        res = supabase.table("bot_bans").select("user_id").eq("user_id", user_id).limit(1).execute()
        return len(res.data) > 0
    except Exception as e:
        logger.error(f"Error checking bot ban for {user_id}: {e}")
        return False

def add_bot_ban(user_id: int):
    try:
        supabase.table("bot_bans").upsert({"user_id": user_id}, on_conflict="user_id").execute()
        logger.info(f"Globally banned user ID {user_id} from using the bot.")
    except Exception as e:
        logger.error(f"Failed to add bot ban for {user_id}: {e}")

def remove_bot_ban(user_id: int):
    try:
        supabase.table("bot_bans").delete().eq("user_id", user_id).execute()
        logger.info(f"Removed global ban for user ID {user_id}.")
    except Exception as e:
        logger.error(f"Failed to remove bot ban for {user_id}: {e}")

async def check_is_owner(interaction: discord.Interaction) -> bool:
    if await bot.is_owner(interaction.user):
        return True
    owner_id_env = os.environ.get("BOT_OWNER_ID")
    if owner_id_env and str(interaction.user.id) == str(owner_id_env).strip():
        return True
    return False

# ------------------------------------------------------------------------------
# SEARCH HELPER (DUCKDUCKGO RETURNING TITLES, URLS, & BODIES)
# ------------------------------------------------------------------------------
def google_search(query: str, num_results: int = 5):
    results = []
    try:
        with DDGS() as ddgs:
            res = list(ddgs.text(query, max_results=num_results))
            for r in res:
                if "href" in r:
                    results.append({
                        "title": r.get("title", "No Title"),
                        "href": r.get("href"),
                        "body": r.get("body", "No description available.")
                    })
    except Exception as e:
        logger.error(f"DuckDuckGo Search error: {e}")

    if not results:
        results = [{
            "title": f"DuckDuckGo Search: {query}",
            "href": f"https://html.duckduckgo.com/html/?q={query.replace(' ', '+')}",
            "body": "Click to view search results directly on DuckDuckGo."
        }]
        
    return results

# ------------------------------------------------------------------------------
# FLASK KEEP-ALIVE SERVER (FOR RENDER UPTIME)
# ------------------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "LUX-NET Bridge Bot is Online!", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)
    flask_app.run(host="0.0.0.0", port=port)

Thread(target=run_flask, daemon=True).start()

# ------------------------------------------------------------------------------
# DISCORD BOT INITIALIZATION
# ------------------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.messages = True
intents.voice_states = True
intents.reactions = True
intents.members = True

member_cache = discord.MemberCacheFlags.none()
member_cache.voice = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    member_cache_flags=member_cache,
    chunk_guilds_at_startup=False,
    max_messages=100,
)

RELAYED_THREAD_IDS = set()

# Global check for all slash commands
@bot.tree.interaction_check
async def global_interaction_check(interaction: discord.Interaction) -> bool:
    if is_user_banned(interaction.user.id):
        await interaction.response.send_message("❌ You are globally banned from using this bot.", ephemeral=True)
        return False
    return True

# ------------------------------------------------------------------------------
# WEBHOOK HELPER FUNCTIONS
# ------------------------------------------------------------------------------
async def get_or_create_webhook(channel: discord.abc.GuildChannel) -> discord.Webhook | None:
    target_channel = channel.parent if isinstance(channel, discord.Thread) else channel

    if isinstance(target_channel, discord.Thread):
        target_channel = target_channel.parent

    if not isinstance(target_channel, (discord.TextChannel, discord.ForumChannel)):
        return None

    try:
        webhooks = await target_channel.webhooks()
        for wh in webhooks:
            if wh.user and wh.user.id == bot.user.id:
                return wh
        return await target_channel.create_webhook(name="LUX-NET Relay Bridge")
    except discord.Forbidden:
        logger.error(f"Missing 'Manage Webhooks' permission in #{target_channel.name}")
        return None
    except Exception as e:
        logger.error(f"Failed to create webhook in #{target_channel.name}: {e}")
        return None

# ------------------------------------------------------------------------------
# BOT EVENTS (TEXT, THREADS, FORUMS, VOICE, & REACTIONS)
# ------------------------------------------------------------------------------
_ready_done = False

@bot.event
async def on_ready():
    global _ready_done
    logger.info(f"Connected to Discord as {bot.user} in {len(bot.guilds)} servers")

    if _ready_done:
        return
    _ready_done = True

    if VOICE_OK and not ensure_opus():
        logger.warning("libopus not found - voice bridge disabled until it is installed (see Dockerfile).")
    asyncio.create_task(keepalive_loop())

    for guild in bot.guilds:
        try:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
        except Exception as e:
            logger.error(f"Could not clear guild commands for {guild.name}: {e}")

    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync global slash commands: {e}")

@bot.event
async def on_reaction_add(reaction: discord.Reaction, user: discord.User | discord.Member):
    if user.bot or is_user_banned(user.id):
        return

    msg = reaction.message
    mirrored_targets = get_mirrored_targets(msg.id)
    if not mirrored_targets:
        return

    emoji = reaction.emoji

    for channel_id, target_msg_id in mirrored_targets:
        if channel_id == msg.channel.id and target_msg_id == msg.id:
            continue

        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            if channel and not isinstance(channel, discord.ForumChannel):
                target_msg = await channel.fetch_message(target_msg_id)
                if target_msg:
                    try:
                        await target_msg.add_reaction(emoji)
                    except discord.HTTPException:
                        pass
        except Exception as e:
            logger.error(f"Failed to add cross-server reaction to message {target_msg_id}: {e}")

@bot.event
async def on_reaction_remove(reaction: discord.Reaction, user: discord.User | discord.Member):
    if user.bot or is_user_banned(user.id):
        return

    msg = reaction.message
    mirrored_targets = get_mirrored_targets(msg.id)
    if not mirrored_targets:
        return

    emoji = reaction.emoji

    for channel_id, target_msg_id in mirrored_targets:
        if channel_id == msg.channel.id and target_msg_id == msg.id:
            continue

        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            if channel and not isinstance(channel, discord.ForumChannel):
                target_msg = await channel.fetch_message(target_msg_id)
                if target_msg:
                    source_count = 0
                    for r in msg.reactions:
                        if str(r.emoji) == str(emoji):
                            source_count = r.count
                            break

                    if source_count == 0:
                        try:
                            await target_msg.remove_reaction(emoji, bot.user)
                        except discord.HTTPException:
                            pass
        except Exception as e:
            logger.error(f"Failed to remove cross-server reaction from message {target_msg_id}: {e}")

@bot.event
async def on_thread_create(thread: discord.Thread):
    if not isinstance(thread.parent, discord.ForumChannel):
        return

    if thread.id in RELAYED_THREAD_IDS:
        return

    current_forum_id = thread.parent.id

    if not supabase:
        return

    try:
        await asyncio.sleep(1.0)
        try:
            webhooks = await thread.parent.webhooks()
            webhook_ids = {wh.id for wh in webhooks}
        except Exception:
            webhook_ids = set()

        starter_message = None
        async for msg in thread.history(limit=1, oldest_first=True):
            starter_message = msg
            break

        if starter_message:
            if is_user_banned(starter_message.author.id):
                return
            if (starter_message.webhook_id and starter_message.webhook_id in webhook_ids) or starter_message.author.bot:
                RELAYED_THREAD_IDS.add(thread.id)
                return

        if not starter_message:
            return

        res = supabase.table("forum_relays").select("network_code").eq("forum_channel_id", current_forum_id).execute()
        codes = [row["network_code"] for row in res.data]
        if not codes:
            return

        target_forum_ids = []
        for code in codes:
            f_res = supabase.table("forum_relays").select("forum_channel_id").eq("network_code", code).neq("forum_channel_id", current_forum_id).execute()
            target_forum_ids.extend([row["forum_channel_id"] for row in f_res.data])
    except Exception as e:
        logger.error(f"Database error in on_thread_create: {e}")
        return

    target_forum_ids = list(set(target_forum_ids))
    if not target_forum_ids:
        return

    try:
        author = starter_message.author
        author_name = author.display_name
        guild_name = thread.guild.name
        post_title = thread.name
        post_body = starter_message.content or ""

        webhook_username = f"{author_name} [{guild_name}]"
        if len(webhook_username) > 80:
            webhook_username = webhook_username[:80]

        avatar_url = author.display_avatar.url

        files = []
        skipped_files = []
        MAX_FILE_SIZE = 10 * 1024 * 1024

        if starter_message.attachments:
            for attachment in starter_message.attachments:
                if attachment.size > MAX_FILE_SIZE:
                    skipped_files.append(attachment.filename)
                    continue
                try:
                    file_bytes = await attachment.read()
                    fp = io.BytesIO(file_bytes)
                    fp.seek(0)
                    file_obj = discord.File(fp=fp, filename=attachment.filename.lower())
                    files.append(file_obj)
                except Exception as e:
                    logger.error(f"Failed to process attachment {attachment.filename}: {e}")

        if skipped_files:
            skip_notice = f"\n*⚠ [Skipped oversized file(s): {', '.join(skipped_files)} - Exceeds Discord size limit]*"
            post_body += skip_notice

        send_kwargs = {
            "username": webhook_username,
            "avatar_url": avatar_url,
            "allowed_mentions": discord.AllowedMentions.none(),
            "thread_name": post_title[:100],
            "wait": True
        }

        if post_body:
            send_kwargs["content"] = post_body

        if starter_message.embeds:
            send_kwargs["embeds"] = starter_message.embeds

        if files:
            send_kwargs["files"] = files

        if "content" not in send_kwargs and "embeds" not in send_kwargs and "files" not in send_kwargs:
            send_kwargs["content"] = f"*[Forum Post: {post_title}]*"

        for fid in target_forum_ids:
            target_forum = bot.get_channel(fid)
            if target_forum and isinstance(target_forum, discord.ForumChannel):
                webhook = await get_or_create_webhook(target_forum)
                if webhook:
                    try:
                        sent_msg = await webhook.send(**send_kwargs)
                        if sent_msg:
                            if isinstance(sent_msg.channel, discord.Thread):
                                RELAYED_THREAD_IDS.add(sent_msg.channel.id)
                            elif sent_msg.thread:
                                RELAYED_THREAD_IDS.add(sent_msg.thread.id)
                                
                            register_message_mapping(starter_message.id, target_forum.id, sent_msg.id)
                    except Exception as e:
                        logger.error(f"Error relaying forum post to {fid}: {e}")
    except Exception as e:
        logger.error(f"Failed to relay new forum post: {e}")

# ------------------------------------------------------------------------------
# VOICE BRIDGE (LIVE AUDIO RELAY BETWEEN LINKED VOICE CHANNELS)
# ------------------------------------------------------------------------------
FRAME_BYTES = 3840                 
SILENCE = b"\x00" * FRAME_BYTES
MAX_BUFFERED_FRAMES = 6            
IDLE_FRAMES_BEFORE_STOP = 50       
MAX_VOICE_SESSIONS = int(os.environ.get("MAX_VOICE_SESSIONS", "4"))

voice_sessions: dict = {}          
_voice_connecting: set = set()     
_opus_warned = False


def ensure_opus() -> bool:
    if discord.opus.is_loaded():
        return True
    for name in ("libopus.so.0", "libopus.so", "opus"):
        try:
            discord.opus.load_opus(name)
            return True
        except Exception:
            continue
    return False


async def keepalive_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        return

    def ping():
        with urllib.request.urlopen(url, timeout=10) as r:
            r.read(16)

    while True:
        await asyncio.sleep(600)
        try:
            await asyncio.to_thread(ping)
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")


OPUS_SILENCE = b"\xf8\xff\xfe"
DAVE_FOOTER = b"\xfa\xfa"

voice_stats = {"dave_ok": 0, "dave_fail": 0, "plain": 0, "decode_err": 0}


def patch_opus_decode_safety():
    if not VOICE_OK:
        return
    try:
        from discord.ext.voice_recv import opus as vr_opus
    except Exception as e:
        logger.warning(f"Could not apply decode safety patch: {e!r}")
        return

    original = vr_opus.PacketDecoder._decode_packet

    def safe_decode(self, packet):
        try:
            return original(self, packet)
        except discord.opus.OpusError as e:
            voice_stats["decode_err"] += 1
            if voice_stats["decode_err"] <= 5:
                d = getattr(packet, "decrypted_data", None) or b""
                logger.warning(
                    f"Skipped undecodable voice packet ({e}): ssrc={self.ssrc} len={len(d)} "
                    f"head={d[:6].hex()} tail={d[-4:].hex()}"
                )
            return packet, SILENCE

    vr_opus.PacketDecoder._decode_packet = safe_decode


patch_opus_decode_safety()


def install_dave_receive(vc):
    reader = getattr(vc, "_reader", None)
    if reader is None or davey is None:
        return
    inner = reader.decryptor.decrypt_rtp

    def decrypt_rtp(packet):
        data = inner(packet)
        if data[-2:] != DAVE_FOOTER:
            voice_stats["plain"] += 1
            return data
        session = getattr(vc._connection, "dave_session", None)
        user_id = vc._get_id_from_ssrc(packet.ssrc)
        if session is None or not session.ready or user_id is None:
            voice_stats["dave_fail"] += 1
            return OPUS_SILENCE
        try:
            out = bytes(session.decrypt(user_id, davey.MediaType.audio, bytes(data)))
            voice_stats["dave_ok"] += 1
            return out
        except Exception as e:
            voice_stats["dave_fail"] += 1
            if voice_stats["dave_fail"] <= 5:
                logger.warning(f"DAVE decrypt failed for user {user_id}: {e!r}")
            return OPUS_SILENCE

    reader.decryptor.decrypt_rtp = decrypt_rtp


if VOICE_OK:

    class MixerSource(discord.AudioSource):
        def __init__(self):
            self.buffers: dict = {}   
            self.idle = 0
            self.playing = False      

        def feed(self, speaker_id: int, pcm: bytes):
            dq = self.buffers.get(speaker_id)
            if dq is None:
                dq = self.buffers[speaker_id] = deque(maxlen=MAX_BUFFERED_FRAMES)
            dq.append(pcm)

        def read(self) -> bytes:
            frames = []
            for dq in list(self.buffers.values()):
                try:
                    frames.append(dq.popleft())
                except IndexError:
                    pass

            if not frames:
                self.idle += 1
                if self.idle >= IDLE_FRAMES_BEFORE_STOP:
                    self.buffers.clear()
                    return b""
                return SILENCE

            self.idle = 0
            out = frames[0]
            for f in frames[1:]:
                out = audioop.add(out, f, 2)
            return out

        def is_opus(self) -> bool:
            return False

        def cleanup(self):
            pass

    class VoiceSession:
        __slots__ = ("guild_id", "vc", "channel_id", "codes", "mixer",
                     "frames_in", "frames_out", "loop")

        def __init__(self, guild_id, vc, channel_id, codes, loop):
            self.guild_id = guild_id
            self.vc = vc
            self.channel_id = channel_id
            self.codes = codes
            self.mixer = MixerSource()
            self.frames_in = 0
            self.frames_out = 0
            self.loop = loop

        def ensure_playing(self):
            if self.mixer.playing or not self.vc.is_connected():
                return
            self.mixer.playing = True
            self.mixer.idle = 0
            try:
                self.vc.play(
                    self.mixer,
                    after=self._after_play,
                    application="voip",
                    bitrate=64,
                    fec=True,
                    expected_packet_loss=0.05,
                    signal_type="voice",
                )
            except Exception as e:
                self.mixer.playing = False
                logger.error(f"Could not start voice playback in guild {self.guild_id}: {e}")

        def _after_play(self, error):
            if error:
                logger.error(f"Voice playback error in guild {self.guild_id}: {error}")
            self.loop.call_soon_threadsafe(self._on_play_end)

        def _on_play_end(self):
            self.mixer.playing = False
            if any(self.mixer.buffers.values()):
                self.ensure_playing()

    def route_audio(src: "VoiceSession", speaker_id: int, pcm: bytes):
        src.frames_in += 1
        for dst in list(voice_sessions.values()):
            if dst is src or not (dst.codes & src.codes):
                continue
            dst.mixer.feed(speaker_id, pcm)
            dst.frames_out += 1
            if not dst.mixer.playing:
                dst.loop.call_soon_threadsafe(dst.ensure_playing)

    class RelaySink(voice_recv.AudioSink):
        def __init__(self, session: VoiceSession):
            super().__init__()
            self.session = session

        def wants_opus(self) -> bool:
            return False  

        def write(self, user, data):
            if user is None or user.bot or is_user_banned(user.id):
                return
            pcm = data.pcm
            if pcm and len(pcm) == FRAME_BYTES:
                route_audio(self.session, user.id, pcm)

        def cleanup(self):
            pass


async def get_vc_codes(channel_id: int) -> set:
    def query():
        res = supabase.table("vc_code_relays").select("network_code").eq("vc_id", channel_id).execute()
        return {row["network_code"] for row in res.data}
    try:
        return await asyncio.to_thread(query)
    except Exception as e:
        logger.error(f"Error checking VC bridge database: {e}")
        return set()


def humans_in(channel: discord.VoiceChannel) -> int:
    return sum(1 for uid in channel.voice_states if uid != bot.user.id)


async def end_voice_session(guild_id: int):
    session = voice_sessions.pop(guild_id, None)
    if not session:
        return
    try:
        if session.vc.is_listening():
            session.vc.stop_listening()
    except Exception:
        pass
    try:
        await session.vc.disconnect(force=True)
    except Exception as e:
        logger.error(f"Error disconnecting voice in guild {guild_id}: {e}")
    session.mixer.buffers.clear()
    logger.info(f"Voice session ended in guild {guild_id}")


async def maybe_join_voice(channel: discord.VoiceChannel):
    global _opus_warned
    if not VOICE_OK or not isinstance(channel, discord.VoiceChannel):
        return
    gid = channel.guild.id
    if gid in voice_sessions or gid in _voice_connecting:
        return
    if len(voice_sessions) >= MAX_VOICE_SESSIONS:
        return
    codes = await get_vc_codes(channel.id)
    if not codes:
        return
    if not ensure_opus():
        if not _opus_warned:
            logger.error("Cannot start voice bridge: libopus is not installed.")
            _opus_warned = True
        return

    _voice_connecting.add(gid)
    try:
        vc = await channel.connect(cls=voice_recv.VoiceRecvClient, self_deaf=False, timeout=30)
        session = VoiceSession(gid, vc, channel.id, codes, asyncio.get_running_loop())
        voice_sessions[gid] = session
        vc.listen(RelaySink(session))
        install_dave_receive(vc)
        logger.info(f"DAVE receive patch active: {davey is not None}")
        logger.info(f"Voice session started in guild {gid} on #{channel.name} (codes: {sorted(codes)})")
    except Exception as e:
        logger.error(f"Failed to join voice channel {channel.id}: {e}")
        try:
            if channel.guild.voice_client:
                await channel.guild.voice_client.disconnect(force=True)
        except Exception:
            pass
    finally:
        _voice_connecting.discard(gid)


async def maybe_leave_voice(channel: discord.abc.GuildChannel):
    session = voice_sessions.get(channel.guild.id)
    if session and session.channel_id == channel.id and humans_in(channel) == 0:
        await end_voice_session(channel.guild.id)


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if bot.user and member.id == bot.user.id:
        session = voice_sessions.get(member.guild.id)
        if session and (after.channel is None or after.channel.id != session.channel_id):
            await end_voice_session(member.guild.id)
        return

    if member.bot or is_user_banned(member.id) or not VOICE_OK:
        return

    if after.channel and after.channel != before.channel:
        await maybe_join_voice(after.channel)
    if before.channel and before.channel != after.channel:
        await maybe_leave_voice(before.channel)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild or message.webhook_id:
        return

    if is_user_banned(message.author.id):
        return

    await bot.process_commands(message)

    current_channel = message.channel
    is_thread = isinstance(current_channel, discord.Thread)
    is_forum_thread = is_thread and isinstance(current_channel.parent, discord.ForumChannel)

    target_channel_ids = []
    target_thread_ids = []

    try:
        if is_forum_thread:
            current_forum_id = current_channel.parent.id
            res = supabase.table("forum_relays").select("network_code").eq("forum_channel_id", current_forum_id).execute()
            codes = [row["network_code"] for row in res.data]
            if not codes:
                return
            for code in codes:
                f_res = supabase.table("forum_relays").select("forum_channel_id").eq("network_code", code).neq("forum_channel_id", current_forum_id).execute()
                forum_ids = [row["forum_channel_id"] for row in f_res.data]
                for fid in forum_ids:
                    f_ch = bot.get_channel(fid)
                    if f_ch and isinstance(f_ch, discord.ForumChannel):
                        for active_thread in f_ch.threads:
                            if active_thread.name.lower() == current_channel.name.lower():
                                target_thread_ids.append(active_thread.id)
        elif is_thread:
            res = supabase.table("thread_relays").select("network_code").eq("thread_id", current_channel.id).execute()
            codes = [row["network_code"] for row in res.data]
            if not codes:
                return
            for code in codes:
                t_res = supabase.table("thread_relays").select("thread_id").eq("network_code", code).neq("thread_id", current_channel.id).execute()
                target_thread_ids.extend([row["thread_id"] for row in t_res.data])
        else:
            res = supabase.table("text_relays").select("network_code").eq("channel_id", current_channel.id).execute()
            codes = [row["network_code"] for row in res.data]
            
            if not codes:
                return

            for code in codes:
                c_res = supabase.table("text_relays").select("channel_id").eq("network_code", code).neq("channel_id", current_channel.id).execute()
                target_channel_ids.extend([row["channel_id"] for row in c_res.data])
    except Exception as e:
        logger.error(f"Database query error in on_message: {e}")
        return

    target_channel_ids = list(set(target_channel_ids))
    target_thread_ids = list(set(target_thread_ids))

    if not target_channel_ids and not target_thread_ids:
        return

    author_name = message.author.display_name
    guild_name = message.guild.name

    webhook_username = f"{author_name} [{guild_name}]"
    if len(webhook_username) > 80:
        webhook_username = webhook_username[:80]

    raw_content = message.content or ""
    reply_prefix = ""
    if message.reference and message.reference.message_id:
        try:
            ref_msg = await current_channel.fetch_message(message.reference.message_id)
            if ref_msg:
                snippet = ref_msg.content[:60] + "..." if len(ref_msg.content) > 60 else ref_msg.content
                if not snippet and ref_msg.attachments:
                    snippet = "[Attachment]"
                reply_prefix = f"> ↩ **Replying to {ref_msg.author.display_name}:** *{snippet or '[Embed/Sticker]'}*\n"
        except Exception:
            pass

    final_content = f"{reply_prefix}{raw_content}".strip()

    send_kwargs = {
        "username": webhook_username,
        "avatar_url": message.author.display_avatar.url,
        "allowed_mentions": discord.AllowedMentions.none(),
        "wait": True
    }

    if final_content:
        send_kwargs["content"] = final_content

    if message.embeds:
        send_kwargs["embeds"] = message.embeds

    files = []
    skipped_files = []
    MAX_FILE_SIZE = 10 * 1024 * 1024

    if message.attachments:
        for attachment in message.attachments:
            if attachment.size > MAX_FILE_SIZE:
                skipped_files.append(attachment.filename)
                continue
            try:
                file_bytes = await attachment.read()
                fp = io.BytesIO(file_bytes)
                fp.seek(0)
                file_obj = discord.File(fp=fp, filename=attachment.filename.lower())
                files.append(file_obj)
            except Exception as e:
                logger.error(f"Failed to process attachment {attachment.filename}: {e}")

    if skipped_files:
        skip_notice = f"\n*⚠ [Skipped oversized file(s): {', '.join(skipped_files)} - Exceeds Discord size limit]*"
        if "content" in send_kwargs:
            send_kwargs["content"] += skip_notice
        else:
            send_kwargs["content"] = skip_notice.strip()

    if files:
        send_kwargs["files"] = files

    if "content" not in send_kwargs and "embeds" not in send_kwargs and "files" not in send_kwargs:
        if message.stickers:
            send_kwargs["content"] = f"*[Sticker: {message.stickers[0].name}]*"
        else:
            return

    if target_thread_ids:
        for tid in target_thread_ids:
            t_channel = bot.get_channel(tid)
            if t_channel and isinstance(t_channel, discord.Thread):
                webhook = await get_or_create_webhook(t_channel)
                if webhook:
                    try:
                        sent_msg = await webhook.send(thread=t_channel, **send_kwargs)
                        if sent_msg:
                            register_message_mapping(message.id, t_channel.id, sent_msg.id)
                    except Exception as e:
                        logger.error(f"Error relaying thread message to {tid}: {e}")

    if target_channel_ids:
        for target_id in target_channel_ids:
            target_channel = bot.get_channel(target_id)
            if target_channel and isinstance(target_channel, discord.TextChannel):
                webhook = await get_or_create_webhook(target_channel)
                if webhook:
                    try:
                        sent_msg = await webhook.send(**send_kwargs)
                        if sent_msg:
                            register_message_mapping(message.id, target_channel.id, sent_msg.id)
                    except Exception as e:
                        logger.error(f"Error relaying message to {target_channel.id}: {e}")

# ------------------------------------------------------------------------------
# SLASH COMMANDS (BRIDGES, POLLS, YOUTUBE, SEARCH, & BANS)
# ------------------------------------------------------------------------------
@bot.tree.command(name="ping", description="Check the bot's latency.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(f"Pong! 🏓 `{latency}ms`", ephemeral=True)

@bot.tree.command(name="ban-bot", description="Globally ban a user from using this bot anywhere.")
@app_commands.describe(user_id="The Discord User ID of the person to ban")
async def ban_bot(interaction: discord.Interaction, user_id: str):
    if not await check_is_owner(interaction):
        await interaction.response.send_message("❌ Only the bot owner can use this command.", ephemeral=True)
        return

    try:
        target_id = int(user_id.strip())
    except ValueError:
        await interaction.response.send_message("❌ Please provide a valid numeric Discord User ID.", ephemeral=True)
        return

    if target_id == bot.user.id:
        await interaction.response.send_message("❌ You cannot ban the bot itself.", ephemeral=True)
        return

    await asyncio.to_thread(add_bot_ban, target_id)
    await interaction.response.send_message(f"🔨 Successfully banned user ID `{target_id}` from using the bot globally.", ephemeral=True)

@bot.tree.command(name="unban-bot", description="Remove a global bot ban from a user.")
@app_commands.describe(user_id="The Discord User ID to unban")
async def unban_bot(interaction: discord.Interaction, user_id: str):
    if not await check_is_owner(interaction):
        await interaction.response.send_message("❌ Only the bot owner can use this command.", ephemeral=True)
        return

    try:
        target_id = int(user_id.strip())
    except ValueError:
        await interaction.response.send_message("❌ Please provide a valid numeric Discord User ID.", ephemeral=True)
        return

    await asyncio.to_thread(remove_bot_ban, target_id)
    await interaction.response.send_message(f"✅ Successfully removed global bot ban for user ID `{target_id}`.", ephemeral=True)

@bot.tree.command(name="list-bot-bans", description="List all globally banned user IDs.")
async def list_bot_bans(interaction: discord.Interaction):
    if not await check_is_owner(interaction):
        await interaction.response.send_message("❌ Only the bot owner can use this command.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        res = await asyncio.to_thread(lambda: supabase.table("bot_bans").select("user_id").execute())
        banned_ids = [row["user_id"] for row in res.data]
        if not banned_ids:
            await interaction.followup.send("📋 There are currently no globally banned users.", ephemeral=True)
            return

        desc = "\n".join([f"• `{uid}`" for uid in banned_ids])
        embed = discord.Embed(title=f"🔨 Globally Banned Users ({len(banned_ids)})", description=desc, color=discord.Color.red())
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Error fetching banned users: {e}", ephemeral=True)

@bot.tree.command(name="poll", description="Create a native poll and broadcast it across connected relay channels.")
@app_commands.describe(
    question="The question for your poll",
    option1="First option",
    option2="Second option",
    option3="Third option (optional)",
    option4="Fourth option (optional)",
    duration_hours="How many hours the poll should stay open (default 24)"
)
async def poll(
    interaction: discord.Interaction, 
    question: str, 
    option1: str, 
    option2: str, 
    option3: str = None, 
    option4: str = None, 
    duration_hours: int = 24
):
    await interaction.response.defer()

    p = discord.Poll(
        question=question, 
        duration=timedelta(hours=duration_hours)
    )
    p.add_answer(text=option1)
    p.add_answer(text=option2)
    if option3:
        p.add_answer(text=option3)
    if option4:
        p.add_answer(text=option4)

    temp_msg = await interaction.followup.send(poll=p, wait=True)

    channel_id = interaction.channel.id
    try:
        res = supabase.table("text_relays").select("network_code").eq("channel_id", channel_id).execute()
        codes = [row["network_code"] for row in res.data]

        if codes:
            target_channel_ids = []
            for code in codes:
                c_res = supabase.table("text_relays").select("channel_id").eq("network_code", code).neq("channel_id", channel_id).execute()
                target_channel_ids.extend([row["channel_id"] for row in c_res.data])

            target_channel_ids = list(set(target_channel_ids))
            webhook_username = f"{interaction.user.display_name} [{interaction.guild.name}] (Poll)"

            for target_id in target_channel_ids:
                target_channel = bot.get_channel(target_id)
                if target_channel and isinstance(target_channel, discord.TextChannel):
                    webhook = await get_or_create_webhook(target_channel)
                    if webhook:
                        try:
                            await webhook.send(
                                username=webhook_username[:80],
                                avatar_url=interaction.user.display_avatar.url,
                                poll=p
                            )
                        except Exception as e:
                            logger.error(f"Error broadcasting poll to channel {target_id}: {e}")
    except Exception as e:
        logger.error(f"Error handling cross-server poll relay: {e}")

@bot.tree.command(name="list-bridges", description="List all channels, threads, and forums connected to a network code.")
@app_commands.describe(network_code="The network code to inspect")
async def list_bridges(interaction: discord.Interaction, network_code: str):
    code = network_code.strip().lower()
    await interaction.response.defer(ephemeral=True)

    text_ids = get_links("text_relays", code)
    thread_ids = get_links("thread_relays", code)
    forum_ids = get_links("forum_relays", code)

    embed = discord.Embed(title=f"🌐 Bridge Status for Network: `{code}`", color=discord.Color.blurple())

    text_desc = []
    for cid in text_ids:
        ch = bot.get_channel(cid)
        if ch:
            text_desc.append(f"• #{ch.name} (*{ch.guild.name}*)")
        else:
            text_desc.append(f"• ID: `{cid}` (Cached/Missing)")
    embed.add_field(name=f"💬 Text Channels ({len(text_ids)})", value="\n".join(text_desc) if text_desc else "None", inline=False)

    thread_desc = []
    for tid in thread_ids:
        th = bot.get_channel(tid)
        if th:
            thread_desc.append(f"• Thread: {th.name} (*{th.guild.name}*)")
        else:
            thread_desc.append(f"• ID: `{tid}` (Cached/Missing)")
    embed.add_field(name=f"🧵 Threads ({len(thread_ids)})", value="\n".join(thread_desc) if thread_desc else "None", inline=False)

    forum_desc = []
    for fid in forum_ids:
        fch = bot.get_channel(fid)
        if fch:
            forum_desc.append(f"• Forum: {fch.name} (*{fch.guild.name}*)")
        else:
            forum_desc.append(f"• ID: `{fid}` (Cached/Missing)")
    embed.add_field(name=f"📌 Forum Channels ({len(forum_ids)})", value="\n".join(forum_desc) if forum_desc else "None", inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="link-relay", description="Link this text channel to a cross-server relay network.")
@app_commands.describe(network_code="The network code to link this text channel to")
async def link_relay(interaction: discord.Interaction, network_code: str):
    channel_id = interaction.channel.id
    code = network_code.strip().lower()

    add_link("text_relays", code, channel_id)
    channels = get_links("text_relays", code)

    await interaction.response.send_message(
        f"✅ Linked **#{interaction.channel.name}** to relay network `{code}`! "
        f"({len(channels)} channels currently connected)",
        ephemeral=False
    )

@bot.tree.command(name="unlink-relay", description="Unlink this text channel from its active relay network.")
async def unlink_relay(interaction: discord.Interaction):
    channel_id = interaction.channel.id
    try:
        res = supabase.table("text_relays").select("network_code").eq("channel_id", channel_id).execute()
        codes = [row["network_code"] for row in res.data]

        if codes:
            for code in codes:
                remove_link("text_relays", code, channel_id)
            await interaction.response.send_message(
                f"🔌 Disconnected **#{interaction.channel.name}** from the text relay network.",
                ephemeral=False
            )
        else:
            await interaction.response.send_message(
                f"⚠ **#{interaction.channel.name}** is not currently linked to any relay network.",
                ephemeral=True
            )
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking: {e}", ephemeral=True)

@bot.tree.command(name="link-forum", description="Link a forum channel to a cross-server forum network.")
@app_commands.describe(
    network_code="The shared network code for this forum bridge",
    forum_channel="Select the forum channel to link (optional, defaults to current channel)"
)
async def link_forum(interaction: discord.Interaction, network_code: str, forum_channel: discord.ForumChannel = None):
    target_forum = forum_channel
    if not target_forum:
        current_channel = interaction.channel
        if isinstance(current_channel, discord.Thread) and isinstance(current_channel.parent, discord.ForumChannel):
            target_forum = current_channel.parent
        elif isinstance(current_channel, discord.ForumChannel):
            target_forum = current_channel

    if not target_forum:
        await interaction.response.send_message(
            "❌ Please select a `forum_channel` in the option, run this inside a forum post, or use it directly in a Forum Channel.", 
            ephemeral=True
        )
        return

    code = network_code.strip().lower()
    add_link("forum_relays", code, target_forum.id)
    forums = get_links("forum_relays", code)
    
    await interaction.response.send_message(
        f"📌 Linked forum **{target_forum.name}** to network `{code}`! ({len(forums)} connected channels)",
        ephemeral=False
    )

@bot.tree.command(name="unlink-forum", description="Disconnect this forum channel from its active network.")
async def unlink_forum(interaction: discord.Interaction):
    current_channel = interaction.channel
    forum_ch = None
    if isinstance(current_channel, discord.Thread) and isinstance(current_channel.parent, discord.ForumChannel):
        forum_ch = current_channel.parent
    elif isinstance(current_channel, discord.ForumChannel):
        forum_ch = current_channel

    if not forum_ch:
        await interaction.response.send_message("❌ Please run this command inside a forum post or channel.", ephemeral=True)
        return

    forum_id = forum_ch.id
    try:
        res = supabase.table("forum_relays").select("network_code").eq("forum_channel_id", forum_id).execute()
        codes = [row["network_code"] for row in res.data]

        if codes:
            for code in codes:
                remove_link("forum_relays", code, forum_id)
            await interaction.response.send_message(f"🔌 Disconnected forum **{forum_ch.name}** from the network.", ephemeral=False)
        else:
            await interaction.response.send_message("⚠️ This forum is not currently linked to any network.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking forum: {e}", ephemeral=True)

@bot.tree.command(name="link-thread", description="Link or create a matching thread across servers using a shared thread code.")
@app_commands.describe(network_code="The unique code for this thread bridge", thread_name="Name of the thread to create if needed")
async def link_thread(interaction: discord.Interaction, network_code: str, thread_name: str = None):
    code = network_code.strip().lower()
    current_channel = interaction.channel

    if isinstance(current_channel, discord.Thread):
        add_link("thread_relays", code, current_channel.id)
        threads = get_links("thread_relays", code)
        await interaction.response.send_message(
            f"🧵 Linked this thread (**{current_channel.name}**) to thread network `{code}`! ({len(threads)} connected)",
            ephemeral=False
        )
    elif isinstance(current_channel, discord.TextChannel):
        if not thread_name:
            await interaction.response.send_message("❌ Please provide a `thread_name` if running this command in a text channel.", ephemeral=True)
            return

        try:
            new_thread = await current_channel.create_thread(name=thread_name, auto_archive_duration=60)
            add_link("thread_relays", code, new_thread.id)
            threads = get_links("thread_relays", code)
            await interaction.response.send_message(
                f"🧵 Created and linked new thread **#{thread_name}** to thread network `{code}`!",
                ephemeral=False
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to create thread: {e}", ephemeral=True)
    else:
        await interaction.response.send_message("❌ This command can only be used in text channels or threads.", ephemeral=True)

@bot.tree.command(name="unlink-thread", description="Disconnect this thread from its active network.")
async def unlink_thread(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message("❌ You must run this command inside the thread you want to unlink.", ephemeral=True)
        return

    thread_id = interaction.channel.id
    try:
        res = supabase.table("thread_relays").select("network_code").eq("thread_id", thread_id).execute()
        codes = [row["network_code"] for row in res.data]

        if codes:
            for code in codes:
                remove_link("thread_relays", code, thread_id)
            await interaction.response.send_message(
                f"🔌 Disconnected thread **{interaction.channel.name}** from the cross-server network.",
                ephemeral=False
            )
        else:
            await interaction.response.send_message("⚠️ This thread is not currently linked to any network.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking thread: {e}", ephemeral=True)

# ------------------------------------------------------------------------------
# VOICE BRIDGE COMMANDS
# ------------------------------------------------------------------------------
def _resolve_voice_channel(interaction: discord.Interaction, chosen):
    if chosen:
        return chosen
    user_voice = getattr(interaction.user, "voice", None)
    if user_voice and isinstance(user_voice.channel, discord.VoiceChannel):
        return user_voice.channel
    return None

@bot.tree.command(name="link-vc", description="Link a voice channel to a cross-server voice call network.")
@app_commands.describe(
    network_code="The shared network code for this voice bridge",
    voice_channel="Voice channel to link (optional, defaults to the one you're in)"
)
async def link_vc(interaction: discord.Interaction, network_code: str, voice_channel: discord.VoiceChannel = None):
    if not VOICE_OK:
        await interaction.response.send_message("❌ Voice bridge dependencies are not installed on this bot.", ephemeral=True)
        return
    if not interaction.guild or not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need the **Manage Channels** permission to link a voice channel.", ephemeral=True)
        return

    target = _resolve_voice_channel(interaction, voice_channel)
    if not target:
        await interaction.response.send_message(
            "❌ Pick a `voice_channel` in the option, or join the voice channel you want to link first.", ephemeral=True
        )
        return

    await interaction.response.defer()
    code = network_code.strip().lower()
    await asyncio.to_thread(add_link, "vc_code_relays", code, target.id)
    linked = await asyncio.to_thread(get_links, "vc_code_relays", code)

    await interaction.followup.send(
        f"🎙️ Linked voice channel **{target.name}** to voice network `{code}`! "
        f"({len(linked)} channels connected)\n"
        f"The bot joins automatically when someone enters a linked channel and leaves when it's empty."
    )

    if humans_in(target) > 0:
        await maybe_join_voice(target)

@bot.tree.command(name="unlink-vc", description="Disconnect a voice channel from its voice call network.")
@app_commands.describe(voice_channel="Voice channel to unlink (optional, defaults to the one you're in)")
async def unlink_vc(interaction: discord.Interaction, voice_channel: discord.VoiceChannel = None):
    if not VOICE_OK:
        await interaction.response.send_message("❌ Voice bridge dependencies are not installed on this bot.", ephemeral=True)
        return
    if not interaction.guild or not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need the **Manage Channels** permission to unlink a voice channel.", ephemeral=True)
        return

    target = _resolve_voice_channel(interaction, voice_channel)
    if not target:
        await interaction.response.send_message(
            "❌ Pick a `voice_channel` in the option, or join the voice channel you want to unlink first.", ephemeral=True
        )
        return

    await interaction.response.defer()
    codes = await get_vc_codes(target.id)
    if not codes:
        await interaction.followup.send(f"⚠️ **{target.name}** is not linked to any voice network.", ephemeral=True)
        return

    for code in codes:
        await asyncio.to_thread(remove_link, "vc_code_relays", code, target.id)

    session = voice_sessions.get(interaction.guild.id)
    if session and session.channel_id == target.id:
        await end_voice_session(interaction.guild.id)

    await interaction.followup.send(f"🔌 Disconnected voice channel **{target.name}** from the voice network.")

@bot.tree.command(name="voice-status", description="Show the live status of the voice bridge in this server.")
async def voice_status(interaction: discord.Interaction):
    if not VOICE_OK:
        await interaction.response.send_message("❌ Voice bridge dependencies are not installed on this bot.", ephemeral=True)
        return

    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  
    lines = [
        f"Active voice sessions (all servers): `{len(voice_sessions)}/{MAX_VOICE_SESSIONS}`",
        f"libopus loaded: `{discord.opus.is_loaded()}`",
        f"Peak memory: `{rss_mb:.0f} MB`",
    ]

    session = voice_sessions.get(interaction.guild.id) if interaction.guild else None
    if session:
        peers = [s for s in voice_sessions.values() if s is not session and (s.codes & session.codes)]
        lines += [
            f"This server: connected to <#{session.channel_id}>, networks `{', '.join(sorted(session.codes))}`",
            f"Other servers on a call with you: `{len(peers)}`",
            f"Audio frames heard here: `{session.frames_in}` · frames played here: `{session.frames_out}`",
            f"Decrypt/decode (all servers): DAVE ok `{voice_stats['dave_ok']}`, DAVE failed `{voice_stats['dave_fail']}`, "
            f"unencrypted `{voice_stats['plain']}`, bad packets skipped `{voice_stats['decode_err']}`",
        ]
        if session.frames_in == 0:
            lines.append("_No audio received yet. If people are talking and this stays 0, receive is failing (check the logs / DAVE note)._")
    else:
        lines.append("This server: not in a call right now (the bot joins when someone enters a linked voice channel).")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)

# ------------------------------------------------------------------------------
# YOUTUBE & WEB SEARCH COMMANDS
# ------------------------------------------------------------------------------
class YouTubeDropdown(discord.ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="Select the correct video from the search...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        selected_url = self.values[0]
        await interaction.response.send_message(
            f"✅ You selected: {selected_url}\n(Click the link to open and watch the video directly!)",
            ephemeral=False
        )

class YouTubeSelectView(discord.ui.View):
    def __init__(self, entries):
        super().__init__(timeout=60)
        options = []
        for entry in entries[:5]:
            title = entry.get('title', 'Unknown Title')[:100]
            uploader = entry.get('uploader', 'Unknown Channel')[:100]
            url = entry.get('webpage_url', '')
            options.append(
                discord.SelectOption(
                    label=title[:100],
                    description=f"By: {uploader}"[:100],
                    value=url
                )
            )
        self.add_item(YouTubeDropdown(options))

@bot.tree.command(name="play", description="Search YouTube or paste a direct YouTube URL.")
@app_commands.describe(search="Search keywords or paste a YouTube URL")
async def play(interaction: discord.Interaction, search: str):
    await interaction.response.defer(ephemeral=True)

    entries = []
    if "youtube.com/watch" in search or "youtu.be/" in search:
        entries.append({
            'title': 'Direct YouTube Link',
            'uploader': 'Provided URL',
            'webpage_url': search.strip()
        })
    else:
        try:
            search_opts = {
                'extract_flat': True,
                'quiet': True,
            }
            query = f"ytsearch5:{search}"
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(search_opts).extract_info(query, download=False))
            entries = data.get('entries', [])
        except Exception as e:
            logger.error(f"yt_dlp search error: {e}")

        if not entries:
            try:
                for item in google_search(f"{search} site:youtube.com/watch", num_results=5):
                    url = item.get("href", "")
                    if "youtube.com/watch" in url:
                        entries.append({
                            'title': item.get("title", 'YouTube Video Result'),
                            'uploader': 'Google Search',
                            'webpage_url': url
                        })
            except Exception as e:
                logger.error(f"Fallback video search error: {e}")

    if not entries:
        await interaction.followup.send(
            f"⚠️ Could not find any videos for `{search}`. Try pasting the direct YouTube URL!", 
            ephemeral=True
        )
        return

    view = YouTubeSelectView(entries)
    await interaction.followup.send("🔍 **Select the correct video below:**", view=view, ephemeral=True)

@bot.tree.command(name="search", description="Perform a fast web search via DuckDuckGo.")
@app_commands.describe(query="What would you like to search for?")
async def search(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: google_search(query, num_results=5))

        if not results:
            await interaction.followup.send(f"⚠ No results found for `{query}`.")
            return

        embed = discord.Embed(
            title=f"🔍 Search Results for: `{query}`",
            color=discord.Color.green()
        )

        for i, item in enumerate(results[:5], 1):
            title = item.get("title", "Result")
            url = item.get("href", "#")
            body = item.get("body", "")[:250]  
            
            field_value = f"[{title}]({url})\n{body}"
            embed.add_field(name=f"Result {i}", value=field_value, inline=False)

        await interaction.followup.send(embed=embed)
    except Exception as e:
        logger.error(f"Search error: {e}")
        await interaction.followup.send(f"❌ An error occurred while performing the search: {e}")

# ------------------------------------------------------------------------------
# MAIN RUNNER
# ------------------------------------------------------------------------------
def main():
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        logger.critical("DISCORD_TOKEN environment variable is missing!")
        sys.exit(1)

    bot.run(token)

if __name__ == "__main__":
    main()
