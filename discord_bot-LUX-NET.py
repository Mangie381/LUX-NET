import os
os.environ.setdefault("MALLOC_ARENA_MAX", "2")   # keeps memory use down on glibc hosts
import sys
import asyncio
import logging
import io
import json
import re
import uuid
import urllib.request
from urllib.parse import urlparse, urljoin
import socket
import ipaddress
import time
import resource
import pickle
import struct
import zipfile
import atexit
import secrets
import shutil
import random
import csv
import math
from collections import deque, OrderedDict
from dataclasses import dataclass
from difflib import SequenceMatcher
from threading import Lock
from datetime import datetime, timezone

import discord
import aiohttp
from discord import app_commands
from discord.ext import commands
from aiohttp import web   # ships with discord.py

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
# yt_dlp and the search client are heavy, so they are imported on first use (saves memory while idle).

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
        invalidate_relay_cache()
    except Exception as e:
        logger.error(f"CRITICAL Supabase insert error in ({table}): {e}")

def remove_link(table: str, code: str, item_id: int):
    try:
        col = _LINK_COLS.get(table, "thread_id")
        supabase.table(table).delete().eq("network_code", code).eq(col, item_id).execute()
        invalidate_relay_cache()
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
def register_message_mappings(source_msg_id: int, source_channel_id: int, copies: list):
    """Store a source message and all of its relayed copies with ONE upsert.
    copies = [(channel_id, message_id), ...]"""
    if not copies:
        return
    try:
        rows = [{"root_message_id": source_msg_id, "channel_id": source_channel_id, "message_id": source_msg_id}]
        rows += [{"root_message_id": source_msg_id, "channel_id": int(c), "message_id": int(m)} for c, m in copies]
        supabase.table("message_mappings").upsert(rows, on_conflict="channel_id,message_id").execute()
    except Exception as e:
        logger.error(f"Failed to register message mapping in Supabase: {e}")

def get_mirror_rows(msg_id: int):
    """Return (root_id, [(channel_id, message_id), ...]) for every message in the same relay group."""
    try:
        res = supabase.table("message_mappings").select("root_message_id").eq("message_id", msg_id).limit(1).execute()
        if not res.data:
            return None, []
        root_id = int(res.data[0]["root_message_id"])
        all_res = supabase.table("message_mappings").select("channel_id,message_id").eq("root_message_id", root_id).execute()
        return root_id, [(int(r["channel_id"]), int(r["message_id"])) for r in all_res.data]
    except Exception as e:
        logger.error(f"Failed to fetch message mapping from Supabase: {e}")
        return None, []

def get_mirrored_targets(msg_id: int):
    """Copies of msg_id elsewhere (excludes msg_id itself and legacy placeholder rows)."""
    _root, rows = get_mirror_rows(msg_id)
    return [(c, m) for c, m in rows if m != msg_id and c != m]

# ------------------------------------------------------------------------------
# GLOBAL BOT BAN HELPERS
# ------------------------------------------------------------------------------
_banned_ids: set = set()

def refresh_ban_cache() -> bool:
    global _banned_ids
    try:
        res = supabase.table("bot_bans").select("user_id").execute()
        _banned_ids = {int(row["user_id"]) for row in res.data}
        return True
    except Exception as e:
        logger.error(f"Failed to refresh bot ban cache: {e}")
        return False

def is_user_banned(user_id: int) -> bool:
    return user_id in _banned_ids

async def ban_cache_loop():
    while True:
        await asyncio.sleep(60)
        await asyncio.to_thread(refresh_ban_cache)

def add_bot_ban(user_id: int):
    try:
        supabase.table("bot_bans").upsert({"user_id": user_id}, on_conflict="user_id").execute()
        _banned_ids.add(user_id)
        logger.info(f"Globally banned user ID {user_id} from using the bot.")
    except Exception as e:
        logger.error(f"Failed to add bot ban for {user_id}: {e}")

def remove_bot_ban(user_id: int):
    try:
        supabase.table("bot_bans").delete().eq("user_id", user_id).execute()
        _banned_ids.discard(user_id)
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
        try:
            from ddgs import DDGS  # new package name
        except ImportError:
            from duckduckgo_search import DDGS  # legacy package name
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
# GEMINI AI OVERVIEW
# ------------------------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")


def gemini_overview(query: str):
    if not GEMINI_API_KEY:
        return None
    payload = {
        "contents": [{"parts": [{"text": (
            "Give a short, factual overview answering this web search, like a search engine's AI overview: "
            "2 to 4 sentences, or up to 4 short bullet points if it is a list. "
            "Use Discord markdown only (**bold**, - bullets). No headings, no preamble, "
            "no links, and keep it under 120 words.\n\nSearch: " + query
        )}]}],
        "tools": [{"google_search": {}}],
    }
    req = urllib.request.Request(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        parts = data["candidates"][0]["content"]["parts"]
        text = "".join(part.get("text", "") for part in parts).strip()
        return text or None
    except Exception as e:
        logger.error(f"Gemini overview error: {e}")
        return None

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

@bot.tree.interaction_check
async def global_interaction_check(interaction: discord.Interaction) -> bool:
    if is_user_banned(interaction.user.id):
        await interaction.response.send_message("❌ You are globally banned from using this bot.", ephemeral=True)
        return False
    return True

# ------------------------------------------------------------------------------
# WEBHOOK HELPER FUNCTIONS
# ------------------------------------------------------------------------------
_webhook_cache: dict = {}
_webhook_locks: dict = {}
_bg_tasks: set = set()


def spawn(coro):
    """Fire-and-forget a coroutine while keeping a reference so it isn't garbage-collected."""
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


def _webhook_root(channel):
    root = channel.parent if isinstance(channel, discord.Thread) else channel
    return root if isinstance(root, (discord.TextChannel, discord.ForumChannel)) else None


async def get_or_create_webhook(channel: discord.abc.GuildChannel, refresh: bool = False) -> discord.Webhook | None:
    """Return the bot's webhook for a channel. Cached, so a relayed message no longer
    costs an extra Discord API call per target."""
    root = _webhook_root(channel)
    if root is None:
        return None
    if not refresh:
        cached = _webhook_cache.get(root.id)
        if cached is not None:
            return cached
    lock = _webhook_locks.setdefault(root.id, asyncio.Lock())
    async with lock:
        if not refresh and root.id in _webhook_cache:
            return _webhook_cache[root.id]
        try:
            for wh in await root.webhooks():
                if wh.user and wh.user.id == bot.user.id and wh.token:
                    _webhook_cache[root.id] = wh
                    return wh
            wh = await root.create_webhook(name="LUX-NET Relay Bridge")
            _webhook_cache[root.id] = wh
            return wh
        except discord.Forbidden:
            logger.error(f"Missing 'Manage Webhooks' permission in #{root.name}")
        except Exception as e:
            logger.error(f"Failed to get/create webhook in #{root.name}: {e}")
        return None


def forget_webhook(channel):
    root = _webhook_root(channel)
    if root is not None:
        _webhook_cache.pop(root.id, None)


# ------------------------------------------------------------------------------
# RELAY ENGINE (cached lookups, concurrent sends, safe attachment handling)
# ------------------------------------------------------------------------------
# Largest file the bot will download to re-upload (Discord's biggest server limit is 100 MB). Lower it if the
# host runs out of memory. Files above this, or above a target server's own limit, are sent as a link instead.
RELAY_MAX_FILE_MB = int(os.environ.get("RELAY_MAX_FILE_MB", "100"))
_big_download_sem = asyncio.Semaphore(2)     # at most 2 large downloads in memory at once
RELAY_SYNC_EDITS = os.environ.get("RELAY_SYNC_EDITS", "1") == "1"
RELAY_SYNC_DELETES = os.environ.get("RELAY_SYNC_DELETES", "1") == "1"
RELAY_CACHE_TTL = int(os.environ.get("RELAY_CACHE_TTL", "120"))

_target_cache: dict = {}          # (table, id) -> (expires_at, tuple_of_ids)
_thread_pairs: dict = {}          # forum thread id -> set of linked thread ids (trimmed when it grows)
_chunked_sources: OrderedDict = OrderedDict()   # source ids that were split into several messages
_reply_prefixes: OrderedDict = OrderedDict()    # source id -> reply prefix, so edits keep it


def _remember(od: OrderedDict, key, value=True, cap: int = 3000):
    od[key] = value
    od.move_to_end(key)
    while len(od) > cap:
        od.popitem(last=False)


def invalidate_relay_cache():
    _target_cache.clear()


def _query_targets(table: str, item_id: int) -> list:
    col = _LINK_COLS[table]
    res = supabase.table(table).select("network_code").eq(col, item_id).execute()
    codes = list({row["network_code"] for row in res.data})
    if not codes:
        return []
    r = supabase.table(table).select(col).in_("network_code", codes).neq(col, item_id).execute()
    return list({int(x[col]) for x in r.data})


def cached_targets(table: str, item_id: int) -> list:
    key = (table, item_id)
    now = time.monotonic()
    hit = _target_cache.get(key)
    if hit and hit[0] > now:
        return list(hit[1])
    try:
        ids = _query_targets(table, item_id)
    except Exception:
        if hit:                      # database hiccup: fall back to the stale answer
            return list(hit[1])
        raise
    _target_cache[key] = (now + RELAY_CACHE_TTL, tuple(ids))
    return ids


async def get_targets(table: str, item_id: int) -> list:
    hit = _target_cache.get((table, item_id))
    if hit and hit[0] > time.monotonic():
        return list(hit[1])          # fast path, no thread hop, no network
    return await asyncio.to_thread(cached_targets, table, item_id)


def relay_targets_for(channel) -> list:
    table = "thread_relays" if isinstance(channel, discord.Thread) else "text_relays"
    return cached_targets(table, channel.id)


def forum_targets_for(forum_id: int) -> list:
    return cached_targets("forum_relays", forum_id)


async def resolve_channel(cid: int):
    ch = bot.get_channel(cid)
    if ch is not None:
        return ch
    try:
        return await bot.fetch_channel(cid)
    except Exception:
        return None


async def relay_channels_for(channel) -> list:
    """Every channel/thread that mirrors `channel` (empty list = channel isn't relayed)."""
    if not supabase:
        return []
    out = []
    if isinstance(channel, discord.Thread) and isinstance(channel.parent, discord.ForumChannel):
        for tid in _thread_pairs.get(channel.id, ()):
            t = bot.get_channel(tid)
            if t is not None:
                out.append(t)
        forum_ids = await get_targets("forum_relays", channel.parent.id)
        for fid in forum_ids:
            forum = bot.get_channel(fid)
            if isinstance(forum, discord.ForumChannel):
                name = channel.name.lower()
                out.extend(t for t in forum.threads if t.name.lower() == name)
    elif isinstance(channel, discord.Thread):
        for tid in await get_targets("thread_relays", channel.id):
            t = await resolve_channel(tid)
            if isinstance(t, discord.Thread):
                out.append(t)
    elif isinstance(channel, discord.TextChannel):
        for cid in await get_targets("text_relays", channel.id):
            c = bot.get_channel(cid)
            if isinstance(c, discord.TextChannel):
                out.append(c)
    seen, uniq = set(), []
    for c in out:
        if c.id not in seen and c.id != channel.id:
            seen.add(c.id)
            uniq.append(c)
    return uniq


_BAD_WEBHOOK_WORDS = (("discord", "disc\u043erd"), ("clyde", "cl\u0443de"))


def safe_webhook_name(name: str) -> str:
    """Discord rejects webhook usernames containing 'discord' or 'clyde'. Swap one letter for a
    look-alike so users/servers with those words in their names can still relay."""
    for word, repl in _BAD_WEBHOOK_WORDS:
        name = re.sub(word, lambda m, r=repl: r if m.group(0).islower() else r.capitalize(), name, flags=re.IGNORECASE)
    name = name.strip()[:80]
    return name if len(name) >= 1 else "User"


def split_text(text: str, limit: int = 2000) -> list:
    text = text or ""
    if len(text) <= limit:
        return [text] if text else []
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = text.rfind(" ", 0, limit)
        if cut < limit // 2:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        parts.append(text)
    return parts


@dataclass
class RelayAttachment:
    filename: str
    url: str
    size: int
    data: bytes | None = None
    description: str | None = None


@dataclass
class RelayPayload:
    username: str
    avatar_url: str
    content: str
    embeds: list
    attachments: list
    source_id: int
    source_channel_id: int


async def _download_attachment(att: discord.Attachment, limit_bytes: int) -> RelayAttachment:
    ra = RelayAttachment(filename=att.filename, url=att.url, size=att.size, description=att.description)
    if att.size <= limit_bytes:
        try:
            if att.size > 20 * 1024 * 1024:
                async with _big_download_sem:
                    ra.data = await att.read()
            else:
                ra.data = await att.read()
        except Exception as e:
            logger.warning(f"Could not download attachment {att.filename}: {e}")
    else:
        logger.info(f"{att.filename} ({att.size // 1048576} MB) is above every target server's upload limit; relaying as a link")
    return ra


def _fmt_size(n: int) -> str:
    return f"{n / 1048576:.1f} MB" if n >= 1048576 else f"{max(1, n // 1024)} KB"


def _link_line(a) -> str:
    return f"\U0001F4CE **{a.filename}** ({_fmt_size(a.size)}):\n{a.url}"


async def build_relay_payload(message: discord.Message, *, with_reply: bool = True, extra_text: str = "", targets: list = None) -> RelayPayload:
    content = message.content or ""
    embeds = list(message.embeds)
    atts = list(message.attachments)
    stickers = list(message.stickers)

    snaps = getattr(message, "message_snapshots", None) or []     # forwarded messages
    if snaps and not (content or atts or embeds):
        snap = snaps[0]
        content = "\u21AA\uFE0F **Forwarded**\n" + (getattr(snap, "content", "") or "")
        embeds = list(getattr(snap, "embeds", []) or [])
        atts = list(getattr(snap, "attachments", []) or [])
        stickers = list(getattr(snap, "stickers", []) or [])

    # Link previews (video / gifv / image / article) are regenerated by Discord from the URL in the
    # text; copying them as embeds duplicates previews, so only real rich embeds are forwarded.
    rich = [e for e in embeds if e.type == "rich"][:10]

    for st in stickers:
        if str(getattr(st, "format", "")).lower().endswith("lottie"):
            content += f"\n*[Sticker: {st.name}]*"
        else:
            content += f"\n{st.url}"

    prefix = ""
    if with_reply and message.reference:
        ref = message.reference.resolved if isinstance(message.reference.resolved, discord.Message) else None
        if ref is None and message.reference.message_id:
            try:
                ref = await message.channel.fetch_message(message.reference.message_id)
            except Exception:
                ref = None
        if ref is not None:
            snippet = ref.content[:60] + "..." if len(ref.content) > 60 else ref.content
            if not snippet and ref.attachments:
                snippet = "[Attachment]"
            prefix = f"> \u21A9 **Replying to {ref.author.display_name}:** *{snippet or '[Embed/Sticker]'}*\n"
    if prefix:
        _remember(_reply_prefixes, message.id, prefix)

    # Download anything that at least one target server can accept (that server's own limit decides, not the
    # uploader's: a Nitro user's 200 MB video still can't be uploaded into a server limited to 10 MB).
    target_limits = [t.guild.filesize_limit for t in (targets or []) if getattr(t, "guild", None)]
    limit_bytes = min(RELAY_MAX_FILE_MB * 1024 * 1024, max(target_limits)) if target_limits else RELAY_MAX_FILE_MB * 1024 * 1024
    attachments = await asyncio.gather(*(_download_attachment(a, limit_bytes) for a in atts[:10]))

    guild_name = message.guild.name if message.guild else "Unknown"
    username = safe_webhook_name(f"{message.author.display_name} [{guild_name}]")
    text = (prefix + content + extra_text).strip("\n") if (prefix or content or extra_text) else ""
    return RelayPayload(
        username=username,
        avatar_url=message.author.display_avatar.url,
        content=text,
        embeds=rich,
        attachments=list(attachments),
        source_id=message.id,
        source_channel_id=message.channel.id,
    )


async def webhook_send(channel, *, files_factory=None, thread=None, **kwargs):
    """Send through the cached webhook. Fresh File objects are built for every attempt (a File can
    only be uploaded once, which is what used to break video/image relays on all but the first server)."""
    last_err = None
    for attempt in (0, 1):
        wh = await get_or_create_webhook(channel, refresh=bool(attempt))
        if wh is None:
            return None
        kw = dict(kwargs)
        if files_factory:
            files = files_factory()
            if files:
                kw["files"] = files
        t = thread if thread is not None else _thread_kw(channel).get("thread")
        if t is not None:
            kw["thread"] = t
        try:
            return await wh.send(wait=True, **kw)
        except discord.NotFound as e:           # webhook (or thread) was deleted: rebuild once
            last_err = e
            forget_webhook(channel)
            continue
        except discord.HTTPException:
            raise
    if last_err:
        raise last_err
    return None


async def relay_to_target(ch, p: RelayPayload, *, thread_name: str | None = None):
    """Relay one payload to one channel/thread. Returns the first sent message or None."""
    limit = ch.guild.filesize_limit if getattr(ch, "guild", None) else 10 * 1024 * 1024
    inline, total = [], 0
    for a in p.attachments:          # pack files into what this server accepts; the rest go as links
        if a.data is not None and a.size <= limit and total + a.size <= limit and len(inline) < 10:
            inline.append(a)
            total += a.size
    inline_ids = {id(a) for a in inline}
    links = [_link_line(a) for a in p.attachments if id(a) not in inline_ids]
    no_mentions = discord.AllowedMentions.none()

    def build(use_files: bool):
        extra = list(links) + ([] if use_files else [_link_line(a) for a in inline])
        text = (p.content + ("\n" if p.content and extra else "") + "\n".join(extra)).strip()
        return split_text(text)

    def make_files():
        return [discord.File(io.BytesIO(a.data), filename=a.filename, description=a.description) for a in inline]

    async def run(use_files: bool):
        chunks = build(use_files)
        first = None
        thread_obj = None
        for i, chunk in enumerate(chunks or [""]):
            kw = dict(username=p.username, avatar_url=p.avatar_url, allowed_mentions=no_mentions)
            if chunk:
                kw["content"] = chunk
            if i == 0:
                if p.embeds:
                    kw["embeds"] = p.embeds
                if thread_name:
                    kw["thread_name"] = thread_name[:100]
                factory = make_files if (use_files and inline) else None
                if not chunk and not p.embeds and not factory:
                    return None
            else:
                factory = None
            sent = await webhook_send(ch, files_factory=factory, thread=thread_obj, **kw)
            if sent is None:
                return first
            if first is None:
                first = sent
                if thread_name:
                    thread_obj = sent.channel if isinstance(sent.channel, discord.Thread) else None
                    if thread_obj is None and getattr(sent, "thread", None) is not None:
                        thread_obj = sent.thread
        if len(chunks) > 1:
            _remember(_chunked_sources, p.source_id)
        return first

    try:
        return await run(True)
    except discord.HTTPException as e:
        if inline:
            logger.warning(f"Relay with files failed in {ch.id} ({e.status}/{e.code}); retrying with links")
            try:
                return await run(False)
            except Exception as e2:
                logger.warning(f"Relay retry failed in {ch.id}: {e2}")
                return None
        logger.warning(f"Relay failed in {ch.id}: {e}")
    except Exception as e:
        logger.warning(f"Relay failed in {ch.id}: {e}")
    return None


async def relay_payload(p: RelayPayload, targets: list) -> list:
    """Send to every target at once. Returns [(target_channel, first_message), ...] for successes."""
    results = await asyncio.gather(*(relay_to_target(t, p) for t in targets), return_exceptions=True)
    out = []
    for t, r in zip(targets, results):
        if isinstance(r, discord.Message) or (r is not None and not isinstance(r, Exception)):
            out.append((t, r))
    return out


def schedule_mapping(source_id: int, source_channel_id: int, sent: list):
    pairs = [(t.id, m.id) for t, m in sent]
    spawn(asyncio.to_thread(register_message_mappings, source_id, source_channel_id, pairs))


# ------------------------------------------------------------------------------
# STAR RATING POLLS
# ------------------------------------------------------------------------------
_LEGACY_DEFAULT_PROMPT = "So go on and react to television programs by reacting one of the five ratings mentioned!"
RATING_TEXT = {
    1: ("1 star", "the worst rating"),
    2: ("2 stars", "a bad rating"),
    3: ("3 stars", "a neutral rating"),
    4: ("4 stars", "a good rating"),
    5: ("5 stars", "the best rating"),
}
RATING_STYLES = {
    1: discord.ButtonStyle.secondary,
    2: discord.ButtonStyle.secondary,
    3: discord.ButtonStyle.primary,
    4: discord.ButtonStyle.primary,
    5: discord.ButtonStyle.success,
}
_rating_meta: dict = {}


def _parse_ts(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        m = re.match(r"^(\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d)(?:\.(\d+))?(.*)$", value)
        if not m:
            return datetime.now(timezone.utc)
        frac = (m.group(2) or "0")[:6].ljust(6, "0")
        tz = (m.group(3) or "+00:00").replace("Z", "+00:00")
        return datetime.fromisoformat(f"{m.group(1)}.{frac}{tz}")


def build_rating_embed(title: str, prompt: str, counts: dict, created_at: datetime) -> discord.Embed:
    total = sum(counts.get(n, 0) for n in range(1, 6))
    average = (sum(n * counts.get(n, 0) for n in range(1, 6)) / total) if total else 0.0

    lines = []
    for n in range(1, 6):
        name, blurb = RATING_TEXT[n]
        lines.append(f"- **{name}**, {blurb} ({counts.get(n, 0)} votes)")

    description = (
        "\n".join(lines)
        + f"\n\n\u2b50 **Total Votes:** {total} | \U0001F4CA **Average Rating:** {average:.1f} / 5.0"
    )
    prompt = (prompt or "").strip()
    if prompt and prompt != _LEGACY_DEFAULT_PROMPT:
        description += f"\n\n{prompt}"
    return discord.Embed(
        title=f"\u2b50 {title}"[:256],
        description=description[:4000],
        color=discord.Color.gold(),
        timestamp=created_at,
    )


class RatingButton(discord.ui.DynamicItem[discord.ui.Button], template=r"lux_rate:(?P<poll>[0-9a-f]{8,32}):(?P<n>[1-5])"):
    def __init__(self, poll_id: str, n: int):
        super().__init__(
            discord.ui.Button(
                style=RATING_STYLES[n],
                label=f"{n} \u2b50",
                custom_id=f"lux_rate:{poll_id}:{n}",
            )
        )
        self.poll_id = poll_id
        self.n = n

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match: re.Match, /):
        return cls(match["poll"], int(match["n"]))

    async def callback(self, interaction: discord.Interaction):
        if is_user_banned(interaction.user.id):
            await interaction.response.send_message("\u274c You are globally banned from using this bot.", ephemeral=True)
            return

        await interaction.response.defer()
        try:
            result = await asyncio.to_thread(rating_vote, self.poll_id, interaction.user.id, self.n)
        except Exception as e:
            logger.error(f"Rating vote failed for poll {self.poll_id}: {e}")
            await interaction.followup.send("\u274c Couldn't record your vote, please try again.", ephemeral=True)
            return

        if result is None:
            await interaction.followup.send("\u274c This rating poll no longer exists.", ephemeral=True)
            return

        meta, counts, my_vote, targets = result
        embed = build_rating_embed(meta["title"], meta["prompt"], counts, meta["created_at"])
        try:
            await interaction.edit_original_response(embed=embed)
        except Exception as e:
            logger.warning(f"Could not update clicked rating poll message: {e}")

        if my_vote is None:
            note = "\u21a9\ufe0f Your rating was removed."
        else:
            note = f"\u2705 You rated this **{my_vote} \u2b50**."
        await interaction.followup.send(note, ephemeral=True)

        clicked_id = interaction.message.id if interaction.message else None
        asyncio.create_task(sync_rating_copies(embed, targets, exclude_message_id=clicked_id))


def build_rating_view(poll_id: str) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    for n in range(1, 6):
        view.add_item(RatingButton(poll_id, n))
    return view


async def _setup_hook():
    bot.add_dynamic_items(RatingButton)

bot.setup_hook = _setup_hook


def rating_db_create(poll_id: str, title: str, prompt: str, created_at: datetime, author_id: int) -> bool:
    try:
        supabase.table("rating_polls").insert({
            "poll_id": poll_id,
            "title": title,
            "prompt": prompt,
            "created_by": author_id,
            "created_at": created_at.isoformat(),
        }).execute()
        _rating_meta[poll_id] = {"title": title, "prompt": prompt, "created_at": created_at}
        return True
    except Exception as e:
        logger.error(f"Failed to create rating poll: {e}")
        return False


def rating_db_add_message(poll_id: str, channel_id: int, message_id: int):
    try:
        supabase.table("rating_poll_messages").insert(
            {"poll_id": poll_id, "channel_id": channel_id, "message_id": message_id}
        ).execute()
    except Exception as e:
        logger.error(f"Failed to store rating poll message: {e}")


def _rating_get_meta(poll_id: str):
    meta = _rating_meta.get(poll_id)
    if meta:
        return meta
    res = supabase.table("rating_polls").select("title,prompt,created_at").eq("poll_id", poll_id).limit(1).execute()
    if not res.data:
        return None
    row = res.data[0]
    meta = {"title": row["title"], "prompt": row["prompt"], "created_at": _parse_ts(row["created_at"])}
    _rating_meta[poll_id] = meta
    return meta


def _rating_counts(poll_id: str) -> dict:
    counts = {n: 0 for n in range(1, 6)}
    try:
        res = supabase.rpc("rating_counts", {"p_poll_id": poll_id}).execute()
        for row in res.data:
            counts[int(row["star"])] = int(row["votes"])
        return counts
    except Exception:
        counts = {n: 0 for n in range(1, 6)}
        res = supabase.table("rating_votes").select("rating").eq("poll_id", poll_id).limit(100000).execute()
        for row in res.data:
            counts[int(row["rating"])] += 1
        return counts


def rating_vote(poll_id: str, user_id: int, rating: int):
    meta = _rating_get_meta(poll_id)
    if meta is None:
        return None

    existing = supabase.table("rating_votes").select("rating").eq("poll_id", poll_id).eq("user_id", user_id).limit(1).execute()
    if existing.data and int(existing.data[0]["rating"]) == rating:
        supabase.table("rating_votes").delete().eq("poll_id", poll_id).eq("user_id", user_id).execute()
        my_vote = None
    else:
        supabase.table("rating_votes").upsert(
            {"poll_id": poll_id, "user_id": user_id, "rating": rating},
            on_conflict="poll_id,user_id",
        ).execute()
        my_vote = rating

    counts = _rating_counts(poll_id)
    msgs = supabase.table("rating_poll_messages").select("channel_id,message_id").eq("poll_id", poll_id).execute()
    targets = [(int(r["channel_id"]), int(r["message_id"])) for r in msgs.data]
    return meta, counts, my_vote, targets


def _thread_kw(channel) -> dict:
    return {"thread": channel} if isinstance(channel, discord.Thread) else {}


async def sync_rating_copies(embed: discord.Embed, targets: list, exclude_message_id: int | None = None):
    async def one(channel_id: int, message_id: int):
        if message_id == exclude_message_id:
            return
        channel = bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(channel_id)
            except Exception:
                return
        webhook = await get_or_create_webhook(channel)
        if webhook:
            try:
                await webhook.edit_message(message_id, embed=embed, **_thread_kw(channel))
                return
            except Exception:
                pass
        try:
            await channel.get_partial_message(message_id).edit(embed=embed)
        except Exception as e:
            logger.warning(f"Could not sync rating poll copy {message_id} in {channel_id}: {e}")

    await asyncio.gather(*(one(c, m) for c, m in targets))


async def post_rating_copy(channel, embed: discord.Embed, poll_id: str, username: str, avatar_url: str, reply_to_id: int = None):
    if reply_to_id:
        try:
            return await channel.send(
                embed=embed,
                view=build_rating_view(poll_id),
                reference=channel.get_partial_message(reply_to_id),
                mention_author=False,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except Exception as e:
            logger.warning(f"Could not reply to first message {reply_to_id} in {channel.id}: {e}")
    webhook = await get_or_create_webhook(channel)
    if webhook:
        try:
            return await webhook.send(
                username=username,
                avatar_url=avatar_url,
                embed=embed,
                view=build_rating_view(poll_id),
                allowed_mentions=discord.AllowedMentions.none(),
                wait=True,
                **_thread_kw(channel),
            )
        except Exception as e:
            logger.warning(f"Webhook poll post failed in {channel.id}: {e}")
    try:
        return await channel.send(embed=embed, view=build_rating_view(poll_id))
    except Exception as e:
        logger.error(f"Could not post rating poll in {channel.id}: {e}")
        return None


_auto_poll_forums: set = set()


def refresh_auto_poll_cache() -> bool:
    global _auto_poll_forums
    try:
        res = supabase.table("auto_poll_forums").select("forum_channel_id").execute()
        _auto_poll_forums = {int(row["forum_channel_id"]) for row in res.data}
        return True
    except Exception as e:
        logger.error(f"Failed to load auto poll forums: {e}")
        return False


def set_auto_poll(forum_id: int, enabled: bool) -> bool:
    try:
        if enabled:
            supabase.table("auto_poll_forums").upsert(
                {"forum_channel_id": forum_id}, on_conflict="forum_channel_id"
            ).execute()
            _auto_poll_forums.add(forum_id)
        else:
            supabase.table("auto_poll_forums").delete().eq("forum_channel_id", forum_id).execute()
            _auto_poll_forums.discard(forum_id)
        return True
    except Exception as e:
        logger.error(f"Failed to update auto poll for forum {forum_id}: {e}")
        return False


async def post_auto_poll(thread: discord.Thread, starter_message: discord.Message, copies: list):
    try:
        poll_id = uuid.uuid4().hex[:12]
        title = thread.name.strip()[:200] or "Rating"
        created_at = datetime.now(timezone.utc)

        created = await asyncio.to_thread(rating_db_create, poll_id, title, "", created_at, starter_message.author.id)
        if not created:
            return

        embed = build_rating_embed(title, "", {}, created_at)
        username = f"{starter_message.author.display_name} [{thread.guild.name}] (Poll)"[:80]
        avatar_url = starter_message.author.display_avatar.url

        seen = set()
        for channel in [thread] + list(copies):
            if channel.id in seen:
                continue
            seen.add(channel.id)
            sent = await post_rating_copy(channel, embed, poll_id, username, avatar_url, reply_to_id=channel.id)
            if sent:
                await asyncio.to_thread(rating_db_add_message, poll_id, channel.id, sent.id)
    except Exception as e:
        logger.error(f"Auto poll failed for thread {thread.id}: {e}")

# ------------------------------------------------------------------------------
# BOT EVENTS
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
        logger.warning("libopus not found - voice bridge disabled.")
    asyncio.create_task(keepalive_loop())
    asyncio.create_task(ban_cache_loop())
    asyncio.create_task(emu_reaper_loop())
    asyncio.create_task(emu_startup())
    asyncio.create_task(radio_warm())

    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync global slash commands: {e}")

    if os.environ.get("SKIP_GUILD_CLEANUP", "0") != "1":
        spawn(_cleanup_guild_commands())


async def _cleanup_guild_commands():
    """Remove leftover per-guild commands. Runs in the background (a few at a time) so it no longer
    delays startup or the global command sync."""
    sem = asyncio.Semaphore(3)

    async def one(guild):
        async with sem:
            try:
                bot.tree.clear_commands(guild=guild)
                await bot.tree.sync(guild=guild)
            except Exception as e:
                logger.error(f"Could not clear guild commands for {guild.name}: {e}")

    await asyncio.gather(*(one(g) for g in list(bot.guilds)))


async def _mirror_copies(msg_id: int):
    return await asyncio.to_thread(get_mirrored_targets, msg_id)


async def _react_copy(cid: int, mid: int, emoji, add: bool, source_still_has: bool = False):
    ch = await resolve_channel(cid)
    if ch is None or isinstance(ch, discord.ForumChannel) or not hasattr(ch, "get_partial_message"):
        return
    try:
        pm = ch.get_partial_message(mid)
        if add:
            await pm.add_reaction(emoji)
        elif not source_still_has:
            await pm.remove_reaction(emoji, bot.user)
    except discord.HTTPException:
        pass
    except Exception as e:
        logger.error(f"Failed to mirror reaction: {e}")


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if not payload.guild_id or payload.user_id == bot.user.id or is_user_banned(payload.user_id):
        return
    if payload.member and payload.member.bot:
        return
    ch = bot.get_channel(payload.channel_id)
    if ch is None or not supabase or not await relay_channels_for(ch):
        return
    copies = await _mirror_copies(payload.message_id)
    if copies:
        await asyncio.gather(*(_react_copy(c, m, payload.emoji, True) for c, m in copies))


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if not payload.guild_id or payload.user_id == bot.user.id or is_user_banned(payload.user_id):
        return
    ch = bot.get_channel(payload.channel_id)
    if ch is None or not supabase or not await relay_channels_for(ch):
        return
    copies = await _mirror_copies(payload.message_id)
    if not copies:
        return
    still = False
    try:
        msg = await ch.fetch_message(payload.message_id)
        still = any(str(r.emoji) == str(payload.emoji) and r.count > 0 for r in msg.reactions)
    except Exception:
        return
    if not still:
        await asyncio.gather(*(_react_copy(c, m, payload.emoji, False, False) for c, m in copies))


async def _edit_copy(cid: int, mid: int, content: str):
    ch = await resolve_channel(cid)
    if ch is None:
        return
    wh = await get_or_create_webhook(ch)
    if wh is None:
        return
    try:
        await wh.edit_message(mid, content=content, allowed_mentions=discord.AllowedMentions.none(), **_thread_kw(ch))
    except Exception as e:
        logger.warning(f"Could not sync edit to {cid}/{mid}: {e}")


async def _delete_copy(cid: int, mid: int):
    ch = await resolve_channel(cid)
    if ch is None:
        return
    wh = await get_or_create_webhook(ch)
    if wh is None:
        return
    try:
        await wh.delete_message(mid, **_thread_kw(ch))
    except discord.NotFound:
        pass
    except Exception as e:
        logger.warning(f"Could not sync delete to {cid}/{mid}: {e}")


@bot.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    if not RELAY_SYNC_EDITS or not payload.guild_id or not supabase:
        return
    data = payload.data or {}
    if "content" not in data or data.get("webhook_id") or (data.get("author") or {}).get("bot"):
        return
    new_content = data["content"] or ""
    cached = payload.cached_message
    if cached is not None and cached.content == new_content:
        return                                           # embed/link-preview refresh, not a real edit
    if payload.message_id in _chunked_sources or len(new_content) > 2000:
        return
    ch = bot.get_channel(payload.channel_id)
    if ch is None or not await relay_channels_for(ch):
        return
    root, rows = await asyncio.to_thread(get_mirror_rows, payload.message_id)
    if root != payload.message_id:
        return                                           # only the original drives edits
    text = (_reply_prefixes.get(payload.message_id, "") + new_content).strip("\n")[:2000]
    copies = [(c, m) for c, m in rows if m != root and c != m]
    if text and copies:
        await asyncio.gather(*(_edit_copy(c, m, text) for c, m in copies))


@bot.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    if not RELAY_SYNC_DELETES or not payload.guild_id or not supabase:
        return
    ch = bot.get_channel(payload.channel_id)
    if ch is None or not await relay_channels_for(ch):
        return
    root, rows = await asyncio.to_thread(get_mirror_rows, payload.message_id)
    if root != payload.message_id:
        return                                           # deleting a copy never cascades
    copies = [(c, m) for c, m in rows if m != root and c != m]
    if copies:
        await asyncio.gather(*(_delete_copy(c, m) for c, m in copies))

async def _relay_new_forum_post(thread: discord.Thread):
    copies = []
    if not isinstance(thread.parent, discord.ForumChannel) or thread.id in RELAYED_THREAD_IDS or not supabase:
        return None, copies

    starter_message = None
    for delay in (0, 0.5, 1.0):          # starter message usually exists immediately; don't always wait
        if delay:
            await asyncio.sleep(delay)
        try:
            async for msg in thread.history(limit=1, oldest_first=True):
                starter_message = msg
                break
        except Exception:
            pass
        if starter_message:
            break
    if not starter_message:
        return None, copies

    if is_user_banned(starter_message.author.id):
        return None, copies
    if starter_message.webhook_id or starter_message.author.bot:   # our own relayed copy
        RELAYED_THREAD_IDS.add(thread.id)
        return None, copies

    try:
        target_forum_ids = await get_targets("forum_relays", thread.parent.id)
    except Exception as e:
        logger.error(f"Database error in on_thread_create: {e}")
        return starter_message, copies
    targets = [f for f in (bot.get_channel(i) for i in target_forum_ids) if isinstance(f, discord.ForumChannel)]
    if not targets:
        return starter_message, copies

    try:
        payload = await build_relay_payload(starter_message, with_reply=False, targets=targets)
        if not payload.content and not payload.embeds and not payload.attachments:
            payload.content = f"*[Forum Post: {thread.name}]*"
        results = await asyncio.gather(*(relay_to_target(f, payload, thread_name=thread.name) for f in targets),
                                       return_exceptions=True)
        sent = []
        for _forum, r in zip(targets, results):
            if not r or isinstance(r, Exception):
                continue
            t = r.channel if isinstance(r.channel, discord.Thread) else getattr(r, "thread", None)
            if t is None:
                continue
            RELAYED_THREAD_IDS.add(t.id)
            copies.append(t)
            sent.append((t, r))
        if len(_thread_pairs) > 5000:
            for k in list(_thread_pairs)[:2500]:
                _thread_pairs.pop(k, None)
        group = [thread] + copies
        for x in group:
            for y in group:
                if x.id != y.id:
                    _thread_pairs.setdefault(x.id, set()).add(y.id)
        if sent:
            schedule_mapping(starter_message.id, thread.id, sent)
    except Exception as e:
        logger.error(f"Failed to relay new forum post: {e}")
    return starter_message, copies


@bot.event
async def on_thread_create(thread: discord.Thread):
    if not isinstance(thread.parent, discord.ForumChannel):
        return
    if thread.id in RELAYED_THREAD_IDS:
        return

    starter_message, copies = await _relay_new_forum_post(thread)

    if starter_message is not None and thread.parent.id in _auto_poll_forums:
        await post_auto_poll(thread, starter_message, copies)

# ------------------------------------------------------------------------------
# VOICE BRIDGE
# ------------------------------------------------------------------------------
FRAME_BYTES = 3840
SILENCE = b"\x00" * FRAME_BYTES
MAX_BUFFERED_FRAMES = int(os.environ.get("VOICE_MAX_QUEUE_FRAMES", "4"))
PRIME_FRAMES = int(os.environ.get("VOICE_PRIME_FRAMES", "2"))
PRIME_MAX_WAIT = 2
IDLE_FRAMES_BEFORE_STOP = 30
JITTER_MAX_PACKETS = int(os.environ.get("VOICE_JITTER_MAX", "4"))
JITTER_PREF_PACKETS = int(os.environ.get("VOICE_JITTER_PREF", "1"))
OPUS_APPLICATION = os.environ.get("VOICE_OPUS_APPLICATION", "audio")
OPUS_BITRATE_KBPS = int(os.environ.get("VOICE_BITRATE_KBPS", "96"))
OPUS_EXPECTED_LOSS = float(os.environ.get("VOICE_EXPECTED_LOSS", "0.05"))
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

voice_stats = {"dave_ok": 0, "dave_fail": 0, "plain": 0, "decode_err": 0, "mix_drop": 0, "rebuffer": 0}


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
            return packet, SILENCE

    vr_opus.PacketDecoder._decode_packet = safe_decode

    original_init = vr_opus.PacketDecoder.__init__

    def tuned_init(self, router, ssrc):
        original_init(self, router, ssrc)
        try:
            maxsize = max(2, JITTER_MAX_PACKETS)
            self._buffer = vr_opus.JitterBuffer(
                maxsize=maxsize, prefsize=min(max(0, JITTER_PREF_PACKETS), maxsize), prefill=1
            )
        except Exception as e:
            logger.warning(f"Could not tune jitter buffer: {e!r}")

    vr_opus.PacketDecoder.__init__ = tuned_init


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
            return OPUS_SILENCE

    reader.decryptor.decrypt_rtp = decrypt_rtp


if VOICE_OK:

    class _Speaker:
        __slots__ = ("dq", "primed", "waited")

        def __init__(self):
            self.dq = deque(maxlen=MAX_BUFFERED_FRAMES)
            self.primed = False
            self.waited = 0

    class MixerSource(discord.AudioSource):
        def __init__(self):
            self.buffers: dict = {}
            self.idle = 0
            self.playing = False

        def feed(self, speaker_id: int, pcm: bytes):
            sp = self.buffers.get(speaker_id)
            if sp is None:
                sp = self.buffers[speaker_id] = _Speaker()
            dq = sp.dq
            if len(dq) == dq.maxlen:
                voice_stats["mix_drop"] += 1
            dq.append(pcm)

        def has_audio(self) -> bool:
            return any(sp.dq for sp in list(self.buffers.values()))

        def read(self) -> bytes:
            frames = []
            for sp in list(self.buffers.values()):
                dq = sp.dq
                if not sp.primed:
                    if len(dq) >= PRIME_FRAMES or (dq and sp.waited >= PRIME_MAX_WAIT):
                        sp.primed = True
                    else:
                        if dq:
                            sp.waited += 1
                        continue
                try:
                    frames.append(dq.popleft())
                except IndexError:
                    sp.primed = False
                    sp.waited = 0
                    voice_stats["rebuffer"] += 1

            if not frames:
                self.idle += 1
                if self.idle >= IDLE_FRAMES_BEFORE_STOP and not self.has_audio():
                    self.buffers.clear()
                    self.playing = False
                    return b""
                return SILENCE

            self.idle = 0
            if len(frames) == 1:
                return frames[0]

            gain = 1.0 / math.sqrt(len(frames))
            out = audioop.mul(frames[0], 2, gain)
            for f in frames[1:]:
                out = audioop.add(out, audioop.mul(f, 2, gain), 2)
            return out

        def is_opus(self) -> bool:
            return False

        def cleanup(self):
            pass

    class VoiceSession:
        __slots__ = ("guild_id", "vc", "channel_id", "codes", "mixer",
                     "frames_in", "frames_out", "loop", "_lock", "_gen")

        def __init__(self, guild_id, vc, channel_id, codes, loop):
            self.guild_id = guild_id
            self.vc = vc
            self.channel_id = channel_id
            self.codes = codes
            self.mixer = MixerSource()
            self.frames_in = 0
            self.frames_out = 0
            self.loop = loop
            self._lock = Lock()
            self._gen = 0

        def ensure_playing(self):
            if self.mixer.playing or not self.vc.is_connected():
                return
            with self._lock:
                if self.mixer.playing or not self.vc.is_connected():
                    return
                self.mixer.playing = True
                self.mixer.idle = 0
                self._gen += 1
                gen = self._gen
                try:
                    self.vc.play(
                        self.mixer,
                        after=lambda err, g=gen: self._after_play(err, g),
                        application=OPUS_APPLICATION,
                        bitrate=OPUS_BITRATE_KBPS,
                        fec=True,
                        expected_packet_loss=OPUS_EXPECTED_LOSS,
                        bandwidth="full",
                        signal_type="voice",
                    )
                except Exception as e:
                    self.mixer.playing = False
                    logger.debug(f"Could not start voice playback: {e}")

        def _after_play(self, error, gen):
            if error:
                logger.error(f"Voice playback error: {error}")
            if gen == self._gen:
                self.mixer.playing = False
            if self.mixer.has_audio():
                self.ensure_playing()

    def route_audio(src: "VoiceSession", speaker_id: int, pcm: bytes):
        src.frames_in += 1
        for dst in list(voice_sessions.values()):
            if dst is src or not (dst.codes & src.codes):
                continue
            dst.mixer.feed(speaker_id, pcm)
            dst.frames_out += 1
            if not dst.mixer.playing:
                dst.ensure_playing()

    class RelaySink(voice_recv.AudioSink):
        def __init__(self, session: VoiceSession):
            super().__init__()
            self.session = session

        def wants_opus(self) -> bool:
            return False

        def write(self, user, data):
            if user is None or user.bot or user.id in _banned_ids:
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
        logger.error(f"Error disconnecting voice: {e}")
    session.mixer.buffers.clear()


async def maybe_join_voice(channel: discord.VoiceChannel):
    global _opus_warned
    if not VOICE_OK or not isinstance(channel, discord.VoiceChannel):
        return
    gid = channel.guild.id
    if gid in voice_sessions or gid in _voice_connecting or gid in radio_sessions:
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
    except Exception as e:
        logger.error(f"Failed to join voice channel: {e}")
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
        rs = radio_sessions.get(member.guild.id)
        if rs and after.channel is None:
            rs["gen"] += 1
            radio_sessions.pop(member.guild.id, None)
        session = voice_sessions.get(member.guild.id)
        if session and (after.channel is None or after.channel.id != session.channel_id):
            await end_voice_session(member.guild.id)
        return

    if before.channel and before.channel != after.channel and not member.bot:
        await radio_maybe_leave(before.channel)

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

    if message.channel.id in emu_sessions and await emu_chat_input(message):
        return

    if not supabase:
        return
    if not (message.content or message.attachments or message.embeds or message.stickers
            or getattr(message, "message_snapshots", None)):
        return

    try:
        targets = await relay_channels_for(message.channel)
    except Exception as e:
        logger.error(f"Database query error in on_message: {e}")
        return
    if not targets:
        return

    payload = await build_relay_payload(message, targets=targets)
    sent = await relay_payload(payload, targets)
    if sent:
        schedule_mapping(message.id, message.channel.id, sent)

# ------------------------------------------------------------------------------
# SLASH COMMANDS
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
    await interaction.response.send_message(f"🔨 Successfully banned user ID `{target_id}`.", ephemeral=True)

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

@bot.tree.command(name="poll", description="Post a 1-5 star rating poll and sync it across connected relay channels.")
@app_commands.describe(title="What people are rating", message="Line shown under the ratings (optional)")
async def poll(interaction: discord.Interaction, title: str, message: str = None):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("\u274c This command can only be used in a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    poll_id = uuid.uuid4().hex[:12]
    prompt = (message or "").strip()[:1000]
    clean_title = title.strip()[:200]
    created_at = datetime.now(timezone.utc)

    created = await asyncio.to_thread(rating_db_create, poll_id, clean_title, prompt, created_at, interaction.user.id)
    if not created:
        await interaction.followup.send("\u274c Couldn't create the poll.", ephemeral=True)
        return

    origin = interaction.channel
    target_channels = []
    try:
        if isinstance(origin, discord.Thread) and isinstance(origin.parent, discord.ForumChannel):
            forum_ids = await asyncio.to_thread(forum_targets_for, origin.parent.id)
            for fid in forum_ids:
                forum = bot.get_channel(fid)
                if isinstance(forum, discord.ForumChannel):
                    target_channels.extend(t for t in forum.threads if t.name.lower() == origin.name.lower())
        else:
            target_ids = await asyncio.to_thread(relay_targets_for, origin)
            target_channels = [c for c in (bot.get_channel(i) for i in target_ids) if c is not None]
    except Exception as e:
        logger.error(f"Error looking up poll relay targets: {e}")

    embed = build_rating_embed(clean_title, prompt, {}, created_at)
    username = f"{interaction.user.display_name} [{interaction.guild.name}] (Poll)"[:80]
    avatar_url = interaction.user.display_avatar.url

    posted = 0
    seen = set()
    for channel in [origin] + target_channels:
        if channel.id in seen:
            continue
        seen.add(channel.id)
        sent = await post_rating_copy(channel, embed, poll_id, username, avatar_url)
        if sent:
            await asyncio.to_thread(rating_db_add_message, poll_id, channel.id, sent.id)
            posted += 1

    if posted:
        await interaction.followup.send(f"\u2705 Rating poll posted in {posted} channel(s).", ephemeral=True)
    else:
        await interaction.followup.send("\u274c I couldn't post the poll here.", ephemeral=True)

@bot.tree.command(name="auto-poll", description="Auto-add the 1-5 star rating poll to every new post in a forum.")
@app_commands.describe(enabled="Turn auto poll on or off", forum_channel="Forum to change (optional)")
@app_commands.choices(enabled=[
    app_commands.Choice(name="on", value=1),
    app_commands.Choice(name="off", value=0),
])
async def auto_poll(interaction: discord.Interaction, enabled: app_commands.Choice[int], forum_channel: discord.ForumChannel = None):
    if not interaction.guild or not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("\u274c You need the **Manage Channels** permission.", ephemeral=True)
        return

    target = forum_channel
    if not target:
        ch = interaction.channel
        if isinstance(ch, discord.Thread) and isinstance(ch.parent, discord.ForumChannel):
            target = ch.parent
        elif isinstance(ch, discord.ForumChannel):
            target = ch

    if not target:
        await interaction.response.send_message("\u274c Pick a `forum_channel` or run inside a forum.", ephemeral=True)
        return

    await interaction.response.defer()
    on = bool(enabled.value)
    ok = await asyncio.to_thread(set_auto_poll, target.id, on)
    if not ok:
        await interaction.followup.send("\u274c Couldn't save that.", ephemeral=True)
        return

    if on:
        await interaction.followup.send(f"\u2b50 Auto poll is **on** for **{target.name}**.")
    else:
        await interaction.followup.send(f"\U0001F6D1 Auto poll is **off** for **{target.name}**.")


@bot.tree.command(name="list-bridges", description="List all connected channels/forums to a network code.")
@app_commands.describe(network_code="The network code to inspect")
async def list_bridges(interaction: discord.Interaction, network_code: str):
    code = network_code.strip().lower()
    await interaction.response.defer(ephemeral=True)

    text_ids = get_links("text_relays", code)
    thread_ids = get_links("thread_relays", code)
    forum_ids = get_links("forum_relays", code)
    vc_ids = get_links("vc_code_relays", code)

    embed = discord.Embed(title=f"🌐 Bridge Status for Network: `{code}`", color=discord.Color.blurple())

    guilds = {}
    for cid in text_ids + thread_ids + forum_ids + vc_ids:
        ch = bot.get_channel(cid)
        if ch and getattr(ch, "guild", None):
            guilds[ch.guild.id] = ch.guild
    ranked = sorted(guilds.values(), key=lambda g: g.member_count or 0, reverse=True)
    total_members = sum(g.member_count or 0 for g in ranked)

    server_lines = []
    used = 0
    for i, g in enumerate(ranked):
        line = f"• **{g.name}** — {g.member_count or 0:,} members"
        if used + len(line) + 1 > 900:
            server_lines.append(f"…and {len(ranked) - i} more")
            break
        server_lines.append(line)
        used += len(line) + 1
    embed.add_field(
        name=f"🏠 Servers ({len(ranked)}) • {total_members:,} members total",
        value=("\n".join(server_lines) if server_lines else "None"),
        inline=False,
    )

    text_desc = []
    for cid in text_ids:
        ch = bot.get_channel(cid)
        if ch:
            text_desc.append(f"• #{ch.name} (*{ch.guild.name}*)")
        else:
            text_desc.append(f"• ID: `{cid}`")
    embed.add_field(name=f"💬 Text Channels ({len(text_ids)})", value=("\n".join(text_desc)[:1015]) if text_desc else "None", inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="link-relay", description="Link this text channel to a cross-server relay network.")
@app_commands.describe(network_code="The network code to link this text channel to")
async def link_relay(interaction: discord.Interaction, network_code: str):
    channel_id = interaction.channel.id
    code = network_code.strip().lower()

    add_link("text_relays", code, channel_id)
    channels = get_links("text_relays", code)

    await interaction.response.send_message(
        f"✅ Linked **#{interaction.channel.name}** to relay network `{code}`! ({len(channels)} connected)",
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
            await interaction.response.send_message(f"🔌 Disconnected from the text relay network.", ephemeral=False)
        else:
            await interaction.response.send_message(f"⚠ Not currently linked.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking: {e}", ephemeral=True)

@bot.tree.command(name="link-forum", description="Link a forum channel to a cross-server forum network.")
@app_commands.describe(network_code="Shared network code", forum_channel="Forum channel (optional)")
async def link_forum(interaction: discord.Interaction, network_code: str, forum_channel: discord.ForumChannel = None):
    target_forum = forum_channel
    if not target_forum:
        current_channel = interaction.channel
        if isinstance(current_channel, discord.Thread) and isinstance(current_channel.parent, discord.ForumChannel):
            target_forum = current_channel.parent
        elif isinstance(current_channel, discord.ForumChannel):
            target_forum = current_channel

    if not target_forum:
        await interaction.response.send_message("❌ Please select a `forum_channel` or run inside a forum.", ephemeral=True)
        return

    code = network_code.strip().lower()
    add_link("forum_relays", code, target_forum.id)
    forums = get_links("forum_relays", code)

    await interaction.response.send_message(f"📌 Linked forum **{target_forum.name}** to `{code}`! ({len(forums)} connected)", ephemeral=False)

@bot.tree.command(name="unlink-forum", description="Disconnect this forum channel from its active network.")
async def unlink_forum(interaction: discord.Interaction):
    current_channel = interaction.channel
    forum_ch = None
    if isinstance(current_channel, discord.Thread) and isinstance(current_channel.parent, discord.ForumChannel):
        forum_ch = current_channel.parent
    elif isinstance(current_channel, discord.ForumChannel):
        forum_ch = current_channel

    if not forum_ch:
        await interaction.response.send_message("❌ Please run this inside a forum.", ephemeral=True)
        return

    forum_id = forum_ch.id
    try:
        res = supabase.table("forum_relays").select("network_code").eq("forum_channel_id", forum_id).execute()
        codes = [row["network_code"] for row in res.data]

        if codes:
            for code in codes:
                remove_link("forum_relays", code, forum_id)
            await interaction.response.send_message(f"🔌 Disconnected forum.", ephemeral=False)
        else:
            await interaction.response.send_message("⚠️ Not currently linked.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking: {e}", ephemeral=True)

@bot.tree.command(name="link-thread", description="Link or create a matching thread across servers.")
@app_commands.describe(network_code="Unique code", thread_name="Name of thread to create if needed")
async def link_thread(interaction: discord.Interaction, network_code: str, thread_name: str = None):
    code = network_code.strip().lower()
    current_channel = interaction.channel

    if isinstance(current_channel, discord.Thread):
        add_link("thread_relays", code, current_channel.id)
        threads = get_links("thread_relays", code)
        await interaction.response.send_message(f"🧵 Linked thread to network `{code}`! ({len(threads)} connected)", ephemeral=False)
    elif isinstance(current_channel, discord.TextChannel):
        if not thread_name:
            await interaction.response.send_message("❌ Provide a `thread_name`.", ephemeral=True)
            return
        try:
            new_thread = await current_channel.create_thread(name=thread_name, auto_archive_duration=60)
            add_link("thread_relays", code, new_thread.id)
            await interaction.response.send_message(f"🧵 Created and linked new thread **#{thread_name}**!", ephemeral=False)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to create thread: {e}", ephemeral=True)
    else:
        await interaction.response.send_message("❌ Use in text channels or threads.", ephemeral=True)

@bot.tree.command(name="unlink-thread", description="Disconnect this thread from its active network.")
async def unlink_thread(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message("❌ Run inside the thread.", ephemeral=True)
        return

    thread_id = interaction.channel.id
    try:
        res = supabase.table("thread_relays").select("network_code").eq("thread_id", thread_id).execute()
        codes = [row["network_code"] for row in res.data]

        if codes:
            for code in codes:
                remove_link("thread_relays", code, thread_id)
            await interaction.response.send_message(f"🔌 Disconnected thread.", ephemeral=False)
        else:
            await interaction.response.send_message("⚠️ Not linked.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking: {e}", ephemeral=True)

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

@bot.tree.command(name="link-vc", description="Link a voice channel to a cross-server voice network.")
@app_commands.describe(network_code="Shared network code", voice_channel="Voice channel (optional)")
async def link_vc(interaction: discord.Interaction, network_code: str, voice_channel: discord.VoiceChannel = None):
    if not VOICE_OK:
        await interaction.response.send_message("❌ Voice bridge dependencies are not installed.", ephemeral=True)
        return
    if not interaction.guild or not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need **Manage Channels** permission.", ephemeral=True)
        return

    target = _resolve_voice_channel(interaction, voice_channel)
    if not target:
        await interaction.response.send_message("❌ Join a voice channel first or select one.", ephemeral=True)
        return

    await interaction.response.defer()
    code = network_code.strip().lower()
    await asyncio.to_thread(add_link, "vc_code_relays", code, target.id)
    linked = await asyncio.to_thread(get_links, "vc_code_relays", code)

    live = voice_sessions.get(interaction.guild.id)
    if live and live.channel_id == target.id:
        live.codes = live.codes | {code}

    await interaction.followup.send(f"🎙️ Linked voice channel **{target.name}** to `{code}`! ({len(linked)} connected)")

    if humans_in(target) > 0:
        await maybe_join_voice(target)

@bot.tree.command(name="unlink-vc", description="Disconnect a voice channel from its voice network.")
@app_commands.describe(voice_channel="Voice channel (optional)")
async def unlink_vc(interaction: discord.Interaction, voice_channel: discord.VoiceChannel = None):
    if not VOICE_OK:
        await interaction.response.send_message("❌ Voice bridge dependencies not installed.", ephemeral=True)
        return
    if not interaction.guild or not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need **Manage Channels** permission.", ephemeral=True)
        return

    target = _resolve_voice_channel(interaction, voice_channel)
    if not target:
        await interaction.response.send_message("❌ Join a voice channel first or select one.", ephemeral=True)
        return

    await interaction.response.defer()
    codes = await get_vc_codes(target.id)
    if not codes:
        await interaction.followup.send("⚠️ Not linked.", ephemeral=True)
        return

    for code in codes:
        await asyncio.to_thread(remove_link, "vc_code_relays", code, target.id)

    session = voice_sessions.get(interaction.guild.id)
    if session and session.channel_id == target.id:
        await end_voice_session(interaction.guild.id)

    await interaction.followup.send("🔌 Disconnected voice channel.")

@bot.tree.command(name="voice-status", description="Show live status of the voice bridge.")
async def voice_status(interaction: discord.Interaction):
    if not VOICE_OK:
        await interaction.response.send_message("❌ Voice bridge dependencies not installed.", ephemeral=True)
        return

    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    lines = [
        f"Active voice sessions: `{len(voice_sessions)}/{MAX_VOICE_SESSIONS}`",
        f"libopus loaded: `{discord.opus.is_loaded()}`",
        f"Peak memory: `{rss_mb:.0f} MB`",
    ]

    session = voice_sessions.get(interaction.guild.id) if interaction.guild else None
    if session:
        lines += [f"This server: connected to <#{session.channel_id}>, networks `{', '.join(sorted(session.codes))}`"]
    else:
        lines.append("This server: not in a call right now.")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)

# ------------------------------------------------------------------------------
# YOUTUBE & WEB SEARCH COMMANDS
# ------------------------------------------------------------------------------
_YT_ID = re.compile(r"(?:youtu\.be/|youtube\.com/(?:watch\?(?:[^#\s]*&)?v=|shorts/|embed/|live/)"
                    r"|music\.youtube\.com/watch\?(?:[^#\s]*&)?v=)([\w-]{11})")


def yt_video_id(text: str):
    m = _YT_ID.search(text or "")
    return m.group(1) if m else None


def yt_url(video_id: str) -> str:
    # The plain watch URL is what makes Discord expand the link into its inline YouTube player.
    return f"https://www.youtube.com/watch?v={video_id}"


def fmt_duration(sec) -> str:
    if not sec:
        return ""
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s_ = divmod(rem, 60)
    return f"{h}:{m:02d}:{s_:02d}" if h else f"{m}:{s_:02d}"


def fmt_views(n) -> str:
    if not n:
        return ""
    n = int(n)
    for div, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if n >= div:
            return f"{n / div:.1f}".rstrip("0").rstrip(".") + suffix + " views"
    return f"{n} views"


def yt_item(entry: dict):
    vid = entry.get("id") if re.fullmatch(r"[\w-]{11}", entry.get("id") or "") else \
        yt_video_id(entry.get("webpage_url") or entry.get("url") or "")
    if not vid:
        return None
    live = entry.get("live_status") in ("is_live", "is_upcoming") or bool(entry.get("is_live"))
    return {"id": vid, "title": (entry.get("title") or "Untitled video").strip()[:100],
            "channel": entry.get("channel") or entry.get("uploader") or "", "duration": entry.get("duration"),
            "views": entry.get("view_count"), "live": live}


def yt_describe(item: dict) -> str:
    bits = [item["channel"], "LIVE" if item["live"] else fmt_duration(item["duration"]), fmt_views(item["views"])]
    return " \u2022 ".join(b for b in bits if b)[:100]


async def yt_search(query: str, limit: int = 10) -> list:
    """Search YouTube. Falls back to a web search if YouTube's own search is unavailable."""
    def run():
        import yt_dlp
        opts = {"extract_flat": True, "quiet": True, "no_warnings": True, "skip_download": True, "socket_timeout": 10}
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
    items, seen = [], set()
    try:
        data = await asyncio.wait_for(asyncio.to_thread(run), 25)
        for entry in (data or {}).get("entries") or []:
            item = yt_item(entry or {})
            if item and item["id"] not in seen:
                seen.add(item["id"])
                items.append(item)
    except Exception as e:
        logger.warning(f"yt_dlp search failed: {e}")
    if not items:
        try:
            for r in await asyncio.to_thread(google_search, f"{query} site:youtube.com/watch", limit):
                vid = yt_video_id(r.get("href", ""))
                if vid and vid not in seen:
                    seen.add(vid)
                    items.append({"id": vid, "title": (r.get("title") or "YouTube video")[:100], "channel": "",
                                  "duration": None, "views": None, "live": False})
        except Exception as e:
            logger.warning(f"Fallback video search failed: {e}")
    return items


async def yt_lookup(video_id: str):
    """Title/channel for a pasted link (best effort, short timeout)."""
    def run():
        import yt_dlp
        opts = {"quiet": True, "no_warnings": True, "skip_download": True, "noplaylist": True, "socket_timeout": 8}
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(yt_url(video_id), download=False)
    try:
        return yt_item(await asyncio.wait_for(asyncio.to_thread(run), 10) or {})
    except Exception:
        return None


def watch_message(who: str, item) -> str:
    # Title line, then the bare link on its own line so Discord turns it into the playable embed.
    title = f"**{item['title']}**" if item and item.get("title") else "a video"
    return f"\u25B6\uFE0F **{who}** is watching: {title}\n{yt_url(item['id'])}"[:2000]


def link_button_view(video_id: str) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(label="Open on YouTube", url=yt_url(video_id)))
    return view


async def relay_watch_post(source_channel, requester: discord.abc.User, item: dict):
    try:
        targets = await relay_channels_for(source_channel)
    except Exception as e:
        logger.error(f"Could not look up relay targets: {e}")
        return
    if not targets:
        return
    guild = getattr(source_channel, "guild", None)
    guild_name = guild.name if guild else "Unknown"
    username = safe_webhook_name(f"{requester.display_name} [{guild_name}]")
    content = watch_message(requester.display_name, item)

    async def one(ch):
        try:
            await webhook_send(ch, username=username, avatar_url=requester.display_avatar.url, content=content,
                               allowed_mentions=discord.AllowedMentions.none())
        except Exception as e:
            logger.warning(f"Could not relay video link: {e}")

    await asyncio.gather(*(one(ch) for ch in targets))


class YouTubeResultsView(discord.ui.View):
    PAGE = 5

    def __init__(self, items: list, requester_id: int, query: str):
        super().__init__(timeout=180)
        self.videos, self.requester_id, self.query, self.page = items, requester_id, query, 0
        self._build()

    def pages(self) -> int:
        return max(1, -(-len(self.videos) // self.PAGE))

    def chunk(self) -> list:
        return self.videos[self.page * self.PAGE:(self.page + 1) * self.PAGE]

    def _build(self):
        self.clear_items()
        select = discord.ui.Select(
            placeholder="Pick a video to post with an inline player...",
            options=[discord.SelectOption(label=i["title"], description=yt_describe(i) or None, value=i["id"])
                     for i in self.chunk()])
        select.callback = self._picked
        self.add_item(select)
        if self.pages() > 1:
            prev_b = discord.ui.Button(label="\u25C0 Prev", style=discord.ButtonStyle.secondary, disabled=self.page == 0, row=1)
            next_b = discord.ui.Button(label="Next \u25B6", style=discord.ButtonStyle.secondary,
                                       disabled=self.page >= self.pages() - 1, row=1)
            prev_b.callback = lambda i: self._turn(i, -1)
            next_b.callback = lambda i: self._turn(i, 1)
            self.add_item(prev_b)
            self.add_item(next_b)
        top = discord.ui.Button(label="\u25B6 Post top result", style=discord.ButtonStyle.success, row=1)
        top.callback = lambda i: self._post(i, self.videos[0])
        self.add_item(top)

    def embed(self) -> discord.Embed:
        lines = []
        for n, i in enumerate(self.chunk(), self.page * self.PAGE + 1):
            lines.append(f"**{n}.** {i['title']}\n\u2003{yt_describe(i) or 'YouTube'}")
        e = discord.Embed(title=f"\U0001F50D YouTube: {self.query}"[:256], description="\n".join(lines)[:4000], color=0xFF0033)
        if self.chunk():
            e.set_thumbnail(url=f"https://i.ytimg.com/vi/{self.chunk()[0]['id']}/hqdefault.jpg")
        e.set_footer(text=f"Page {self.page + 1}/{self.pages()} \u2022 the video appears with Discord's own player")
        return e

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("\u274c This menu belongs to someone else. Run `/play` yourself.", ephemeral=True)
            return False
        return True

    async def _turn(self, interaction: discord.Interaction, d: int):
        if not await self._guard(interaction):
            return
        self.page = max(0, min(self.pages() - 1, self.page + d))
        self._build()
        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def _picked(self, interaction: discord.Interaction):
        vid = interaction.data["values"][0]
        item = next((i for i in self.videos if i["id"] == vid), None)
        if item:
            await self._post(interaction, item)

    async def _post(self, interaction: discord.Interaction, item: dict):
        if not await self._guard(interaction):
            return
        self.stop()
        await interaction.response.edit_message(content=f"\u2705 Posted **{item['title']}**", embed=None, view=None)
        await post_video(interaction, item)


async def post_video(interaction: discord.Interaction, item: dict):
    content = watch_message(interaction.user.display_name, item)
    try:
        await interaction.followup.send(content, view=link_button_view(item["id"]))
    except Exception:
        if interaction.channel:
            await interaction.channel.send(content, view=link_button_view(item["id"]))
    if interaction.channel and supabase:
        await relay_watch_post(interaction.channel, interaction.user, item)


@bot.tree.command(name="play", description="Search YouTube (or paste a link) and post the video with an inline player.")
@app_commands.describe(search="Search keywords, or a YouTube link")
async def play(interaction: discord.Interaction, search: str):
    search = search.strip()
    vid = yt_video_id(search)
    if vid:                                              # pasted link: post straight away, no menu
        await interaction.response.defer()
        item = await yt_lookup(vid) or {"id": vid, "title": ""}
        await post_video(interaction, item)
        return

    await interaction.response.defer(ephemeral=True)
    items = await yt_search(search, 10)
    if not items:
        await interaction.followup.send(f"\u26a0\ufe0f Couldn't find videos for `{search}`. Try different keywords.", ephemeral=True)
        return
    view = YouTubeResultsView(items, interaction.user.id, search)
    await interaction.followup.send(embed=view.embed(), view=view, ephemeral=True)


@bot.tree.command(name="search", description="Web search with Gemini AI overview.")
@app_commands.describe(query="What would you like to search for?")
async def search(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    try:
        results, overview = await asyncio.gather(
            asyncio.to_thread(google_search, query, 5),
            asyncio.to_thread(gemini_overview, query),
        )

        if not results:
            await interaction.followup.send(f"⚠ No results found for `{query}`.")
            return

        embed = discord.Embed(title=f"🔍 Search Results for: `{query}`", color=discord.Color.green())
        if overview:
            embed.description = f"✨ **AI Overview**\n{overview[:1800]}"

        for i, item in enumerate(results[:5], 1):
            title = item.get("title", "Result")
            url = item.get("href", "#")
            body = item.get("body", "")[:250]
            embed.add_field(name=f"Result {i}", value=f"[{title}]({url})\n{body}", inline=False)

        await interaction.followup.send(embed=embed)
    except Exception as e:
        logger.error(f"Search error: {e}")
        await interaction.followup.send(f"❌ An error occurred: {e}")

# ------------------------------------------------------------------------------
# NES / SNES EMULATOR
# ------------------------------------------------------------------------------
EMU_BASE = os.path.dirname(os.path.abspath(__file__))
ROM_DIR = os.environ.get("ROM_DIR", os.path.join(EMU_BASE, "roms"))
CORE_DIR = os.environ.get("CORE_DIR", os.path.join(EMU_BASE, "cores"))
STATE_DIR = os.environ.get("STATE_DIR", os.path.join(EMU_BASE, "states"))
ROM_BUCKET = os.environ.get("ROM_BUCKET", "roms")
WORKER_PATH = os.path.join(EMU_BASE, "emu_worker.py")
WORKER_PATH_N64 = os.path.join(EMU_BASE, "emu_worker_n64.py")   # N64 runs in its own worker (analog stick, frame skipping)
CORE_BASE_URL = os.environ.get("CORE_BASE_URL", "https://buildbot.libretro.com/nightly/linux/x86_64/latest")

MAX_EMU_SESSIONS = int(os.environ.get("MAX_EMU_SESSIONS", "2"))
EMU_IDLE_SECONDS = int(os.environ.get("EMU_IDLE_SECONDS", "300"))
EMU_SCALE = int(os.environ.get("EMU_SCALE", "2"))
EMU_HOLD_STEPS = [4, 8, 16, 30]
EMU_SETTLE_FRAMES = int(os.environ.get("EMU_SETTLE_FRAMES", "4"))
EMU_MAX_ROM_BYTES = int(os.environ.get("EMU_MAX_ROM_MB", "8")) * 1024 * 1024
EMU_MAX_N64 = int(os.environ.get("EMU_MAX_N64", "1"))        # N64 is heavy: one game at a time by default
EMU_ADD_OPEN = os.environ.get("EMU_ADD_OPEN", "0") == "1"
EMU_STREAM_FPS = int(os.environ.get("EMU_STREAM_FPS", "60"))
EMU_AUDIO = os.environ.get("EMU_AUDIO", "1") == "1"
EMU_JPEG_QUALITY = int(os.environ.get("EMU_JPEG_QUALITY", "72"))
EMU_AFK_PAUSE = int(os.environ.get("EMU_AFK_PAUSE", "60"))
EMU_MAX_VIEWERS = int(os.environ.get("MAX_EMU_VIEWERS", "8"))
EMU_EDIT_INTERVAL = float(os.environ.get("EMU_EDIT_INTERVAL", "1.0"))
EMU_GAP_FRAMES = 4
EMU_CHAT_KEYS = os.environ.get("EMU_CHAT_KEYS", "1") == "1"
EMU_CHAT_PREFIX = os.environ.get("EMU_CHAT_PREFIX", "").lower()
EMU_DELETE_KEYS = os.environ.get("EMU_DELETE_KEYS", "1") == "1"
EMU_WEB_BASE = (os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")

EMU_SYSTEMS = {
    "nes": {"label": "NES", "exts": {".nes"}, "core": os.environ.get("CORE_NES", "fceumm_libretro")},
    "snes": {"label": "SNES", "exts": {".sfc", ".smc", ".swc", ".fig"}, "core": os.environ.get("CORE_SNES", "snes9x2010_libretro")},
    "n64": {"label": "N64", "exts": {".z64", ".n64", ".v64"}, "core": os.environ.get("CORE_N64", "mupen64plus_next_libretro"),
            "worker": WORKER_PATH_N64, "max_mb": int(os.environ.get("EMU_MAX_ROM_MB_N64", "64")), "lazy_core": True},
}

EMU_MASKS = {
    "b": 1 << 0, "y": 1 << 1, "select": 1 << 2, "start": 1 << 3,
    "up": 1 << 4, "down": 1 << 5, "left": 1 << 6, "right": 1 << 7,
    "a": 1 << 8, "x": 1 << 9, "l": 1 << 10, "r": 1 << 11,
    "ab": (1 << 0) | (1 << 8),
    # N64 extras: Z trigger, C buttons, and the analog stick as four full-tilt directions
    "z": 1 << 12, "cu": 1 << 13, "cd": 1 << 14, "cl": 1 << 15, "cr": 1 << 16,
    "su": 1 << 17, "sd": 1 << 18, "sl": 1 << 19, "sr": 1 << 20,
}

emu_sessions: dict = {}
_remote_roms: set = set()
_ROM_BAD_CHARS = re.compile(r"[^\w .,'()&!+\-]")


N64_WORKER_SOURCE = r'''#!/usr/bin/env python3
# AUTO-WRITTEN BY THE BOT from the copy inside the bot file. Edit that copy (or delete this line to keep your own edits).
"""LUX-NET libretro worker (N64 first, any software-rendered core works).

Talks to the bot over stdin/stdout with the same framing the NES/SNES worker uses:
    bot -> worker : <u32 length LE><pickle dict>     e.g. {"op": "load", ...}
    worker -> bot : <u32 length LE><kind byte><payload>
                    kind b"R" = pickled reply, b"J" = JPEG/PNG video frame, b"A" = audio (b"A\\x00" + int16 PCM)

Design goals: keep the emulation thread free. Frames are copied out of the core and encoded on a second
thread (latest frame wins, so a slow viewer never slows the game down), audio is forwarded as it is made,
and when the machine falls behind real time the worker keeps the game speed and drops *video* frames
instead of letting the game run in slow motion.

Run `python emu_worker_n64.py --bench CORE.so ROM.z64` on the host to see what speed it really achieves.
"""
from __future__ import annotations

import collections
import ctypes
import gc
import hashlib
import io
import os
import pickle
import queue
import struct
import sys
import threading
import time
import traceback
import zlib

# ----------------------------------------------------------------------------- protocol plumbing
# Cores sometimes print to stdout. Keep the real stdout private for the protocol and point fd 1 at stderr.
_PROTO_OUT = None
_PROTO_IN = None


def _setup_pipes():
    global _PROTO_OUT, _PROTO_IN
    out_fd = os.dup(1)
    os.dup2(2, 1)
    _PROTO_OUT = os.fdopen(out_fd, "wb", buffering=0)
    _PROTO_IN = sys.stdin.buffer


_out_lock = threading.Lock()


def emit(kind: bytes, payload: bytes):
    body = kind + payload
    with _out_lock:
        _PROTO_OUT.write(struct.pack("<I", len(body)) + body)


def reply(obj: dict):
    emit(b"R", pickle.dumps(obj, protocol=4))


def log(*a):
    print("[n64-worker]", *a, file=sys.stderr, flush=True)


# ----------------------------------------------------------------------------- optional imports
try:
    from PIL import Image
except Exception:                                   # pragma: no cover
    Image = None
try:
    import numpy as np
except Exception:                                   # pragma: no cover
    np = None

# ----------------------------------------------------------------------------- libretro constants
ENV_GET_OVERSCAN, ENV_GET_CAN_DUPE, ENV_SET_MESSAGE, ENV_SHUTDOWN = 2, 3, 6, 7
ENV_SET_PERFORMANCE_LEVEL, ENV_GET_SYSTEM_DIRECTORY, ENV_SET_PIXEL_FORMAT = 8, 9, 10
ENV_SET_INPUT_DESCRIPTORS, ENV_SET_KEYBOARD_CALLBACK, ENV_SET_HW_RENDER = 11, 12, 14
ENV_GET_VARIABLE, ENV_SET_VARIABLES, ENV_GET_VARIABLE_UPDATE, ENV_SET_SUPPORT_NO_GAME = 15, 16, 17, 18
ENV_GET_INPUT_DEVICE_CAPABILITIES = 24
ENV_GET_CORE_ASSETS_DIRECTORY, ENV_GET_SAVE_DIRECTORY = 30, 31
ENV_SET_SYSTEM_AV_INFO, ENV_SET_CONTROLLER_INFO, ENV_SET_GEOMETRY = 32, 35, 37
ENV_GET_LANGUAGE, ENV_SET_SERIALIZATION_QUIRKS = 39, 44
ENV_GET_AUDIO_VIDEO_ENABLE = 47
ENV_GET_CORE_OPTIONS_VERSION, ENV_GET_MESSAGE_INTERFACE_VERSION = 52, 59
ENV_SET_MINIMUM_AUDIO_LATENCY, ENV_SET_CONTENT_INFO_OVERRIDE = 63, 65
ENV_SET_CORE_OPTIONS_UPDATE_DISPLAY_CALLBACK = 69

DEV_JOYPAD, DEV_ANALOG = 1, 5
JOY_MASK_QUERY = 256
MEM_SAVE_RAM = 0

# Bit layout shared with the bot (bits 0-11 equal the libretro joypad ids; 12+ are N64 extras)
B_B, B_Y, B_SELECT, B_START, B_UP, B_DOWN, B_LEFT, B_RIGHT, B_A, B_X, B_L, B_R = (1 << i for i in range(12))
B_Z, B_CU, B_CD, B_CL, B_CR = (1 << 12), (1 << 13), (1 << 14), (1 << 15), (1 << 16)
B_SU, B_SD, B_SL, B_SR = (1 << 17), (1 << 18), (1 << 19), (1 << 20)
STICK_MAX = 32767

# N64 option defaults tuned for a CPU-only host. Unknown keys are ignored by the core, and any of them can be
# overridden with the N64_CORE_OPTIONS environment variable ("key=value;key=value").
N64_DEFAULT_OPTIONS = {
    "mupen64plus-cpucore": "dynamic_recompiler",
    "mupen64plus-rdp-plugin": os.environ.get("N64_RDP", "angrylion"),
    "mupen64plus-rsp-plugin": os.environ.get("N64_RSP", "cxd4"),
    "mupen64plus-angrylion-multithread": "all threads",
    "mupen64plus-angrylion-sync": "Low",
    "mupen64plus-angrylion-vioverlay": "Filtered",
    "mupen64plus-angrylion-overscan": "disabled",
    "mupen64plus-43screensize": "320x240",
    "mupen64plus-EnableLODEmulation": "False",
    "mupen64plus-pak1": "memory",
    "mupen64plus-virefresh": "Auto",
    "mupen64plus-audio-buffer-size": "2048",
}


def parse_option_overrides(text: str) -> dict:
    out = {}
    for part in (text or "").split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            if k.strip():
                out[k.strip()] = v.strip()
    return out


class RetroVariable(ctypes.Structure):
    _fields_ = [("key", ctypes.c_char_p), ("value", ctypes.c_char_p)]


class RetroSystemInfo(ctypes.Structure):
    _fields_ = [("library_name", ctypes.c_char_p), ("library_version", ctypes.c_char_p),
                ("valid_extensions", ctypes.c_char_p), ("need_fullpath", ctypes.c_bool),
                ("block_extract", ctypes.c_bool)]


class RetroGameGeometry(ctypes.Structure):
    _fields_ = [("base_width", ctypes.c_uint), ("base_height", ctypes.c_uint),
                ("max_width", ctypes.c_uint), ("max_height", ctypes.c_uint), ("aspect_ratio", ctypes.c_float)]


class RetroSystemTiming(ctypes.Structure):
    _fields_ = [("fps", ctypes.c_double), ("sample_rate", ctypes.c_double)]


class RetroSystemAvInfo(ctypes.Structure):
    _fields_ = [("geometry", RetroGameGeometry), ("timing", RetroSystemTiming)]


class RetroGameInfo(ctypes.Structure):
    _fields_ = [("path", ctypes.c_char_p), ("data", ctypes.c_void_p), ("size", ctypes.c_size_t),
                ("meta", ctypes.c_char_p)]


ENV_T = ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_uint, ctypes.c_void_p)
VIDEO_T = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_uint, ctypes.c_uint, ctypes.c_size_t)
AUDIO_T = ctypes.CFUNCTYPE(None, ctypes.c_int16, ctypes.c_int16)
AUDIO_BATCH_T = ctypes.CFUNCTYPE(ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t)
POLL_T = ctypes.CFUNCTYPE(None)
STATE_T = ctypes.CFUNCTYPE(ctypes.c_int16, ctypes.c_uint, ctypes.c_uint, ctypes.c_uint, ctypes.c_uint)


def to_pad(mask: int, n64: bool):
    """Our shared bitmask -> (libretro joypad bitmask, lx, ly, rx, ry)."""
    if not n64:
        return mask & 0xFFFF, 0, 0, 0, 0
    pad = 0
    if mask & B_A:
        pad |= 1 << 0           # N64 A  <- RetroPad B
    if mask & B_B:
        pad |= 1 << 1           # N64 B  <- RetroPad Y
    if mask & B_START:
        pad |= 1 << 3
    if mask & B_UP:
        pad |= 1 << 4
    if mask & B_DOWN:
        pad |= 1 << 5
    if mask & B_LEFT:
        pad |= 1 << 6
    if mask & B_RIGHT:
        pad |= 1 << 7
    if mask & B_L:
        pad |= 1 << 10
    if mask & B_R:
        pad |= 1 << 11
    if mask & B_Z:
        pad |= 1 << 12          # N64 Z  <- RetroPad L2
    lx = (STICK_MAX if mask & B_SR else 0) - (STICK_MAX if mask & B_SL else 0)
    ly = (STICK_MAX if mask & B_SD else 0) - (STICK_MAX if mask & B_SU else 0)
    rx = (STICK_MAX if mask & B_CR else 0) - (STICK_MAX if mask & B_CL else 0)   # C buttons = right stick
    ry = (STICK_MAX if mask & B_CD else 0) - (STICK_MAX if mask & B_CU else 0)
    return pad, lx, ly, rx, ry


# ----------------------------------------------------------------------------- image encoding
def _rgb_from_frame(buf: bytes, w: int, h: int, pitch: int, fmt: int):
    """Return a PIL RGB image (or raw RGB bytes if PIL is missing) for a libretro frame."""
    if fmt == 1:                                                   # XRGB8888
        if Image is not None:
            return Image.frombuffer("RGB", (w, h), buf, "raw", "BGRX", pitch, 1)
        rows = []
        for y in range(h):
            row = buf[y * pitch:y * pitch + w * 4]
            ba = bytearray(w * 3)
            ba[0::3], ba[1::3], ba[2::3] = row[2::4], row[1::4], row[0::4]
            rows.append(bytes(ba))
        return b"".join(rows)
    if np is None:
        raise RuntimeError("16-bit video needs numpy installed")
    arr = np.frombuffer(buf, dtype="<u2", count=(pitch // 2) * h).reshape(h, pitch // 2)[:, :w]
    if fmt == 2:                                                   # RGB565
        r, g, b = (arr >> 11) & 31, (arr >> 5) & 63, arr & 31
        r, g, b = (r << 3) | (r >> 2), (g << 2) | (g >> 4), (b << 3) | (b >> 2)
    else:                                                          # 0RGB1555
        r, g, b = (arr >> 10) & 31, (arr >> 5) & 31, arr & 31
        r, g, b = (r << 3) | (r >> 2), (g << 3) | (g >> 2), (b << 3) | (b >> 2)
    rgb = np.stack([r, g, b], axis=-1).astype(np.uint8)
    if Image is not None:
        return Image.fromarray(rgb, "RGB")
    return rgb.tobytes()


def _png_from_rgb_bytes(rgb: bytes, w: int, h: int) -> bytes:
    raw = b"".join(b"\x00" + rgb[y * w * 3:(y + 1) * w * 3] for y in range(h))

    def chunk(tag, data):
        c = struct.pack(">I", len(data)) + tag + data
        return c + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 1)) + chunk(b"IEND", b""))


def encode_frame(frame, kind: str, quality: int, max_w: int = 0, scale: int = 1) -> bytes:
    buf, w, h, pitch, fmt = frame
    img = _rgb_from_frame(buf, w, h, pitch, fmt)
    if Image is None:                                              # no Pillow: PNG only, no scaling
        return _png_from_rgb_bytes(img, w, h)
    if scale > 1:
        img = img.resize((w * scale, h * scale), Image.NEAREST)
    elif max_w and w > max_w:
        img = img.resize((max_w, max(1, h * max_w // w)), Image.BILINEAR)
    bio = io.BytesIO()
    if kind == "jpeg":
        img.save(bio, "JPEG", quality=quality, subsampling=2)
    else:
        img.save(bio, "PNG", compress_level=1)
    return bio.getvalue()


class Encoder(threading.Thread):
    """Encodes the newest frame on its own thread; older frames are dropped, never queued."""

    def __init__(self):
        super().__init__(daemon=True)
        self.cond = threading.Condition()
        self.pending = None
        self.quality = 72
        self.max_w = int(os.environ.get("EMU_N64_MAX_W", "640"))
        self.enabled = False
        self.sent = 0

    def submit(self, frame):
        with self.cond:
            self.pending = frame
            self.cond.notify()

    def run(self):
        while True:
            with self.cond:
                while self.pending is None:
                    self.cond.wait()
                frame, self.pending = self.pending, None
            if not self.enabled:
                continue
            try:
                kind = "jpeg" if Image is not None else "png"
                emit(b"J", encode_frame(frame, kind, self.quality, self.max_w))
                self.sent += 1
            except Exception as e:                                 # never let a bad frame kill the stream
                log("encode error:", e)


# ----------------------------------------------------------------------------- the core wrapper
class Core:
    def __init__(self, core_path: str, sysdir: str, savedir: str, options: dict, n64: bool):
        self.lib = ctypes.CDLL(core_path, mode=ctypes.RTLD_LOCAL)
        self.sysdir = os.fsencode(sysdir)
        self.savedir = os.fsencode(savedir)
        self.options = dict(options)
        self.defaults = {}
        self.n64 = n64
        self.pixfmt = 0
        self.hw_requested = False
        self.shutdown = False
        self.vars_dirty = False
        self.av_enable = 3                       # bit0 = draw video, bit1 = make audio
        self._vals = {}
        self.mask = 0
        self.ax = self.ay = 0.0
        self.frame = None
        self.new_frame = False
        self.audio = []
        self.av = None
        self.rom_buf = None
        self.loaded = False
        self.srm_path = None
        self._srm_hash = None

        L = self.lib
        L.retro_get_system_info.argtypes = [ctypes.POINTER(RetroSystemInfo)]
        L.retro_get_system_av_info.argtypes = [ctypes.POINTER(RetroSystemAvInfo)]
        L.retro_load_game.argtypes = [ctypes.POINTER(RetroGameInfo)]
        L.retro_load_game.restype = ctypes.c_bool
        L.retro_serialize_size.restype = ctypes.c_size_t
        L.retro_serialize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        L.retro_serialize.restype = ctypes.c_bool
        L.retro_unserialize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        L.retro_unserialize.restype = ctypes.c_bool
        L.retro_get_memory_data.argtypes = [ctypes.c_uint]
        L.retro_get_memory_data.restype = ctypes.c_void_p
        L.retro_get_memory_size.argtypes = [ctypes.c_uint]
        L.retro_get_memory_size.restype = ctypes.c_size_t
        L.retro_set_controller_port_device.argtypes = [ctypes.c_uint, ctypes.c_uint]

        # callbacks must stay referenced for the life of the core
        self._cb_env = ENV_T(self._environment)
        self._cb_video = VIDEO_T(self._video)
        self._cb_audio = AUDIO_T(self._audio_sample)
        self._cb_batch = AUDIO_BATCH_T(self._audio_batch)
        self._cb_poll = POLL_T(lambda: None)
        self._cb_state = STATE_T(self._input_state)
        L.retro_set_environment(self._cb_env)
        L.retro_set_video_refresh(self._cb_video)
        L.retro_set_audio_sample(self._cb_audio)
        L.retro_set_audio_sample_batch(self._cb_batch)
        L.retro_set_input_poll(self._cb_poll)
        L.retro_set_input_state(self._cb_state)
        L.retro_init()

    # ---- libretro callbacks (they must never raise)
    def _environment(self, cmd, data):
        try:
            cmd &= 0xFFFF
            if cmd == ENV_GET_OVERSCAN:
                ctypes.cast(data, ctypes.POINTER(ctypes.c_bool))[0] = False
                return True
            if cmd == ENV_GET_CAN_DUPE:
                ctypes.cast(data, ctypes.POINTER(ctypes.c_bool))[0] = True
                return True
            if cmd in (ENV_SET_MESSAGE, ENV_SET_PERFORMANCE_LEVEL, ENV_SET_INPUT_DESCRIPTORS,
                       ENV_SET_KEYBOARD_CALLBACK, ENV_SET_SUPPORT_NO_GAME, ENV_SET_CONTROLLER_INFO,
                       ENV_SET_SERIALIZATION_QUIRKS, ENV_SET_MINIMUM_AUDIO_LATENCY,
                       ENV_SET_CONTENT_INFO_OVERRIDE, ENV_SET_CORE_OPTIONS_UPDATE_DISPLAY_CALLBACK):
                return True
            if cmd == ENV_SHUTDOWN:
                self.shutdown = True
                return True
            if cmd in (ENV_GET_SYSTEM_DIRECTORY, ENV_GET_CORE_ASSETS_DIRECTORY):
                ctypes.cast(data, ctypes.POINTER(ctypes.c_char_p))[0] = self.sysdir
                return True
            if cmd == ENV_GET_SAVE_DIRECTORY:
                ctypes.cast(data, ctypes.POINTER(ctypes.c_char_p))[0] = self.savedir
                return True
            if cmd == ENV_SET_PIXEL_FORMAT:
                fmt = ctypes.cast(data, ctypes.POINTER(ctypes.c_uint))[0]
                if fmt in (0, 1, 2):
                    self.pixfmt = fmt
                    return True
                return False
            if cmd == ENV_SET_HW_RENDER:
                self.hw_requested = True          # no GPU context here: refuse, the core should fall back
                return False
            if cmd == ENV_SET_VARIABLES:
                arr = ctypes.cast(data, ctypes.POINTER(RetroVariable))
                i = 0
                while arr[i].key:
                    desc = (arr[i].value or b"").decode("utf-8", "replace")
                    first = desc.split(";", 1)[-1].strip().split("|")[0].strip()
                    self.defaults[arr[i].key.decode()] = first
                    i += 1
                return True
            if cmd == ENV_GET_VARIABLE:
                var = ctypes.cast(data, ctypes.POINTER(RetroVariable))[0]
                key = (var.key or b"").decode()
                val = self.options.get(key, self.defaults.get(key))
                if val is None:
                    return False
                self._vals[key] = val.encode()
                ctypes.cast(data, ctypes.POINTER(RetroVariable))[0].value = self._vals[key]
                return True
            if cmd == ENV_GET_VARIABLE_UPDATE:
                ctypes.cast(data, ctypes.POINTER(ctypes.c_bool))[0] = self.vars_dirty
                self.vars_dirty = False
                return True
            if cmd == ENV_GET_INPUT_DEVICE_CAPABILITIES:
                ctypes.cast(data, ctypes.POINTER(ctypes.c_uint64))[0] = (1 << DEV_JOYPAD) | (1 << DEV_ANALOG)
                return True
            if cmd in (ENV_SET_SYSTEM_AV_INFO, ENV_SET_GEOMETRY):
                return True
            if cmd == ENV_GET_AUDIO_VIDEO_ENABLE:
                ctypes.cast(data, ctypes.POINTER(ctypes.c_int))[0] = self.av_enable
                return True
            if cmd == ENV_GET_LANGUAGE:
                ctypes.cast(data, ctypes.POINTER(ctypes.c_uint))[0] = 0
                return True
            if cmd in (ENV_GET_CORE_OPTIONS_VERSION, ENV_GET_MESSAGE_INTERFACE_VERSION):
                ctypes.cast(data, ctypes.POINTER(ctypes.c_uint))[0] = 0      # legacy variables API
                return True
        except Exception as e:                                               # pragma: no cover
            log("environment error:", cmd, e)
        return False

    def _video(self, data, w, h, pitch):
        if not data or data > (1 << 60):          # NULL = duplicate frame, -1 = hardware frame
            return
        try:
            self.frame = (ctypes.string_at(data, pitch * h), w, h, pitch, self.pixfmt)
            self.new_frame = True
        except Exception as e:                                               # pragma: no cover
            log("video error:", e)

    def _audio_sample(self, left, right):
        self.audio.append(struct.pack("<hh", left, right))

    def _audio_batch(self, data, frames):
        if data and frames:
            self.audio.append(ctypes.string_at(data, frames * 4))
        return frames

    def _input_state(self, port, device, index, id_):
        if port != 0:
            return 0
        pad, lx, ly, rx, ry = to_pad(self.mask, self.n64)
        if device == DEV_JOYPAD:
            if id_ == JOY_MASK_QUERY:
                return ctypes.c_int16(pad).value
            return 1 if (pad >> id_) & 1 and id_ < 16 else 0
        if device == DEV_ANALOG:
            if index == 0:
                v = lx if id_ == 0 else ly
                if id_ == 0 and self.ax:
                    v = int(max(-1.0, min(1.0, self.ax)) * STICK_MAX)
                elif id_ == 1 and self.ay:
                    v = int(max(-1.0, min(1.0, self.ay)) * STICK_MAX)
                return v
            if index == 1:
                return rx if id_ == 0 else ry
        return 0

    # ---- lifecycle
    def load(self, rom_path: str, system_label: str):
        info = RetroSystemInfo()
        self.lib.retro_get_system_info(ctypes.byref(info))
        game = RetroGameInfo()
        game.path = os.fsencode(rom_path)
        if info.need_fullpath:
            game.data, game.size = None, 0
        else:
            with open(rom_path, "rb") as f:
                self.rom_buf = f.read()
            game.data = ctypes.cast(ctypes.c_char_p(self.rom_buf), ctypes.c_void_p)
            game.size = len(self.rom_buf)
        if not self.lib.retro_load_game(ctypes.byref(game)):
            hint = ""
            if self.hw_requested:
                hint = (" The core asked for GPU rendering, which this host can't provide. "
                        "Use the software renderer: N64_RDP=angrylion and N64_RSP=cxd4.")
            raise RuntimeError("the core refused to load this game." + hint)
        self.loaded = True
        av = RetroSystemAvInfo()
        self.lib.retro_get_system_av_info(ctypes.byref(av))
        self.av = av
        self.lib.retro_set_controller_port_device(0, DEV_JOYPAD)
        stem = os.path.splitext(os.path.basename(rom_path))[0]
        os.makedirs(self.savedir, exist_ok=True)
        self.srm_path = os.path.join(os.fsdecode(self.savedir), stem + ".srm")
        self._load_srm()

    @property
    def fps(self) -> float:
        f = self.av.timing.fps if self.av else 60.0
        return f if 20.0 < f < 130.0 else 60.0

    @property
    def sample_rate(self) -> int:
        r = int(self.av.timing.sample_rate) if self.av else 44100
        return r if 8000 <= r <= 96000 else 44100

    def run_frame(self):
        self.lib.retro_run()

    def take_audio(self) -> bytes:
        if not self.audio:
            return b""
        data, self.audio = b"".join(self.audio), []
        return data

    # ---- save RAM (battery saves)
    def _srm_region(self):
        size = self.lib.retro_get_memory_size(MEM_SAVE_RAM)
        ptr = self.lib.retro_get_memory_data(MEM_SAVE_RAM)
        return (ptr, size) if ptr and size else (None, 0)

    def _load_srm(self):
        ptr, size = self._srm_region()
        if not ptr or not os.path.exists(self.srm_path):
            return
        try:
            with open(self.srm_path, "rb") as f:
                data = f.read(size)
            ctypes.memmove(ptr, data, len(data))
            self._srm_hash = hashlib.md5(data).digest()
        except Exception as e:
            log("could not load save file:", e)

    def flush_srm(self, force: bool = False):
        ptr, size = self._srm_region()
        if not ptr:
            return
        data = ctypes.string_at(ptr, size)
        h = hashlib.md5(data).digest()
        if h == self._srm_hash and not force:
            return
        tmp = self.srm_path + ".part"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, self.srm_path)
        self._srm_hash = h

    # ---- save states
    def save_state(self, path: str):
        size = self.lib.retro_serialize_size()
        if not size:
            raise RuntimeError("this core can't make save states")
        buf = ctypes.create_string_buffer(size)
        if not self.lib.retro_serialize(buf, size):
            raise RuntimeError("save state failed")
        tmp = path + ".part"
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "wb") as f:
            f.write(zlib.compress(buf.raw, 1))
        os.replace(tmp, path)

    def load_state(self, path: str):
        with open(path, "rb") as f:
            raw = zlib.decompress(f.read())
        buf = ctypes.create_string_buffer(raw, len(raw))
        if not self.lib.retro_unserialize(buf, len(raw)):
            raise RuntimeError("could not load that save state")


# ----------------------------------------------------------------------------- the worker
class Worker:
    def __init__(self):
        self.q = queue.Queue()
        self.core = None
        self.n64 = False
        self.encoder = Encoder()
        self.encoder.start()
        self.live = False
        self.audio_on = False
        self.send_every = 1
        self.seq = collections.deque()          # (mask, frames_left)
        self.base_mask = 0
        self.next_t = 0.0
        self.frame_counter = 0
        self.last_srm = time.monotonic()
        self.stop = False

    # ---- helpers
    def effective_mask(self) -> int:
        m = self.base_mask
        if self.seq:
            m |= self.seq[0][0]
        return m

    def step_seq(self):
        if self.seq:
            self.seq[0][1] -= 1
            if self.seq[0][1] <= 0:
                self.seq.popleft()

    def run_once(self, render: bool):
        c = self.core
        c.mask = self.effective_mask()
        c.new_frame = False
        c.av_enable = 3 if render else 2          # behind schedule: ask the core to skip drawing this frame
        c.run_frame()
        c.av_enable = 3
        self.step_seq()
        if self.audio_on and self.live:
            pcm = c.take_audio()
            if pcm:
                emit(b"A", b"\x00" + pcm)
        else:
            c.audio = []
        if c.new_frame and c.frame is not None:
            self.frame_counter += 1
            if self.encoder.enabled and self.frame_counter % self.send_every == 0:
                self.encoder.submit(c.frame)

    def snapshot_png(self, scale: int) -> bytes:
        c = self.core
        if c.frame is None:
            return _png_from_rgb_bytes(bytes(320 * 240 * 3), 320, 240)
        return encode_frame(c.frame, "png", 0, 0, max(1, scale))

    # ---- ops
    def op_load(self, m):
        n64 = "mupen64plus" in os.path.basename(m["core"]).lower() or m.get("system") == "n64"
        self.n64 = n64
        options = dict(N64_DEFAULT_OPTIONS) if n64 else {}
        options.update(parse_option_overrides(os.environ.get("N64_CORE_OPTIONS", "")))
        options.update(m.get("options") or {})
        savedir = m.get("savedir") or "saves"
        self.core = Core(m["core"], m.get("sysdir") or "system", savedir, options, n64)
        self.core.load(m["rom"], "N64" if n64 else "")
        t0 = time.perf_counter()
        for _ in range(int(m.get("warmup", 6))):
            self.core.run_frame()
            self.core.audio = []
            if self.core.frame is not None and time.perf_counter() - t0 > 20:
                break
        gc.collect()
        gc.freeze()
        gc.set_threshold(100000, 50, 50)
        c = self.core
        w, h = (c.frame[1], c.frame[2]) if c.frame else (320, 240)
        return {"ok": True, "png": self.snapshot_png(1), "audio_rate": c.sample_rate, "audio_ch": 2,
                "fps": round(c.fps, 3), "w": w, "h": h, "core_hw_refused": c.hw_requested}

    def op_steps(self, m):
        steps = m.get("steps") or []
        c = self.core
        for mask, frames in steps:
            c.mask = int(mask)
            for _ in range(max(0, int(frames))):
                c.run_frame()
        c.mask = self.base_mask
        for _ in range(int(m.get("settle", 4))):
            c.run_frame()
        c.audio = []
        return {"ok": True}

    def op_bench(self, m):
        c = self.core
        n = max(10, min(int(m.get("frames", 120)), 600))
        c.mask = 0
        t0 = time.perf_counter()
        for _ in range(n):
            c.run_frame()
        dt = max(1e-6, time.perf_counter() - t0)
        c.audio = []
        return {"ok": True, "fps": n / dt, "frames": n, "sec": dt, "target": c.fps,
                "rdp": (self.core.options.get("mupen64plus-rdp-plugin") if self.n64 else None),
                "rsp": (self.core.options.get("mupen64plus-rsp-plugin") if self.n64 else None)}

    def handle(self, m):
        op = m.get("op")
        if op == "load":
            reply(self.op_load(m))
            return
        if self.core is None or not self.core.loaded:
            reply({"ok": False, "err": "no game loaded"})
            return
        if op == "input":
            self.base_mask = int(m.get("mask", 0))
            self.core.ax = float(m.get("ax", 0.0) or 0.0)
            self.core.ay = float(m.get("ay", 0.0) or 0.0)
        elif op == "seq":
            for mask, frames in m.get("steps") or []:
                self.seq.append([int(mask), max(1, int(frames))])
        elif op == "live":
            on = bool(m.get("on"))
            if on and not self.live:
                self.next_t = time.perf_counter()
            self.live = on
            self.encoder.enabled = on
            self.encoder.quality = max(30, min(int(m.get("quality", 72)), 95))
            want = max(1, int(m.get("fps", 60)))
            self.send_every = max(1, round(self.core.fps / want))
            if not on:
                self.core.audio = []
        elif op == "audio":
            self.audio_on = bool(m.get("on"))
        elif op == "steps":
            reply(self.op_steps(m))
        elif op == "snap":
            reply({"ok": True, "png": self.snapshot_png(int(m.get("scale", 2)))})
        elif op == "save":
            self.core.flush_srm(force=True)
            try:
                self.core.save_state(m["path"])
            except Exception as e:                      # SRAM was still saved
                log("state save skipped:", e)
            reply({"ok": True})
        elif op == "loadstate":
            self.core.load_state(m["path"])
            reply({"ok": True})
        elif op == "bench":
            reply(self.op_bench(m))
        elif op == "quit":
            self.stop = True
        else:
            reply({"ok": False, "err": f"unknown op {op}"})

    # ---- main loop
    def reader(self):
        try:
            while True:
                head = _PROTO_IN.read(4)
                if len(head) < 4:
                    break
                (n,) = struct.unpack("<I", head)
                data = b""
                while len(data) < n:
                    part = _PROTO_IN.read(n - len(data))
                    if not part:
                        raise EOFError
                    data += part
                self.q.put(pickle.loads(data))
        except Exception:
            pass
        self.q.put({"op": "quit"})

    def safe_handle(self, m):
        try:
            self.handle(m)
        except Exception as e:
            log(traceback.format_exc())
            if m.get("op") in ("load", "steps", "snap", "save", "loadstate", "bench"):
                reply({"ok": False, "err": f"{type(e).__name__}: {e}"[:300]})

    def main(self):
        threading.Thread(target=self.reader, daemon=True).start()
        while not self.stop:
            if self.live and self.core and self.core.loaded:
                try:
                    m = self.q.get_nowait()
                    self.safe_handle(m)
                    continue
                except queue.Empty:
                    pass
                now = time.perf_counter()
                dt = 1.0 / self.core.fps
                if now < self.next_t:
                    time.sleep(max(0.0, min(self.next_t - now - 0.0005, 0.004)))
                    continue
                behind = now - self.next_t
                try:
                    # Behind by more than a frame: skip drawing so the game catches up to real time instead of
                    # slowing down. Cores that ignore the hint just run as fast as they can.
                    self.run_once(render=behind < dt)
                except Exception as e:
                    log("run error:", e)
                    self.live = False
                    continue
                self.next_t += dt
                if now - self.next_t > 0.25:                    # fell badly behind: resync instead of sprinting
                    self.next_t = now
                if self.core.shutdown:
                    self.stop = True
            else:
                try:
                    m = self.q.get(timeout=1.0)
                except queue.Empty:
                    m = None
                if m:
                    self.safe_handle(m)
            if self.core and self.core.loaded and time.monotonic() - self.last_srm > 30:
                self.last_srm = time.monotonic()
                try:
                    self.core.flush_srm()
                except Exception as e:
                    log("save flush failed:", e)
        try:
            if self.core and self.core.loaded:
                self.core.flush_srm()
        except Exception:
            pass
        os._exit(0)


def bench_cli(core_path: str, rom_path: str):
    n64 = "mupen64plus" in os.path.basename(core_path).lower()
    opts = dict(N64_DEFAULT_OPTIONS) if n64 else {}
    opts.update(parse_option_overrides(os.environ.get("N64_CORE_OPTIONS", "")))
    core = Core(core_path, os.path.join(os.path.dirname(core_path), "system"),
                os.path.join(os.path.dirname(core_path), "saves"), opts, n64)
    core.load(rom_path, "")
    for _ in range(30):
        core.run_frame()
    n = 180
    t0 = time.perf_counter()
    for _ in range(n):
        core.run_frame()
        core.audio = []
    dt = time.perf_counter() - t0
    print(f"{n / dt:.1f} fps achieved (game wants {core.fps:.1f}); "
          f"rdp={opts.get('mupen64plus-rdp-plugin')} rsp={opts.get('mupen64plus-rsp-plugin')}; "
          f"hw_render_requested={core.hw_requested}")


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "--bench":
        bench_cli(sys.argv[2], sys.argv[3])
        sys.exit(0)
    _setup_pipes()
    Worker().main()
'''


def ensure_n64_worker() -> None:
    """Write the built-in N64 worker next to the bot if it's missing or outdated. A copy you edited yourself
    (one without the AUTO-WRITTEN line) is never overwritten."""
    path = WORKER_PATH_N64
    try:
        if os.path.exists(path):
            with open(path, "rb") as f:
                current = f.read()
            if b"AUTO-WRITTEN BY THE BOT" not in current[:400] or current == N64_WORKER_SOURCE.encode("utf-8"):
                return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".part"
        with open(tmp, "wb") as f:
            f.write(N64_WORKER_SOURCE.encode("utf-8"))
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"Could not write the N64 worker file: {e}")


def ensure_core(system: str) -> str:
    name = EMU_SYSTEMS[system]["core"]
    path = os.path.join(CORE_DIR, name + ".so")
    if os.path.exists(path):
        return path
    os.makedirs(CORE_DIR, exist_ok=True)
    url = f"{CORE_BASE_URL}/{name}.so.zip"
    with urllib.request.urlopen(url, timeout=60) as r:
        blob = r.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        member = next(m for m in z.namelist() if m.endswith(".so"))
        tmp = path + ".part"
        with z.open(member) as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst)
    os.replace(tmp, path)
    return path


def system_for(filename: str):
    ext = os.path.splitext(filename)[1].lower()
    for key, spec in EMU_SYSTEMS.items():
        if ext in spec["exts"]:
            return key
    return None


def clean_rom_filename(name: str):
    base = os.path.basename((name or "").replace("\\", "/"))
    stem, ext = os.path.splitext(base)
    stem = _ROM_BAD_CHARS.sub("", stem).strip(" .")[:60]
    return (stem + ext.lower()) if stem else None


def valid_rom_ref(rel: str):
    parts = (rel or "").split("/")
    if len(parts) != 2 or parts[0] not in EMU_SYSTEMS:
        return None
    if clean_rom_filename(parts[1]) != parts[1] or system_for(parts[1]) != parts[0]:
        return None
    return parts[0], parts[1]


def list_local_roms() -> set:
    found = set()
    for system in EMU_SYSTEMS:
        d = os.path.join(ROM_DIR, system)
        if os.path.isdir(d):
            for f in os.listdir(d):
                if system_for(f) == system:
                    found.add(f"{system}/{f}")
    return found


def all_roms() -> list:
    return sorted(list_local_roms() | _remote_roms, key=str.lower)


def refresh_remote_roms() -> None:
    global _remote_roms
    found = set()
    try:
        for system in EMU_SYSTEMS:
            for item in supabase.storage.from_(ROM_BUCKET).list(system):
                name = item.get("name", "")
                if system_for(name) == system:
                    found.add(f"{system}/{name}")
        _remote_roms = found
    except Exception as e:
        logger.warning(f"ROM bucket unavailable: {e}")


def ensure_rom_local(rel: str) -> str:
    ref = valid_rom_ref(rel)
    if not ref:
        raise ValueError("invalid ROM name")
    path = os.path.join(ROM_DIR, ref[0], ref[1])
    if os.path.exists(path):
        return path
    data = supabase.storage.from_(ROM_BUCKET).download(rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)
    return path


def add_rom_blocking(system: str, filename: str, data: bytes) -> bool:
    path = os.path.join(ROM_DIR, system, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    try:
        supabase.storage.from_(ROM_BUCKET).upload(
            path=f"{system}/{filename}", file=data,
            file_options={"content-type": "application/octet-stream", "upsert": "true"},
        )
        _remote_roms.add(f"{system}/{filename}")
        return True
    except Exception as e:
        logger.warning(f"Could not persist ROM: {e}")
        return False


def remove_rom_blocking(rel: str) -> None:
    ref = valid_rom_ref(rel)
    if not ref:
        return
    try:
        os.remove(os.path.join(ROM_DIR, ref[0], ref[1]))
    except FileNotFoundError:
        pass
    try:
        supabase.storage.from_(ROM_BUCKET).remove([rel])
    except Exception as e:
        logger.warning(f"Could not remove ROM: {e}")
    _remote_roms.discard(rel)


def sync_roms_to_disk() -> None:
    for rel in sorted(_remote_roms):
        try:
            ensure_rom_local(rel)
        except Exception:
            pass


def rom_limit(system: str) -> int:
    return int(EMU_SYSTEMS[system].get("max_mb", EMU_MAX_ROM_BYTES // 1048576)) * 1048576


_N64_MAGIC = (b"\x80\x37\x12\x40", b"\x37\x80\x40\x12", b"\x40\x12\x37\x80")   # .z64 / .v64 / .n64 byte orders


def check_rom_header(system: str, data: bytes):
    if len(data) < 1024:
        return "That file is too small to be a game."
    if system == "nes" and data[:4] != b"NES\x1a":
        return "That doesn't look like a valid .nes file (bad header)."
    if system == "n64" and data[:4] not in _N64_MAGIC:
        return "That doesn't look like a valid N64 ROM (.z64, .n64 or .v64)."
    return None


def rom_from_zip(blob: bytes):
    """First supported ROM inside a zip -> (filename, bytes). Reads with a hard cap so a zip bomb can't fill memory."""
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        for info in z.infolist():
            if info.is_dir() or info.flag_bits & 0x1:
                continue
            name = clean_rom_filename(info.filename)
            system = system_for(name) if name else None
            if not system:
                continue
            cap = rom_limit(system)
            with z.open(info) as f:
                data = f.read(cap + 1)
            if len(data) > cap:
                raise ValueError(f"The ROM inside the zip is bigger than the {cap // 1048576} MB limit.")
            return name, data
    raise ValueError("No supported ROM was found inside that zip.")


async def download_rom_url(url: str, max_bytes: int):
    """Download a public http(s) file with a size cap. Returns (filename_or_None, bytes)."""
    session = await radio_http()
    for _ in range(5):
        if not await radio_url_is_public(url):
            raise ValueError("That isn't a public web address.")
        async with session.get(url, allow_redirects=False,
                               timeout=aiohttp.ClientTimeout(total=240, sock_connect=10, sock_read=30)) as r:
            if r.status in (301, 302, 303, 307, 308) and r.headers.get("Location"):
                url = urljoin(url, r.headers["Location"])
                continue
            if r.status != 200:
                raise ValueError(f"The server answered HTTP {r.status}.")
            if int(r.headers.get("Content-Length") or 0) > max_bytes:
                raise ValueError(f"That file is bigger than the {max_bytes // 1048576} MB limit.")
            buf = bytearray()
            async for chunk in r.content.iter_chunked(1 << 16):
                buf += chunk
                if len(buf) > max_bytes:
                    raise ValueError(f"That file is bigger than the {max_bytes // 1048576} MB limit.")
            disp = r.headers.get("Content-Disposition", "")
            m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)', disp)
            from urllib.parse import unquote
            name = unquote(m.group(1)) if m else unquote(os.path.basename(urlparse(url).path))
            return name or None, bytes(buf)
    raise ValueError("Too many redirects.")


async def ingest_rom(raw_name: str, data: bytes):
    """Validate, unzip if needed, and store a ROM. Returns (ok, message)."""
    name = clean_rom_filename(raw_name)
    if name and name.endswith(".zip"):
        try:
            name, data = await asyncio.to_thread(rom_from_zip, data)
        except Exception as e:
            return False, f"\u274c {e}"
    system = system_for(name) if name else None
    if not system:
        exts = ", ".join(sorted(e for spec in EMU_SYSTEMS.values() for e in spec["exts"]))
        return False, f"\u274c Unsupported file. Supported: {exts} (or a .zip containing one)."
    if len(data) > rom_limit(system):
        return False, (f"\u274c That {EMU_SYSTEMS[system]['label']} ROM is {len(data) / 1048576:.1f} MB; "
                       f"the limit is {rom_limit(system) // 1048576} MB.")
    problem = check_rom_header(system, data)
    if problem:
        return False, f"\u274c {problem}"
    rel = f"{system}/{name}"
    replaced = rel in all_roms()
    saved_remote = await asyncio.to_thread(add_rom_blocking, system, name, data)
    note = "" if saved_remote else ("\n\u26a0\ufe0f Saved on the host only. The cloud backup failed (storage limit or "
                                    "connection), so it may vanish if the host resets.")
    return True, (f"\u2705 {'Replaced' if replaced else 'Added'} **{os.path.splitext(name)[0]}** "
                  f"({EMU_SYSTEMS[system]['label']}, {len(data) / 1048576:.1f} MB). Play it with `/emu-play`.{note}")


async def emu_rom_autocomplete(interaction: discord.Interaction, current: str):
    cur = current.lower()
    out = []
    for rel in all_roms():
        if cur in rel.lower():
            system, fname = rel.split("/", 1)
            out.append(app_commands.Choice(name=f"[{EMU_SYSTEMS[system]['label']}] {os.path.splitext(fname)[0]}"[:100], value=rel[:100]))
            if len(out) >= 25:
                break
    return out


def _proc_rss_mb(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1048576
    except Exception:
        return 0.0


class EmuClient:
    def __init__(self, ws, uid: int, name: str):
        self.ws, self.uid, self.name = ws, uid, name
        self.mask = 0
        self.ax = 0.0           # analog stick (N64), -1..1
        self.ay = 0.0
        self.latest = None
        self.texts = deque(maxlen=20)
        self.audio = deque(maxlen=30)
        self.want_audio = False
        self.evt = asyncio.Event()
        self.closing = False

    def offer_frame(self, jpeg: bytes):
        self.latest = jpeg
        self.evt.set()

    def offer_audio(self, chunk: bytes):
        if self.want_audio:
            self.audio.append(chunk)
            self.evt.set()

    def offer_text(self, obj: dict):
        self.texts.append(json.dumps(obj, separators=(",", ":")))
        self.evt.set()

    async def sender(self):
        try:
            while not self.ws.closed:
                await self.evt.wait()
                self.evt.clear()
                while self.texts:
                    await self.ws.send_str(self.texts.popleft())
                if self.audio:
                    chunks = []
                    while self.audio:
                        chunks.append(self.audio.popleft())
                    await self.ws.send_bytes(chunks[0] if len(chunks) == 1
                                             else b"A\x00" + b"".join(c[2:] for c in chunks))
                jpeg, self.latest = self.latest, None
                if jpeg:
                    await self.ws.send_bytes(jpeg)
                if self.closing:
                    await self.ws.close()
                    return
        except Exception:
            pass


class EmuSession:
    def __init__(self, channel_id: int, rel: str, starter_id: int):
        self.channel_id = channel_id
        self.rel = rel
        self.system, self.filename = rel.split("/", 1)
        self.title = os.path.splitext(self.filename)[0]
        self.label = EMU_SYSTEMS[self.system]["label"]
        self.starter_id = starter_id
        self.proc = None
        self.message_id = None
        self.view = None
        self.lock = asyncio.Lock()
        self.closed = False
        self.closing = False
        self.last_used = time.monotonic()
        self.hold_idx = 1
        self.last_mask = 0
        self.seq = 0
        self.inputs = 0
        self.err_tail = deque(maxlen=20)
        self.pending = None
        self._err_task = None
        self._reader_task = None
        self.clients: set = set()
        self.tokens: dict = {}
        self.live = False
        self.paused = False
        self.last_jpeg = None
        self.audio_rate = 0
        self.audio_ch = 2
        self._audio_sent = False
        self.stats: dict = {}
        self._sent_mask = 0
        self.last_edit = 0.0
        self._refresh_due = 0.0
        self._refresh_task = None
        self.footer = ""
        self.chat_backlog = 0
        self.can_delete = True

    @property
    def hold(self) -> int:
        return EMU_HOLD_STEPS[self.hold_idx]

    @property
    def state_path(self) -> str:
        return os.path.join(STATE_DIR, f"{self.channel_id}__{self.system}__{self.filename}.state")

    @property
    def auto_path(self) -> str:
        return os.path.join(STATE_DIR, f"{self.channel_id}__{self.system}__{self.filename}.auto")

    def touch(self):
        self.last_used = time.monotonic()

    async def spawn(self):
        env = dict(os.environ, MALLOC_ARENA_MAX="2", PYTHONUNBUFFERED="1")
        worker = EMU_SYSTEMS[self.system].get("worker", WORKER_PATH)
        if worker == WORKER_PATH_N64:
            await asyncio.to_thread(ensure_n64_worker)
        if not os.path.exists(worker):
            raise RuntimeError(f"{os.path.basename(worker)} is missing. Put it in the same folder as the bot file.")
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", worker,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=env,
        )
        self._err_task = asyncio.ensure_future(self._drain_stderr(self.proc))
        self._reader_task = asyncio.ensure_future(self._reader(self.proc))

    async def _drain_stderr(self, proc):
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode(errors="replace").rstrip()
                if text:
                    self.err_tail.append(text)
        except Exception:
            pass

    async def _death_reason(self) -> str:
        code = None
        try:
            code = await asyncio.wait_for(self.proc.wait(), timeout=1.5)
        except Exception:
            pass
        await asyncio.sleep(0.2)
        tail = " | ".join(list(self.err_tail)[-4:])[-350:]
        return f"the emulator process stopped (exit code {code})" + (f": {tail}" if tail else "")

    async def _reader(self, proc):
        try:
            while True:
                (n,) = struct.unpack("<I", await proc.stdout.readexactly(4))
                body = await proc.stdout.readexactly(n)
                kind, payload = body[:1], body[1:]
                if kind == b"J":
                    self._on_frame(body)
                elif kind == b"A":
                    self._on_audio(body)
                elif kind == b"R":
                    fut = self.pending
                    if fut is not None and not fut.done():
                        fut.set_result(pickle.loads(payload))
                elif kind == b"E":
                    pass
        except Exception:
            pass
        finally:
            if not self.closed:
                reason = await self._death_reason()
                fut = self.pending
                if fut is not None and not fut.done():
                    fut.set_result({"ok": False, "err": reason, "dead": True})
                asyncio.ensure_future(emu_close(self, "emulator crashed", autosave=False))

    def _write(self, msg: dict):
        data = pickle.dumps(msg, protocol=4)
        self.proc.stdin.write(struct.pack("<I", len(data)) + data)

    def send(self, msg: dict):
        if self.closed or self.proc is None or self.proc.returncode is not None:
            return
        try:
            self._write(msg)
        except Exception:
            pass

    async def call(self, **msg) -> dict:
        async with self.lock:
            if self.closed or self.proc is None or self._reader_task is None or self._reader_task.done():
                return {"ok": False, "err": "session ended", "dead": True}
            self.pending = asyncio.get_running_loop().create_future()
            try:
                self._write(msg)
                return await self.pending
            except Exception as e:
                return {"ok": False, "err": str(e), "dead": True}
            finally:
                self.pending = None

    def kill(self):
        proc, self.proc = self.proc, None
        if proc and proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    def _broadcast(self, obj: dict):
        for c in self.clients:
            c.offer_text(obj)

    def _on_frame(self, body: bytes):
        self.last_jpeg = body
        for c in self.clients:
            c.offer_frame(body)

    def _on_audio(self, body: bytes):
        for c in self.clients:
            c.offer_audio(body)

    def update_audio(self):
        want = EMU_AUDIO and any(c.want_audio for c in self.clients)
        if want != self._audio_sent:
            self._audio_sent = want
            self.send({"op": "audio", "on": want})

    def _set_live(self, on: bool):
        if on == self.live:
            return
        self.live = on
        if not on:
            self._audio_sent = False
        self.send({"op": "live", "on": on, "fps": EMU_STREAM_FPS,
                   "quality": EMU_JPEG_QUALITY, "afk": EMU_AFK_PAUSE})

    def _players_msg(self) -> dict:
        names = sorted({c.name for c in self.clients})
        return {"t": "players", "n": len(self.clients), "names": names[:20]}

    def add_client(self, c: EmuClient):
        self.clients.add(c)
        self.touch()
        c.offer_text({"t": "hello", "title": f"{self.label}: {self.title}", "paused": self.paused,
                      "audio": bool(EMU_AUDIO and self.audio_rate), "rate": self.audio_rate, "ch": self.audio_ch,
                      "sys": self.system})
        if self.last_jpeg:
            c.offer_frame(self.last_jpeg)
        self._broadcast(self._players_msg())
        self._set_live(True)

    def remove_client(self, c: EmuClient):
        self.clients.discard(c)
        self._broadcast(self._players_msg())
        self.update_audio()
        self.recalc_input()
        if not self.clients:
            self._set_live(False)

    def recalc_input(self):
        m, ax, ay = 0, 0.0, 0.0
        for c in self.clients:
            m |= c.mask
            if abs(c.ax) > abs(ax):
                ax = c.ax
            if abs(c.ay) > abs(ay):
                ay = c.ay
        key = (m, round(ax, 2), round(ay, 2))
        if key != self._sent_mask:
            self._sent_mask = key
            msg = {"op": "input", "mask": m}
            if ax or ay:
                msg["ax"], msg["ay"] = ax, ay
            self.send(msg)
            self.touch()
            self.schedule_refresh(0.6)

    def token_for(self, user) -> str:
        token = self.tokens.get(user.id)
        if not token:
            token = secrets.token_urlsafe(9)
            self.tokens[user.id] = token
            emu_web_tokens[token] = (self, user.id, user.display_name)
        return token

    async def press(self, steps: list) -> dict:
        self.touch()
        if self.live:
            self.send({"op": "seq", "steps": steps})
            self.schedule_refresh(sum(f for _, f in steps) / 60.0 + 0.1)
            return {"ok": True}
        res = await self.call(op="steps", steps=steps, settle=EMU_SETTLE_FRAMES)
        if res.get("ok"):
            self.schedule_refresh()
        return res

    def schedule_refresh(self, delay: float = 0.0):
        self._refresh_due = max(self._refresh_due, time.monotonic() + delay)
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.ensure_future(self._refresh())

    async def _refresh(self):
        while not self.closed:
            interval = EMU_EDIT_INTERVAL * (3 if self.clients else 1)
            due = max(self._refresh_due, self.last_edit + interval)
            wait = due - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            stamp = time.monotonic()
            res = await self.call(op="snap", scale=EMU_SCALE)
            if not res.get("ok"):
                return
            await self.edit_message(res["png"])
            self.last_edit = time.monotonic()
            if self._refresh_due <= stamp:
                return

    def make_embed(self, fname: str) -> discord.Embed:
        embed = discord.Embed(color=discord.Color.dark_grey())
        embed.set_author(name=f"{self.label}: {self.title}")
        embed.set_image(url=f"attachment://{fname}")
        embed.set_footer(text=self.footer[:400])
        return embed

    async def edit_message(self, png: bytes):
        ch = bot.get_channel(self.channel_id)
        if not ch or not self.message_id:
            return
        self.seq += 1
        fname = f"f{self.seq}.png"
        try:
            await ch.get_partial_message(self.message_id).edit(
                embed=self.make_embed(fname), attachments=[discord.File(io.BytesIO(png), filename=fname)], view=self.view)
        except Exception:
            pass


emu_web_tokens: dict = {}


async def emu_close(sess: EmuSession, reason: str, autosave: bool = True):
    if emu_sessions.get(sess.channel_id) is sess:
        emu_sessions.pop(sess.channel_id, None)
    if sess.closing:
        return
    sess.closing = True
    if autosave and sess.inputs and sess.proc and sess.proc.returncode is None:
        try:
            await asyncio.wait_for(sess.call(op="save", path=sess.auto_path), timeout=3)
        except Exception:
            pass
    sess.closed = True
    if sess._refresh_task:
        sess._refresh_task.cancel()
    for token in sess.tokens.values():
        emu_web_tokens.pop(token, None)
    for c in list(sess.clients):
        c.offer_text({"t": "end", "why": reason})
        c.closing = True
        c.evt.set()
    if sess.proc and sess.proc.returncode is None:
        try:
            sess.proc.stdin.close()
            await asyncio.wait_for(sess.proc.wait(), timeout=2)
        except Exception:
            pass
    sess.kill()
    if sess.message_id:
        ch = bot.get_channel(sess.channel_id)
        if ch:
            try:
                await ch.get_partial_message(sess.message_id).edit(content=f"⏹️ **{sess.label}: {sess.title}** ended.", view=None)
            except Exception:
                pass


async def emu_reaper_loop():
    while True:
        await asyncio.sleep(30)
        now = time.monotonic()
        for sess in list(emu_sessions.values()):
            if now - sess.last_used > EMU_IDLE_SECONDS:
                await emu_close(sess, "idle, memory freed")


def _kill_all_emulators():
    for sess in list(emu_sessions.values()):
        sess.kill()

atexit.register(_kill_all_emulators)


async def emu_startup():
    for d in (ROM_DIR, CORE_DIR, STATE_DIR):
        os.makedirs(d, exist_ok=True)
    await asyncio.to_thread(ensure_n64_worker)
    for system, spec in EMU_SYSTEMS.items():
        if spec.get("lazy_core"):
            continue
        try:
            await asyncio.to_thread(ensure_core, system)
        except Exception:
            pass
    await asyncio.to_thread(refresh_remote_roms)
    await asyncio.to_thread(sync_roms_to_disk)


_KEY_ALIASES = {"w": "up", "a": "left", "s": "down", "d": "right", "j": "b", "k": "a", "u": "y",
                "i": "x", "q": "l", "e": "r", "up": "up", "down": "down", "left": "left",
                "right": "right", "start": "start", "select": "select"}
_KEY_ONE = r"(?:start|select|up|down|left|right|[wasdjkuiqe])"
_KEY_GROUP = rf"{_KEY_ONE}(?:\+{_KEY_ONE})*(?:\d{{1,2}})?"
_KEY_FULL = re.compile(rf"{_KEY_GROUP}(?:[ ,]*{_KEY_GROUP})*")
_KEY_ITER = re.compile(rf"({_KEY_ONE}(?:\+{_KEY_ONE})*)(\d{{1,2}})?")


def parse_chat_keys(text: str, hold: int):
    t = (text or "").strip().lower()
    if EMU_CHAT_PREFIX:
        if not t.startswith(EMU_CHAT_PREFIX):
            return None
        t = t[len(EMU_CHAT_PREFIX):].strip()
    if not t or len(t) > 60 or not _KEY_FULL.fullmatch(t):
        return None
    steps, taps = [], 0
    for m in _KEY_ITER.finditer(t):
        mask = 0
        for key in m.group(1).split("+"):
            mask |= EMU_MASKS[_KEY_ALIASES[key]]
        for _ in range(max(1, min(int(m.group(2) or 1), 20))):
            if taps >= 24:
                break
            steps += [(mask, hold), (0, EMU_GAP_FRAMES)]
            taps += 1
    return steps or None


async def _try_delete(sess: EmuSession, message: discord.Message):
    try:
        await message.delete()
    except discord.Forbidden:
        sess.can_delete = False
    except Exception:
        pass


async def emu_chat_input(message: discord.Message) -> bool:
    if not EMU_CHAT_KEYS:
        return False
    sess = emu_sessions.get(message.channel.id)
    if sess is None or sess.closed:
        return False
    steps = parse_chat_keys(message.content, sess.hold)
    if not steps:
        return False
    if EMU_DELETE_KEYS and sess.can_delete:
        asyncio.ensure_future(_try_delete(sess, message))
    if sess.chat_backlog >= 4:
        return True
    sess.chat_backlog += 1
    try:
        sess.inputs += 1
        sess.footer = f"{message.author.display_name}: {message.content.strip()[:24]}  •  {sess.inputs} inputs"
        await sess.press(steps)
    finally:
        sess.chat_backlog -= 1
    return True


EMU_PAGE_HTML = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover,user-scalable=no">
<title>LUX-NET Arcade</title>
<style>
:root{color-scheme:dark}
html,body{margin:0;height:100%;background:#0b0c10;color:#e8e8ee;font:14px system-ui,sans-serif;overflow:hidden;touch-action:none;-webkit-user-select:none;user-select:none}
#app{display:flex;flex-direction:column;height:100%}
#bar{display:flex;gap:8px;align-items:center;padding:6px 10px;background:#14161d;font-size:13px;flex-wrap:wrap}
#title{font-weight:600;flex:1;min-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#bar button{background:#2a2d3a;color:inherit;border:0;border-radius:6px;padding:5px 9px;cursor:pointer;font:inherit}
#bar button.on{background:#5865f2}
#stage{flex:1;display:flex;align-items:center;justify-content:center;min-height:0;position:relative}
canvas{max-width:100%;max-height:100%;image-rendering:pixelated;background:#000;outline:none}
#msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;padding:20px;background:rgba(0,0,0,.6);font-size:16px}
.pad{display:none;justify-content:space-between;align-items:center;padding:8px 14px calc(8px + env(safe-area-inset-bottom));gap:10px}
@media (pointer:coarse){body.classic #pad-classic{display:flex}body.n64 #pad-n64{display:flex}}
.grp{display:grid;gap:6px}
.k{background:#2a2d3a;border-radius:10px;display:flex;align-items:center;justify-content:center;font-weight:700;touch-action:none;min-width:44px;min-height:44px}
.k.p{background:#5865f2}.k.g{background:#2f8f4e}.k.r{background:#b3403a}.k.y{background:#b8962e;color:#111}
#dpad{grid-template-columns:repeat(3,52px);grid-template-rows:repeat(3,52px)}
#face{grid-template-columns:repeat(3,52px);grid-template-rows:repeat(2,52px)}
#mid{display:flex;gap:8px}#mid .k{padding:10px 14px;font-size:12px;border-radius:16px}
#stick{width:120px;height:120px;border-radius:50%;background:#1d2030;border:2px solid #343850;position:relative;touch-action:none}
#knob{position:absolute;width:52px;height:52px;border-radius:50%;background:#5865f2;left:34px;top:34px;pointer-events:none}
#n64r{display:grid;grid-template-columns:repeat(3,44px);grid-template-rows:repeat(3,44px);gap:5px}
#n64mid{display:flex;flex-direction:column;gap:8px;align-items:center}
#n64btn{display:grid;grid-template-columns:repeat(2,56px);gap:8px}
</style></head><body class="classic"><div id="app">
<div id="bar"><span id="title">Connecting...</span><span id="players"></span><span id="fps"></span>
<button id="snd">Sound off</button><button id="fs">Fullscreen</button></div>
<div id="stage"><canvas id="cv" width="320" height="240" tabindex="0"></canvas><div id="msg">Connecting...</div></div>
<div class="pad" id="pad-classic"><div class="grp" id="dpad">
<div></div><div class="k" data-b="up">&#9650;</div><div></div>
<div class="k" data-b="left">&#9664;</div><div></div><div class="k" data-b="right">&#9654;</div>
<div></div><div class="k" data-b="down">&#9660;</div><div></div></div>
<div id="mid"><div class="k" data-b="select">SELECT</div><div class="k" data-b="start">START</div></div>
<div class="grp" id="face"><div class="k" data-b="x">X</div><div class="k" data-b="y">Y</div><div></div>
<div class="k p" data-b="b">B</div><div class="k p" data-b="a">A</div><div></div></div></div>
<div class="pad" id="pad-n64"><div id="stick"><div id="knob"></div></div>
<div id="n64mid"><div class="k" data-b="z">Z</div><div class="k" data-b="start">START</div><div style="display:flex;gap:8px"><div class="k" data-b="l">L</div><div class="k" data-b="r">R</div></div></div>
<div id="n64r"><div></div><div class="k y" data-b="cu">C&#9650;</div><div></div>
<div class="k y" data-b="cl">C&#9664;</div><div></div><div class="k y" data-b="cr">C&#9654;</div>
<div></div><div class="k y" data-b="cd">C&#9660;</div><div></div></div>
<div id="n64btn"><div class="k r" data-b="b">B</div><div class="k g" data-b="a">A</div></div></div>
</div>
<script>
const MASKS=__MASKS__;
const cv=document.getElementById('cv'),ctx=cv.getContext('2d'),msg=document.getElementById('msg');
const $=id=>document.getElementById(id);
const wsUrl=(location.protocol==='https:'?'wss://':'ws://')+location.host+location.pathname.replace(/\/$/,'')+'/ws';
let ws=null,ended=false,rate=0,ch=2,actx=null,nextT=0,soundOn=false,audioOk=false,sys='';
let pending=null,busy=false,frames=0,held={},mask=0,ax=0,ay=0,sentKey='',dirty=true;
function show(t){msg.textContent=t;msg.style.display=t?'flex':'none'}
function connect(){
  ws=new WebSocket(wsUrl);ws.binaryType='arraybuffer';
  ws.onopen=()=>{show('');sentKey='';dirty=true;if(soundOn)ws.send(JSON.stringify({t:'audio',on:true}))};
  ws.onclose=()=>{if(!ended){show('Disconnected. Reconnecting...');setTimeout(connect,1500)}};
  ws.onmessage=e=>{
    if(typeof e.data==='string'){text(JSON.parse(e.data));return}
    const u=new Uint8Array(e.data);
    if(u[0]===74){pending=u.subarray(1);if(!busy)decode()}
    else if(u[0]===65&&soundOn)pcm(u.subarray(2));
  };
}
function text(o){
  if(o.t==='hello'){$('title').textContent=o.title;rate=o.rate||0;ch=o.ch||2;audioOk=!!(o.audio&&rate);
    $('snd').style.display=audioOk?'':'none';sys=o.sys||'';
    document.body.className=sys==='n64'?'n64':'classic'}
  else if(o.t==='players'){$('players').textContent=o.n+(o.n===1?' player':' players')+(o.names&&o.names.length?': '+o.names.join(', '):'')}
  else if(o.t==='end'){ended=true;show('Game ended ('+(o.why||'closed')+'). You can close this tab.')}
}
async function decode(){
  busy=true;
  while(pending){
    const b=pending;pending=null;
    try{const bm=await createImageBitmap(new Blob([b]));
      if(cv.width!==bm.width||cv.height!==bm.height){cv.width=bm.width;cv.height=bm.height}
      ctx.drawImage(bm,0,0);bm.close();frames++}catch(_){}
  }
  busy=false;
}
setInterval(()=>{$('fps').textContent=frames+' fps';frames=0},1000);
function pcm(u){
  if(!actx||!rate)return;
  const n=u.byteLength-(u.byteLength%(2*ch));if(n<=0)return;
  const i16=new Int16Array(u.buffer.slice(u.byteOffset,u.byteOffset+n));
  const fr=i16.length/ch,buf=actx.createBuffer(ch,fr,rate);
  for(let c=0;c<ch;c++){const d=buf.getChannelData(c);for(let i=0;i<fr;i++)d[i]=i16[i*ch+c]/32768}
  const s=actx.createBufferSource();s.buffer=buf;s.connect(actx.destination);
  const now=actx.currentTime;if(nextT<now+0.03||nextT>now+0.4)nextT=now+0.06;
  s.start(nextT);nextT+=buf.duration;
}
$('snd').onclick=()=>{
  soundOn=!soundOn;$('snd').textContent=soundOn?'Sound on':'Sound off';$('snd').classList.toggle('on',soundOn);
  if(soundOn){actx=actx||new (window.AudioContext||window.webkitAudioContext)();actx.resume();nextT=0}
  if(ws&&ws.readyState===1)ws.send(JSON.stringify({t:'audio',on:soundOn}));
};
$('fs').onclick=()=>{const el=document.documentElement;(document.fullscreenElement?document.exitFullscreen():el.requestFullscreen&&el.requestFullscreen())};
// ---- input: everything funnels into held{} (buttons) and ax/ay (analog stick); one send per animation frame, only on change
function flush(){
  if(!dirty)return;dirty=false;
  let m=0;for(const k in held)if(held[k])m|=MASKS[k]||0;
  const rx=Math.round(ax*100)/100,ry=Math.round(ay*100)/100,key=m+'|'+rx+'|'+ry;
  if(key!==sentKey&&ws&&ws.readyState===1){sentKey=key;mask=m;ws.send(JSON.stringify({t:'in',m:m,ax:rx,ay:ry}))}
}
function setBtn(n,d){if(!!held[n]!==d){held[n]=d;dirty=true}}
function setStick(x,y){x=Math.max(-1,Math.min(1,x));y=Math.max(-1,Math.min(1,y));if(x!==ax||y!==ay){ax=x;ay=y;dirty=true}}
const KEYS_CLASSIC={ArrowUp:'up',ArrowDown:'down',ArrowLeft:'left',ArrowRight:'right',w:'up',s:'down',a:'left',d:'right',
  j:'b',z:'b',k:'a',x:'a',u:'y',i:'x',q:'l',e:'r',Enter:'start',' ':'start',Shift:'select',Backspace:'select'};
// N64: WASD = analog stick, arrows = C buttons, K = A, J = B, Space/Z = Z trigger
const KEYS_N64={w:'su',s:'sd',a:'sl',d:'sr',ArrowUp:'cu',ArrowDown:'cd',ArrowLeft:'cl',ArrowRight:'cr',
  k:'a',j:'b',' ':'z',z:'z',q:'l',e:'r',Enter:'start',t:'up',g:'down',f:'left',h:'right'};
function keyName(e){const m=sys==='n64'?KEYS_N64:KEYS_CLASSIC;return m[e.key]||m[(e.key||'').toLowerCase()]}
addEventListener('keydown',e=>{const n=keyName(e);if(n){e.preventDefault();setBtn(n,true)}});
addEventListener('keyup',e=>{const n=keyName(e);if(n){e.preventDefault();setBtn(n,false)}});
addEventListener('blur',()=>{held={};setStick(0,0);dirty=true});
document.querySelectorAll('.k[data-b]').forEach(el=>{
  const n=el.dataset.b;
  el.addEventListener('pointerdown',e=>{e.preventDefault();el.setPointerCapture(e.pointerId);setBtn(n,true)});
  const up=e=>{e.preventDefault();setBtn(n,false)};
  el.addEventListener('pointerup',up);el.addEventListener('pointercancel',up);
});
// touch analog stick
const stick=$('stick'),knob=$('knob');let stickId=null;
function moveStick(e){const r=stick.getBoundingClientRect(),R=r.width/2;
  let dx=(e.clientX-(r.left+R))/R,dy=(e.clientY-(r.top+R))/R;const len=Math.hypot(dx,dy);if(len>1){dx/=len;dy/=len}
  knob.style.left=(34+dx*34)+'px';knob.style.top=(34+dy*34)+'px';setStick(dx,dy)}
stick.addEventListener('pointerdown',e=>{e.preventDefault();stickId=e.pointerId;stick.setPointerCapture(e.pointerId);moveStick(e)});
stick.addEventListener('pointermove',e=>{if(e.pointerId===stickId)moveStick(e)});
const endStick=e=>{if(e.pointerId===stickId){stickId=null;knob.style.left='34px';knob.style.top='34px';setStick(0,0)}};
stick.addEventListener('pointerup',endStick);stick.addEventListener('pointercancel',endStick);
// gamepad (standard mapping)
const GP_CLASSIC={0:'b',1:'a',2:'y',3:'x',4:'l',5:'r',8:'select',9:'start',12:'up',13:'down',14:'left',15:'right'};
const GP_N64={0:'a',2:'b',1:'cr',3:'cu',4:'l',5:'r',6:'z',7:'z',9:'start',12:'up',13:'down',14:'left',15:'right'};
const gpHeld={};let gpActive=false;
function dz(v){const a=Math.abs(v);return a<0.15?0:Math.sign(v)*(a-0.15)/0.85}
function pollPad(){
  const p=(navigator.getGamepads?navigator.getGamepads():[])[0];
  if(!p&&gpActive){gpActive=false;for(const n in gpHeld){if(gpHeld[n]){gpHeld[n]=false;setBtn(n,false)}}setStick(0,0)}
  if(p){
    gpActive=true;
    const map=sys==='n64'?GP_N64:GP_CLASSIC,cur={};
    for(const i in map)if(p.buttons[i]&&p.buttons[i].pressed)cur[map[i]]=true;
    const lx=p.axes[0]||0,ly=p.axes[1]||0,rx=p.axes[2]||0,ry=p.axes[3]||0;
    if(sys==='n64'){
      setStick(dz(lx),dz(ly));
      if(rx<-.5)cur.cl=true;if(rx>.5)cur.cr=true;if(ry<-.5)cur.cu=true;if(ry>.5)cur.cd=true;
    }else{if(lx<-.5)cur.left=true;if(lx>.5)cur.right=true;if(ly<-.5)cur.up=true;if(ly>.5)cur.down=true}
    for(const n of new Set([...Object.keys(gpHeld),...Object.keys(cur)])){
      const d=!!cur[n];if(!!gpHeld[n]!==d){gpHeld[n]=d;setBtn(n,d)}
    }
  }
  flush();requestAnimationFrame(pollPad);
}
pollPad();connect();cv.focus();
</script></body></html>
'''.replace("__MASKS__", json.dumps(EMU_MASKS))


async def web_home(request):
    return web.Response(text="LUX-NET Bridge Bot is Online!")


async def web_emu_page(request):
    if request.match_info["token"] not in emu_web_tokens:
        return web.Response(status=404, text="Expired.")
    return web.Response(text=EMU_PAGE_HTML, content_type="text/html")


async def web_emu_ws(request):
    info = emu_web_tokens.get(request.match_info["token"])
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=4096)
    await ws.prepare(request)
    if not info:
        await ws.close()
        return ws
    sess, uid, name = info
    if sess.closed or len(sess.clients) >= EMU_MAX_VIEWERS:
        await ws.send_str(json.dumps({"t": "end", "why": "game ended" if sess.closed else "too many players"}))
        await ws.close()
        return ws
    client = EmuClient(ws, uid, name)
    sess.add_client(client)
    sender = asyncio.ensure_future(client.sender())
    try:
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except ValueError:
                continue
            kind = data.get("t")
            if kind == "in":
                try:
                    mask = int(data.get("m", 0)) & 0x1FFFFF
                    ax = max(-1.0, min(1.0, float(data.get("ax", 0) or 0)))
                    ay = max(-1.0, min(1.0, float(data.get("ay", 0) or 0)))
                except (TypeError, ValueError):
                    continue
                if (mask, ax, ay) != (client.mask, client.ax, client.ay):
                    client.mask, client.ax, client.ay = mask, ax, ay
                    if mask or ax or ay:
                        sess.inputs += 1
                    sess.recalc_input()
            elif kind == "audio":
                client.want_audio = bool(data.get("on")) and EMU_AUDIO
                sess.update_audio()
    finally:
        client.closing = True
        client.evt.set()
        sender.cancel()
        client.mask, client.ax, client.ay = 0, 0.0, 0.0
        sess.remove_client(client)
    return ws


class EmuButton(discord.ui.Button):
    def __init__(self, action: str, label: str, style, row: int, disabled: bool = False, emoji=None):
        super().__init__(label=label or None, style=style, row=row, disabled=disabled, emoji=emoji)
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        await self.view.handle(interaction, self.action)


class EmuView(discord.ui.View):
    def __init__(self, sess: EmuSession):
        super().__init__(timeout=None)
        self.sess = sess
        snes = sess.system == "snes"
        S, P, G, D = (discord.ButtonStyle.secondary, discord.ButtonStyle.primary,
                      discord.ButtonStyle.success, discord.ButtonStyle.danger)
        if sess.system == "n64":
            layout = [
                [("l", "L", S, False), ("su", "⬆", S, False), ("r", "R", S, False), ("cu", "C⬆", P, False), ("z", "Z", D, False)],
                [("sl", "⬅", S, False), ("hold", "", S, False), ("sr", "➡", S, False), ("cl", "C⬅", P, False), ("cr", "C➡", P, False)],
                [("b", "B", D, False), ("sd", "⬇", S, False), ("a", "A", G, False), ("cd", "C⬇", P, False), ("start", "Start", G, False)],
                [("live", "Play live", P, False), ("save", "Save", S, False), ("load", "Load", S, False), ("quit", "Quit", D, False)],
            ]
        else:
            layout = [
                [("l", "L", S, not snes), ("up", "⬆", S, False), ("r", "R", S, not snes), ("select", "Select", S, False), ("start", "Start", G, False)],
                [("left", "⬅", S, False), ("down", "⬇", S, False), ("right", "➡", S, False), ("x", "X", P, not snes), ("y", "Y", P, not snes)],
                [("b", "B", D, False), ("a", "A", G, False), ("ab", "A+B", P, False), ("hold", "", S, False), ("clip", "\U0001F39E Clip", S, False)],
                [("live", "Play live", P, False), ("save", "Save", S, False), ("load", "Load", S, False), ("quit", "Quit", D, False)],
            ]
        self.hold_btn = None
        for row, items in enumerate(layout):
            for action, label, style, disabled in items:
                btn = EmuButton(action, label, style, row, disabled)
                if action == "hold":
                    self.hold_btn = btn
                    self._relabel_hold()
                self.add_item(btn)

    def _relabel_hold(self):
        self.hold_btn.label = f"Hold {self.sess.hold}f"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if is_user_banned(interaction.user.id):
            await interaction.response.send_message("❌ Banned.", ephemeral=True)
            return False
        return True

    async def handle(self, interaction: discord.Interaction, action: str):
        sess = self.sess
        if sess.closed:
            await interaction.response.send_message("⏹️ Session ended.", ephemeral=True)
            return
        sess.touch()

        if action in EMU_MASKS:
            await interaction.response.defer()
            sess.last_mask = EMU_MASKS[action]
            sess.inputs += 1
            sess.footer = f"{interaction.user.display_name}: {action.upper()}"
            await sess.press([(sess.last_mask, sess.hold), (0, EMU_GAP_FRAMES)])
        elif action == "hold":
            sess.hold_idx = (sess.hold_idx + 1) % len(EMU_HOLD_STEPS)
            self._relabel_hold()
            await interaction.response.edit_message(view=self)
        elif action == "live":
            if not EMU_WEB_BASE:
                await interaction.response.send_message(
                    "\u274c Live play needs the bot's public web address. Set PUBLIC_URL (or RENDER_EXTERNAL_URL) "
                    "to your bot's https:// URL and restart.", ephemeral=True)
                return
            token = sess.token_for(interaction.user)
            keys = ("WASD = stick, arrows = C buttons, K = A, J = B, Space = Z, Q/E = L/R, Enter = Start"
                    if sess.system == "n64" else
                    "arrows or WASD, J/Z = B, K/X = A, U = Y, I = X, Q/E = L/R, Enter = Start, Shift = Select")
            await interaction.response.send_message(
                f"\U0001F3AE **Play live ({EMU_STREAM_FPS} fps):** {EMU_WEB_BASE}/emu/{token}\n"
                f"Keys: {keys}. Controllers and touch controls work too. The link is personal to you and stops "
                "working when the game ends.", ephemeral=True)
        elif action in ("save", "load") and sess.system == "n64":
            await interaction.response.defer(ephemeral=True)
            if action == "save":
                res = await sess.call(op="save", path=sess.state_path)
                text = "\U0001F4BE Game saved." if res.get("ok") else f"\u274c Couldn't save: {res.get('err')}"
            elif os.path.exists(sess.state_path):
                res = await sess.call(op="loadstate", path=sess.state_path)
                if res.get("ok"):
                    sess.schedule_refresh()
                text = "\U0001F4C2 Save loaded." if res.get("ok") else f"\u274c Couldn't load: {res.get('err')}"
            else:
                text = "\u26a0\ufe0f There's no save for this game yet. Press Save first."
            await interaction.followup.send(text, ephemeral=True)
        elif action == "quit":
            await interaction.response.defer()
            await emu_close(sess, "stopped")
        else:
            await interaction.response.send_message("\u26a0\ufe0f That button isn't available for this game.", ephemeral=True)


@bot.tree.command(name="emu-play", description="Play a NES, SNES or N64 game.")
@app_commands.describe(game="Pick a game", resume="Continue previous")
@app_commands.autocomplete(game=emu_rom_autocomplete)
async def emu_play(interaction: discord.Interaction, game: str, resume: bool = True):
    if not interaction.guild:
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return
    ref = valid_rom_ref(game)
    if not ref or game not in all_roms():
        await interaction.response.send_message("❌ Pick a valid game.", ephemeral=True)
        return
    if interaction.channel.id in emu_sessions:
        await interaction.response.send_message("⚠️ Game already running.", ephemeral=True)
        return
    if len(emu_sessions) >= MAX_EMU_SESSIONS:
        await interaction.response.send_message(
            f"⚠️ {MAX_EMU_SESSIONS} games are already running. Try again when one finishes.", ephemeral=True)
        return
    if ref[0] == "n64" and sum(1 for x in emu_sessions.values() if x.system == "n64") >= EMU_MAX_N64:
        await interaction.response.send_message(
            "⚠️ An N64 game is already running. N64 is heavy, so only one runs at a time. Stop it with `/emu-stop` first.",
            ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    sess = EmuSession(interaction.channel.id, game, interaction.user.id)
    emu_sessions[sess.channel_id] = sess
    try:
        rom_path = await asyncio.to_thread(ensure_rom_local, game)
        core_path = await asyncio.to_thread(ensure_core, sess.system)
        await sess.spawn()
        load_args = dict(core=core_path, rom=rom_path, sysdir=os.path.join(EMU_BASE, "system"),
                         savedir=os.path.join(STATE_DIR, "sram"))
        if sess.system == "n64":
            load_args["system"] = "n64"
        res = await sess.call(op="load", **load_args)
        if not res.get("ok"):
            raise RuntimeError(res.get("err"))
        sess.audio_rate = int(res.get("audio_rate") or res.get("rate") or 0)
        sess.audio_ch = int(res.get("audio_ch") or 2)
        if sess.system == "n64" and resume and os.path.exists(sess.auto_path):
            await sess.call(op="loadstate", path=sess.auto_path)
        sess.view = EmuView(sess)
        sess.seq += 1
        fname = f"f{sess.seq}.png"
        msg = await interaction.channel.send(embed=sess.make_embed(fname),
                                             file=discord.File(io.BytesIO(res.get("png", b"")), filename=fname), view=sess.view)
        sess.message_id = msg.id
        tip = " N64 is best played with **Play live** on the message." if sess.system == "n64" else ""
        await interaction.followup.send("✅ Game started!" + tip, ephemeral=True)
    except Exception as e:
        emu_sessions.pop(sess.channel_id, None)
        await interaction.followup.send(f"❌ Failed: {e}", ephemeral=True)


@bot.tree.command(name="emu-games", description="List games.")
async def emu_games(interaction: discord.Interaction):
    roms = all_roms()
    groups: dict = {}
    for rel in roms:
        system, fname = rel.split("/", 1)
        groups.setdefault(system, []).append(os.path.splitext(fname)[0])
    embed = discord.Embed(title=f"Library ({len(roms)})", color=discord.Color.blurple())
    for system, names in groups.items():
        text = "\n".join(names)
        if len(text) > 1000:
            text = text[:990].rsplit("\n", 1)[0] + f"\n... and more ({len(names)} total)"
        embed.add_field(name=f"{EMU_SYSTEMS[system]['label']} ({len(names)})", value=text, inline=False)
    if not roms:
        embed.description = "No games yet. Add one with `/emu-add` or `/emu-add-url`."
    await interaction.response.send_message(embed=embed, ephemeral=True)


async def _can_add_roms(interaction: discord.Interaction) -> bool:
    if EMU_ADD_OPEN or await check_is_owner(interaction):
        return True
    await interaction.response.send_message("\u274c Only the bot owner can add games.", ephemeral=True)
    return False


@bot.tree.command(name="emu-add", description="Upload a game file (NES, SNES or N64; zips work too).")
@app_commands.describe(rom="The game file: .nes, .sfc/.smc, .z64/.n64/.v64, or a .zip containing one")
async def emu_add(interaction: discord.Interaction, rom: discord.Attachment):
    if not await _can_add_roms(interaction):
        return
    biggest = max(rom_limit(k) for k in EMU_SYSTEMS)
    if rom.size > biggest + (1 << 20):
        await interaction.response.send_message(
            f"\u274c That file is {rom.size / 1048576:.1f} MB, over the {biggest // 1048576} MB limit.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        data = await rom.read()
    except Exception as e:
        await interaction.followup.send(f"\u274c Couldn't download that file: {e}", ephemeral=True)
        return
    _ok, text = await ingest_rom(rom.filename, data)
    await interaction.followup.send(text, ephemeral=True)


@bot.tree.command(name="emu-add-url", description="Add a game from a direct download link (good for big N64 files).")
@app_commands.describe(url="A direct https:// link to the game file (.z64, .nes, .sfc, or a .zip)")
async def emu_add_url(interaction: discord.Interaction, url: str):
    if not await _can_add_roms(interaction):
        return
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        await interaction.response.send_message("\u274c That needs to start with http:// or https://", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        name, data = await download_rom_url(url, max(rom_limit(k) for k in EMU_SYSTEMS) + (1 << 20))
    except Exception as e:
        await interaction.followup.send(f"\u274c Download failed: {e}", ephemeral=True)
        return
    _ok, text = await ingest_rom(name or "", data)
    await interaction.followup.send(text, ephemeral=True)


@bot.tree.command(name="emu-remove", description="Remove a game from the library (owner only).")
@app_commands.describe(game="Game to remove")
@app_commands.autocomplete(game=emu_rom_autocomplete)
async def emu_remove(interaction: discord.Interaction, game: str):
    if not await check_is_owner(interaction):
        await interaction.response.send_message("\u274c Only the bot owner can remove games.", ephemeral=True)
        return
    if not valid_rom_ref(game) or game not in all_roms():
        await interaction.response.send_message("\u274c Pick a game from the list.", ephemeral=True)
        return
    if any(x.rel == game for x in emu_sessions.values()):
        await interaction.response.send_message("\u26a0\ufe0f That game is running right now. Stop it first.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await asyncio.to_thread(remove_rom_blocking, game)
    await interaction.followup.send(f"\U0001F5D1\uFE0F Removed **{os.path.splitext(game.split('/', 1)[1])[0]}**.", ephemeral=True)


@bot.tree.command(name="emu-bench", description="Owner: measure how fast a game really runs on this host.")
@app_commands.describe(game="Game to test (an N64 game shows whether 60 fps is realistic)")
@app_commands.autocomplete(game=emu_rom_autocomplete)
async def emu_bench(interaction: discord.Interaction, game: str):
    if not await check_is_owner(interaction):
        await interaction.response.send_message("\u274c Owner only.", ephemeral=True)
        return
    ref = valid_rom_ref(game)
    if not ref or game not in all_roms():
        await interaction.response.send_message("\u274c Pick a game from the list.", ephemeral=True)
        return
    if sum(1 for x in emu_sessions.values() if x.system == ref[0]) >= (EMU_MAX_N64 if ref[0] == "n64" else MAX_EMU_SESSIONS):
        await interaction.response.send_message("\u26a0\ufe0f Stop the running game first so the test is accurate.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    sess = EmuSession(-interaction.user.id, game, interaction.user.id)       # private, never shown in a channel
    try:
        rom_path = await asyncio.to_thread(ensure_rom_local, game)
        core_path = await asyncio.to_thread(ensure_core, sess.system)
        await sess.spawn()
        load_args = dict(core=core_path, rom=rom_path, sysdir=os.path.join(EMU_BASE, "system"),
                         savedir=os.path.join(STATE_DIR, "sram"))
        if sess.system == "n64":
            load_args["system"] = "n64"
        res = await asyncio.wait_for(sess.call(op="load", **load_args), 240)
        if not res.get("ok"):
            raise RuntimeError(res.get("err"))
        res = await asyncio.wait_for(sess.call(op="bench", frames=120), 300)
        if not res.get("ok"):
            raise RuntimeError(res.get("err"))
        fps, target = float(res["fps"]), float(res.get("target") or 60)
        pct = fps / target * 100
        if pct >= 98:
            verdict = f"\u2705 Full speed ({target:.0f} fps) with room to spare."
        elif pct >= 60:
            verdict = ("\U0001F7E1 Playable but below full speed. The live player keeps the game at the right speed by "
                       "skipping drawn frames, so expect fewer than 60 visible frames per second.")
        else:
            verdict = ("\U0001F534 Too slow for comfortable play on this host. A machine with more and faster CPU cores, "
                       "or a GPU, is needed for 60 fps.")
        extra = f"\nRenderer: `{res['rdp']}` / RSP: `{res['rsp']}`" if res.get("rdp") else ""
        await interaction.followup.send(
            f"**{ref[1]}** ran at **{fps:.1f} fps** (the game wants {target:.1f}, {pct:.0f}%).{extra}\n{verdict}", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"\u274c Benchmark failed: {e}", ephemeral=True)
    finally:
        sess.closed = True
        sess.kill()


@bot.tree.command(name="emu-stop", description="Stop the game.")
async def emu_stop(interaction: discord.Interaction):
    sess = emu_sessions.get(interaction.channel.id)
    if not sess:
        await interaction.response.send_message("⚠️ No game running.", ephemeral=True)
        return
    await interaction.response.send_message("⏹️ Stopping...", ephemeral=True)
    await emu_close(sess, "stopped")

# ------------------------------------------------------------------------------
# RADIO: MULTI-SOURCE ADAPTER LAYER & FREQUENCY LOOKUP
# ------------------------------------------------------------------------------
# Every source (Radio Browser, SomaFM, Radio Garden, Wikidata, FCC file) is a small adapter that
# returns stations in ONE common dict format. Results are merged, de-duplicated (same stream URL, or
# same name + country), ranked, and shown with their source. A source that errors out or changes its
# API is skipped (circuit breaker) and the others keep working.
RADIO_DEFAULT_HOSTS = "de1.api.radio-browser.info,de2.api.radio-browser.info,fi1.api.radio-browser.info,at1.api.radio-browser.info,nl1.api.radio-browser.info"
RADIO_HOSTS = [h.strip() for h in os.environ.get("RADIO_HOSTS", RADIO_DEFAULT_HOSTS).split(",") if h.strip()]
RADIO_UA = "LUX-NET-Bridge/2.0 (Discord bot; radio directory client)"
RADIO_PROXY_MAX = int(os.environ.get("RADIO_PROXY_MAX", "6"))
RADIO_GARDEN_ON = os.environ.get("RADIO_GARDEN", "1") == "1"        # unofficial API: optional
RADIO_WIKIDATA_ON = os.environ.get("RADIO_WIKIDATA", "1") == "1"
RADIO_FCC_FILE = os.environ.get("FCC_FM_FILE", "").strip()           # CSV: callsign,frequency,city,state

radio_sessions: dict = {}
_radio_cache: dict = {}
_radio_by_id: OrderedDict = OrderedDict()     # station id -> station dict (lets menus/autocomplete refer to ids)
_radio_http = None
_radio_proxy_active = 0
_radio_hosts_cache = {"hosts": None, "at": 0.0}
_cc_names: dict = {}
_src_health: dict = {}


async def radio_http():
    global _radio_http
    if _radio_http is None or _radio_http.closed:
        _radio_http = aiohttp.ClientSession(headers={"User-Agent": RADIO_UA})
    return _radio_http


async def radio_shutdown():
    if _radio_http is not None and not _radio_http.closed:
        await _radio_http.close()


async def http_json(url: str, params: dict = None, headers: dict = None, timeout: float = 8, ttl: int = 0):
    key = url + "?" + json.dumps(params or {}, sort_keys=True)
    now = time.monotonic()
    hit = _radio_cache.get(key)
    if ttl and hit and hit[0] > now:
        return hit[1]
    session = await radio_http()
    async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        data = await r.json(content_type=None)
    if ttl:
        _radio_cache[key] = (now + ttl, data)
        if len(_radio_cache) > 600:
            for k in list(_radio_cache)[:200]:
                _radio_cache.pop(k, None)
    return data


# ---------------------------------------------------------------- source health (circuit breaker)
def _health(name: str) -> dict:
    return _src_health.setdefault(name, {"fails": 0, "until": 0.0, "err": "", "ok": 0})


async def run_source(name: str, factory, timeout: float, track: bool = True) -> list:
    h = _health(name)
    if time.monotonic() < h["until"]:
        return []
    try:
        res = await asyncio.wait_for(factory(), timeout)
        if track:
            h["fails"] = 0
            h["ok"] += 1
        return res or []
    except Exception as e:
        if track:
            h["fails"] += 1
            h["err"] = f"{type(e).__name__}: {e}"[:120]
            if h["fails"] >= 3:
                h["until"] = time.monotonic() + 300     # skip this source for 5 minutes
                logger.warning(f"Radio source '{name}' disabled for 5 min: {h['err']}")
        return []


# ---------------------------------------------------------------- frequency parsing
_RE_FM_DEC = re.compile(r"(?<![\d.,])(\d{2,3})[.,](\d{1,2})(?!\d|\s*(?:k|kb|kbps|%))", re.I)
_RE_FM_INT = re.compile(r"(?<![\d.,])(\d{2,3})\s*(?:fm|mhz|f\.m\.)(?![a-z])", re.I)
_RE_FM_PRE = re.compile(r"\bfm\s*(\d{2,3})(?:[.,](\d{1,2}))?(?!\d)", re.I)
_RE_AM = re.compile(r"(?<![\d.,])(\d{3,4})\s*(?:am|khz)(?![a-z])|\bam\s*(\d{3,4})(?!\d)", re.I)
_RE_FREQ_Q = re.compile(r"^\s*(am|fm)?\s*(\d{2,4}(?:[.,]\d{1,2})?)\s*(fm|mhz|am|khz)?\s*$", re.I)


def _fm_ok(v: float) -> bool:
    return 64.0 <= v <= 108.0


def extract_frequencies(name: str, tags: str = "") -> dict:
    """Frequencies written in a station's name/tags -> {(band, value): 'name'}."""
    text = f"{name} {tags}"
    out = {}
    for m in _RE_FM_DEC.finditer(text):
        v = float(f"{m.group(1)}.{m.group(2)}")
        if _fm_ok(v):
            out[("FM", round(v, 2))] = "name"
    for m in _RE_FM_INT.finditer(text):
        v = float(m.group(1))
        if _fm_ok(v):
            out[("FM", v)] = "name"
    for m in _RE_FM_PRE.finditer(text):
        v = float(f"{m.group(1)}.{m.group(2) or 0}")
        if _fm_ok(v):
            out[("FM", round(v, 2))] = "name"
    for m in _RE_AM.finditer(text):
        v = float(m.group(1) or m.group(2))
        if 500 <= v <= 1800:
            out[("AM", v)] = "name"
    return out


def parse_frequency(text: str):
    """'101.1', '101.1 FM', 'FM 101,1', '1010 AM' -> ('FM', 101.1) / ('AM', 1010.0); None if not a frequency."""
    m = _RE_FREQ_Q.match(text or "")
    if not m:
        return None
    pre, num, unit = (m.group(1) or "").lower(), m.group(2), (m.group(3) or "").lower()
    has_dec = bool(re.search(r"[.,]", num))
    val = float(num.replace(",", "."))
    if "am" in (pre, unit) or unit == "khz":
        band = "AM"
    elif "fm" in (pre, unit) or unit == "mhz":
        band = "FM"
    elif _fm_ok(val):
        band = "FM"
    elif 500 <= val <= 1800 and not has_dec:
        band = "AM"
    else:
        return None
    if band == "FM" and not _fm_ok(val):
        return None
    if band == "AM" and not (500 <= val <= 1800):
        return None
    return band, round(val, 2)


def fmt_freq(band: str, val: float) -> str:
    return f"{val:g} FM" if band == "FM" else f"{int(val)} AM"


def freq_match(st: dict, band: str, val: float) -> bool:
    tol = 0.011 if band == "FM" else 0.5
    return any(b == band and abs(v - val) < tol for (b, v) in st["freq_ev"])


def freq_query_variants(band: str, val: float) -> list:
    if band == "FM":
        if abs(val - round(val)) < 1e-9:
            n = int(round(val))
            return [f"{n} fm", f"fm {n}", f"{n}.0"]
        out = [f"{val:.1f}"] if abs(val * 10 - round(val * 10)) < 1e-9 else [f"{val:.2f}"]
        return out
    n = int(val)
    return [f"{n} am", f"am {n}", f"{n} khz"]


# ---------------------------------------------------------------- common station format
def _norm_name(n: str) -> str:
    n = re.sub(r"[^a-z0-9]+", " ", (n or "").lower())
    drop = {"radio", "fm", "am", "online", "live", "the", "stream", "hq", "mp3", "aac", "hd", "web"}
    return " ".join(t for t in n.split() if t not in drop)


def _norm_url(u: str) -> str:
    u = (u or "").strip().lower()
    u = re.sub(r"^https?://", "", u).split("#")[0].split("?")[0]
    return u.rstrip("/")


def mk_station(*, id: str, name: str, url: str, source: str, cc: str = "", country: str = "", city: str = "",
               tags: str = "", votes: int = 0, codec: str = "", bitrate: int = 0, favicon: str = "",
               hls=None, freq_ev: dict = None, alts: list = None) -> dict:
    url = (url or "").strip()
    name = (name or "Unknown").strip()[:80]
    if hls is None:
        hls = ".m3u8" in url.lower().split("?")[0]
    ev = dict(freq_ev) if freq_ev is not None else extract_frequencies(name, tags)
    return {
        "id": id, "name": name, "url": url, "alts": list(alts or []), "source": source, "sources": [source],
        "cc": (cc or "").upper(), "country": country or "", "city": city or "", "tags": (tags or "")[:60],
        "votes": int(votes or 0), "codec": codec or "", "bitrate": int(bitrate or 0),
        "favicon": favicon if (favicon or "").startswith("https://") else "", "hls": bool(hls),
        "freq_ev": ev, "score": 0.0,
    }


def radio_register(st: dict) -> dict:
    _radio_by_id[st["id"]] = st
    _radio_by_id.move_to_end(st["id"])
    while len(_radio_by_id) > 1500:
        _radio_by_id.popitem(last=False)
    return st


def merge_stations(stations: list) -> list:
    """De-duplicate: same stream URL, or same normalised name + country. Alternate URLs are kept as fallbacks."""
    idx, by_name, out = {}, {}, []
    for st in stations:
        ukey = ("u", _norm_url(st["url"]))
        nn = _norm_name(st["name"])
        ex = idx.get(ukey)
        if ex is None and nn:        # same name + same country (an unknown country matches any)
            ex = next((c for c in by_name.get(nn, []) if not c["cc"] or not st["cc"] or c["cc"] == st["cc"]), None)
        if ex is None:
            out.append(st)
            idx[ukey] = st
            if nn:
                by_name.setdefault(nn, []).append(st)
            continue
        idx.setdefault(ukey, ex)
        if not ex["cc"] and st["cc"]:
            ex["cc"] = st["cc"]
        if _norm_url(st["url"]) != _norm_url(ex["url"]) and st["url"] not in ex["alts"]:
            ex["alts"].append(st["url"])
        for a in st["alts"]:
            if a not in ex["alts"] and a != ex["url"]:
                ex["alts"].append(a)
        for s in st["sources"]:
            if s not in ex["sources"]:
                ex["sources"].append(s)
        for f, e in st["freq_ev"].items():
            if f not in ex["freq_ev"] or e != "name":
                ex["freq_ev"][f] = e if e != "name" else ex["freq_ev"].get(f, "name")
        for fld in ("favicon", "city", "country", "tags", "codec"):
            if not ex[fld] and st[fld]:
                ex[fld] = st[fld]
        ex["votes"] = max(ex["votes"], st["votes"])
        ex["bitrate"] = max(ex["bitrate"], st["bitrate"])
    for st in out:
        st["alts"] = st["alts"][:4]
    return out


_EVIDENCE_WEIGHT = {"name": 0.0, "Wikidata": 6.0, "FCC": 8.0}
_SOURCE_WEIGHT = {"SomaFM": 3.0, "Radio Browser": 0.0, "Radio Garden": -1.0}


def rank_stations(stations: list, query: str = "", freq=None, city: str = None) -> list:
    qn = _norm_name(query)
    qtok = set(qn.split())
    cl = (city or "").lower().strip()
    for st in stations:
        s = math.log1p(st["votes"])
        nn = _norm_name(st["name"])
        if qn:
            if nn == qn:
                s += 15
            elif qn in nn:
                s += 8
            if qtok:
                s += 5 * len(qtok & set(nn.split())) / len(qtok)
        if freq:
            band, val = freq
            tol = 0.011 if band == "FM" else 0.5
            for (b, v), ev in st["freq_ev"].items():
                if b == band and abs(v - val) < tol:
                    s += 40 + _EVIDENCE_WEIGHT.get(ev, 0.0)
                    break
        if cl and (cl in st["city"].lower() or cl in st["name"].lower() or cl in st["tags"].lower()):
            s += 10
        s += max((_SOURCE_WEIGHT.get(x, 0.0) for x in st["sources"]), default=0.0)
        s += 2.0 * (len(st["sources"]) - 1)
        if st["url"].startswith("https://"):
            s += 1
        if st["bitrate"] >= 64:
            s += 1
        st["score"] = s
    stations.sort(key=lambda x: x["score"], reverse=True)
    return stations


# ---------------------------------------------------------------- source: Radio Browser
async def radio_hosts() -> list:
    now = time.monotonic()
    if _radio_hosts_cache["hosts"] and now - _radio_hosts_cache["at"] < 21600:
        return _radio_hosts_cache["hosts"]
    hosts = list(RADIO_HOSTS)
    try:
        data = await http_json("https://all.api.radio-browser.info/json/servers", timeout=5)
        for item in data:
            n = (item.get("name") or "").strip()
            if n and n not in hosts:
                hosts.append(n)
    except Exception:
        pass
    _radio_hosts_cache.update(hosts=hosts, at=now)
    return hosts


async def radio_api(path: str, params: dict = None, ttl: int = 900):
    key = path + "?" + json.dumps(params or {}, sort_keys=True)
    now = time.monotonic()
    hit = _radio_cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    session = await radio_http()
    hosts = (await radio_hosts())[:]
    random.shuffle(hosts)
    last = None
    for host in hosts[:4]:
        try:
            async with session.get(f"https://{host}/json/{path}", params=params,
                                   timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200:
                    last = f"{host}: HTTP {r.status}"
                    continue
                data = await r.json(content_type=None)
            _radio_cache[key] = (now + ttl, data)
            return data
        except Exception as e:
            last = f"{host}: {e}"
    if hit:
        return hit[1]
    raise RuntimeError(f"radio directory unavailable ({last})")


def rb_norm(s: dict):
    url = (s.get("url_resolved") or s.get("url") or "").strip()
    uid = s.get("stationuuid")
    if not uid or not url.startswith(("http://", "https://")):
        return None
    return mk_station(
        id=f"rb:{uid}", name=s.get("name") or "Unknown", url=url, source="Radio Browser",
        cc=s.get("countrycode") or "", country=s.get("country") or "", city=s.get("state") or "",
        tags=s.get("tags") or "", votes=s.get("votes") or 0, codec=s.get("codec") or "",
        bitrate=s.get("bitrate") or 0, favicon=(s.get("favicon") or "").strip(),
        hls=True if s.get("hls") == 1 else None,
    )


async def src_rb_name(q: str, cc: str = None, limit: int = 60) -> list:
    params = {"name": q, "hidebroken": "true", "order": "votes", "reverse": "true", "limit": limit}
    if cc:
        params["countrycode"] = cc
    data = await radio_api("stations/search", params, ttl=300)
    return [x for x in map(rb_norm, data) if x]


async def src_rb_tag(tag: str, cc: str = None, limit: int = 40) -> list:
    params = {"tag": tag, "hidebroken": "true", "order": "votes", "reverse": "true", "limit": limit}
    if cc:
        params["countrycode"] = cc
    data = await radio_api("stations/search", params, ttl=300)
    return [x for x in map(rb_norm, data) if x]


async def radio_countries() -> list:
    data = await radio_api("countries", {"hidebroken": "true", "order": "name"}, ttl=3600)
    out = []
    for c in data:
        cc = (c.get("iso_3166_1") or "").upper()
        if cc and (c.get("stationcount") or 0) > 0:
            out.append({"name": c.get("name") or cc, "cc": cc, "count": int(c.get("stationcount") or 0)})
            _cc_names[cc] = c.get("name") or cc
    return out


_ALL_WORDS = {"", "all", "any", "world", "worldwide", "global", "everywhere", "all countries"}


async def resolve_cc(text: str | None):
    """Country text -> ISO code. None = worldwide. Accepts codes ('GB'), names ('united kingdom'), 'ALL'."""
    t = (text or "").replace("\U0001F30D", "").strip()
    if t.lower() in _ALL_WORDS:
        return None
    if len(t) == 2 and t.isalpha():
        return t.upper()
    try:
        countries = await radio_countries()
    except Exception:
        return None
    tl = t.lower()
    for c in countries:
        if c["name"].lower() == tl:
            return c["cc"]
    for c in countries:
        if tl in c["name"].lower():
            return c["cc"]
    return None


# ---------------------------------------------------------------- source: SomaFM (official public API)
async def src_somafm(q: str, cc: str = None) -> list:
    if cc and cc != "US":
        return []
    data = await http_json("https://somafm.com/channels.json", timeout=6, ttl=3600)
    toks = [t for t in re.split(r"\W+", q.lower()) if t]
    out = []
    for ch in data.get("channels", []):
        hay = f"{ch.get('title', '')} {ch.get('description', '')} {ch.get('genre', '')} somafm".lower()
        if toks and not all(t in hay for t in toks):
            continue
        cid = ch.get("id")
        if not cid:
            continue
        out.append(mk_station(
            id=f"sm:{cid}", name=f"SomaFM {ch.get('title', cid)}", url=f"https://ice1.somafm.com/{cid}-128-mp3",
            alts=[f"https://ice2.somafm.com/{cid}-128-mp3", f"https://ice4.somafm.com/{cid}-128-mp3"],
            source="SomaFM", cc="US", country="United States", city="San Francisco",
            tags=(ch.get("genre") or "").replace("|", ","), votes=int(ch.get("listeners") or 0) * 5,
            codec="MP3", bitrate=128, favicon=ch.get("image") or "", hls=False,
        ))
    return out


# ---------------------------------------------------------------- source: Radio Garden (unofficial)
async def src_garden(q: str, cc: str = None) -> list:
    data = await http_json("https://radio.garden/api/search", params={"q": q}, timeout=6, ttl=300)
    hits = ((data.get("hits") or {}).get("hits")) or []
    cc_name = (_cc_names.get(cc) or "").lower() if cc else ""
    out = []
    for h in hits:
        src = h.get("_source") or {}
        url = src.get("url") or ""
        if src.get("type") not in (None, "channel") or not url.startswith("/listen/"):
            continue
        cid = url.rstrip("/").split("/")[-1]
        title = src.get("title") or ""
        sub = src.get("subtitle") or ""
        city, _, country = sub.rpartition(",")
        country = country.strip() or sub
        if cc_name and country and country.lower() != cc_name:
            continue
        out.append(mk_station(
            id=f"rg:{cid}", name=title, url=f"https://radio.garden/api/ara/content/listen/{cid}/channel.mp3",
            source="Radio Garden", cc=cc or "", country=country, city=city.strip(), hls=False,
        ))
    return out


# ---------------------------------------------------------------- source: Wikidata (frequency -> station name)


def _name_sim(a: str, b: str) -> float:
    a, b = _norm_name(a), _norm_name(b)
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


async def src_wikidata_freq(val: float, cc: str = None) -> list:
    lo, hi = int(val * 1e6 - 5000), int(val * 1e6 + 5000)
    cc_filter = f'FILTER(!BOUND(?cc) || STR(?cc) = "{cc}")' if cc else ""
    query = f"""SELECT ?itemLabel ?cityLabel ?cc WHERE {{
  ?item p:P2144 ?st . ?st psn:P2144 ?v . ?v wikibase:quantityAmount ?amt .
  FILTER(?amt >= {lo} && ?amt <= {hi})
  ?item wdt:P31/wdt:P279* wd:Q14350 .
  OPTIONAL {{ ?item wdt:P17 ?country . OPTIONAL {{ ?country wdt:P297 ?cc }} }}
  OPTIONAL {{ ?item wdt:P131 ?city }}
  {cc_filter}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}} LIMIT 25"""
    data = await http_json("https://query.wikidata.org/sparql", params={"query": query, "format": "json"},
                           headers={"Accept": "application/sparql-results+json"}, timeout=12, ttl=3600)
    rows, seen = [], set()
    for b in (data.get("results") or {}).get("bindings", []):
        label = (b.get("itemLabel") or {}).get("value", "")
        if not label or re.fullmatch(r"Q\d+", label) or label.lower() in seen:
            continue
        seen.add(label.lower())
        rows.append((label, (b.get("cityLabel") or {}).get("value", ""), ((b.get("cc") or {}).get("value") or "").upper()))
    rows = rows[:8]

    async def resolve(label, city, rcc):
        try:
            cands = await src_rb_name(label, rcc or cc, 20)
        except Exception:
            return []
        good = [c for c in cands if _name_sim(label, c["name"]) >= 0.6]
        good.sort(key=lambda c: (_name_sim(label, c["name"]), c["votes"]), reverse=True)
        for c in good[:3]:
            c["freq_ev"][("FM", round(val, 2))] = "Wikidata"
            if city and not c["city"]:
                c["city"] = city
            c["sources"] = ["Radio Browser", "Wikidata"]
        return good[:3]

    lists = await asyncio.gather(*(resolve(*r) for r in rows))
    return [s for lst in lists for s in lst]


# ---------------------------------------------------------------- source: FCC (US) from a local data file
_fcc_rows = None


def _fcc_load() -> list:
    """Reads a CSV exported from FCC broadcast station data. Needs columns for call sign and frequency
    (names like callsign/call_sign, frequency/freq); city/community and state are optional."""
    global _fcc_rows
    if _fcc_rows is not None:
        return _fcc_rows
    rows = []
    with open(RADIO_FCC_FILE, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            low = {(k or "").strip().lower(): (v or "").strip() for k, v in r.items()}
            call = low.get("callsign") or low.get("call_sign") or low.get("call sign") or low.get("call")
            fr = low.get("frequency") or low.get("freq") or low.get("frequency_mhz")
            try:
                fv = float(re.sub(r"[^\d.]", "", fr or ""))
            except ValueError:
                continue
            if call and _fm_ok(fv):
                rows.append((call.upper(), fv, low.get("city") or low.get("community") or low.get("community_city") or "",
                             (low.get("state") or "").upper()))
    _fcc_rows = rows
    logger.info(f"FCC file loaded: {len(rows)} FM stations")
    return rows


async def src_fcc_freq(val: float, cc: str = None, city: str = None) -> list:
    if not RADIO_FCC_FILE or (cc and cc != "US"):
        return []
    rows = await asyncio.to_thread(_fcc_load)
    hits = [r for r in rows if abs(r[1] - val) < 0.011 and (not city or city.lower() in r[2].lower() or city.upper() == r[3])]
    hits = hits[:10]

    async def resolve(call, _fv, rcity, state):
        base = re.sub(r"-(FM|AM|TV)$", "", call)
        try:
            cands = await src_rb_name(base, "US", 20)
        except Exception:
            return []
        out = []
        for c in cands:
            if re.search(rf"\b{re.escape(base)}\b", c["name"].upper()):
                c["freq_ev"][("FM", round(val, 2))] = "FCC"
                c["city"] = c["city"] or f"{rcity}, {state}".strip(", ")
                c["sources"] = ["Radio Browser", "FCC"]
                out.append(c)
        return out

    lists = await asyncio.gather(*(resolve(*h) for h in hits))
    return [s for lst in lists for s in lst]


# ---------------------------------------------------------------- search entry points
async def radio_search_all(query: str, cc: str = None, fast: bool = False, limit: int = 25) -> list:
    query = query.strip()
    to = 2.0 if fast else 7.0
    track = not fast
    jobs = [("Radio Browser", lambda: src_rb_name(query, cc, 60)), ("SomaFM", lambda: src_somafm(query, cc))]
    if " " not in query and len(query) >= 3:
        jobs.append(("Radio Browser", lambda: src_rb_tag(query.lower(), cc, 40)))
    if RADIO_GARDEN_ON:
        jobs.append(("Radio Garden", lambda: src_garden(query, cc)))
    lists = await asyncio.gather(*(run_source(n, f, to, track) for n, f in jobs))
    merged = merge_stations([s for lst in lists for s in lst])
    rank_stations(merged, query=query)
    return [radio_register(s) for s in merged[:limit]]


async def radio_find_frequency(text: str, cc: str = None, city: str = None, fast: bool = False, limit: int = 25):
    parsed = parse_frequency(text)
    if not parsed:
        return [], None
    band, val = parsed
    to = 2.0 if fast else 8.0
    track = not fast
    jobs = [("Radio Browser", (lambda q=q: src_rb_name(q, cc, 150))) for q in freq_query_variants(band, val)]
    probe = freq_query_variants(band, val)[0]
    if RADIO_GARDEN_ON:
        jobs.append(("Radio Garden", lambda: src_garden(probe, cc)))
    if not fast and band == "FM":
        if RADIO_WIKIDATA_ON:
            jobs.append(("Wikidata", lambda: src_wikidata_freq(val, cc)))
        if RADIO_FCC_FILE:
            jobs.append(("FCC", lambda: src_fcc_freq(val, cc, city)))
    lists = await asyncio.gather(*(run_source(n, f, to, track) for n, f in jobs))
    stations = [s for lst in lists for s in lst if freq_match(s, band, val)]
    if cc:
        stations = [s for s in stations if not s["cc"] or s["cc"] == cc]
    merged = merge_stations(stations)
    rank_stations(merged, freq=(band, val), city=city)
    return [radio_register(s) for s in merged[:limit]], parsed


async def radio_browse_country(cc: str = None, tag: str = None, limit: int = 300) -> list:
    params = {"hidebroken": "true", "order": "votes", "reverse": "true", "limit": limit}
    if cc:
        params["countrycode"] = cc
    if tag:
        params["tag"] = tag
    data = await radio_api("stations/search", params, ttl=900)
    return [radio_register(s) for s in (rb_norm(x) for x in data) if s]


def direct_station(url: str) -> dict:
    u = urlparse(url)
    label = (u.hostname or "stream") + (u.path if len(u.path) > 1 else "")
    return radio_register(mk_station(id="url:" + uuid.uuid5(uuid.NAMESPACE_URL, url).hex, name=label[:60], url=url,
                                     source="Direct URL", freq_ev={}))


# ---------------------------------------------------------------- stream helpers
async def radio_url_is_public(url: str) -> bool:
    try:
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            return False
        port = u.port or (443 if u.scheme == "https" else 80)
        infos = await asyncio.get_running_loop().getaddrinfo(u.hostname, port, type=socket.SOCK_STREAM)
        return bool(infos) and all(ipaddress.ip_address(i[4][0]).is_global for i in infos)
    except Exception:
        return False


async def resolve_stream_url(url: str) -> str:
    """.pls / .m3u / .asx playlist -> the real stream URL. HLS (.m3u8) and plain streams pass straight through."""
    low = url.lower().split("?")[0]
    if not low.endswith((".pls", ".m3u", ".asx")):
        return url
    try:
        session = await radio_http()
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
            body = (await r.content.read(16384)).decode("utf-8", "ignore")
        if low.endswith(".asx"):
            m = re.search(r'href\s*=\s*["\'](https?://[^"\']+)', body, re.I)
            return m.group(1) if m else url
        for line in body.splitlines():
            line = line.strip()
            if line.lower().startswith("file") and "=" in line:
                cand = line.split("=", 1)[1].strip()
            else:
                cand = line
            if cand.lower().startswith(("http://", "https://")):
                return cand
    except Exception as e:
        logger.warning(f"Could not read playlist {url}: {e}")
    return url


def radio_label(st: dict) -> str:
    f = next(iter(sorted(st["freq_ev"])), None)
    bits = [st["name"]]
    if f:
        bits.append(fmt_freq(*f))
    if st["cc"]:
        bits.append(st["cc"])
    return " \u00b7 ".join(bits)[:100]


def radio_describe(st: dict) -> str:
    bits = []
    if st["city"]:
        bits.append(st["city"][:20])
    if st["cc"]:
        bits.append(st["cc"])
    f = next(iter(sorted(st["freq_ev"])), None)
    if f:
        ev = st["freq_ev"][f]
        bits.append(fmt_freq(*f) + ("" if ev == "name" else f" ({ev})"))
    if st["codec"] or st["bitrate"]:
        bits.append(f"{st['codec']} {st['bitrate']}k".strip())
    bits.append("HLS" if st["hls"] else "stream")
    bits.append("+".join(st["sources"]))
    return " \u2022 ".join(bits)[:100]


# ---------------------------------------------------------------- playback
def _radio_start_source(gid: int, vc, sess: dict):
    urls = sess["urls"]
    url = urls[sess["idx"] % len(urls)]
    sess["started"] = time.monotonic()
    gen = sess["gen"]
    source = discord.FFmpegPCMAudio(
        url,
        before_options=("-user_agent \"LUX-NET-Bridge/2.0\" -reconnect 1 -reconnect_streamed 1 "
                        "-reconnect_delay_max 5 -protocol_whitelist http,https,tcp,tls,crypto"),
        options="-vn -loglevel error",
    )

    def after(err):
        if err:
            logger.warning(f"Radio playback error: {err}")
        asyncio.run_coroutine_threadsafe(radio_ended(gid, gen), bot.loop)

    vc.play(source, after=after)


async def radio_ended(gid: int, gen: int):
    """The stream ended or errored. Reconnect (trying alternate URLs) instead of silently dropping."""
    sess = radio_sessions.get(gid)
    if not sess or sess["gen"] != gen:
        return
    now = time.monotonic()
    ran = now - sess.get("started", now)
    sess["restarts"] = [t for t in sess["restarts"] if now - t < 120]
    if ran < 8:
        sess["idx"] += 1
        sess["fast_fails"] = sess.get("fast_fails", 0) + 1
    else:
        sess["fast_fails"] = 0
    if len(sess["restarts"]) >= 6 or sess["fast_fails"] >= max(3, len(sess["urls"]) * 2):
        ch = sess.get("notify")
        await radio_stop(gid)
        if ch:
            try:
                await ch.send("\U0001F4FB The station's stream keeps failing, so I stopped. Try another station or `/radio-search`.")
            except Exception:
                pass
        return
    sess["restarts"].append(now)
    await asyncio.sleep(min(1 + len(sess["restarts"]), 5))
    sess = radio_sessions.get(gid)
    if not sess or sess["gen"] != gen:
        return
    vc = sess["vc"]
    if not vc.is_connected():
        await radio_stop(gid)
        return
    sess["gen"] += 1
    try:
        _radio_start_source(gid, vc, sess)
    except Exception as e:
        logger.warning(f"Radio restart failed: {e}")
        await radio_stop(gid)


async def radio_stop(gid: int) -> bool:
    sess = radio_sessions.pop(gid, None)
    if not sess:
        return False
    sess["gen"] += 1
    try:
        if sess["vc"].is_playing():
            sess["vc"].stop()
    except Exception:
        pass
    try:
        await sess["vc"].disconnect(force=True)
    except Exception:
        pass
    return True


async def radio_maybe_leave(channel):
    sess = radio_sessions.get(channel.guild.id)
    if sess and sess["vc"].channel and sess["vc"].channel.id == channel.id and humans_in(channel) == 0:
        await radio_stop(channel.guild.id)


async def radio_click(uid: str):
    try:
        await radio_api(f"url/{uid}", None, ttl=0)      # tells the directory the station was used
    except Exception:
        pass


async def radio_begin(interaction: discord.Interaction, st: dict, target: discord.VoiceChannel):
    """Start a station in a voice channel. Returns an error string, or None on success."""
    gid = interaction.guild.id
    if not shutil.which("ffmpeg"):
        return "\u274c FFmpeg isn't installed on the host."
    if gid in voice_sessions or gid in _voice_connecting:
        return "\u274c The voice relay is active in this server. End it before starting the radio."
    raw = [st["url"]] + list(st.get("alts", []))
    resolved = await asyncio.gather(*(resolve_stream_url(u) for u in raw[:4]))
    checks = await asyncio.gather(*(radio_url_is_public(u) for u in resolved))
    urls = []
    for u, ok in zip(resolved, checks):
        if ok and u not in urls:
            urls.append(u)
    if not urls:
        return "\u274c That stream address isn't reachable or isn't a public URL."
    try:
        sess = radio_sessions.get(gid)
        if sess:
            sess["gen"] += 1
            vc = sess["vc"]
            if vc.is_playing():
                vc.stop()
            if vc.channel.id != target.id:
                await vc.move_to(target)
        else:
            vc = await target.connect(timeout=20, self_deaf=True)
            sess = radio_sessions[gid] = {"vc": vc, "gen": 0, "restarts": []}
        sess.update(info=st, urls=urls, idx=0, fast_fails=0, restarts=[], notify=interaction.channel)
        _radio_start_source(gid, vc, sess)
    except Exception as e:
        await radio_stop(gid)
        return f"\u274c Couldn't start radio: {e}"
    if st["id"].startswith("rb:"):
        spawn(radio_click(st["id"][3:]))
    return None


def radio_now_embed(st: dict, vc_mention: str) -> discord.Embed:
    e = discord.Embed(title=f"\U0001F4FB {st['name']}", color=discord.Color.blurple())
    e.description = f"Playing in {vc_mention}"
    f = next(iter(sorted(st["freq_ev"])), None)
    if f:
        ev = st["freq_ev"][f]
        e.add_field(name="Frequency", value=fmt_freq(*f) + ("" if ev == "name" else f" (via {ev})"), inline=True)
    place = ", ".join(x for x in (st["city"], st["country"] or st["cc"]) if x)
    if place:
        e.add_field(name="Location", value=place[:100], inline=True)
    e.add_field(name="Source", value=" + ".join(st["sources"]), inline=True)
    if st["codec"] or st["bitrate"]:
        e.add_field(name="Audio", value=f"{st['codec']} {st['bitrate']}k".strip(), inline=True)
    if st["tags"]:
        e.add_field(name="Tags", value=st["tags"][:100], inline=False)
    if st["favicon"]:
        e.set_thumbnail(url=st["favicon"])
    e.set_footer(text="Plays the station's internet stream, which can differ from the air signal (ads, delay, geo-blocks).")
    return e


# ---------------------------------------------------------------- menus
class RadioStationSelect(discord.ui.Select):
    def __init__(self, stations: list, placeholder: str = "Pick a station to play..."):
        opts = []
        for st in stations[:25]:
            radio_register(st)
            opts.append(discord.SelectOption(label=st["name"][:100] or "Station", value=st["id"][:100],
                                             description=radio_describe(st)))
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=opts)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.owner_id:
            await interaction.response.send_message("\u274c This menu belongs to someone else. Run the command yourself.", ephemeral=True)
            return
        st = _radio_by_id.get(self.values[0])
        if not st:
            await interaction.response.send_message("\u274c That result expired. Search again.", ephemeral=True)
            return
        target = _resolve_voice_channel(interaction, view.vc)
        if not target:
            await interaction.response.send_message("\u274c Join a voice channel first, then pick again.", ephemeral=True)
            return
        await interaction.response.defer()
        err = await radio_begin(interaction, st, target)
        if err:
            await interaction.followup.send(err, ephemeral=True)
        else:
            await interaction.followup.send(embed=radio_now_embed(st, target.mention))


class RadioPickView(discord.ui.View):
    def __init__(self, owner_id: int, stations: list, vc=None):
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.vc = vc
        self.add_item(RadioStationSelect(stations))


class RadioCountrySelect(discord.ui.Select):
    def __init__(self, countries: list):
        opts = [discord.SelectOption(label=c["name"][:100], value=c["cc"], description=f"{c['count']} stations")
                for c in countries[:25]]
        super().__init__(placeholder="Choose a country...", min_values=1, max_values=1, options=opts)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if interaction.user.id != view.owner_id:
            await interaction.response.send_message("\u274c This menu belongs to someone else.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            stations = await radio_browse_country(self.values[0], view.tag)
        except Exception as e:
            await interaction.followup.send(f"\u274c Radio directory unavailable: {e}", ephemeral=True)
            return
        if not stations:
            await interaction.followup.send("\u26a0\ufe0f No stations found there.", ephemeral=True)
            return
        nv = RadioBrowseView(view.owner_id, stations, f"{_cc_names.get(self.values[0], self.values[0])}", view.vc)
        await interaction.edit_original_response(content=None, embed=nv.embed(), view=nv)


class RadioCountryView(discord.ui.View):
    def __init__(self, owner_id: int, countries: list, tag: str = None, vc=None):
        super().__init__(timeout=180)
        self.owner_id, self.tag, self.vc = owner_id, tag, vc
        self.add_item(RadioCountrySelect(countries))


class RadioBrowseView(discord.ui.View):
    PAGE = 25

    def __init__(self, owner_id: int, stations: list, title: str, vc=None, page: int = 0):
        super().__init__(timeout=300)
        self.owner_id, self.stations, self.title, self.vc, self.page = owner_id, stations, title, vc, page
        self._build()

    def pages(self) -> int:
        return max(1, math.ceil(len(self.stations) / self.PAGE))

    def _build(self):
        self.clear_items()
        chunk = self.stations[self.page * self.PAGE:(self.page + 1) * self.PAGE]
        self.add_item(RadioStationSelect(chunk))
        prev_b = discord.ui.Button(label="\u25C0 Prev", style=discord.ButtonStyle.secondary, disabled=self.page == 0, row=1)
        next_b = discord.ui.Button(label="Next \u25B6", style=discord.ButtonStyle.secondary,
                                   disabled=self.page >= self.pages() - 1, row=1)
        prev_b.callback = lambda i: self._turn(i, -1)
        next_b.callback = lambda i: self._turn(i, 1)
        self.add_item(prev_b)
        self.add_item(next_b)

    async def _turn(self, interaction: discord.Interaction, d: int):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("\u274c This menu belongs to someone else.", ephemeral=True)
            return
        self.page = max(0, min(self.pages() - 1, self.page + d))
        self._build()
        await interaction.response.edit_message(embed=self.embed(), view=self)

    def embed(self) -> discord.Embed:
        chunk = self.stations[self.page * self.PAGE:(self.page + 1) * self.PAGE]
        lines = [f"**{i}.** {radio_label(s)}" for i, s in enumerate(chunk, self.page * self.PAGE + 1)]
        e = discord.Embed(title=f"\U0001F4FB Browse: {self.title}", description="\n".join(lines)[:4000], color=discord.Color.blurple())
        e.set_footer(text=f"Page {self.page + 1}/{self.pages()} \u2022 {len(self.stations)} stations \u2022 pick one below")
        return e


def results_embed(title: str, stations: list, note: str = "") -> discord.Embed:
    lines = []
    for i, s in enumerate(stations[:10], 1):
        f = next(iter(sorted(s["freq_ev"])), None)
        fs = ""
        if f:
            ev = s["freq_ev"][f]
            fs = f" [{fmt_freq(*f)}{'' if ev == 'name' else ', ' + ev}]"
        place = f" \u2014 {s['city']}, {s['cc']}" if s["city"] and s["cc"] else (f" \u2014 {s['cc']}" if s["cc"] else "")
        extra = f" ({s['codec']} {s['bitrate']}k)" if s["codec"] and s["bitrate"] else ""
        lines.append(f"**{i}.** {s['name']}{fs}{place}{extra}\n\u2003*Source: {' + '.join(s['sources'])}*")
    e = discord.Embed(title=title, description="\n".join(lines)[:4000], color=discord.Color.blurple())
    foot = "Pick a station below to play it in your voice channel."
    e.set_footer(text=(note + " \u2022 " if note else "") + foot)
    return e


async def radio_present(interaction: discord.Interaction, stations: list, title: str, vc=None, autoplay: bool = True, note: str = ""):
    """Single result + voice channel known -> play it; otherwise show results with a play menu."""
    target = _resolve_voice_channel(interaction, vc)
    if autoplay and len(stations) == 1 and target:
        err = await radio_begin(interaction, stations[0], target)
        if err:
            await interaction.followup.send(err, ephemeral=True)
        else:
            await interaction.followup.send(embed=radio_now_embed(stations[0], target.mention))
        return
    view = RadioPickView(interaction.user.id, stations, vc)
    await interaction.followup.send(embed=results_embed(title, stations, note), view=view, ephemeral=True)


# ---------------------------------------------------------------- web player
def _public_station(st: dict) -> dict:
    f = next(iter(sorted(st["freq_ev"])), None)
    return {"id": st["id"], "name": st["name"], "cc": st["cc"], "country": st["country"], "city": st["city"],
            "tags": st["tags"], "codec": st["codec"], "bitrate": st["bitrate"], "hls": st["hls"],
            "source": " + ".join(st["sources"]), "favicon": st["favicon"],
            "frequency": fmt_freq(*f) if f else "", "hlsUrl": st["url"] if st["hls"] else ""}


RADIO_PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LUX-NET Radio</title><style>
:root{color-scheme:dark}body{margin:0;font:15px system-ui,sans-serif;background:#0f1117;color:#e8e8ee}
main{max-width:760px;margin:0 auto;padding:16px}h1{font-size:20px;margin:6px 0 12px}
.row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}select,input,button{padding:9px;border-radius:8px;border:1px solid #333;background:#1a1d27;color:inherit;font:inherit}
input{flex:1;min-width:160px}button{cursor:pointer;background:#5865f2;border-color:#5865f2}
#now{margin:8px 0;min-height:20px;color:#9fb0ff}audio{width:100%}
ul{list-style:none;padding:0;margin:10px 0}li{padding:10px;border:1px solid #262a38;border-radius:8px;margin-bottom:6px;cursor:pointer}
li:hover{background:#171a24}small{color:#8b90a5;display:block;margin-top:2px}</style></head><body><main>
<h1>\U0001F4FB LUX-NET Radio</h1>
<div class="row"><select id="cc"><option value="">\U0001F30D All countries</option></select>
<input id="q" placeholder="Station, genre, or frequency (e.g. 101.1 FM)"><button id="go">Search</button></div>
<audio id="a" controls autoplay></audio><div id="now"></div><ul id="list"></ul>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.8/dist/hls.min.js"></script>
<script>
const $=id=>document.getElementById(id);let hls=null;
async function j(u){const r=await fetch(u);if(!r.ok)throw new Error(r.status);return r.json()}
async function init(){try{const cs=await j('/radio/api/countries');for(const c of cs){const o=document.createElement('option');o.value=c.cc;o.textContent=c.name+' ('+c.count+')';$('cc').appendChild(o)}}catch(e){}search()}
async function search(){$('list').innerHTML='<li>Loading...</li>';
 try{const s=await j('/radio/api/stations?cc='+encodeURIComponent($('cc').value)+'&q='+encodeURIComponent($('q').value));
 $('list').innerHTML='';if(!s.length){$('list').innerHTML='<li>No stations found.</li>';return}
 for(const st of s){const li=document.createElement('li');li.textContent=st.name+(st.frequency?' \\u00b7 '+st.frequency:'');
 const sm=document.createElement('small');sm.textContent=[st.city,st.cc,st.codec&&(st.codec+' '+st.bitrate+'k'),st.hls?'HLS':'',st.source].filter(Boolean).join(' \\u2022 ');li.appendChild(sm);li.onclick=()=>play(st);$('list').appendChild(li)}}
 catch(e){$('list').innerHTML='<li>Directory unavailable. Try again shortly.</li>'}}
function play(st){const a=$('a');if(hls){hls.destroy();hls=null}$('now').textContent='Loading '+st.name+'...';
 if(st.hls&&st.hlsUrl){if(window.Hls&&Hls.isSupported()){hls=new Hls();hls.loadSource(st.hlsUrl);hls.attachMedia(a)}else{a.src=st.hlsUrl}}
 else{a.src='/radio/stream/'+encodeURIComponent(st.id)}
 a.play().then(()=>$('now').textContent='\\u25B6 '+st.name).catch(()=>$('now').textContent='Press play to start '+st.name)}
$('go').onclick=search;$('q').onkeydown=e=>{if(e.key==='Enter')search()};init();
</script></main></body></html>"""


async def web_radio_page(request):
    return web.Response(text=RADIO_PAGE, content_type="text/html")


async def web_radio_countries(request):
    try:
        data = await radio_countries()
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)
    return web.json_response(data)


async def web_radio_stations(request):
    cc = (request.query.get("cc") or "").strip().upper()[:2] or None
    q = (request.query.get("q") or "").strip()[:60]
    try:
        if q and parse_frequency(q):
            stations, _ = await radio_find_frequency(q, cc, fast=True)
        elif q:
            stations = await radio_search_all(q, cc, fast=True, limit=60)
        else:
            stations = (await radio_browse_country(cc, None, 100))
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)
    return web.json_response([_public_station(s) for s in stations])


async def web_radio_stream(request):
    """Proxy a (non-HLS) station so browsers can play http:// streams on this https page."""
    global _radio_proxy_active
    st = _radio_by_id.get(request.match_info["sid"])
    if not st or st["hls"]:
        return web.Response(status=404, text="unknown station")
    if _radio_proxy_active >= RADIO_PROXY_MAX:
        return web.Response(status=503, text="too many listeners right now")
    _radio_proxy_active += 1
    upstream = None
    try:
        session = await radio_http()
        url = await resolve_stream_url(st["url"])
        for _ in range(4):
            if not await radio_url_is_public(url):
                return web.Response(status=403, text="blocked")
            upstream = await session.get(url, allow_redirects=False, headers={"Icy-MetaData": "0"},
                                         timeout=aiohttp.ClientTimeout(total=None, sock_connect=8, sock_read=20))
            if upstream.status in (301, 302, 303, 307, 308) and upstream.headers.get("Location"):
                url = urljoin(url, upstream.headers["Location"])
                upstream.release()
                upstream = None
                continue
            break
        if upstream is None or upstream.status != 200:
            return web.Response(status=502, text="station unavailable")
        resp = web.StreamResponse(headers={"Content-Type": upstream.headers.get("Content-Type", "audio/mpeg"),
                                           "Cache-Control": "no-store"})
        await resp.prepare(request)
        async for chunk in upstream.content.iter_chunked(8192):
            await resp.write(chunk)
        return resp
    except (ConnectionResetError, asyncio.CancelledError, aiohttp.ClientError, asyncio.TimeoutError):
        return web.Response(status=502, text="stream ended")
    finally:
        _radio_proxy_active -= 1
        if upstream is not None:
            upstream.release()


async def radio_warm():
    try:
        await radio_hosts()
        await radio_countries()
    except Exception as e:
        logger.warning(f"Radio warm-up failed: {e}")


# ---------------------------------------------------------------- slash commands
async def radio_country_autocomplete(interaction: discord.Interaction, current: str):
    try:
        countries = await asyncio.wait_for(radio_countries(), 2.5)
    except Exception:
        return []
    cur = current.replace("\U0001F30D", "").strip().lower()
    out = []
    if not cur or cur in "all countries worldwide":
        out.append(app_commands.Choice(name="\U0001F30D All countries", value="ALL"))
    for c in countries:
        if not cur or cur in c["name"].lower() or cur == c["cc"].lower():
            out.append(app_commands.Choice(name=f"{c['name']} ({c['cc']})"[:100], value=c["cc"]))
            if len(out) >= 25:
                break
    return out


async def radio_query_autocomplete(interaction: discord.Interaction, current: str):
    cur = current.strip()
    if len(cur) < 2 or cur.lower().startswith("http"):
        return []
    try:
        cc = await resolve_cc(getattr(interaction.namespace, "country", None))
        if parse_frequency(cur):
            stations, _ = await asyncio.wait_for(radio_find_frequency(cur, cc, fast=True), 2.6)
        else:
            stations = await asyncio.wait_for(radio_search_all(cur, cc, fast=True), 2.6)
    except Exception:
        return []
    return [app_commands.Choice(name=radio_label(s), value=s["id"][:100]) for s in stations[:25]]


@bot.tree.command(name="radio", description="How to use the radio, plus the web player link.")
async def radio_cmd(interaction: discord.Interaction):
    e = discord.Embed(title="\U0001F4FB LUX-NET Radio", color=discord.Color.blurple())
    e.description = (
        "`/radio-play` \u2014 station name, frequency (`101.1 FM`) or a direct stream URL\n"
        "`/radio-frequency` \u2014 find stations on a frequency (add a country/city to narrow it)\n"
        "`/radio-search` \u2014 search every source worldwide\n"
        "`/radio-browse` \u2014 browse by country, then pick a station\n"
        "`/radio-now`, `/radio-stop`, `/radio-sources`"
    )
    if EMU_WEB_BASE:
        e.add_field(name="Web player", value=f"{EMU_WEB_BASE}/radio", inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="radio-search", description="Search radio stations across every directory, worldwide.")
@app_commands.describe(query="Station name, genre, or keyword", country="Limit to a country (optional)", voice_channel="Voice channel to play in")
@app_commands.autocomplete(country=radio_country_autocomplete)
async def radio_search(interaction: discord.Interaction, query: str, country: str = None, voice_channel: discord.VoiceChannel = None):
    await interaction.response.defer(ephemeral=True)
    cc = await resolve_cc(country)
    stations = await radio_search_all(query, cc)
    if not stations:
        await interaction.followup.send(f"\u26a0\ufe0f No stations found for `{query}`" + (f" in {country}" if cc else "") + ".", ephemeral=True)
        return
    await radio_present(interaction, stations, f"\U0001F4FB Results for '{query}'", voice_channel, autoplay=False)


@bot.tree.command(name="radio-frequency", description="Find a station by its radio frequency (e.g. 101.1 FM or 1010 AM).")
@app_commands.describe(frequency="e.g. 101.1, 101.1 FM, 1010 AM", country="Country (strongly recommended: frequencies repeat worldwide)",
                       city="City or region to prefer (optional)", voice_channel="Voice channel to play in")
@app_commands.autocomplete(country=radio_country_autocomplete)
async def radio_frequency(interaction: discord.Interaction, frequency: str, country: str = None, city: str = None,
                          voice_channel: discord.VoiceChannel = None):
    await interaction.response.defer(ephemeral=True)
    if not parse_frequency(frequency):
        await interaction.followup.send("\u274c I couldn't read that as a frequency. Try `101.1`, `101.1 FM` or `1010 AM`.", ephemeral=True)
        return
    cc = await resolve_cc(country)
    stations, parsed = await radio_find_frequency(frequency, cc, city)
    label = fmt_freq(*parsed)
    where = f" in {country}" if cc else ""
    if not stations:
        await interaction.followup.send(
            f"\u26a0\ufe0f I couldn't find an internet stream for **{label}**{where}. Many small stations have no online stream, "
            "and some listings don't include their frequency. Adding a country (and city) helps, or try `/radio-search` with the station name.",
            ephemeral=True)
        return
    note = "Matches are read from station listings, so double-check the source" if any(
        s["freq_ev"].get(parsed) == "name" for s in stations) else ""
    await radio_present(interaction, stations, f"\U0001F4FB {label}{where}", voice_channel, autoplay=False, note=note)


@bot.tree.command(name="radio-play", description="Play a radio station: by name, frequency, or direct stream URL.")
@app_commands.describe(query="Station name, frequency (101.1 FM), or a stream URL", country="Limit to a country (optional)",
                       voice_channel="Voice channel to play in")
@app_commands.autocomplete(query=radio_query_autocomplete, country=radio_country_autocomplete)
async def radio_play(interaction: discord.Interaction, query: str, country: str = None, voice_channel: discord.VoiceChannel = None):
    if not interaction.guild:
        await interaction.response.send_message("\u274c Server only.", ephemeral=True)
        return
    await interaction.response.defer()
    query = query.strip()
    cc = await resolve_cc(country)
    st = _radio_by_id.get(query)
    target = _resolve_voice_channel(interaction, voice_channel)

    if st is None and re.match(r"^rb:[0-9a-fA-F-]{36}$", query):      # id from an older menu (cache was reset)
        try:
            data = await radio_api(f"stations/byuuid/{query[3:]}", None, ttl=3600)
            st = radio_register(rb_norm(data[0])) if data and rb_norm(data[0]) else None
        except Exception:
            st = None
    if st is None and re.match(r"^https?://", query, re.I):
        st = direct_station(query)

    if st is not None:
        if not target:
            await interaction.followup.send("\u274c Join a voice channel first (or choose one in the command).", ephemeral=True)
            return
        err = await radio_begin(interaction, st, target)
        if err:
            await interaction.followup.send(err, ephemeral=True)
        else:
            await interaction.followup.send(embed=radio_now_embed(st, target.mention))
        return

    if parse_frequency(query):
        stations, parsed = await radio_find_frequency(query, cc)
        title = f"\U0001F4FB {fmt_freq(*parsed)}" + (f" in {country}" if cc else "")
        empty = (f"\u26a0\ufe0f No internet stream found for **{fmt_freq(*parsed)}**. Try adding a country, or search by name.")
    else:
        stations = await radio_search_all(query, cc)
        title = f"\U0001F4FB Results for '{query}'"
        empty = f"\u26a0\ufe0f No stations found for `{query}`."
    if not stations:
        await interaction.followup.send(empty, ephemeral=True)
        return
    await radio_present(interaction, stations, title, voice_channel, autoplay=True)


@bot.tree.command(name="radio-browse", description="Browse stations by country, then pick one to play.")
@app_commands.describe(country="Country (leave empty to pick from a menu)", genre="Genre/tag filter (optional)",
                       voice_channel="Voice channel to play in")
@app_commands.autocomplete(country=radio_country_autocomplete)
async def radio_browse(interaction: discord.Interaction, country: str = None, genre: str = None, voice_channel: discord.VoiceChannel = None):
    await interaction.response.defer(ephemeral=True)
    tag = genre.strip().lower() if genre else None
    try:
        cc = await resolve_cc(country)
        if cc or (country and country.strip().lower() not in _ALL_WORDS):
            if not cc:
                await interaction.followup.send(f"\u26a0\ufe0f I don't know a country called `{country}`.", ephemeral=True)
                return
            stations = await radio_browse_country(cc, tag)
            if not stations:
                await interaction.followup.send("\u26a0\ufe0f No stations found there.", ephemeral=True)
                return
            view = RadioBrowseView(interaction.user.id, stations, _cc_names.get(cc, cc), voice_channel)
            await interaction.followup.send(embed=view.embed(), view=view, ephemeral=True)
            return
        if country:      # explicit "all": worldwide top stations
            stations = await radio_browse_country(None, tag)
            view = RadioBrowseView(interaction.user.id, stations, "Worldwide", voice_channel)
            await interaction.followup.send(embed=view.embed(), view=view, ephemeral=True)
            return
        countries = sorted(await radio_countries(), key=lambda c: c["count"], reverse=True)[:25]
        view = RadioCountryView(interaction.user.id, countries, tag, voice_channel)
        await interaction.followup.send("\U0001F30D **Pick a country** (the 25 biggest are shown; type a country in the `country` option for any other):",
                                        view=view, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"\u274c Radio directory unavailable: {e}", ephemeral=True)


@bot.tree.command(name="radio-now", description="Show what's playing on the radio.")
async def radio_now(interaction: discord.Interaction):
    sess = radio_sessions.get(interaction.guild.id) if interaction.guild else None
    if not sess:
        await interaction.response.send_message("\u26a0\ufe0f No radio playing.", ephemeral=True)
        return
    await interaction.response.send_message(embed=radio_now_embed(sess["info"], sess["vc"].channel.mention if sess["vc"].channel else "voice"))


@bot.tree.command(name="radio-sources", description="Show which radio directories are working.")
async def radio_sources(interaction: discord.Interaction):
    def line(name: str, enabled: bool = True, note: str = "") -> str:
        if not enabled:
            return f"\u26aa **{name}** \u2014 off ({note})"
        h = _health(name)
        if time.monotonic() < h["until"]:
            return f"\U0001F534 **{name}** \u2014 paused after repeated errors: `{h['err']}`"
        if h["fails"]:
            return f"\U0001F7E1 **{name}** \u2014 last error: `{h['err']}`"
        return f"\U0001F7E2 **{name}** \u2014 ok ({h['ok']} requests)" if h["ok"] else f"\u26aa **{name}** \u2014 not used yet"
    lines = [
        line("Radio Browser"), line("SomaFM"),
        line("Radio Garden", RADIO_GARDEN_ON, "RADIO_GARDEN=0"),
        line("Wikidata", RADIO_WIKIDATA_ON, "RADIO_WIKIDATA=0"),
        line("FCC", bool(RADIO_FCC_FILE), "set FCC_FM_FILE to a CSV of US FM stations"),
    ]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@bot.tree.command(name="radio-stop", description="Stop the radio.")
async def radio_stop_cmd(interaction: discord.Interaction):
    if not interaction.guild or not await radio_stop(interaction.guild.id):
        await interaction.response.send_message("\u26a0\ufe0f No radio playing.", ephemeral=True)
        return
    await interaction.response.send_message("\u23F9\uFE0F Radio stopped.")


async def start_web():
    app = web.Application()
    app.add_routes([
        web.get("/", web_home),
        web.get("/emu/{token}", web_emu_page),
        web.get("/emu/{token}/ws", web_emu_ws),
        web.get("/radio", web_radio_page),
        web.get("/radio/api/countries", web_radio_countries),
        web.get("/radio/api/stations", web_radio_stations),
        web.get("/radio/stream/{sid}", web_radio_stream),
    ])
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    await web.TCPSite(runner, "0.0.0.0", port).start()


def main():
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        logger.critical("DISCORD_TOKEN missing!")
        sys.exit(1)

    refresh_ban_cache()
    refresh_auto_poll_cache()

    async def runner():
        await start_web()
        try:
            async with bot:
                await bot.start(token)
        finally:
            await radio_shutdown()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
