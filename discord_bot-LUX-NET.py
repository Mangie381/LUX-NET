import os
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
from dataclasses import dataclass, field
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
import yt_dlp
try:
    from ddgs import DDGS  # new package name
except ImportError:
    from duckduckgo_search import DDGS  # legacy package name

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
RELAY_MAX_FILE_MB = int(os.environ.get("RELAY_MAX_FILE_MB", "25"))
RELAY_SYNC_EDITS = os.environ.get("RELAY_SYNC_EDITS", "1") == "1"
RELAY_SYNC_DELETES = os.environ.get("RELAY_SYNC_DELETES", "1") == "1"
RELAY_CACHE_TTL = int(os.environ.get("RELAY_CACHE_TTL", "120"))

_target_cache: dict = {}          # (table, id) -> (expires_at, tuple_of_ids)
_thread_pairs: dict = {}          # forum thread id -> set of linked thread ids
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
            ra.data = await att.read()
        except Exception as e:
            logger.warning(f"Could not download attachment {att.filename}: {e}")
    return ra


async def build_relay_payload(message: discord.Message, *, with_reply: bool = True, extra_text: str = "") -> RelayPayload:
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

    limit_bytes = RELAY_MAX_FILE_MB * 1024 * 1024
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
    inline = [a for a in p.attachments if a.data is not None and a.size <= limit]
    links = [a.url for a in p.attachments if a not in inline]
    no_mentions = discord.AllowedMentions.none()

    def build(use_files: bool):
        extra = list(links) + ([] if use_files else [a.url for a in inline])
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
        payload = await build_relay_payload(starter_message, with_reply=False)
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

    await bot.process_commands(message)

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

    payload = await build_relay_payload(message)
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
def _entry_url(entry: dict) -> str:
    url = entry.get("webpage_url") or entry.get("url") or ""
    if not url.startswith("http") and entry.get("id"):
        url = f"https://www.youtube.com/watch?v={entry['id']}"
    return url


async def relay_watch_post(source_channel, requester: discord.abc.User, title: str, url: str):
    try:
        target_ids = await asyncio.to_thread(relay_targets_for, source_channel)
    except Exception as e:
        logger.error(f"Could not look up relay targets: {e}")
        return

    guild_name = source_channel.guild.name if source_channel.guild else "Unknown"
    username = f"{requester.display_name} [{guild_name}]"[:80]
    content = f"\u25B6\uFE0F **{requester.display_name}** is watching: **{title}**\n{url}"[:2000]

    async def one(cid: int):
        ch = bot.get_channel(cid)
        if not ch:
            return
        try:
            webhook = await get_or_create_webhook(ch)
            if webhook:
                await webhook.send(
                    username=username,
                    avatar_url=requester.display_avatar.url,
                    content=content,
                    allowed_mentions=discord.AllowedMentions.none(),
                    **_thread_kw(ch),
                )
            else:
                await ch.send(content, allowed_mentions=discord.AllowedMentions.none())
        except Exception as e:
            logger.warning(f"Could not relay video link: {e}")

    await asyncio.gather(*(one(cid) for cid in target_ids))


class YouTubeDropdown(discord.ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="Select the correct video...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        url = self.values[0]
        title = next((o.label for o in self.options if o.value == url), "YouTube video")

        await interaction.response.send_message(f"\u25B6\uFE0F **{interaction.user.display_name}** is watching: **{title}**\n{url}")
        if interaction.channel and supabase:
            await relay_watch_post(interaction.channel, interaction.user, title, url)


class YouTubeSelectView(discord.ui.View):
    def __init__(self, entries):
        super().__init__(timeout=60)
        options = []
        for entry in entries[:5]:
            url = _entry_url(entry)
            if not url:
                continue
            title = (entry.get("title") or "Unknown Title")[:100]
            uploader = entry.get("uploader") or entry.get("channel") or "Unknown"
            options.append(discord.SelectOption(label=title, description=f"By: {uploader}"[:100], value=url[:100]))
        self.has_options = bool(options)
        if options:
            self.add_item(YouTubeDropdown(options))

@bot.tree.command(name="play", description="Search YouTube, pick a video, and play inline.")
@app_commands.describe(search="Search keywords or URL")
async def play(interaction: discord.Interaction, search: str):
    await interaction.response.defer(ephemeral=True)

    entries = []
    if "youtube.com/watch" in search or "youtu.be/" in search:
        entries.append({'title': 'Direct YouTube Link', 'uploader': 'Provided URL', 'webpage_url': search.strip()})
    else:
        try:
            search_opts = {'extract_flat': True, 'quiet': True}
            query = f"ytsearch5:{search}"
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(search_opts).extract_info(query, download=False))
            entries = data.get('entries', [])
        except Exception as e:
            logger.error(f"yt_dlp search error: {e}")

        if not entries:
            try:
                for item in await asyncio.to_thread(google_search, f"{search} site:youtube.com/watch", 5):
                    url = item.get("href", "")
                    if "youtube.com/watch" in url:
                        entries.append({'title': item.get("title", 'YouTube Video Result'), 'uploader': 'Google Search', 'webpage_url': url})
            except Exception as e:
                logger.error(f"Fallback search error: {e}")

    if not entries:
        await interaction.followup.send(f"⚠️ Could not find videos for `{search}`.", ephemeral=True)
        return

    view = YouTubeSelectView(entries)
    if not view.has_options:
        await interaction.followup.send(f"⚠️ Could not find playable results.", ephemeral=True)
        return
    await interaction.followup.send("🔍 **Select the correct video below:**", view=view, ephemeral=True)

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
CORE_BASE_URL = os.environ.get("CORE_BASE_URL", "https://buildbot.libretro.com/nightly/linux/x86_64/latest")

MAX_EMU_SESSIONS = int(os.environ.get("MAX_EMU_SESSIONS", "2"))
EMU_IDLE_SECONDS = int(os.environ.get("EMU_IDLE_SECONDS", "300"))
EMU_SCALE = int(os.environ.get("EMU_SCALE", "2"))
EMU_HOLD_STEPS = [4, 8, 16, 30]
EMU_SETTLE_FRAMES = int(os.environ.get("EMU_SETTLE_FRAMES", "4"))
EMU_MAX_ROM_BYTES = int(os.environ.get("EMU_MAX_ROM_MB", "8")) * 1024 * 1024
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
}

EMU_MASKS = {
    "b": 1 << 0, "y": 1 << 1, "select": 1 << 2, "start": 1 << 3,
    "up": 1 << 4, "down": 1 << 5, "left": 1 << 6, "right": 1 << 7,
    "a": 1 << 8, "x": 1 << 9, "l": 1 << 10, "r": 1 << 11,
    "ab": (1 << 0) | (1 << 8),
}

emu_sessions: dict = {}
_remote_roms: set = set()
_ROM_BAD_CHARS = re.compile(r"[^\w .,'()&!+\-]")


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
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", WORKER_PATH,
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
                      "audio": bool(EMU_AUDIO and self.audio_rate), "rate": self.audio_rate, "ch": self.audio_ch})
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
        m = 0
        for c in self.clients:
            m |= c.mask
        if m != self._sent_mask:
            self._sent_mask = m
            self.send({"op": "input", "mask": m})
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
    for system in EMU_SYSTEMS:
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
<html lang="en">
<head><meta charset="utf-8"><title>LUX-NET Arcade</title></head>
<body><h1>LUX-NET Arcade</h1></body>
</html>
'''


async def web_home(request):
    return web.Response(text="LUX-NET Bridge Bot is Online!")


async def web_emu_page(request):
    if request.match_info["token"] not in emu_web_tokens:
        return web.Response(status=404, text="Expired.")
    return web.Response(text=EMU_PAGE_HTML, content_type="text/html")


async def web_emu_ws(request):
    info = emu_web_tokens.get(request.match_info["token"])
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    if not info:
        await ws.close()
        return ws
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
            await interaction.response.send_message("Live player link generated.", ephemeral=True)
        elif action == "quit":
            await interaction.response.defer()
            await emu_close(sess, "stopped")


@bot.tree.command(name="emu-play", description="Play a NES/SNES game.")
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

    await interaction.response.defer(ephemeral=True)
    sess = EmuSession(interaction.channel.id, game, interaction.user.id)
    emu_sessions[sess.channel_id] = sess
    try:
        rom_path = await asyncio.to_thread(ensure_rom_local, game)
        core_path = await asyncio.to_thread(ensure_core, sess.system)
        await sess.spawn()
        res = await sess.call(op="load", core=core_path, rom=rom_path,
                              sysdir=os.path.join(EMU_BASE, "system"), savedir=os.path.join(STATE_DIR, "sram"))
        if not res.get("ok"):
            raise RuntimeError(res.get("err"))
        sess.view = EmuView(sess)
        sess.seq += 1
        fname = f"f{sess.seq}.png"
        msg = await interaction.channel.send(embed=sess.make_embed(fname),
                                             file=discord.File(io.BytesIO(res.get("png", b"")), filename=fname), view=sess.view)
        sess.message_id = msg.id
        await interaction.followup.send("✅ Game started!", ephemeral=True)
    except Exception as e:
        emu_sessions.pop(sess.channel_id, None)
        await interaction.followup.send(f"❌ Failed: {e}", ephemeral=True)


@bot.tree.command(name="emu-games", description="List games.")
async def emu_games(interaction: discord.Interaction):
    roms = all_roms()
    embed = discord.Embed(title=f"Library ({len(roms)})")
    await interaction.response.send_message(embed=embed, ephemeral=True)


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
    while len(_radio_by_id) > 4000:
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
_STOP = re.compile(r"[^a-z0-9]+")


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
