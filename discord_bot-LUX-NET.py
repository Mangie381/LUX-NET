import os
import sys
import asyncio
import logging
import io
import json
import resource
import math
import re
import uuid
import urllib.request
import time
import pickle
import struct
import zipfile
import shutil
import atexit
import secrets
from collections import deque
from threading import Thread, Lock
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands
from aiohttp import web, WSMsgType   # ships with discord.py

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
# Bans are kept in memory and refreshed in the background. is_user_banned() is called for
# every message, reaction, button click and (most importantly) every 20 ms voice frame, so it
# must never touch the network.
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
# GEMINI AI OVERVIEW (shown above the search links in /search)
# Needs GEMINI_API_KEY (free key from Google AI Studio). GEMINI_MODEL is optional.
# Uses Gemini's built-in Google Search grounding so the overview reflects current results.
# ------------------------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")


def gemini_overview(query: str):
    """Returns a short AI overview for the query, or None if unavailable."""
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
# WEB SERVER (Render health check on '/' plus the live emulator player).
# It is started from main() before the bot logs in, see start_web() in the emulator section.
# ------------------------------------------------------------------------------

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
# STAR RATING POLLS (1-5 stars, persistent buttons, synced across relayed channels)
# Needs the tables in rating_poll_setup.sql.
# ------------------------------------------------------------------------------
# Old polls stored this default text in the database; it is hidden when their embeds refresh.
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
    """Tolerant ISO parser (Postgres can return 5-digit fractions, which older Pythons reject)."""
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
    # No footer text: Discord renders the timestamp on its own as "\u2014 dd/mm/yyyy, hh:mm".
    return discord.Embed(
        title=f"\u2b50 {title}"[:256],
        description=description[:4000],
        color=discord.Color.gold(),
        timestamp=created_at,
    )


class RatingButton(discord.ui.DynamicItem[discord.ui.Button], template=r"lux_rate:(?P<poll>[0-9a-f]{8,32}):(?P<n>[1-5])"):
    """One star button. Registered as a dynamic item so it keeps working after bot restarts."""

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
        logger.error(f"Failed to create rating poll (did you run rating_poll_setup.sql?): {e}")
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
        # Fallback if the rating_counts() SQL function was not created.
        counts = {n: 0 for n in range(1, 6)}
        res = supabase.table("rating_votes").select("rating").eq("poll_id", poll_id).limit(100000).execute()
        for row in res.data:
            counts[int(row["rating"])] += 1
        return counts


def rating_vote(poll_id: str, user_id: int, rating: int):
    """Toggle/replace a user's vote. Returns (meta, counts, my_vote_or_None, [(channel_id, message_id)])."""
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
    """Push the fresh embed to every copy of a poll in every linked server."""
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
    # Attach the poll to a specific message (the first post) as a reply. Only the bot can reply;
    # webhooks can't, so this goes first and the webhook path below stays as the fallback.
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
            logger.warning(f"Could not reply to first message {reply_to_id} in {channel.id}, using normal poll post: {e}")
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
            logger.warning(f"Webhook poll post failed in {channel.id}, falling back to bot message: {e}")
    try:
        return await channel.send(embed=embed, view=build_rating_view(poll_id))
    except Exception as e:
        logger.error(f"Could not post rating poll in {channel.id}: {e}")
        return None


def relay_targets_for(channel) -> list:
    if isinstance(channel, discord.Thread):
        table, col = "thread_relays", _LINK_COLS["thread_relays"]
    else:
        table, col = "text_relays", _LINK_COLS["text_relays"]
    ids = set()
    res = supabase.table(table).select("network_code").eq(col, channel.id).execute()
    for row in res.data:
        r = supabase.table(table).select(col).eq("network_code", row["network_code"]).neq(col, channel.id).execute()
        ids.update(int(x[col]) for x in r.data)
    return list(ids)


def forum_targets_for(forum_id: int) -> list:
    """Forum channels linked (via /link-forum) to the given forum."""
    col = _LINK_COLS["forum_relays"]
    ids = set()
    res = supabase.table("forum_relays").select("network_code").eq(col, forum_id).execute()
    for row in res.data:
        r = supabase.table("forum_relays").select(col).eq("network_code", row["network_code"]).neq(col, forum_id).execute()
        ids.update(int(x[col]) for x in r.data)
    return list(ids)


# ------------------------------------------------------------------------------
# AUTO POLL (automatically adds the 1-5 rating poll to every new post in a forum)
# Needs the auto_poll_forums table in auto_poll_setup.sql.
# ------------------------------------------------------------------------------
_auto_poll_forums: set = set()


def refresh_auto_poll_cache() -> bool:
    global _auto_poll_forums
    try:
        res = supabase.table("auto_poll_forums").select("forum_channel_id").execute()
        _auto_poll_forums = {int(row["forum_channel_id"]) for row in res.data}
        return True
    except Exception as e:
        logger.error(f"Failed to load auto poll forums (did you run auto_poll_setup.sql?): {e}")
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
    """Post a rating poll in a new forum post and in every relayed copy of that post."""
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

        # In a forum post the first message shares its ID with the thread, in the original
        # and in every relayed copy, so the poll replies straight to that first message.
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
    asyncio.create_task(ban_cache_loop())
    asyncio.create_task(emu_reaper_loop())
    asyncio.create_task(emu_startup())
    asyncio.create_task(radio_warm())

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

async def _relay_new_forum_post(thread: discord.Thread):
    """Relays a new forum post to linked forums. Returns (starter_message or None, [copied threads]).
    starter_message is None when the post should not get an auto poll (bot/webhook/banned/empty)."""
    copies = []
    if not isinstance(thread.parent, discord.ForumChannel):
        return None, copies

    if thread.id in RELAYED_THREAD_IDS:
        return None, copies

    current_forum_id = thread.parent.id

    if not supabase:
        return None, copies

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
                return None, copies
            if (starter_message.webhook_id and starter_message.webhook_id in webhook_ids) or starter_message.author.bot:
                RELAYED_THREAD_IDS.add(thread.id)
                return None, copies

        if not starter_message:
            return None, copies

        res = supabase.table("forum_relays").select("network_code").eq("forum_channel_id", current_forum_id).execute()
        codes = [row["network_code"] for row in res.data]
        if not codes:
            return starter_message, copies

        target_forum_ids = []
        for code in codes:
            f_res = supabase.table("forum_relays").select("forum_channel_id").eq("network_code", code).neq("forum_channel_id", current_forum_id).execute()
            target_forum_ids.extend([row["forum_channel_id"] for row in f_res.data])
    except Exception as e:
        logger.error(f"Database error in on_thread_create: {e}")
        return starter_message, copies

    target_forum_ids = list(set(target_forum_ids))
    if not target_forum_ids:
        return starter_message, copies

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
                                copies.append(sent_msg.channel)
                            elif sent_msg.thread:
                                RELAYED_THREAD_IDS.add(sent_msg.thread.id)
                                copies.append(sent_msg.thread)
                                
                            register_message_mapping(starter_message.id, target_forum.id, sent_msg.id)
                    except Exception as e:
                        logger.error(f"Error relaying forum post to {fid}: {e}")
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

    # Auto poll: add the 1-5 rating poll to new posts in forums where /auto-poll is enabled.
    if starter_message is not None and thread.parent.id in _auto_poll_forums:
        await post_auto_poll(thread, starter_message, copies)

# ------------------------------------------------------------------------------
# VOICE BRIDGE (LIVE AUDIO RELAY BETWEEN LINKED VOICE CHANNELS)
# ------------------------------------------------------------------------------
FRAME_BYTES = 3840                 
SILENCE = b"\x00" * FRAME_BYTES
# --- latency / quality tuning (all overridable with environment variables) ---------
# Per-speaker playback queue, in 20 ms frames. Hard cap on how far behind live audio can get.
MAX_BUFFERED_FRAMES = int(os.environ.get("VOICE_MAX_QUEUE_FRAMES", "4"))
# A new talk-spurt waits until this many frames are queued (or PRIME_MAX_WAIT ticks pass).
# This is the only place network jitter gets smoothed, and it costs at most ~20-40 ms.
PRIME_FRAMES = int(os.environ.get("VOICE_PRIME_FRAMES", "2"))
PRIME_MAX_WAIT = 2
# Keep the player alive ~0.6 s after the last frame so short pauses don't restart it.
IDLE_FRAMES_BEFORE_STOP = 30
# voice_recv jitter buffer (per speaker). maxsize also bounds how long output stalls when a
# packet is lost/late (library default is 10 frames = 200 ms). prefsize is reorder tolerance.
JITTER_MAX_PACKETS = int(os.environ.get("VOICE_JITTER_MAX", "4"))
JITTER_PREF_PACKETS = int(os.environ.get("VOICE_JITTER_PREF", "1"))
# Outgoing Opus encoder.
OPUS_APPLICATION = os.environ.get("VOICE_OPUS_APPLICATION", "audio")   # audio | voip | lowdelay
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
            if voice_stats["decode_err"] <= 5:
                d = getattr(packet, "decrypted_data", None) or b""
                logger.warning(
                    f"Skipped undecodable voice packet ({e}): ssrc={self.ssrc} len={len(d)} "
                    f"head={d[:6].hex()} tail={d[-4:].hex()}"
                )
            return packet, SILENCE

    vr_opus.PacketDecoder._decode_packet = safe_decode

    # Smaller jitter buffer = lower latency and no 200 ms stall after a lost packet.
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
            if voice_stats["dave_fail"] <= 5:
                logger.warning(f"DAVE decrypt failed for user {user_id}: {e!r}")
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
                voice_stats["mix_drop"] += 1   # oldest frame is discarded to stay near live
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
                    self.playing = False   # lets the next frame restart playback immediately
                    return b""
                return SILENCE

            self.idle = 0
            if len(frames) == 1:
                return frames[0]           # one speaker: pass through untouched

            # Several speakers: scale each so the sum has headroom instead of hard clipping.
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
            """Safe to call from any thread; starts the player without waiting on the event loop."""
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
                    logger.debug(f"Could not start voice playback in guild {self.guild_id}: {e}")

        def _after_play(self, error, gen):
            if error:
                logger.error(f"Voice playback error in guild {self.guild_id}: {error}")
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
            # Runs on the packet-router thread ~50x/second per speaker: no network, no awaits.
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
        logger.error(f"Error disconnecting voice in guild {guild_id}: {e}")
    session.mixer.buffers.clear()
    logger.info(f"Voice session ended in guild {guild_id}")


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

    # Typed emulator controls (w/a/s/d ...) in a channel with a running game.
    if message.channel.id in emu_sessions and await emu_chat_input(message):
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

@bot.tree.command(name="poll", description="Post a 1-5 star rating poll and sync it across connected relay channels.")
@app_commands.describe(
    title="What people are rating (e.g. a TV show)",
    message="Line shown under the ratings (optional)"
)
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
        await interaction.followup.send(
            "\u274c Couldn't create the poll. Make sure `rating_poll_setup.sql` has been run in Supabase.", ephemeral=True
        )
        return

    origin = interaction.channel
    target_channels = []
    try:
        if isinstance(origin, discord.Thread) and isinstance(origin.parent, discord.ForumChannel):
            # Forum post: copy the poll into the matching post (same title) in every linked forum,
            # exactly like messages are matched by the relay.
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
        await interaction.followup.send("\u274c I couldn't post the poll here (check my Send Messages / Manage Webhooks permissions).", ephemeral=True)

@bot.tree.command(name="auto-poll", description="Auto-add the 1-5 star rating poll to every new post in a forum.")
@app_commands.describe(
    enabled="Turn auto poll on or off for the forum",
    forum_channel="Forum to change (optional, defaults to the forum you're in)"
)
@app_commands.choices(enabled=[
    app_commands.Choice(name="on", value=1),
    app_commands.Choice(name="off", value=0),
])
async def auto_poll(interaction: discord.Interaction, enabled: app_commands.Choice[int], forum_channel: discord.ForumChannel = None):
    if not interaction.guild or not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("\u274c You need the **Manage Channels** permission to change auto poll.", ephemeral=True)
        return

    target = forum_channel
    if not target:
        ch = interaction.channel
        if isinstance(ch, discord.Thread) and isinstance(ch.parent, discord.ForumChannel):
            target = ch.parent
        elif isinstance(ch, discord.ForumChannel):
            target = ch

    if not target:
        await interaction.response.send_message(
            "\u274c Pick a `forum_channel` in the option, or run this inside a forum or forum post.", ephemeral=True
        )
        return

    await interaction.response.defer()
    on = bool(enabled.value)
    ok = await asyncio.to_thread(set_auto_poll, target.id, on)
    if not ok:
        await interaction.followup.send(
            "\u274c Couldn't save that. Make sure `auto_poll_setup.sql` has been run in Supabase.", ephemeral=True
        )
        return

    if on:
        await interaction.followup.send(
            f"\u2b50 Auto poll is **on** for **{target.name}**. Every new post there gets a 1-5 star rating poll "
            f"(also copied into the matching post in linked forums)."
        )
    else:
        await interaction.followup.send(f"\U0001F6D1 Auto poll is **off** for **{target.name}**.")


@bot.tree.command(name="list-bridges", description="List all servers (with member counts), channels, threads, and forums connected to a network code.")
@app_commands.describe(network_code="The network code to inspect")
async def list_bridges(interaction: discord.Interaction, network_code: str):
    code = network_code.strip().lower()
    await interaction.response.defer(ephemeral=True)

    text_ids = get_links("text_relays", code)
    thread_ids = get_links("thread_relays", code)
    forum_ids = get_links("forum_relays", code)
    vc_ids = get_links("vc_code_relays", code)

    embed = discord.Embed(title=f"🌐 Bridge Status for Network: `{code}`", color=discord.Color.blurple())

    # Every server in the network, counted once no matter how many of its channels are linked.
    # guild.member_count comes with the guild itself, so this needs no extra intent or API call.
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
        if used + len(line) + 1 > 900:       # embed field limit is 1024 characters
            server_lines.append(f"…and {len(ranked) - i} more")
            break
        server_lines.append(line)
        used += len(line) + 1
    embed.add_field(
        name=f"🏠 Servers ({len(ranked)}) • {total_members:,} members total",
        value=("\n".join(server_lines) if server_lines else "None")
              + ("\n\n*Total adds up each server's member count, so someone who is in several of these servers is counted once per server.*" if len(ranked) > 1 else ""),
        inline=False,
    )

    text_desc = []
    for cid in text_ids:
        ch = bot.get_channel(cid)
        if ch:
            text_desc.append(f"• #{ch.name} (*{ch.guild.name}* · {ch.guild.member_count or 0:,} members)")
        else:
            text_desc.append(f"• ID: `{cid}` (Cached/Missing)")
    embed.add_field(name=f"💬 Text Channels ({len(text_ids)})", value=("\n".join(text_desc)[:1015] + ("…" if len("\n".join(text_desc)) > 1015 else "")) if text_desc else "None", inline=False)

    thread_desc = []
    for tid in thread_ids:
        th = bot.get_channel(tid)
        if th:
            thread_desc.append(f"• Thread: {th.name} (*{th.guild.name}* · {th.guild.member_count or 0:,} members)")
        else:
            thread_desc.append(f"• ID: `{tid}` (Cached/Missing)")
    embed.add_field(name=f"🧵 Threads ({len(thread_ids)})", value=("\n".join(thread_desc)[:1015] + ("…" if len("\n".join(thread_desc)) > 1015 else "")) if thread_desc else "None", inline=False)

    forum_desc = []
    for fid in forum_ids:
        fch = bot.get_channel(fid)
        if fch:
            forum_desc.append(f"• Forum: {fch.name} (*{fch.guild.name}* · {fch.guild.member_count or 0:,} members)")
        else:
            forum_desc.append(f"• ID: `{fid}` (Cached/Missing)")
    embed.add_field(name=f"📌 Forum Channels ({len(forum_ids)})", value=("\n".join(forum_desc)[:1015] + ("…" if len("\n".join(forum_desc)) > 1015 else "")) if forum_desc else "None", inline=False)

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

    live = voice_sessions.get(interaction.guild.id)
    if live and live.channel_id == target.id:
        live.codes = live.codes | {code}

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
            f"Smoothing (all servers): frames dropped to stay live `{voice_stats['mix_drop']}`, re-buffers `{voice_stats['rebuffer']}`",
            f"Tuning: jitter `{JITTER_MAX_PACKETS}` pkts, queue `{MAX_BUFFERED_FRAMES}` frames, Opus `{OPUS_APPLICATION}` @ `{OPUS_BITRATE_KBPS}` kbps",
        ]
        if session.frames_in == 0:
            lines.append("_No audio received yet. If people are talking and this stays 0, receive is failing (check the logs / DAVE note)._")
    else:
        lines.append("This server: not in a call right now (the bot joins when someone enters a linked voice channel).")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)

# ------------------------------------------------------------------------------
# YOUTUBE & WEB SEARCH COMMANDS
# ------------------------------------------------------------------------------
def _entry_url(entry: dict) -> str:
    """Flat yt-dlp search results don't always carry webpage_url, so fall back to url / id."""
    url = entry.get("webpage_url") or entry.get("url") or ""
    if not url.startswith("http") and entry.get("id"):
        url = f"https://www.youtube.com/watch?v={entry['id']}"
    return url


async def relay_watch_post(source_channel, requester: discord.abc.User, title: str, url: str):
    """Post the picked video link into every channel linked to source_channel.
    Only the URL is sent, so Discord unfurls the playable YouTube player in each server."""
    try:
        target_ids = await asyncio.to_thread(relay_targets_for, source_channel)
    except Exception as e:
        logger.error(f"Could not look up relay targets for /play: {e}")
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
            logger.warning(f"Could not relay video link to {cid}: {e}")

    await asyncio.gather(*(one(cid) for cid in target_ids))


class YouTubeDropdown(discord.ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="Select the correct video from the search...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        url = self.values[0]
        title = next((o.label for o in self.options if o.value == url), "YouTube video")

        # Public message in this channel: the bare URL makes Discord show the playable embed.
        await interaction.response.send_message(
            f"\u25B6\uFE0F **{interaction.user.display_name}** is watching: **{title}**\n{url}"
        )
        # Same post in every linked channel / server.
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
            uploader = entry.get("uploader") or entry.get("channel") or "Unknown Channel"
            options.append(
                discord.SelectOption(
                    label=title,
                    description=f"By: {uploader}"[:100],
                    value=url[:100],
                )
            )
        self.has_options = bool(options)
        if options:
            self.add_item(YouTubeDropdown(options))

@bot.tree.command(name="play", description="Search YouTube, pick a video, and it plays inline here and in linked servers.")
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
    if not view.has_options:
        await interaction.followup.send(f"\u26A0\uFE0F Could not find any playable results for `{search}`.", ephemeral=True)
        return
    await interaction.followup.send("🔍 **Select the correct video below:**", view=view, ephemeral=True)

@bot.tree.command(name="search", description="Web search with a Gemini AI overview and result links.")
@app_commands.describe(query="What would you like to search for?")
async def search(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    try:
        # Links and the AI overview are fetched at the same time.
        results, overview = await asyncio.gather(
            asyncio.to_thread(google_search, query, 5),
            asyncio.to_thread(gemini_overview, query),
        )

        if not results:
            await interaction.followup.send(f"⚠ No results found for `{query}`.")
            return

        embed = discord.Embed(
            title=f"🔍 Search Results for: `{query}`",
            color=discord.Color.green()
        )
        if overview:
            embed.description = f"✨ **AI Overview**\n{overview[:1800]}"
            embed.set_footer(text="AI overview by Gemini. It can make mistakes, so check the links below.")

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
# NES / SNES EMULATOR (libretro cores in a throw-away worker process per game)
# ------------------------------------------------------------------------------
# Needs emu_worker.py next to this file and Pillow in requirements.txt.
# Cores (fceumm / snes9x2010) are downloaded automatically on first start. ROMs are NOT
# included: drop your own dumps in roms/nes and roms/snes, or use /emu-add.
# Optional persistence: create a private Supabase Storage bucket named "roms"
# (ROM_BUCKET env var changes the name) so uploaded ROMs survive redeploys.
EMU_BASE = os.path.dirname(os.path.abspath(__file__))
ROM_DIR = os.environ.get("ROM_DIR", os.path.join(EMU_BASE, "roms"))
CORE_DIR = os.environ.get("CORE_DIR", os.path.join(EMU_BASE, "cores"))
STATE_DIR = os.environ.get("STATE_DIR", os.path.join(EMU_BASE, "states"))
ROM_BUCKET = os.environ.get("ROM_BUCKET", "roms")
WORKER_PATH = os.path.join(EMU_BASE, "emu_worker.py")
CORE_BASE_URL = os.environ.get("CORE_BASE_URL", "https://buildbot.libretro.com/nightly/linux/x86_64/latest")

MAX_EMU_SESSIONS = int(os.environ.get("MAX_EMU_SESSIONS", "2"))          # one worker process each
EMU_IDLE_SECONDS = int(os.environ.get("EMU_IDLE_SECONDS", "300"))        # idle sessions are killed, freeing RAM
EMU_SCALE = int(os.environ.get("EMU_SCALE", "2"))                        # PNG upscale (nearest neighbour)
EMU_HOLD_STEPS = [4, 8, 16, 30]                                          # frames a tap holds a button
EMU_SETTLE_FRAMES = int(os.environ.get("EMU_SETTLE_FRAMES", "4"))        # frames run after release
EMU_MAX_ROM_BYTES = int(os.environ.get("EMU_MAX_ROM_MB", "8")) * 1024 * 1024
EMU_ADD_OPEN = os.environ.get("EMU_ADD_OPEN", "0") == "1"                # let Manage Server holders add ROMs

# Live streaming / controls (all optional, tune for the free tier)
EMU_STREAM_FPS = int(os.environ.get("EMU_STREAM_FPS", "60"))              # frames per second sent to browsers (the core's own rate is the ceiling)
EMU_AUDIO = os.environ.get("EMU_AUDIO", "1") == "1"                       # stream game sound to the live player (only while a browser has sound on)
# EMU_AUDIO_MONO=1 (read by the worker) halves the audio bandwidth
EMU_JPEG_QUALITY = int(os.environ.get("EMU_JPEG_QUALITY", "72"))          # lower = less bandwidth/CPU
EMU_AFK_PAUSE = int(os.environ.get("EMU_AFK_PAUSE", "60"))                # no input for this long: emulation sleeps (0 CPU)
EMU_MAX_VIEWERS = int(os.environ.get("EMU_MAX_VIEWERS", "8"))             # browsers per game
EMU_EDIT_INTERVAL = float(os.environ.get("EMU_EDIT_INTERVAL", "1.0"))     # min seconds between Discord picture updates
EMU_GAP_FRAMES = 4                                                        # frames between two typed/button taps
EMU_CHAT_KEYS = os.environ.get("EMU_CHAT_KEYS", "1") == "1"               # type w/a/s/d in the game channel
EMU_CHAT_PREFIX = os.environ.get("EMU_CHAT_PREFIX", "").lower()           # e.g. "." to require ".wasd" (avoids eating words like "sad")
EMU_DELETE_KEYS = os.environ.get("EMU_DELETE_KEYS", "1") == "1"           # delete typed inputs (needs Manage Messages)
EMU_WEB_BASE = (os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")

EMU_SYSTEMS = {
    "nes": {"label": "NES", "exts": {".nes"}, "core": os.environ.get("CORE_NES", "fceumm_libretro")},
    # snes9x2010 is the light/fast SNES core; set CORE_SNES=snes9x_libretro for max accuracy.
    "snes": {"label": "SNES", "exts": {".sfc", ".smc", ".swc", ".fig"}, "core": os.environ.get("CORE_SNES", "snes9x2010_libretro")},
}

# libretro joypad bit positions
EMU_MASKS = {
    "b": 1 << 0, "y": 1 << 1, "select": 1 << 2, "start": 1 << 3,
    "up": 1 << 4, "down": 1 << 5, "left": 1 << 6, "right": 1 << 7,
    "a": 1 << 8, "x": 1 << 9, "l": 1 << 10, "r": 1 << 11,
    "ab": (1 << 0) | (1 << 8),
}

emu_sessions: dict = {}        # channel_id -> EmuSession
_remote_roms: set = set()      # "nes/Name.nes" entries that live in the Supabase bucket
_ROM_BAD_CHARS = re.compile(r"[^\w .,'()&!+\-]")


# ---- cores ---------------------------------------------------------------------------------
def ensure_core(system: str) -> str:
    """Return the path of the libretro core for a system, downloading it once if missing."""
    name = EMU_SYSTEMS[system]["core"]
    path = os.path.join(CORE_DIR, name + ".so")
    if os.path.exists(path):
        return path
    os.makedirs(CORE_DIR, exist_ok=True)
    url = f"{CORE_BASE_URL}/{name}.so.zip"
    logger.info(f"Downloading libretro core {name} ...")
    with urllib.request.urlopen(url, timeout=60) as r:
        blob = r.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        member = next(m for m in z.namelist() if m.endswith(".so"))
        tmp = path + ".part"
        with z.open(member) as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst)
    os.replace(tmp, path)
    return path


# ---- ROM library ---------------------------------------------------------------------------
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
    """'nes/Game.nes' -> (system, filename), or None if it is not a safe library reference."""
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
        logger.warning(f"ROM bucket '{ROM_BUCKET}' not available, using local ROM files only: {e}")


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
    """Save a ROM locally and (best effort) in the Supabase bucket. Returns True if it is persistent."""
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
        logger.warning(f"Could not persist ROM to bucket '{ROM_BUCKET}': {e}")
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
        logger.warning(f"Could not remove {rel} from bucket: {e}")
    _remote_roms.discard(rel)


def sync_roms_to_disk() -> None:
    """Pull bucket ROMs onto local disk at startup so the first /emu-play has no download delay."""
    for rel in sorted(_remote_roms):
        try:
            ensure_rom_local(rel)
        except Exception as e:
            logger.warning(f"Could not pre-load {rel}: {e}")


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


# ---- sessions ------------------------------------------------------------------------------
def _proc_rss_mb(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1048576
    except Exception:
        return 0.0


class EmuClient:
    """One connected browser tab. Slow tabs only ever get the newest frame, so memory stays flat."""

    def __init__(self, ws, uid: int, name: str):
        self.ws, self.uid, self.name = ws, uid, name
        self.mask = 0
        self.latest = None
        self.texts = deque(maxlen=20)
        self.audio = deque(maxlen=30)       # ~0.5 s: a stalled tab loses sound, it never grows memory
        self.want_audio = False             # the page says so once its sound is unlocked and un-muted
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
                if self.audio:                       # whatever piled up goes out as ONE message
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
        self.pending = None             # future for the reply to the request in flight
        self._err_task = None
        self._reader_task = None
        # live streaming
        self.clients: set = set()
        self.tokens: dict = {}          # user_id -> web token
        self.live = False
        self.paused = False
        self.last_jpeg = None
        self.audio_rate = 0
        self.audio_ch = 2
        self._audio_sent = False
        self.stats: dict = {}
        self._sent_mask = 0
        # Discord preview (edits are rate limited, so they are coalesced)
        self.last_edit = 0.0
        self._refresh_due = 0.0
        self._refresh_task = None
        self.footer = ""
        self.chat_backlog = 0
        self.can_delete = True

    # -- paths / small helpers -----------------------------------------------------------------
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

    # -- worker process ------------------------------------------------------------------------
    async def spawn(self):
        env = dict(os.environ, MALLOC_ARENA_MAX="2", PYTHONUNBUFFERED="1")   # MALLOC_ARENA_MAX keeps RSS small
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", WORKER_PATH,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=env,
        )
        self._err_task = asyncio.ensure_future(self._drain_stderr(self.proc))
        self._reader_task = asyncio.ensure_future(self._reader(self.proc))

    async def _drain_stderr(self, proc):
        """Forward worker/core stderr to the bot log and keep the last lines for error messages."""
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode(errors="replace").rstrip()
                if text:
                    self.err_tail.append(text)
                    logger.warning(f"[emu worker] {text}")
        except Exception:
            pass

    async def _death_reason(self) -> str:
        code = None
        try:
            code = await asyncio.wait_for(self.proc.wait(), timeout=1.5)
        except Exception:
            pass
        await asyncio.sleep(0.2)   # let the stderr reader catch up
        tail = " | ".join(list(self.err_tail)[-4:])[-350:]
        hint = " (crashed: segmentation fault)" if code == -11 else ""
        return f"the emulator process stopped (exit code {code}){hint}" + (f": {tail}" if tail else "")

    async def _reader(self, proc):
        """Single reader for everything the worker says: replies, video frames and events."""
        try:
            while True:
                (n,) = struct.unpack("<I", await proc.stdout.readexactly(4))
                body = await proc.stdout.readexactly(n)
                kind, payload = body[:1], body[1:]
                if kind == b"J":
                    self._on_frame(body)              # "J" + pad + jpeg, forwarded to browsers untouched
                elif kind == b"A":
                    self._on_audio(body)              # "A" + pad + int16 PCM
                elif kind == b"R":
                    fut = self.pending
                    if fut is not None and not fut.done():
                        fut.set_result(pickle.loads(payload))
                elif kind == b"E":
                    self._on_event(pickle.loads(payload))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as e:
            logger.error(f"Emulator reader failed: {e}")
        finally:
            if not self.closed:
                reason = await self._death_reason()
                logger.error(f"Emulator worker died: {reason}")
                fut = self.pending
                if fut is not None and not fut.done():
                    fut.set_result({"ok": False, "err": reason, "dead": True})
                asyncio.ensure_future(emu_close(self, "emulator crashed", autosave=False))

    def _write(self, msg: dict):
        data = pickle.dumps(msg, protocol=4)
        self.proc.stdin.write(struct.pack("<I", len(data)) + data)

    def send(self, msg: dict):
        """Fire and forget (ops the worker never answers: input, seq, live)."""
        if self.closed or self.proc is None or self.proc.returncode is not None:
            return
        try:
            self._write(msg)
        except Exception:
            pass

    async def call(self, **msg) -> dict:
        """Request/reply. Never raises. One request at a time per game."""
        async with self.lock:
            if self.closed or self.proc is None or self._reader_task is None or self._reader_task.done():
                return {"ok": False, "err": "session ended", "dead": True}
            self.pending = asyncio.get_running_loop().create_future()
            try:
                self._write(msg)
                return await self.pending
            except (BrokenPipeError, ConnectionResetError):
                return {"ok": False, "err": await self._death_reason(), "dead": True}
            except Exception as e:
                logger.error(f"Emulator RPC failed: {e}")
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

    # -- live stream ---------------------------------------------------------------------------
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
        """The worker only produces sound while at least one browser actually has it switched on."""
        want = EMU_AUDIO and any(c.want_audio for c in self.clients)
        if want != self._audio_sent:
            self._audio_sent = want
            self.send({"op": "audio", "on": want})

    def _on_event(self, ev: dict):
        kind = ev.get("ev")
        if kind == "stats":
            self.stats = ev
        elif kind == "paused":
            self.paused = bool(ev.get("on"))
            self._broadcast({"t": "paused", "on": self.paused})
        elif kind == "error":
            logger.warning(f"[emu worker] {ev.get('err')}")

    def _set_live(self, on: bool):
        if on == self.live:
            return
        self.live = on
        if not on:
            self._audio_sent = False      # the worker switches sound off together with live mode
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
        """All connected players are OR-ed together into one controller."""
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

    # -- Discord message (the "slow" screen) ---------------------------------------------------
    async def press(self, steps: list) -> dict:
        """Button taps from Discord. Live: queued into the real-time loop. Otherwise a fast burst."""
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
        """Update the Discord picture at most once per EMU_EDIT_INTERVAL (3x slower while someone is on the live player), always with the newest frame."""
        while not self.closed:
            interval = EMU_EDIT_INTERVAL * (3 if self.clients else 1)   # live viewers have the real picture
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
        except discord.NotFound:
            asyncio.ensure_future(emu_close(self, "the game message was deleted"))
        except discord.HTTPException as e:
            logger.warning(f"Could not update emulator message: {e}")


emu_web_tokens: dict = {}      # web token -> (EmuSession, user_id, display_name)


async def emu_close(sess: EmuSession, reason: str, autosave: bool = True):
    if emu_sessions.get(sess.channel_id) is sess:
        emu_sessions.pop(sess.channel_id, None)
    if sess.closing:
        return
    sess.closing = True
    # Autosave so the next /emu-play can pick up where the players left off.
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
            sess.proc.stdin.close()      # worker exits on EOF
            await asyncio.wait_for(sess.proc.wait(), timeout=2)
        except Exception:
            pass
    sess.kill()
    if sess.message_id:
        ch = bot.get_channel(sess.channel_id)
        if ch:
            try:
                await ch.get_partial_message(sess.message_id).edit(content=f"⏹️ **{sess.label}: {sess.title}** ended ({reason}).", view=None)
            except Exception:
                pass
    logger.info(f"Emulator session closed in channel {sess.channel_id}: {reason}")


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
        except Exception as e:
            logger.error(f"Could not get the {system} core (set CORE_BASE_URL or place the .so in {CORE_DIR}): {e}")
    await asyncio.to_thread(refresh_remote_roms)
    await asyncio.to_thread(sync_roms_to_disk)
    logger.info(f"Emulator ready: {len(all_roms())} game(s) in the library.")
    while True:    # keep the bucket listing fresh for autocomplete without hitting the network per keystroke
        await asyncio.sleep(300)
        await asyncio.to_thread(refresh_remote_roms)


# ---- typed controls: just type w a s d in the game channel -----------------------------------
# w a s d = D-pad, j = B, k = A, u = Y, i = X, q = L, e = R, start, select.
# "d3" = tap right 3 times, "d+j" = right and B together, "wwdk" = a sequence.
_KEY_ALIASES = {"w": "up", "a": "left", "s": "down", "d": "right", "j": "b", "k": "a", "u": "y",
                "i": "x", "q": "l", "e": "r", "up": "up", "down": "down", "left": "left",
                "right": "right", "start": "start", "select": "select"}
_KEY_ONE = r"(?:start|select|up|down|left|right|[wasdjkuiqe])"
_KEY_GROUP = rf"{_KEY_ONE}(?:\+{_KEY_ONE})*(?:\d{{1,2}})?"
_KEY_FULL = re.compile(rf"{_KEY_GROUP}(?:[ ,]*{_KEY_GROUP})*")
_KEY_ITER = re.compile(rf"({_KEY_ONE}(?:\+{_KEY_ONE})*)(\d{{1,2}})?")


def parse_chat_keys(text: str, hold: int):
    """'wwd k3' -> [(mask, frames), ...] or None when the message is normal chat."""
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
        sess.can_delete = False      # no Manage Messages here: stop trying
    except Exception:
        pass


async def emu_chat_input(message: discord.Message) -> bool:
    """Called from on_message. True = the message was a controller input and is handled."""
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
    if sess.chat_backlog >= 4:      # swallow floods instead of queueing forever
        return True
    sess.chat_backlog += 1
    try:
        sess.inputs += 1
        sess.footer = f"{message.author.display_name}: {message.content.strip()[:24]}  •  {sess.inputs} inputs"
        await sess.press(steps)
    finally:
        sess.chat_backlog -= 1
    return True


# ---- live web player -------------------------------------------------------------------------
EMU_PAGE_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
<meta name="referrer" content="no-referrer">
<title>LUX-NET Arcade</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{margin:0;height:100%;background:#0b0d12;color:#e6e8ee;font:14px/1.4 system-ui,-apple-system,Segoe UI,sans-serif;overflow:hidden;touch-action:none;user-select:none;-webkit-user-select:none}
#wrap{display:flex;flex-direction:column;height:100%}
#bar{display:flex;gap:8px;align-items:center;padding:6px 10px;background:#12151c;border-bottom:1px solid #23283a;font-size:13px}
#title{font-weight:600;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.pill{padding:2px 8px;border-radius:99px;background:#1c2130;color:#9aa4bd;white-space:nowrap}
button.t{background:#1c2130;color:#cfd6ea;border:0;border-radius:8px;padding:4px 9px;font-size:14px;cursor:pointer}
#stage{flex:1;min-height:0;display:flex;align-items:center;justify-content:center;position:relative}
canvas{image-rendering:pixelated;background:#000;border-radius:4px}
#msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;padding:20px;background:rgba(8,10,14,.72);font-size:17px;pointer-events:none}
#msg:empty{display:none}
#help{padding:5px 10px;font-size:12px;color:#7d87a3;text-align:center;border-top:1px solid #1b2030}
#pad{display:none;height:min(38vh,250px);padding:6px 10px calc(10px + env(safe-area-inset-bottom));position:relative}
body.touch #pad{display:block}
body.touch #help{display:none}
.g{position:absolute;display:grid;gap:6px}
.b{width:54px;height:54px;border-radius:50%;background:#232a3d;border:1px solid #33405e;color:#cfd6ea;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:17px;touch-action:none}
.b.on{background:#4d6bff;border-color:#8ea2ff}
.b.sm{width:64px;height:30px;border-radius:15px;font-size:12px}
.b.sh{width:74px;height:36px;border-radius:12px;font-size:13px}
#dp{left:8px;bottom:8px;grid-template-columns:repeat(3,54px);grid-template-rows:repeat(3,54px)}
#ab{right:8px;bottom:8px;grid-template-columns:repeat(3,54px);grid-template-rows:repeat(3,54px)}
#mid{position:absolute;left:50%;transform:translateX(-50%);bottom:14px;display:flex;gap:10px}
#sh1{left:8px;top:2px}#sh2{right:8px;top:2px}
</style>
</head>
<body>
<div id="wrap">
  <div id="bar">
    <div id="title">Connecting…</div>
    <span class="pill" id="pl">👥 –</span>
    <span class="pill" id="fps">– fps</span>
    <button class="t" id="mu" title="Click to enable sound">🔇</button>
    <button class="t" id="tp" title="Touch controls">🎮</button>
    <button class="t" id="fs" title="Fullscreen">⛶</button>
  </div>
  <div id="stage"><canvas id="cv" width="256" height="224"></canvas><div id="msg">Connecting…</div></div>
  <div id="help">WASD / arrows move · J K = B A · U I = Y X · Q E = L R · Enter = Start · Shift = Select · M = mute · gamepads work too</div>
  <div id="pad"></div>
</div>
<script>
(function(){
"use strict";
var $=function(i){return document.getElementById(i)};
var cv=$("cv"),ctx=cv.getContext("2d",{alpha:false}),stage=$("stage"),msg=$("msg");
// libretro joypad bits: B0 Y1 SELECT2 START3 UP4 DOWN5 LEFT6 RIGHT7 A8 X9 L10 R11
var KEYS={KeyW:4,ArrowUp:4,KeyS:5,ArrowDown:5,KeyA:6,ArrowLeft:6,KeyD:7,ArrowRight:7,
  KeyJ:0,KeyK:8,KeyU:1,KeyI:9,KeyQ:10,KeyE:11,
  Enter:3,ShiftLeft:2,ShiftRight:2,Backspace:2};
var down=new Set(),held=new Map(),padMask=0,last=-1,ws=null,over=false,tries=0;

function mask(){
  var m=padMask;
  down.forEach(function(c){m|=1<<KEYS[c]});
  held.forEach(function(b){m|=1<<b});
  return m;
}
function push(force){
  var m=mask();
  if(!ws||ws.readyState!==1||(m===last&&!force))return;
  var b=new DataView(new ArrayBuffer(2));b.setUint16(0,m,true);ws.send(b.buffer);last=m;
}
addEventListener("keydown",function(e){
  if(e.code==="KeyM"&&!e.repeat){e.preventDefault();toggleMute();return}
  if(!(e.code in KEYS))return;
  e.preventDefault();
  if(!down.has(e.code)){down.add(e.code);push()}
});
addEventListener("keyup",function(e){
  if(!(e.code in KEYS))return;
  e.preventDefault();down.delete(e.code);push();
});
addEventListener("blur",function(){down.clear();held.clear();paint();push()});

// ---- gamepad (standard mapping) ----
function pollPad(){
  var m=0,gp=(navigator.getGamepads?navigator.getGamepads():[])||[];
  for(var i=0;i<gp.length;i++){
    var g=gp[i];if(!g||!g.connected)continue;
    var p=function(n){return g.buttons[n]&&g.buttons[n].pressed};
    if(p(0))m|=1<<0;if(p(1))m|=1<<8;if(p(2))m|=1<<1;if(p(3))m|=1<<9;
    if(p(4))m|=1<<10;if(p(5))m|=1<<11;if(p(8))m|=1<<2;if(p(9))m|=1<<3;
    if(p(12))m|=1<<4;if(p(13))m|=1<<5;if(p(14))m|=1<<6;if(p(15))m|=1<<7;
    var x=g.axes[0]||0,y=g.axes[1]||0;
    if(x<-.5)m|=1<<6;if(x>.5)m|=1<<7;if(y<-.5)m|=1<<4;if(y>.5)m|=1<<5;
  }
  if(m!==padMask){padMask=m;push()}
  requestAnimationFrame(pollPad);
}
requestAnimationFrame(pollPad);

// ---- touch pad ----
var pad=$("pad");
function btn(parent,label,bit,cls,style){
  var d=document.createElement("div");d.className="b "+(cls||"");d.textContent=label;
  if(style)d.style.cssText=style;
  d.addEventListener("pointerdown",function(e){e.preventDefault();d.setPointerCapture(e.pointerId);held.set(e.pointerId,bit);d.classList.add("on");push();if(navigator.vibrate)navigator.vibrate(8)});
  var up=function(e){held.delete(e.pointerId);d.classList.remove("on");push()};
  d.addEventListener("pointerup",up);d.addEventListener("pointercancel",up);
  parent.appendChild(d);return d;
}
function grid(id,cells){
  var g=document.createElement("div");g.className="g";g.id=id;
  cells.forEach(function(c){if(c)btn(g,c[0],c[1]);else g.appendChild(document.createElement("div"))});
  pad.appendChild(g);
}
grid("dp",[null,["▲",4],null,["◀",6],null,["▶",7],null,["▼",5],null]);
grid("ab",[null,["X",9],null,["Y",1],null,["A",8],null,["B",0],null]);
var mid=document.createElement("div");mid.id="mid";pad.appendChild(mid);
btn(mid,"SELECT",2,"sm");btn(mid,"START",3,"sm");
btn(pad,"L",10,"sh","left:8px;top:2px;position:absolute");btn(pad,"R",11,"sh","right:8px;top:2px;position:absolute");
function paint(){document.querySelectorAll(".b.on").forEach(function(d){d.classList.remove("on")})}
var coarse=matchMedia("(pointer: coarse)").matches;
if(coarse)document.body.classList.add("touch");
$("tp").onclick=function(){document.body.classList.toggle("touch");fit()};
$("fs").onclick=function(){
  if(document.fullscreenElement)document.exitFullscreen();
  else if(document.documentElement.requestFullscreen)document.documentElement.requestFullscreen();
};

// ---- layout: keep a 4:3 picture as large as the stage allows ----
function fit(){
  var w=stage.clientWidth-8,h=stage.clientHeight-8;
  var cw=Math.max(64,Math.min(w,h*4/3));
  cv.style.width=Math.floor(cw)+"px";cv.style.height=Math.floor(cw*3/4)+"px";
}
addEventListener("resize",fit);fit();

// ---- video: decode only the newest frame, paint once per display refresh ----
// Messages from the bot are tagged: byte0 "J" (74) = JPEG at offset 2, "A" (65) = int16 PCM at offset 2.
var pendingBuf=null,busy=false,shown=0,bmp=null,drawQ=false;
function pump(){
  if(busy||!pendingBuf)return;
  var d=pendingBuf;pendingBuf=null;busy=true;
  createImageBitmap(new Blob([new Uint8Array(d,2)],{type:"image/jpeg"})).then(function(b){
    if(bmp&&bmp.close)bmp.close();
    bmp=b;
    if(!drawQ){drawQ=true;requestAnimationFrame(draw)}
  }).catch(function(){}).then(function(){busy=false;pump()});
}
function draw(){
  drawQ=false;if(!bmp)return;
  if(cv.width!==bmp.width||cv.height!==bmp.height){cv.width=bmp.width;cv.height=bmp.height}
  ctx.drawImage(bmp,0,0);shown++;
}
setInterval(function(){$("fps").textContent=shown+" fps";shown=0},1000);

// ---- audio: gapless scheduling of the PCM chunks that arrive with every frame ----
var ac=null,gain=null,aRate=0,aCh=2,audioAvail=false,unlocked=false,muted=false,nextT=0;
function wantAudio(){return audioAvail&&unlocked&&!muted}
function sendCtl(){if(ws&&ws.readyState===1)ws.send(JSON.stringify({audio:wantAudio()}))}
function updateMu(){
  var on=audioAvail&&unlocked&&!muted&&ac&&ac.state==="running";
  $("mu").textContent=on?"🔊":"🔇";
  $("mu").title=!audioAvail?"This game has no sound":(!unlocked?"Click to enable sound":(muted?"Unmute (M)":"Mute (M)"));
}
function initAudio(){
  if(ac||!aRate||!audioAvail)return;
  var AC=window.AudioContext||window.webkitAudioContext;
  if(!AC){audioAvail=false;return}
  try{ac=new AC({sampleRate:aRate,latencyHint:"interactive"})}   // run the context at the core's rate: no resampling per chunk
  catch(e){try{ac=new AC({latencyHint:"interactive"})}catch(e2){audioAvail=false;return}}
  gain=ac.createGain();gain.connect(ac.destination);
  ac.onstatechange=updateMu;
}
function unlock(e){
  if(e&&(e.type==="keydown"&&e.code==="KeyM"||e.target&&e.target.id==="mu"))return;
  var first=!unlocked;unlocked=true;initAudio();
  if(ac&&ac.state!=="running")ac.resume().then(updateMu).catch(function(){});
  if(first){sendCtl()}
  updateMu();
}
function toggleMute(){
  if(!unlocked){unlock();return}
  muted=!muted;nextT=0;updateMu();sendCtl();
}
["pointerdown","keydown","touchend"].forEach(function(n){addEventListener(n,unlock,{passive:true})});
$("mu").onclick=function(){toggleMute()};
function playPCM(d){
  if(!ac||ac.state!=="running"||muted||!gain)return;
  var s=new Int16Array(d,2,(d.byteLength-2)>>1),ch=aCh,n=(s.length/ch)|0,i;
  if(!n)return;
  var ab=ac.createBuffer(ch,n,aRate),L=ab.getChannelData(0);
  if(ch>1){var R=ab.getChannelData(1);for(i=0;i<n;i++){L[i]=s[2*i]/32768;R[i]=s[2*i+1]/32768}}
  else{for(i=0;i<n;i++)L[i]=s[i]/32768}
  var src=ac.createBufferSource();src.buffer=ab;src.connect(gain);
  var now=ac.currentTime;
  if(nextT<now+0.005||nextT>now+0.22)nextT=now+0.045;   // (re)start with a ~45 ms cushion against network jitter
  src.start(nextT);nextT+=ab.duration;
}

function setMsg(t){msg.textContent=t||""}
function onText(m){
  if(m.t==="hello"){$("title").textContent=m.title;document.title=m.title;setMsg(m.paused?"⏸ Paused (idle). Press any key to resume.":"");
    aRate=m.rate|0;aCh=m.ch||2;audioAvail=!!m.audio&&aRate>0;initAudio();updateMu();sendCtl()}
  else if(m.t==="players"){$("pl").textContent="👥 "+m.n;$("pl").title=m.names.join(", ")}
  else if(m.t==="paused"){setMsg(m.on?"⏸ Paused (idle). Press any key to resume.":"")}
  else if(m.t==="end"){over=true;setMsg("Game ended: "+m.why)}
}
function connect(){
  var proto=location.protocol==="https:"?"wss://":"ws://";
  ws=new WebSocket(proto+location.host+location.pathname.replace(/\/+$/,"")+"/ws");
  ws.binaryType="arraybuffer";
  ws.onopen=function(){tries=0;push(true);sendCtl()};
  ws.onmessage=function(e){
    if(typeof e.data==="string"){onText(JSON.parse(e.data))}
    else{
      var tag=new Uint8Array(e.data,0,1)[0];
      if(tag===74){pendingBuf=e.data;pump()}
      else if(tag===65){playPCM(e.data)}
    }
  };
  ws.onclose=function(){
    if(over)return;
    setMsg("Connection lost, reconnecting…");
    setTimeout(connect,Math.min(4000,400*(++tries)));
  };
}
connect();
})();
</script>
</body>
</html>
'''


async def web_home(request):
    return web.Response(text="LUX-NET Bridge Bot is Online!")


async def web_emu_page(request):
    if request.match_info["token"] not in emu_web_tokens:
        return web.Response(status=404, content_type="text/plain",
                            text="This game link has expired. Press 'Play live' on the game message in Discord for a new one.")
    return web.Response(text=EMU_PAGE_HTML, content_type="text/html",
                        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer",
                                 "X-Content-Type-Options": "nosniff"})


async def web_emu_ws(request):
    info = emu_web_tokens.get(request.match_info["token"])
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=256, compress=False)
    await ws.prepare(request)
    if not info or info[0].closed:
        await ws.send_str(json.dumps({"t": "end", "why": "this game has ended"}))
        await ws.close()
        return ws
    sess, uid, name = info
    if is_user_banned(uid):
        await ws.send_str(json.dumps({"t": "end", "why": "you are banned from this bot"}))
        await ws.close()
        return ws
    if len(sess.clients) >= EMU_MAX_VIEWERS:
        await ws.send_str(json.dumps({"t": "end", "why": f"this game already has {EMU_MAX_VIEWERS} players watching"}))
        await ws.close()
        return ws
    client = EmuClient(ws, uid, name)
    sender = asyncio.ensure_future(client.sender())
    sess.add_client(client)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.BINARY and len(msg.data) >= 2:
                mask = struct.unpack("<H", msg.data[:2])[0] & 0x0FFF
                if mask != client.mask:
                    client.mask = mask
                    sess.recalc_input()
            elif msg.type == WSMsgType.TEXT:
                try:
                    ctl = json.loads(msg.data)
                except ValueError:
                    continue
                if isinstance(ctl, dict) and "audio" in ctl:
                    want = bool(ctl["audio"]) and EMU_AUDIO
                    if want != client.want_audio:
                        client.want_audio = want
                        if not want:
                            client.audio.clear()
                        sess.update_audio()
            elif msg.type == WSMsgType.ERROR:
                break
    finally:
        sender.cancel()
        sess.remove_client(client)
    return ws


_web_runner = None


async def start_web():
    """One HTTP server for everything: Render's health check ('/') and the live game player."""
    global _web_runner
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
    _web_runner = runner
    logger.info(f"Web server listening on :{port}" + (f" (live player base {EMU_WEB_BASE})" if EMU_WEB_BASE else " (no PUBLIC_URL/RENDER_EXTERNAL_URL: live player links disabled)"))


# ---- controller UI -------------------------------------------------------------------------
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
            [("live", "Play live (60 FPS + sound)", P, False), ("save", "\U0001F4BE Save", S, False), ("load", "\U0001F4C2 Load", S, False), ("quit", "⏹ Quit", D, False)],
        ]
        self.hold_btn = None
        for row, items in enumerate(layout):
            for action, label, style, disabled in items:
                emoji = "\U0001F5A5️" if action == "live" else None
                btn = EmuButton(action, label, style, row, disabled, emoji)
                if action == "hold":
                    self.hold_btn = btn
                    self._relabel_hold()
                self.add_item(btn)

    def _relabel_hold(self):
        self.hold_btn.label = f"⏩ Hold {self.sess.hold}f"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if is_user_banned(interaction.user.id):
            await interaction.response.send_message("❌ You are globally banned from using this bot.", ephemeral=True)
            return False
        return True

    async def _fail(self, interaction, res):
        text = f"❌ {res.get('err', 'Emulator error')}"
        if res.get("dead"):
            await emu_close(self.sess, "emulator stopped", autosave=False)
        try:
            await interaction.followup.send(text, ephemeral=True)
        except Exception:
            pass

    def _can_stop(self, interaction: discord.Interaction) -> bool:
        perms = getattr(interaction.user, "guild_permissions", None)
        return interaction.user.id == self.sess.starter_id or bool(perms and perms.manage_messages)

    async def handle(self, interaction: discord.Interaction, action: str):
        sess = self.sess
        if sess.closed:
            await interaction.response.send_message("⏹️ This game session has ended. Start a new one with `/emu-play`.", ephemeral=True)
            return
        sess.touch()

        if action in EMU_MASKS:
            await interaction.response.defer()                      # acknowledge instantly, never expires
            sess.last_mask = EMU_MASKS[action]
            sess.inputs += 1
            sess.footer = f"{interaction.user.display_name}: {action.upper()}  •  {sess.inputs} inputs"
            res = await sess.press([(sess.last_mask, sess.hold), (0, EMU_GAP_FRAMES)])
            if not res.get("ok"):
                await self._fail(interaction, res)

        elif action == "hold":
            sess.hold_idx = (sess.hold_idx + 1) % len(EMU_HOLD_STEPS)
            self._relabel_hold()
            await interaction.response.edit_message(view=self)

        elif action == "live":
            if not EMU_WEB_BASE:
                await interaction.response.send_message(
                    "⚠️ The live player needs a public address. Set the `PUBLIC_URL` environment variable "
                    "(Render sets `RENDER_EXTERNAL_URL` automatically for web services).", ephemeral=True)
                return
            if len(sess.clients) >= EMU_MAX_VIEWERS:
                await interaction.response.send_message(f"⚠️ This game already has {EMU_MAX_VIEWERS} live players.", ephemeral=True)
                return
            url = f"{EMU_WEB_BASE}/emu/{sess.token_for(interaction.user)}"
            link = discord.ui.View()
            link.add_item(discord.ui.Button(style=discord.ButtonStyle.link, label="Open live player", url=url))
            await interaction.response.send_message(
                "\U0001F5A5️ **Live player (smooth 60 FPS + sound)**\n"
                "**W A S D** move • **J / K** = B / A • **U / I** = Y / X • **Q / E** = L / R • **Enter** = Start • **Shift** = Select\n"
                "Click the page once to turn the sound on (**M** mutes). Gamepads and phone touch controls work too. This link is tied to you, so don't share it.",
                view=link, ephemeral=True)

        elif action == "clip":
            await interaction.response.defer(thinking=True)
            res = await sess.call(op="clip", mask=sess.last_mask, hold=sess.hold, frames=90, every=3, scale=1)
            if not res.get("ok"):
                return await self._fail(interaction, res)
            await interaction.followup.send(file=discord.File(io.BytesIO(res["gif"]), filename="clip.gif"))

        elif action == "save":
            await interaction.response.defer(ephemeral=True)
            res = await sess.call(op="save", path=sess.state_path)
            if not res.get("ok"):
                return await self._fail(interaction, res)
            await interaction.followup.send("\U0001F4BE Saved your progress.", ephemeral=True)

        elif action == "load":
            if not os.path.exists(sess.state_path):
                await interaction.response.send_message("⚠️ No save for this game in this channel yet. Press **Save** first.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            res = await sess.call(op="loadstate", path=sess.state_path)
            if not res.get("ok"):
                return await self._fail(interaction, res)
            sess.footer = f"{interaction.user.display_name}: loaded save"
            sess.schedule_refresh()
            await interaction.followup.send("\U0001F4C2 Loaded.", ephemeral=True)

        elif action == "quit":
            if not self._can_stop(interaction):
                await interaction.response.send_message("❌ Only the person who started the game (or someone with Manage Messages) can quit it.", ephemeral=True)
                return
            await interaction.response.defer()
            await emu_close(sess, f"stopped by {interaction.user.display_name}")


# ---- slash commands ------------------------------------------------------------------------
@bot.tree.command(name="emu-play", description="Play a NES/SNES game in this channel (buttons, typed WASD, or the live 60 FPS player with sound).")
@app_commands.describe(game="Pick a game from the library", resume="Continue from where this channel last stopped (default: yes)")
@app_commands.autocomplete(game=emu_rom_autocomplete)
async def emu_play(interaction: discord.Interaction, game: str, resume: bool = True):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return
    ref = valid_rom_ref(game)
    if not ref or game not in all_roms():
        await interaction.response.send_message("❌ Pick a game from the list (use `/emu-games` to see the library).", ephemeral=True)
        return
    if interaction.channel.id in emu_sessions:
        await interaction.response.send_message("⚠️ A game is already running in this channel. Use its **Quit** button or `/emu-stop` first.", ephemeral=True)
        return
    if len(emu_sessions) >= MAX_EMU_SESSIONS:
        await interaction.response.send_message(f"⚠️ The bot is already running {MAX_EMU_SESSIONS} games. Try again when one ends.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    sess = EmuSession(interaction.channel.id, game, interaction.user.id)
    emu_sessions[sess.channel_id] = sess      # reserve the slot before any await
    try:
        rom_path = await asyncio.to_thread(ensure_rom_local, game)
        core_path = await asyncio.to_thread(ensure_core, sess.system)
        await sess.spawn()
        res = await sess.call(op="load", core=core_path, rom=rom_path,
                              sysdir=os.path.join(EMU_BASE, "system"), savedir=os.path.join(STATE_DIR, "sram"))
        if not res.get("ok"):
            raise RuntimeError(res.get("err", "load failed"))
        sess.audio_rate = int(res.get("rate") or 0)
        sess.audio_ch = int(res.get("ch") or 2)
        resumed = False
        if resume and os.path.exists(sess.auto_path):
            r = await sess.call(op="loadstate", path=sess.auto_path)
            resumed = bool(r.get("ok"))
        if not resumed:
            r = await sess.call(op="steps", steps=[(0, 90)], settle=1)       # skip the first second and a half of boot
            if not r.get("ok"):
                raise RuntimeError(r.get("err", "no frame"))
        res = await sess.call(op="snap", scale=EMU_SCALE)
        if not res.get("ok"):
            raise RuntimeError(res.get("err", "no frame"))

        sess.view = EmuView(sess)
        sess.seq += 1
        fname = f"f{sess.seq}.png"
        sess.footer = (f"Started by {interaction.user.display_name}"
                       + (" (resumed)" if resumed else "")
                       + " • type w a s d j k in chat • or press 🖥️ Play live")
        msg = await interaction.channel.send(embed=sess.make_embed(fname),
                                             file=discord.File(io.BytesIO(res["png"]), filename=fname), view=sess.view)
        sess.message_id = msg.id
        sess.last_edit = time.monotonic()
        sess.touch()
        await interaction.followup.send("\U0001F3AE Game started! Type `w` `a` `s` `d` in this channel, or press **Play live** for 60 FPS with sound.", ephemeral=True)
    except Exception as e:
        logger.error(f"emu-play failed: {e}")
        emu_sessions.pop(sess.channel_id, None)
        sess.closing = sess.closed = True
        sess.kill()
        await interaction.followup.send(f"❌ Couldn't start the game: {e}", ephemeral=True)


@bot.tree.command(name="emu-keys", description="Show the keyboard controls for the emulator.")
async def emu_keys(interaction: discord.Interaction):
    pre = f"`{EMU_CHAT_PREFIX}` + " if EMU_CHAT_PREFIX else ""
    await interaction.response.send_message(
        "**Typed controls** (send these as messages in the game channel)\n"
        f"{pre}`w a s d` = up / left / down / right • `j` = B • `k` = A • `u` = Y • `i` = X • `q` = L • `e` = R • `start` • `select`\n"
        "`d3` = tap right 3 times • `d+j` = right and B together • `wwdk` = a quick sequence\n\n"
        "**Live player** (press **Play live** on the game message): real keyboard, smooth 60 FPS, game sound (click the page once to unmute). "
        "`W A S D` / arrows, `J K` = B A, `U I` = Y X, `Q E` = L R, `Enter` = Start, `Shift` = Select. "
        "Gamepads and phones (touch pad) work too.", ephemeral=True)


@bot.tree.command(name="emu-games", description="List the NES/SNES games in the library.")
async def emu_games(interaction: discord.Interaction):
    roms = all_roms()
    if not roms:
        await interaction.response.send_message(
            "\U0001F4ED The library is empty. The bot owner can add games with `/emu-add`, or put files in `roms/nes` and `roms/snes`.",
            ephemeral=True)
        return
    embed = discord.Embed(title=f"\U0001F3AE Game library ({len(roms)})", color=discord.Color.blurple())
    for system, spec in EMU_SYSTEMS.items():
        names = [os.path.splitext(r.split("/", 1)[1])[0] for r in roms if r.startswith(system + "/")]
        if names:
            text = "\n".join(f"• {n}" for n in names)
            embed.add_field(name=f"{spec['label']} ({len(names)})", value=text[:1000], inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


def _can_manage_roms(interaction: discord.Interaction, is_owner: bool) -> bool:
    if is_owner:
        return True
    perms = getattr(interaction.user, "guild_permissions", None)
    return EMU_ADD_OPEN and bool(perms and perms.manage_guild)


@bot.tree.command(name="emu-add", description="Add a NES/SNES game file to the library.")
@app_commands.describe(rom="The ROM file (.nes, .sfc, .smc)", name="Optional display name (defaults to the file name)")
async def emu_add(interaction: discord.Interaction, rom: discord.Attachment, name: str = None):
    if not _can_manage_roms(interaction, await check_is_owner(interaction)):
        await interaction.response.send_message("❌ Only the bot owner can add games.", ephemeral=True)
        return
    ext = os.path.splitext(rom.filename)[1].lower()
    filename = clean_rom_filename((name or os.path.splitext(rom.filename)[0]) + ext)
    system = system_for(filename or "")
    if not filename or not system:
        await interaction.response.send_message("❌ Use a `.nes`, `.sfc`, `.smc`, `.swc` or `.fig` file.", ephemeral=True)
        return
    if rom.size > EMU_MAX_ROM_BYTES:
        await interaction.response.send_message(f"❌ That file is too large (limit {EMU_MAX_ROM_BYTES // 1048576} MB).", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    data = await rom.read()
    if system == "nes" and data[:4] != b"NES\x1a":
        await interaction.followup.send("❌ That doesn't look like an iNES `.nes` file.", ephemeral=True)
        return
    persisted = await asyncio.to_thread(add_rom_blocking, system, filename, data)
    note = "" if persisted else f"\n⚠️ Not saved to the `{ROM_BUCKET}` Supabase bucket, so it may disappear on redeploy."
    await interaction.followup.send(f"✅ Added **{os.path.splitext(filename)[0]}** to the {EMU_SYSTEMS[system]['label']} library.{note}", ephemeral=True)


@bot.tree.command(name="emu-remove", description="Remove a game from the library.")
@app_commands.describe(game="The game to remove")
@app_commands.autocomplete(game=emu_rom_autocomplete)
async def emu_remove(interaction: discord.Interaction, game: str):
    if not _can_manage_roms(interaction, await check_is_owner(interaction)):
        await interaction.response.send_message("❌ Only the bot owner can remove games.", ephemeral=True)
        return
    if not valid_rom_ref(game):
        await interaction.response.send_message("❌ Pick a game from the list.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await asyncio.to_thread(remove_rom_blocking, game)
    await interaction.followup.send(f"\U0001F5D1️ Removed `{game}`.", ephemeral=True)


@bot.tree.command(name="emu-stop", description="Stop the game running in this channel.")
async def emu_stop(interaction: discord.Interaction):
    sess = emu_sessions.get(interaction.channel.id) if interaction.channel else None
    if not sess:
        await interaction.response.send_message("⚠️ No game is running in this channel.", ephemeral=True)
        return
    perms = getattr(interaction.user, "guild_permissions", None)
    if not (interaction.user.id == sess.starter_id or (perms and perms.manage_messages) or await check_is_owner(interaction)):
        await interaction.response.send_message("❌ Only the person who started the game (or someone with Manage Messages) can stop it.", ephemeral=True)
        return
    await interaction.response.send_message("⏹️ Stopping the game...", ephemeral=True)
    await emu_close(sess, f"stopped by {interaction.user.display_name}")


@bot.tree.command(name="emu-status", description="Show running emulator sessions and memory use.")
async def emu_status(interaction: discord.Interaction):
    lines = [f"Running games: `{len(emu_sessions)}/{MAX_EMU_SESSIONS}` • idle timeout `{EMU_IDLE_SECONDS}s` • library `{len(all_roms())}` game(s)"]
    for s in emu_sessions.values():
        pid = s.proc.pid if s.proc else 0
        idle = int(time.monotonic() - s.last_used)
        extra = ""
        if s.clients:
            st = s.stats or {}
            extra = f", `{len(s.clients)}` live player(s)"
            if st:
                extra += f" • `{st.get('emu', 0)}` emu fps / `{st.get('sent', 0)}` sent fps / `{st.get('load', 0)}%` busy"
            if s.paused:
                extra += " • ⏸ paused (AFK)"
        lines.append(f"• <#{s.channel_id}>: **{s.label} {s.title}**, worker `{_proc_rss_mb(pid):.0f} MB`, idle `{idle}s`{extra}")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

# ------------------------------------------------------------------------------
# RADIO (Radio Browser directory: browse by country, web player + voice playback)
# ------------------------------------------------------------------------------
# Uses the free, open Radio Browser API (no key). Two ways to listen:
#   * the web player at  <PUBLIC_URL>/radio   (browse countries, plays in your browser; /radio shows the link)
#   * /radio-play        (the bot streams a station into your voice channel; needs FFmpeg on the host)
# Voice playback is not available in a server while the voice bridge is active there.
import random
import ipaddress
import socket
import aiohttp
from urllib.parse import urlparse, urljoin

RADIO_HOSTS = [h.strip() for h in os.environ.get(
    "RADIO_HOSTS", "de1.api.radio-browser.info,de2.api.radio-browser.info,fi1.api.radio-browser.info"
).split(",") if h.strip()]
RADIO_UA = "LUX-NET-Bridge/1.0 (Discord bot)"
RADIO_PROXY_MAX = int(os.environ.get("RADIO_PROXY_MAX", "6"))      # simultaneous browser listeners proxied through the bot
RADIO_LIST_LIMIT = 300

radio_sessions: dict = {}      # guild_id -> {"vc", "info", "gen", "restarts"}
_radio_cache: dict = {}        # request key -> (expires, data)
_radio_urls: dict = {}         # station uuid -> stream url (for stations we have listed)
_radio_http = None
_radio_proxy_active = 0
_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")


async def radio_http():
    global _radio_http
    if _radio_http is None or _radio_http.closed:
        _radio_http = aiohttp.ClientSession(headers={"User-Agent": RADIO_UA})
    return _radio_http


async def radio_shutdown():
    if _radio_http is not None and not _radio_http.closed:
        await _radio_http.close()


async def radio_api(path: str, params: dict = None, ttl: int = 900):
    key = path + "?" + json.dumps(params or {}, sort_keys=True)
    now = time.monotonic()
    hit = _radio_cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    session = await radio_http()
    hosts = RADIO_HOSTS[:]
    random.shuffle(hosts)
    last = None
    for host in hosts:
        try:
            async with session.get(f"https://{host}/json/{path}", params=params,
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    last = f"{host}: HTTP {r.status}"
                    continue
                data = await r.json(content_type=None)
            if len(_radio_cache) > 300:
                for k in [k for k, v in _radio_cache.items() if v[0] < now] or list(_radio_cache)[:100]:
                    _radio_cache.pop(k, None)
            _radio_cache[key] = (now + ttl, data)
            return data
        except Exception as e:
            last = f"{host}: {e}"
    if hit:
        return hit[1]          # stale data beats an error
    raise RuntimeError(f"radio directory unavailable ({last})")


def radio_norm(s: dict):
    url = (s.get("url_resolved") or s.get("url") or "").strip()
    uid = s.get("stationuuid")
    if not uid or not url.startswith(("http://", "https://")) or s.get("hls") == 1:
        return None
    if len(_radio_urls) > 20000:
        _radio_urls.clear()
    _radio_urls[uid] = url
    icon = (s.get("favicon") or "").strip()

    def num(v):
        try:
            f = float(v)
            return f if f == f else None
        except (TypeError, ValueError):
            return None

    lat, lng = num(s.get("geo_lat")), num(s.get("geo_long"))
    if lat is not None and lng is not None and abs(lat) < 0.01 and abs(lng) < 0.01:
        lat = lng = None
    return {
        "id": uid,
        "name": (s.get("name") or "Unknown").strip()[:80] or "Unknown",
        "cc": s.get("countrycode") or "",
        "country": s.get("country") or "",
        "tags": (s.get("tags") or "")[:60],
        "votes": int(s.get("votes") or 0),
        "codec": s.get("codec") or "",
        "bitrate": int(s.get("bitrate") or 0),
        "favicon": icon if icon.startswith("https://") else "",
        "lat": lat, "lng": lng,
        "url": url,
    }


async def radio_countries() -> list:
    data = await radio_api("countries", {"hidebroken": "true", "order": "name"}, ttl=3600)
    out = [{"cc": c["iso_3166_1"], "name": c["name"], "count": int(c.get("stationcount") or 0)}
           for c in data if c.get("iso_3166_1") and c.get("stationcount")]
    return sorted(out, key=lambda c: c["name"].lower())


async def radio_stations(cc: str = None, q: str = None, limit: int = RADIO_LIST_LIMIT) -> list:
    params = {"hidebroken": "true", "order": "votes", "reverse": "true", "limit": limit}
    if cc:
        params["countrycode"] = cc.upper()
    if q:
        params["name"] = q
    data = await radio_api("stations/search", params, ttl=900 if cc else 300)
    return [s for s in (radio_norm(x) for x in data) if s]


async def radio_station_info(uid: str):
    if not _UUID_RE.match(uid or ""):
        return None
    data = await radio_api(f"stations/byuuid/{uid}", None, ttl=3600)
    return radio_norm(data[0]) if data else None


async def radio_station_url(uid: str):
    if not _UUID_RE.match(uid or ""):
        return None
    url = _radio_urls.get(uid)
    if url:
        return url
    info = await radio_station_info(uid)
    return info["url"] if info else None


async def radio_click(uid: str):
    """Courtesy vote count for the directory (fire and forget)."""
    try:
        await radio_api(f"url/{uid}", None, ttl=0)
    except Exception:
        pass


async def radio_url_is_public(url: str) -> bool:
    """Blocks stream URLs that point at private / internal addresses (SSRF guard)."""
    try:
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            return False
        port = u.port or (443 if u.scheme == "https" else 80)
        infos = await asyncio.get_running_loop().getaddrinfo(u.hostname, port, type=socket.SOCK_STREAM)
        return bool(infos) and all(ipaddress.ip_address(i[4][0]).is_global for i in infos)
    except Exception:
        return False


async def radio_warm():
    try:
        await radio_countries()
    except Exception as e:
        logger.warning(f"Radio directory warm-up failed: {e}")


# ---- web player ---------------------------------------------------------------------------
async def web_radio_page(request):
    return web.Response(text=RADIO_PAGE_HTML, content_type="text/html",
                        headers={"Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff"})


async def web_radio_countries(request):
    try:
        data = await radio_countries()
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)
    return web.json_response(data, headers={"Cache-Control": "public, max-age=600"})


async def web_radio_stations(request):
    cc = (request.query.get("cc") or "").strip()[:2]
    q = (request.query.get("q") or "").strip()[:60]
    if cc and not cc.isalpha():
        return web.json_response([], status=400)
    if not cc and len(q) < 2:
        return web.json_response([])
    try:
        stations = await radio_stations(cc=cc or None, q=q or None, limit=RADIO_LIST_LIMIT if cc else 60)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)
    out = [{"id": s["id"], "name": s["name"], "cc": s["cc"], "country": s["country"], "tags": s["tags"],
            "votes": s["votes"], "codec": s["codec"], "bitrate": s["bitrate"], "favicon": s["favicon"],
            "lat": s["lat"], "lng": s["lng"],
            "direct": s["url"] if s["url"].startswith("https://") else ""} for s in stations]
    return web.json_response(out, headers={"Cache-Control": "public, max-age=120"})


async def web_radio_stream(request):
    """Relays an http:// station through this server so an https page can play it."""
    global _radio_proxy_active
    uid = request.match_info["sid"]
    url = await radio_station_url(uid)
    if not url:
        return web.Response(status=404, text="unknown station")
    if _radio_proxy_active >= RADIO_PROXY_MAX:
        return web.Response(status=503, text="the relay is busy, try again in a moment")
    _radio_proxy_active += 1
    upstream = None
    try:
        session = await radio_http()
        cur = url
        for _ in range(4):
            if not await radio_url_is_public(cur):
                return web.Response(status=403, text="blocked address")
            r = await session.get(cur, allow_redirects=False, headers={"Icy-MetaData": "0"},
                                  timeout=aiohttp.ClientTimeout(total=None, sock_connect=8, sock_read=15))
            if r.status in (301, 302, 303, 307, 308) and r.headers.get("Location"):
                cur = urljoin(cur, r.headers["Location"])
                r.release()
                continue
            upstream = r
            break
        if upstream is None or upstream.status != 200:
            return web.Response(status=502, text="station did not answer")
        ctype = (upstream.headers.get("Content-Type") or "audio/mpeg").split(";")[0].strip().lower()
        if not (ctype.startswith("audio/") or ctype in ("application/ogg", "application/octet-stream", "binary/octet-stream")):
            return web.Response(status=502, text="not an audio stream")
        resp = web.StreamResponse(headers={"Content-Type": ctype, "Cache-Control": "no-store"})
        await resp.prepare(request)
        try:
            async for chunk in upstream.content.iter_chunked(8192):
                await resp.write(chunk)
        except (ConnectionResetError, aiohttp.ClientError, asyncio.TimeoutError):
            pass
        return resp
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return web.Response(status=502, text="station unreachable")
    finally:
        _radio_proxy_active -= 1
        if upstream is not None:
            upstream.release()


RADIO_PAGE_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="referrer" content="no-referrer">
<title>LUX-NET Radio</title>
<style>
:root{color-scheme:dark;--bg:#0b0d12;--panel:#12151c;--line:#23283a;--mut:#8b94ad;--acc:#4d6bff;--ok:#3ddc97}
*{box-sizing:border-box}
html,body{margin:0;height:100%;background:var(--bg);color:#e6e8ee;font:14px/1.4 system-ui,-apple-system,Segoe UI,sans-serif}
#app{display:grid;grid-template-columns:270px 1fr;grid-template-rows:auto 1fr auto;height:100%}
header{grid-column:1/3;display:flex;gap:10px;align-items:center;padding:10px 14px;background:var(--panel);border-bottom:1px solid var(--line)}
header h1{font-size:16px;margin:0;white-space:nowrap}
input[type=search],input[type=text]{background:#0b0d12;border:1px solid var(--line);color:inherit;border-radius:8px;padding:7px 10px;font:inherit;min-width:0}
#q{flex:1;max-width:420px}
#side{background:var(--panel);border-right:1px solid var(--line);display:flex;flex-direction:column;min-height:0}
#cf{margin:10px}
#cl{overflow:auto;flex:1}
.c{display:flex;gap:8px;align-items:center;padding:7px 12px;cursor:pointer;border-left:3px solid transparent}
.c:hover{background:#171b27}.c.on{background:#1b2236;border-left-color:var(--acc)}
.c .n{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.c .k{color:var(--mut);font-size:12px}
#main{display:flex;flex-direction:column;min-height:0;min-width:0}
#mapw{position:relative;height:34%;min-height:150px;border-bottom:1px solid var(--line);background:#0e1119}
#map{width:100%;height:100%;display:block;cursor:pointer}
#maptip{position:absolute;left:10px;top:8px;color:var(--mut);font-size:12px;pointer-events:none;max-width:90%}
#sl{overflow:auto;flex:1}
.s{display:flex;gap:10px;align-items:center;padding:8px 14px;border-bottom:1px solid #171b27;cursor:pointer}
.s:hover{background:#151925}.s.on{background:#1a2338}
.s img,.s .ph{width:36px;height:36px;border-radius:8px;background:#1c2130;object-fit:cover;flex:none}
.s .ph{display:flex;align-items:center;justify-content:center;font-size:18px}
.s .t{flex:1;min-width:0}.s .t b{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600}
.s .t span{color:var(--mut);font-size:12px;display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#empty{padding:30px;text-align:center;color:var(--mut)}
#bar{grid-column:1/3;display:flex;gap:12px;align-items:center;padding:10px 14px calc(10px + env(safe-area-inset-bottom));background:var(--panel);border-top:1px solid var(--line)}
#now{flex:1;min-width:0}#now b{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}#now span{color:var(--mut);font-size:12px}
button{background:var(--acc);color:#fff;border:0;border-radius:8px;padding:8px 14px;font:inherit;cursor:pointer}
button.g{background:#1c2130;color:#cfd6ea}
#vol{width:110px}
.live{color:var(--ok)}
@media(max-width:760px){#app{grid-template-columns:1fr;grid-template-rows:auto auto 1fr auto}
 header{grid-column:1}#bar{grid-column:1}#side{border-right:0;border-bottom:1px solid var(--line);max-height:30vh}#vol{display:none}}
</style>
</head>
<body>
<div id="app">
 <header><h1>📻 LUX-NET Radio</h1><input id="q" type="search" placeholder="Search stations by name…" autocomplete="off"></header>
 <aside id="side"><input id="cf" type="search" placeholder="Filter countries…" autocomplete="off"><div id="cl"></div></aside>
 <section id="main">
  <div id="mapw"><canvas id="map"></canvas><div id="maptip">Pick a country to see its stations on the map</div></div>
  <div id="sl"><div id="empty">Choose a country on the left, or search for a station.</div></div>
 </section>
 <div id="bar">
  <div id="now"><b id="nt">Nothing playing</b><span id="ns">Pick a station to start listening</span></div>
  <input id="vol" type="range" min="0" max="1" step="0.01" value="0.8" title="Volume">
  <button id="stop" class="g">⏹ Stop</button>
 </div>
</div>
<script>
(function(){
"use strict";
var $=function(i){return document.getElementById(i)};
var audio=new Audio();audio.preload="none";
var countries=[],curCC="",stations=[],playing=null,triedProxy=false,dots=[],bounds=null;
function esc(s){return String(s).replace(/[&<>"]/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]})}
function flag(cc){if(!cc||cc.length!==2)return "🌐";return String.fromCodePoint.apply(null,cc.toUpperCase().split("").map(function(c){return 127397+c.charCodeAt(0)}))}
function getJSON(u){return fetch(u).then(function(r){if(!r.ok)throw new Error("HTTP "+r.status);return r.json()})}

try{var v=localStorage.getItem("lux_radio_vol");if(v!==null){$("vol").value=v}}catch(e){}
audio.volume=parseFloat($("vol").value);
$("vol").oninput=function(){audio.volume=parseFloat(this.value);try{localStorage.setItem("lux_radio_vol",this.value)}catch(e){}};

function renderCountries(){
  var f=$("cf").value.trim().toLowerCase(),h="";
  countries.forEach(function(c){
    if(f&&c.name.toLowerCase().indexOf(f)<0&&c.cc.toLowerCase()!==f)return;
    h+='<div class="c'+(c.cc===curCC?' on':'')+'" data-cc="'+esc(c.cc)+'"><span>'+flag(c.cc)+'</span><span class="n">'+esc(c.name)+'</span><span class="k">'+c.count+'</span></div>';
  });
  $("cl").innerHTML=h||'<div id="empty">No match</div>';
}
$("cl").onclick=function(e){var el=e.target.closest(".c");if(el)openCountry(el.getAttribute("data-cc"))};
$("cf").oninput=renderCountries;

function renderStations(note){
  if(!stations.length){$("sl").innerHTML='<div id="empty">'+esc(note||"No stations found.")+'</div>';return}
  var h="";
  stations.forEach(function(s,i){
    var meta=[s.codec,s.bitrate?s.bitrate+"k":"",s.tags,s.votes?"♥ "+s.votes:""].filter(Boolean).join(" · ");
    var img=s.favicon?'<img src="'+esc(s.favicon)+'" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.outerHTML=\'<div class=ph>📻</div>\'">':'<div class="ph">📻</div>';
    h+='<div class="s'+(playing&&playing.id===s.id?' on':'')+'" data-i="'+i+'">'+img+'<div class="t"><b>'+esc(s.name)+'</b><span>'+flag(s.cc)+' '+esc(s.country||s.cc)+(meta?' · '+esc(meta):'')+'</span></div></div>';
  });
  $("sl").innerHTML=h;
}
$("sl").onclick=function(e){var el=e.target.closest(".s");if(el)play(stations[+el.getAttribute("data-i")])};

function openCountry(cc){
  curCC=cc;$("q").value="";renderCountries();
  $("sl").innerHTML='<div id="empty">Loading…</div>';
  getJSON("/radio/api/stations?cc="+encodeURIComponent(cc)).then(function(d){stations=d;renderStations();drawMap()})
   .catch(function(e){stations=[];renderStations("Could not load stations ("+e.message+")");drawMap()});
}
var qt=null;
$("q").oninput=function(){
  clearTimeout(qt);var q=this.value.trim();
  if(q.length<2)return;
  qt=setTimeout(function(){
    curCC="";renderCountries();
    $("sl").innerHTML='<div id="empty">Searching…</div>';
    getJSON("/radio/api/stations?q="+encodeURIComponent(q)).then(function(d){stations=d;renderStations();drawMap()})
     .catch(function(e){stations=[];renderStations("Search failed ("+e.message+")");drawMap()});
  },350);
};

function setNow(t,s,live){$("nt").textContent=t;$("ns").innerHTML=live?'<span class="live">● live</span> '+esc(s):esc(s)}
function play(s){
  if(!s)return;playing=s;triedProxy=false;
  audio.src=s.direct||("/radio/stream/"+s.id);
  setNow(s.name,"Connecting…",false);
  audio.play().catch(function(){});
  renderStations();drawMap();
  if("mediaSession" in navigator){try{navigator.mediaSession.metadata=new MediaMetadata({title:s.name,artist:s.country||"Radio"})}catch(e){}}
}
audio.onplaying=function(){if(playing)setNow(playing.name,(playing.country||"")+(playing.bitrate?" · "+playing.bitrate+"k":""),true)};
audio.onwaiting=function(){if(playing)setNow(playing.name,"Buffering…",false)};
audio.onerror=function(){
  if(!playing)return;
  if(!triedProxy&&playing.direct){triedProxy=true;audio.src="/radio/stream/"+playing.id;audio.play().catch(function(){});setNow(playing.name,"Switching to relay…",false);return}
  setNow(playing.name,"This station is not available right now. Try another one.",false);
};
$("stop").onclick=function(){audio.pause();audio.removeAttribute("src");audio.load();if(playing)setNow(playing.name,"Stopped",false);playing=null;renderStations();drawMap()};

// ---- map: stations that report coordinates, fitted to the selection ----
var cv=$("map"),ctx=cv.getContext("2d");
function drawMap(){
  var dpr=window.devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;
  if(!w||!h)return;
  cv.width=w*dpr;cv.height=h*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);
  var pts=[];stations.forEach(function(s,i){if(s.lat!=null&&s.lng!=null)pts.push({s:s,i:i})});
  dots=[];
  if(!pts.length){$("maptip").textContent=stations.length?"These stations have no map coordinates":"Pick a country to see its stations on the map";return}
  var minX=1e9,maxX=-1e9,minY=1e9,maxY=-1e9;
  pts.forEach(function(p){minX=Math.min(minX,p.s.lng);maxX=Math.max(maxX,p.s.lng);minY=Math.min(minY,p.s.lat);maxY=Math.max(maxY,p.s.lat)});
  var padX=Math.max((maxX-minX)*.08,.6),padY=Math.max((maxY-minY)*.08,.6);
  minX-=padX;maxX+=padX;minY-=padY;maxY+=padY;
  var sx=w/(maxX-minX),sy=h/(maxY-minY),k=Math.min(sx,sy*1.0),ox=(w-(maxX-minX)*k)/2,oy=(h-(maxY-minY)*k)/2;
  ctx.strokeStyle="#1a2030";ctx.lineWidth=1;
  for(var gx=Math.ceil(minX);gx<=maxX;gx+=Math.max(1,Math.round((maxX-minX)/8))){var x=ox+(gx-minX)*k;ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,h);ctx.stroke()}
  for(var gy=Math.ceil(minY);gy<=maxY;gy+=Math.max(1,Math.round((maxY-minY)/5))){var y=h-(oy+(gy-minY)*k);ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(w,y);ctx.stroke()}
  pts.forEach(function(p){
    var x=ox+(p.s.lng-minX)*k,y=h-(oy+(p.s.lat-minY)*k),on=playing&&playing.id===p.s.id;
    dots.push({x:x,y:y,i:p.i});
    ctx.beginPath();ctx.arc(x,y,on?7:4,0,6.283);ctx.fillStyle=on?"#3ddc97":"rgba(77,107,255,.75)";ctx.fill();
    if(on){ctx.strokeStyle="#3ddc97";ctx.beginPath();ctx.arc(x,y,12,0,6.283);ctx.stroke()}
  });
  $("maptip").textContent=pts.length+" station"+(pts.length===1?"":"s")+" on the map · click a dot to listen";
}
cv.onclick=function(e){
  var r=cv.getBoundingClientRect(),x=e.clientX-r.left,y=e.clientY-r.top,best=null,bd=14*14;
  dots.forEach(function(d){var dd=(d.x-x)*(d.x-x)+(d.y-y)*(d.y-y);if(dd<bd){bd=dd;best=d}});
  if(best)play(stations[best.i]);
};
cv.onmousemove=function(e){
  var r=cv.getBoundingClientRect(),x=e.clientX-r.left,y=e.clientY-r.top,best=null,bd=10*10;
  dots.forEach(function(d){var dd=(d.x-x)*(d.x-x)+(d.y-y)*(d.y-y);if(dd<bd){bd=dd;best=d}});
  cv.title=best?stations[best.i].name:"";
};
window.addEventListener("resize",drawMap);

getJSON("/radio/api/countries").then(function(d){countries=d;renderCountries()})
 .catch(function(){$("cl").innerHTML='<div id="empty">Could not load the country list. Reload the page to retry.</div>'});
})();
</script>
</body>
</html>
'''


# ---- Discord voice playback -------------------------------------------------------------------
def _radio_start_source(gid: int, vc, info: dict, gen: int):
    source = discord.FFmpegPCMAudio(
        info["url"],
        before_options="-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 "
                       "-protocol_whitelist http,https,tcp,tls,crypto",
        options="-vn -loglevel error",
    )

    def after(err):
        if err:
            logger.warning(f"Radio playback error in guild {gid}: {err}")
        asyncio.run_coroutine_threadsafe(radio_ended(gid, gen), bot.loop)

    vc.play(source, after=after)


async def radio_ended(gid: int, gen: int):
    """The stream stopped by itself (network drop): try to reconnect a few times."""
    sess = radio_sessions.get(gid)
    if not sess or sess["gen"] != gen:
        return
    now = time.monotonic()
    sess["restarts"] = [t for t in sess["restarts"] if now - t < 120]
    if len(sess["restarts"]) >= 4:
        await radio_stop(gid)
        return
    sess["restarts"].append(now)
    await asyncio.sleep(1.5)
    if radio_sessions.get(gid) is not sess or sess["gen"] != gen or not sess["vc"].is_connected():
        return
    try:
        _radio_start_source(gid, sess["vc"], sess["info"], gen)
    except Exception as e:
        logger.warning(f"Radio restart failed in guild {gid}: {e}")
        await radio_stop(gid)


async def radio_stop(gid: int) -> bool:
    sess = radio_sessions.pop(gid, None)
    if not sess:
        return False
    sess["gen"] += 1
    vc = sess["vc"]
    try:
        if vc.is_playing():
            vc.stop()
    except Exception:
        pass
    try:
        await vc.disconnect(force=True)
    except Exception as e:
        logger.warning(f"Radio disconnect error in guild {gid}: {e}")
    return True


async def radio_maybe_leave(channel):
    sess = radio_sessions.get(channel.guild.id)
    if sess and sess["vc"].channel and sess["vc"].channel.id == channel.id and humans_in(channel) == 0:
        await radio_stop(channel.guild.id)


async def radio_country_autocomplete(interaction: discord.Interaction, current: str):
    try:
        countries = await asyncio.wait_for(radio_countries(), 2.5)
    except Exception:
        return []
    cur = current.strip().lower()
    out = []
    for c in countries:
        if not cur or cur in c["name"].lower() or cur == c["cc"].lower():
            out.append(app_commands.Choice(name=f"{c['name']} ({c['count']})"[:100], value=c["cc"]))
            if len(out) >= 25:
                break
    return out


async def radio_station_autocomplete(interaction: discord.Interaction, current: str):
    cc = getattr(interaction.namespace, "country", None)
    q = current.strip()
    if not cc and len(q) < 2:
        return []
    try:
        if cc:
            stations = await asyncio.wait_for(radio_stations(cc=cc), 2.5)
            if q:
                stations = [s for s in stations if q.lower() in s["name"].lower()]
        else:
            stations = await asyncio.wait_for(radio_stations(q=q, limit=25), 2.5)
    except Exception:
        return []
    return [app_commands.Choice(name=(f"{s['name']} ({s['bitrate']}k)" if s["bitrate"] else s["name"])[:100], value=s["id"])
            for s in stations[:25]]


@bot.tree.command(name="radio", description="Open the LUX-NET Radio web player (browse stations by country).")
async def radio_cmd(interaction: discord.Interaction):
    if not EMU_WEB_BASE:
        await interaction.response.send_message(
            "⚠️ The radio web player needs a public address. Set the `PUBLIC_URL` environment variable "
            "(Render sets `RENDER_EXTERNAL_URL` automatically for web services).", ephemeral=True)
        return
    link = discord.ui.View()
    link.add_item(discord.ui.Button(style=discord.ButtonStyle.link, label="Open LUX-NET Radio", url=f"{EMU_WEB_BASE}/radio"))
    await interaction.response.send_message(
        "\U0001F4FB **LUX-NET Radio**: browse stations from every country and listen in your browser.\n"
        "To play a station for everyone in a voice channel, use `/radio-play`.", view=link, ephemeral=True)


@bot.tree.command(name="radio-play", description="Play a radio station in your voice channel.")
@app_commands.describe(station="Station to play (type to search)", country="Browse a country first (optional)",
                       voice_channel="Voice channel (optional, defaults to the one you're in)")
@app_commands.autocomplete(country=radio_country_autocomplete, station=radio_station_autocomplete)
async def radio_play(interaction: discord.Interaction, station: str, country: str = None, voice_channel: discord.VoiceChannel = None):
    if not interaction.guild:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return
    if not shutil.which("ffmpeg"):
        await interaction.response.send_message("❌ FFmpeg is not installed on the bot host (add `ffmpeg` to the Dockerfile).", ephemeral=True)
        return
    gid = interaction.guild.id
    if gid in voice_sessions or gid in _voice_connecting:
        await interaction.response.send_message("⚠️ The voice bridge is using voice in this server right now, so radio can't play here.", ephemeral=True)
        return
    target = _resolve_voice_channel(interaction, voice_channel)
    if not target:
        await interaction.response.send_message("❌ Join a voice channel first, or pick a `voice_channel`.", ephemeral=True)
        return
    perms = target.permissions_for(interaction.guild.me)
    if not (perms.connect and perms.speak):
        await interaction.response.send_message(f"❌ I need **Connect** and **Speak** permissions in {target.mention}.", ephemeral=True)
        return

    await interaction.response.defer()
    try:
        info = await radio_station_info(station)
    except Exception as e:
        await interaction.followup.send(f"❌ Couldn't reach the radio directory: {e}", ephemeral=True)
        return
    if not info:
        await interaction.followup.send("❌ Pick a station from the list.", ephemeral=True)
        return
    if not await radio_url_is_public(info["url"]):
        await interaction.followup.send("❌ That station's address isn't allowed.", ephemeral=True)
        return

    try:
        sess = radio_sessions.get(gid)
        if sess and not sess["vc"].is_connected():
            radio_sessions.pop(gid, None)
            sess = None
        if sess:
            sess["gen"] += 1
            vc = sess["vc"]
            if vc.is_playing():
                vc.stop()
            if vc.channel.id != target.id:
                await vc.move_to(target)
        else:
            if interaction.guild.voice_client:
                await interaction.followup.send("⚠️ I'm already in a voice channel in this server.", ephemeral=True)
                return
            vc = await target.connect(timeout=20, self_deaf=True)
            sess = radio_sessions[gid] = {"vc": vc, "info": info, "gen": 0, "restarts": []}
        sess["info"] = info
        sess["restarts"] = []
        _radio_start_source(gid, vc, info, sess["gen"])
    except Exception as e:
        logger.error(f"radio-play failed: {e}")
        await radio_stop(gid)
        await interaction.followup.send(f"❌ Couldn't start the radio: {e}", ephemeral=True)
        return

    asyncio.create_task(radio_click(info["id"]))
    place = f" ({info['country']})" if info["country"] else ""
    await interaction.followup.send(f"\U0001F4FB **{interaction.user.display_name}** tuned **{info['name']}**{place} in {target.mention}. Use `/radio-stop` to stop.")


@bot.tree.command(name="radio-stop", description="Stop the radio and leave the voice channel.")
async def radio_stop_cmd(interaction: discord.Interaction):
    if not interaction.guild or not await radio_stop(interaction.guild.id):
        await interaction.response.send_message("⚠️ No radio is playing in this server.", ephemeral=True)
        return
    await interaction.response.send_message("⏹️ Radio stopped.")


# ------------------------------------------------------------------------------
# MAIN RUNNER
# ------------------------------------------------------------------------------
def main():
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        logger.critical("DISCORD_TOKEN environment variable is missing!")
        sys.exit(1)

    refresh_ban_cache()
    refresh_auto_poll_cache()

    async def runner():
        await start_web()          # bind the port first so Render's health check passes even if Discord is slow
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
