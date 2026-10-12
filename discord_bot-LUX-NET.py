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
import random
from collections import deque
from threading import Thread, Lock
from datetime import datetime, timedelta, timezone

import discord
import aiohttp
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
    col = _LINK_COLS["forum_relays"]
    ids = set()
    res = supabase.table("forum_relays").select("network_code").eq(col, forum_id).execute()
    for row in res.data:
        r = supabase.table("forum_relays").select(col).eq("network_code", row["network_code"]).neq(col, forum_id).execute()
        ids.update(int(x[col]) for x in r.data)
    return list(ids)


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
            logger.error(f"Failed to add cross-server reaction: {e}")

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
            logger.error(f"Failed to remove cross-server reaction: {e}")

async def _relay_new_forum_post(thread: discord.Thread):
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
                    logger.error(f"Failed to process attachment: {e}")

        if skipped_files:
            skip_notice = f"\n*⚠ [Skipped oversized file(s)]*"
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
                        logger.error(f"Error relaying forum post: {e}")
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
                logger.error(f"Failed to process attachment: {e}")

    if skipped_files:
        skip_notice = f"\n*⚠ [Skipped oversized file(s)]*"
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
                        logger.error(f"Error relaying thread message: {e}")

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
                        logger.error(f"Error relaying message: {e}")

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
                for item in google_search(f"{search} site:youtube.com/watch", num_results=5):
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
RADIO_HOSTS = [h.strip() for h in os.environ.get(
    "RADIO_HOSTS", "de1.api.radio-browser.info,de2.api.radio-browser.info,fi1.api.radio-browser.info"
).split(",") if h.strip()]
RADIO_UA = "LUX-NET-Bridge/1.0 (Discord bot)"
RADIO_PROXY_MAX = int(os.environ.get("RADIO_PROXY_MAX", "6"))
RADIO_LIST_LIMIT = 300

radio_sessions: dict = {}
_radio_cache: dict = {}
_radio_urls: dict = {}
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
            _radio_cache[key] = (now + ttl, data)
            return data
        except Exception as e:
            last = f"{host}: {e}"
    if hit:
        return hit[1]
    raise RuntimeError(f"radio directory unavailable ({last})")


def radio_norm(s: dict):
    url = (s.get("url_resolved") or s.get("url") or "").strip()
    uid = s.get("stationuuid")
    if not uid or not url.startswith(("http://", "https://")) or s.get("hls") == 1:
        return None
    _radio_urls[uid] = url
    icon = (s.get("favicon") or "").strip()

    def num(v):
        try:
            f = float(v)
            return f if f == f else None
        except (TypeError, ValueError):
            return None

    lat, lng = num(s.get("geo_lat")), num(s.get("geo_long"))
    return {
        "id": uid,
        "name": (s.get("name") or "Unknown").strip()[:80],
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


async def fetch_somafm_stations() -> list:
    stations = []
    try:
        session = await radio_http()
        async with session.get("https://somafm.com/channels.json", timeout=8) as r:
            if r.status == 200:
                data = await r.json()
                for ch in data.get("channels", []):
                    fastest_stream = next((s for s in ch.get("playlists", []) if "mp3" in s.get("format", "") and "highest" in s.get("quality", "")), None)
                    if not fastest_stream and ch.get("playlists"):
                        fastest_stream = ch.get("playlists")[0]
                    stream_url = fastest_stream.get("url") if fastest_stream else None
                    if stream_url:
                        stations.append({
                            "id": f"somafm-{ch.get('id')}",
                            "name": ch.get("title", "SomaFM Station"),
                            "cc": "US",
                            "country": "United States",
                            "tags": ", ".join(ch.get("genres", [])),
                            "votes": ch.get("listeners", 100),
                            "codec": "MP3",
                            "bitrate": 128,
                            "favicon": ch.get("image", ""),
                            "url": stream_url,
                            "source": "SomaFM",
                            "frequency": None
                        })
    except Exception as e:
        logger.warning(f"SomaFM fetch error: {e}")
    return stations


def extract_frequency(name: str, tags: str) -> float | None:
    text = f"{name} {tags}"
    match = re.search(r'\b(8[7-9]\.\d|9\d\.\d|10[0-7]\.\d)\s*(?:MHz|FM)?\b', text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return None


async def multi_source_search(cc: str = None, q: str = None, frequency: float = None) -> list:
    raw_stations = []
    
    try:
        rb_data = await radio_stations(cc=cc, q=q, limit=150)
        for s in rb_data:
            s["source"] = "Radio Browser"
            s["frequency"] = extract_frequency(s["name"], s["tags"])
            raw_stations.append(s)
    except Exception as e:
        logger.warning(f"Radio Browser source skipped: {e}")

    if not cc or cc.upper() == "US":
        soma_data = await fetch_somafm_stations()
        for s in soma_data:
            s["frequency"] = extract_frequency(s["name"], s["tags"])
            raw_stations.append(s)

    seen_urls = set()
    seen_names = set()
    deduped = []

    for s in raw_stations:
        url = s.get("url")
        norm_name = re.sub(r'\W+', '', s.get("name", "")).lower()
        if url in seen_urls or norm_name in seen_names:
            continue
        if frequency and s.get("frequency") and abs(s["frequency"] - frequency) > 0.05:
            continue
        seen_urls.add(url)
        seen_names.add(norm_name)
        deduped.append(s)

    deduped.sort(key=lambda x: x.get("votes", 0), reverse=True)
    return deduped


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
    try:
        await radio_api(f"url/{uid}", None, ttl=0)
    except Exception:
        pass


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


async def radio_warm():
    try:
        await radio_countries()
    except Exception as e:
        logger.warning(f"Radio warm-up failed: {e}")


async def web_radio_page(request):
    return web.Response(text="Radio Player Online", content_type="text/html")


async def web_radio_countries(request):
    try:
        data = await radio_countries()
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)
    return web.json_response(data)


async def web_radio_stations(request):
    cc = (request.query.get("cc") or "").strip()[:2]
    q = (request.query.get("q") or "").strip()[:60]
    try:
        stations = await radio_stations(cc=cc or None, q=q or None)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)
    return web.json_response(stations)


async def web_radio_stream(request):
    global _radio_proxy_active
    uid = request.match_info["sid"]
    url = await radio_station_url(uid)
    if not url:
        return web.Response(status=404, text="unknown")
    return web.Response(status=502, text="proxy stream offline")


def _radio_start_source(gid: int, vc, info: dict, gen: int):
    source = discord.FFmpegPCMAudio(
        info["url"],
        before_options="-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -protocol_whitelist http,https,tcp,tls,crypto",
        options="-vn -loglevel error",
    )

    def after(err):
        if err:
            logger.warning(f"Radio playback error: {err}")
        asyncio.run_coroutine_threadsafe(radio_ended(gid, gen), bot.loop)

    vc.play(source, after=after)


async def radio_ended(gid: int, gen: int):
    sess = radio_sessions.get(gid)
    if not sess or sess["gen"] != gen:
        return
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


async def radio_country_autocomplete(interaction: discord.Interaction, current: str):
    try:
        countries = await asyncio.wait_for(radio_countries(), 2.5)
    except Exception:
        return []
    cur = current.strip().lower()
    out = []
    for c in countries:
        if not cur or cur in c["name"].lower() or cur == c["cc"].lower():
            out.append(app_commands.Choice(name=c["name"][:100], value=c["cc"]))
            if len(out) >= 25:
                break
    return out


async def radio_station_autocomplete(interaction: discord.Interaction, current: str):
    cc = getattr(interaction.namespace, "country", None)
    q = current.strip()
    try:
        if cc:
            stations = await asyncio.wait_for(radio_stations(cc=cc), 2.5)
        else:
            stations = await asyncio.wait_for(radio_stations(q=q, limit=25), 2.5)
    except Exception:
        return []
    return [app_commands.Choice(name=s["name"][:100], value=s["id"]) for s in stations[:25]]


@bot.tree.command(name="radio", description="Open the radio web player.")
async def radio_cmd(interaction: discord.Interaction):
    await interaction.response.send_message("Radio player available.", ephemeral=True)


@bot.tree.command(name="radio-search", description="Search across multiple radio directories.")
@app_commands.describe(query="Station name, genre, or keyword")
async def radio_search(interaction: discord.Interaction, query: str):
    await interaction.response.defer(ephemeral=True)
    stations = await multi_source_search(q=query)
    if not stations:
        await interaction.followup.send(f"⚠️ No stations found matching `{query}`.", ephemeral=True)
        return

    desc = []
    for s in stations[:10]:
        freq_str = f" [{s['frequency']} MHz]" if s.get('frequency') else ""
        desc.append(f"• **{s['name']}**{freq_str} ({s['codec']} {s['bitrate']}k) — *Source: {s['source']}*")
    
    embed = discord.Embed(title=f"📻 Search Results for '{query}'", description="\n".join(desc), color=discord.Color.blurple())
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="radio-frequency", description="Find and tune into a station by its frequency.")
@app_commands.describe(frequency="Frequency in MHz (e.g. 101.1)", country="Country code filter")
async def radio_frequency(interaction: discord.Interaction, frequency: float, country: str = "US"):
    await interaction.response.defer(ephemeral=True)
    stations = await multi_source_search(cc=country, frequency=frequency)
    if not stations:
        await interaction.followup.send(f"⚠️ No streams found matching frequency `{frequency} MHz` in {country}.", ephemeral=True)
        return

    s = stations[0]
    await interaction.followup.send(f"✅ Found matching frequency: **{s['name']}** ({s['frequency']} MHz) from *{s['source']}*.")


@bot.tree.command(name="radio-play", description="Play a radio station in voice.")
@app_commands.describe(country="Country", station="Station ID", voice_channel="Voice channel")
@app_commands.autocomplete(country=radio_country_autocomplete, station=radio_station_autocomplete)
async def radio_play(interaction: discord.Interaction, country: str, station: str, voice_channel: discord.VoiceChannel = None):
    if not interaction.guild:
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return
    if not shutil.which("ffmpeg"):
        await interaction.response.send_message("❌ FFmpeg not installed.", ephemeral=True)
        return
    gid = interaction.guild.id
    target = _resolve_voice_channel(interaction, voice_channel)
    if not target:
        await interaction.response.send_message("❌ Join a voice channel first.", ephemeral=True)
        return

    await interaction.response.defer()
    info = await radio_station_info(station)
    if not info:
        await interaction.followup.send("❌ Pick a valid station.", ephemeral=True)
        return

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
            sess = radio_sessions[gid] = {"vc": vc, "info": info, "gen": 0, "restarts": []}
        sess["info"] = info
        _radio_start_source(gid, vc, info, sess["gen"])
    except Exception as e:
        await radio_stop(gid)
        await interaction.followup.send(f"❌ Couldn't start radio: {e}", ephemeral=True)
        return

    await interaction.followup.send(f"📻 Playing **{info['name']}** in {target.mention}.")


@bot.tree.command(name="radio-stop", description="Stop the radio.")
async def radio_stop_cmd(interaction: discord.Interaction):
    if not interaction.guild or not await radio_stop(interaction.guild.id):
        await interaction.response.send_message("⚠️ No radio playing.", ephemeral=True)
        return
    await interaction.response.send_message("⏹️ Radio stopped.")


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
