import os
import sys
import asyncio
import logging
import io
from threading import Thread

import discord
from discord import app_commands
from discord.ext import commands
from flask import Flask
from supabase import create_client, Client
import yt_dlp
from duckduckgo_search import DDGS

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

def add_link(table: str, code: str, item_id: int):
    try:
        col = "forum_channel_id" if table == "forum_relays" else ("channel_id" if table == "text_relays" else "thread_id")
        
        supabase.table(table).upsert(
            {"network_code": code, col: item_id}, 
            on_conflict="network_code," + col
        ).execute()
        logger.info(f"Successfully added/upserted {item_id} to {table} under {code}")
    except Exception as e:
        logger.error(f"CRITICAL Supabase insert error in ({table}): {e}")

def remove_link(table: str, code: str, item_id: int):
    try:
        col = "forum_channel_id" if table == "forum_relays" else ("channel_id" if table == "text_relays" else "thread_id")
        supabase.table(table).delete().eq("network_code", code).eq(col, item_id).execute()
    except Exception as e:
        logger.error(f"Failed to remove link from Supabase ({table}): {e}")

def get_links(table: str, code: str):
    try:
        col = "forum_channel_id" if table == "forum_relays" else ("channel_id" if table == "text_relays" else "thread_id")
        
        response = supabase.table(table).select(col).eq("network_code", code).execute()
        logger.info(f"Supabase fetch response for {table} code '{code}': {response.data}")
        return [row[col] for row in response.data]
    except Exception as e:
        logger.error(f"CRITICAL Supabase fetch error in ({table}): {e}")
        return []

# Persistent Message Mapping Helpers via Supabase (Replaces in-memory dicts)
def register_message_mapping(source_msg_id: int, target_channel_id: int, target_msg_id: int):
    try:
        # Check if either message is already mapped to a root group
        res = supabase.table("message_mappings").select("root_message_id").or_(f"message_id.eq.{source_msg_id},message_id.eq.{target_msg_id}").limit(1).execute()
        if res.data:
            root_id = res.data[0]["root_message_id"]
        else:
            root_id = source_msg_id

        # Upsert both entries into Supabase
        supabase.table("message_mappings").upsert([
            {"root_message_id": root_id, "channel_id": target_channel_id, "message_id": target_msg_id},
            {"root_message_id": root_id, "channel_id": source_msg_id, "message_id": source_msg_id} # self reference hook
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

bot = commands.Bot(command_prefix="!", intents=intents)

# Set to track thread IDs that are currently being created by our webhook bridge to prevent re-triggering loops
RELAYED_THREAD_IDS = set()

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
@bot.event
async def on_ready():
    logger.info(f"Connected to Discord as {bot.user} in {len(bot.guilds)} servers")

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
    if user.bot:
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
            if channel:
                target_msg = await channel.fetch_message(target_msg_id)
                if target_msg:
                    await target_msg.add_reaction(emoji)
        except Exception as e:
            logger.error(f"Failed to add cross-server reaction to message {target_msg_id}: {e}")

@bot.event
async def on_reaction_remove(reaction: discord.Reaction, user: discord.User | discord.Member):
    if user.bot:
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
            if channel:
                target_msg = await channel.fetch_message(target_msg_id)
                if target_msg:
                    await target_msg.remove_reaction(emoji, bot.user)
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
                    file_obj = discord.File(fp=io.BytesIO(file_bytes), filename=attachment.filename)
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

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    if after.channel:
        joined_vc_id = after.channel.id
        try:
            supabase.table("vc_code_relays").select("network_code").eq("vc_id", joined_vc_id).execute()
        except Exception as e:
            logger.error(f"Error checking VC bridge database: {e}")

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild or message.webhook_id:
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
                file_obj = discord.File(fp=io.BytesIO(file_bytes), filename=attachment.filename)
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
# SLASH COMMANDS (TEXT, THREADS, FORUMS, & VOICE BRIDGES)
# ------------------------------------------------------------------------------
@bot.tree.command(name="ping", description="Check the bot's latency.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(f"Pong! 🏓 `{latency}ms`", ephemeral=True)

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
                f"⚠️ **#{interaction.channel.name}** is not currently linked to any relay network.",
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
    logger.info(f"Attempting to link forum ID {target_forum.id} ({target_forum.name}) to code {code}")
    
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
# YOUTUBE & WEB SEARCH COMMANDS
# ------------------------------------------------------------------------------
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

class YouTubeDropdown(discord.ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="Select the correct video from the search...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        selected_url = self.values[0]
        await interaction.response.send_message(
            f"✅ You selected: {selected_url}\n(Click the link to open and watch the full video directly!)",
            ephemeral=False
        )

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
                'default_search': 'ytsearch5',
                'quiet': True,
            }
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(search_opts).extract_info(search, download=False))
            entries = data.get('entries', [])
        except Exception:
            pass

        if not entries:
            try:
                with DDGS() as ddgs:
                    for r in ddgs.text(f"{search} site:youtube.com/watch", max_results=5):
                        href = r.get("href", "")
                        if "youtube.com/watch" in href:
                            entries.append({
                                'title': r.get("title", "YouTube Video"),
                                'uploader': "Web Result",
                                'webpage_url': href
                            })
            except Exception as e:
                logger.error(f"Fallback search error: {e}")

    if not entries:
        await interaction.followup.send(
            f"⚠️ Could not find any videos for `{search}`. Try pasting the direct YouTube URL directly!", 
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
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=5, backend="lite"):
                results.append(r)

        if not results:
            await interaction.followup.send(f"⚠ No results found for `{query}`.")
            return

        embed = discord.Embed(
            title=f"🔍 Search Results for: `{query}`",
            color=discord.Color.green()
        )

        for i, res in enumerate(results[:5], 1):
            title = res.get("title", "No Title")
            href = res.get("href", "#")
            body = res.get("body", "No description available.")
            embed.add_field(name=f"{i}. {title[:100]}", value=f"{body[:150]}...\n[Link]({href})", inline=False)

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
