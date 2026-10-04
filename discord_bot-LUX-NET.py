import os
import sys
import asyncio
import logging
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
logger = logging.getLogger("discord-voice-bridge")

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
        col = "channel_id" if table == "text_relays" else "thread_id"
        existing = (
            supabase.table(table)
            .select("*")
            .eq("network_code", code)
            .eq(col, item_id)
            .execute()
        )
        if not existing.data:
            supabase.table(table).insert({"network_code": code, col: item_id}).execute()
    except Exception as e:
        logger.error(f"Failed to add link to Supabase: {e}")

def remove_link(table: str, code: str, item_id: int):
    try:
        col = "channel_id" if table == "text_relays" else "thread_id"
        supabase.table(table).delete().eq("network_code", code).eq(col, item_id).execute()
    except Exception as e:
        logger.error(f"Failed to remove link from Supabase: {e}")

def get_links(table: str, code: str):
    try:
        col = "channel_id" if table == "text_relays" else "thread_id"
        response = supabase.table(table).select(col).eq("network_code", code).execute()
        return [row[col] for row in response.data]
    except Exception as e:
        logger.error(f"Failed to fetch links from Supabase: {e}")
        return []

# ------------------------------------------------------------------------------
# FLASK KEEP-ALIVE SERVER (FOR RENDER UPTIME)
# ------------------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "LUX-NET Telephone Company Bridge is Online!", 200

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

bot = commands.Bot(command_prefix="!", intents=intents)

# ------------------------------------------------------------------------------
# YOUTUBE & AUDIO HELPER SETUP
# ------------------------------------------------------------------------------
ytdl_format_options = {
    'format': 'bestaudio/best',
    'noplaylist': True,
    'default_search': 'auto',
    'quiet': True,
    'extract_flat': False,
}

ffmpeg_options = {
    'options': '-vn',
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5'
}

ytdl = yt_dlp.YoutubeDL(ytdl_format_options)

class YTDLSource(discord.PCMVolumeTransformer):
    def __init__(self, source, *, data, volume=0.5):
        super().__init__(source, volume)
        self.data = data
        self.title = data.get('title')
        self.url = data.get('webpage_url')

    @classmethod
    async def from_url(cls, url, *, loop=None, stream=True):
        loop = loop or asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: ytdl.extract_info(url, download=not stream))
        
        if 'entries' in data:
            data = data['entries'][0]

        filename = data['url'] if stream else ytdl.prepare_filename(data)
        return cls(discord.FFmpegPCMAudio(filename, **ffmpeg_options), data=data)

# ------------------------------------------------------------------------------
# WEBHOOK HELPER FUNCTIONS
# ------------------------------------------------------------------------------
async def get_or_create_webhook(channel: discord.abc.GuildChannel) -> discord.Webhook | None:
    target_channel = channel.parent if isinstance(channel, discord.Thread) else channel

    if not isinstance(target_channel, discord.TextChannel):
        return None

    try:
        webhooks = await target_channel.webhooks()
        for wh in webhooks:
            if wh.user == bot.user:
                return wh
        return await target_channel.create_webhook(name="LUX-NET Relay Bridge")
    except discord.Forbidden:
        logger.error(f"Missing 'Manage Webhooks' permission in #{target_channel.name}")
        return None
    except Exception as e:
        logger.error(f"Failed to create webhook in #{target_channel.name}: {e}")
        return None

# ------------------------------------------------------------------------------
# BOT EVENTS (TEXT, THREADS, & WEBHOOKS)
# ------------------------------------------------------------------------------
@bot.event
async def on_ready():
    logger.info(f"Connected to Discord as {bot.user} in {len(bot.guilds)} servers")

    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync global slash commands: {e}")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    await bot.process_commands(message)

    current_channel = message.channel
    is_thread = isinstance(current_channel, discord.Thread)

    target_channel_ids = []
    target_thread_ids = []

    try:
        if is_thread:
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
        available_len = max(10, 80 - len(author_name) - 3)
        webhook_username = f"{author_name} [{guild_name[:available_len]}]"

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
    }

    if final_content:
        send_kwargs["content"] = final_content

    if message.embeds:
        send_kwargs["embeds"] = message.embeds

    files = []
    if message.attachments:
        for attachment in message.attachments:
            try:
                file = await attachment.to_file()
                files.append(file)
            except Exception as e:
                logger.error(f"Failed to process attachment: {e}")

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
                        await webhook.send(thread=t_channel, **send_kwargs)
                    except Exception as e:
                        logger.error(f"Error relaying thread message to {tid}: {e}")

    if target_channel_ids:
        for target_id in target_channel_ids:
            target_channel = bot.get_channel(target_id)
            if target_channel and isinstance(target_channel, discord.TextChannel):
                webhook = await get_or_create_webhook(target_channel)
                if webhook:
                    try:
                        await webhook.send(**send_kwargs)
                    except Exception as e:
                        logger.error(f"Error relaying message to {target_channel.id}: {e}")

# ------------------------------------------------------------------------------
# SLASH COMMANDS (HELP, RELAYS, MEDIA & UTILS)
# ------------------------------------------------------------------------------
@bot.tree.command(name="help", description="Displays instructions, command guides, and setup guidelines for LUX-NET.")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="📖 LUX-NET Bot Guide & Setup",
        description="Welcome to **LUX-NET**, your multi-server bridge, YouTube audio streamer, and internet search tool!",
        color=discord.Color.blurple()
    )

    embed.add_field(
        name="💬 Cross-Server Relays",
        value=(
            "• `/link-relay [network_code]` — Links a text channel to a shared network.\n"
            "• `/unlink-relay` — Disconnects the channel from relaying.\n"
            "• `/link-thread [network_code] [thread_name]` — Links/creates a cross-server thread bridge.\n"
            "• `/unlink-thread` — Disconnects the current thread.\n"
            "• `/list-bridges` — Displays all active bridge networks."
        ),
        inline=False
    )

    embed.add_field(
        name="🎵 YouTube Audio & 🔍 Web Search",
        value=(
            "• `/play [search]` — Plays audio from a YouTube URL or query in your VC.\n"
            "• `/stop` — Stops playback and disconnects the bot from the voice channel.\n"
            "• `/search [query]` — Queries the internet via DuckDuckGo and returns results."
        ),
        inline=False
    )

    embed.add_field(
        name="⚙️ Bot Setup Guide",
        value=(
            "1. **Discord Bot Token**: Create an application on the Discord Developer Portal, enable `Message Content`, `Guilds`, and `Voice States` intents, and set `DISCORD_TOKEN`.\n"
            "2. **Supabase Database**: Create tables named `text_relays` and `thread_relays` with columns `network_code` and `channel_id`/`thread_id`, then provide `SUPABASE_URL` and `SUPABASE_KEY`.\n"
            "3. **Permissions**: Ensure the bot has `Manage Webhooks` permissions in any text channels you plan to bridge.\n"
            "4. **Hosting**: Host on Render (or similar platforms) using the built-in Flask keep-alive web server."
        ),
        inline=False
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="ping", description="Check the bot's latency.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(f"Pong! 🏓 `{latency}ms`", ephemeral=True)

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

@bot.tree.command(name="link-thread", description="Link or create a matching thread across servers using a shared thread code.")
@app_commands.describe(
    network_code="The unique code for this thread bridge",
    thread_name="Name of the thread to create if it doesn't exist here yet"
)
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
            await interaction.response.send_message("❌ Please provide a `thread_name` if you are running this command in a text channel to create a new thread.", ephemeral=True)
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

@bot.tree.command(name="unlink-thread", description="Disconnect this thread from its active cross-server thread network.")
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

@bot.tree.command(name="list-bridges", description="List all active text, thread, and voice bridge connections.")
async def list_bridges(interaction: discord.Interaction):
    try:
        t_res = supabase.table("text_relays").select("network_code").execute()
        text_codes = list(set([row["network_code"] for row in t_res.data]))

        th_res = supabase.table("thread_relays").select("network_code").execute()
        thread_codes = list(set([row["network_code"] for row in th_res.data]))
    except Exception as e:
        await interaction.response.send_message(f"❌ Error fetching bridges: {e}", ephemeral=True)
        return

    embed = discord.Embed(
        title="🌐 LUX-NET Active Bridges",
        description="Current active network connections (saved securely in Supabase):",
        color=discord.Color.blue()
    )

    if text_codes:
        text_summary = ""
        for code in text_codes:
            channels = get_links("text_relays", code)
            text_summary += f"• **`{code}`**: {len(channels)} channel(s)\n"
        embed.add_field(name="Text Relays", value=text_summary, inline=False)
    else:
        embed.add_field(name="Text Relays", value="No active text relays linked.", inline=False)

    if thread_codes:
        thread_summary = ""
        for code in thread_codes:
            threads = get_links("thread_relays", code)
            thread_summary += f"• **`{code}`**: {len(threads)} thread(s)\n"
        embed.add_field(name="Thread Relays", value=thread_summary, inline=False)
    else:
        embed.add_field(name="Thread Relays", value="No active thread bridges linked.", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

# ------------------------------------------------------------------------------
# YOUTUBE / AUDIO PLAYBACK COMMANDS
# ------------------------------------------------------------------------------
@bot.tree.command(name="play", description="Play audio from a YouTube link or search query in your voice channel.")
@app_commands.describe(search="YouTube URL or search keywords")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("❌ You must be connected to a voice channel to use this command.", ephemeral=True)
        return

    voice_channel = interaction.user.voice.channel
    await interaction.response.defer()

    try:
        if interaction.guild.voice_client is not None:
            await interaction.guild.voice_client.move_to(voice_channel)
        else:
            await voice_channel.connect()

        player = await YTDLSource.from_url(search, loop=bot.loop, stream=True)
        
        def after_playing(error):
            if error:
                logger.error(f"Player error: {error}")

        interaction.guild.voice_client.play(player, after=after_playing)
        await interaction.followup.send(f"🎶 Now playing: **{player.title}**")
    except Exception as e:
        logger.error(f"Playback error: {e}")
        await interaction.followup.send(f"❌ An error occurred while trying to play that video: {e}")

@bot.tree.command(name="stop", description="Stop playback and disconnect the bot from the voice channel.")
async def stop(interaction: discord.Interaction):
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.disconnect()
        await interaction.response.send_message("⏹️ Stopped playback and left the voice channel.", ephemeral=False)
    else:
        await interaction.response.send_message("⚠ The bot is not connected to a voice channel.", ephemeral=True)

# ------------------------------------------------------------------------------
# INTERNET SEARCH COMMAND
# ------------------------------------------------------------------------------
@bot.tree.command(name="search", description="Search the internet using DuckDuckGo.")
@app_commands.describe(query="What would you like to search for?")
async def search(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    try:
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=5):
                results.append(r)

        if not results:
            await interaction.followup.send(f"⚠️ No results found for `{query}`.")
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
