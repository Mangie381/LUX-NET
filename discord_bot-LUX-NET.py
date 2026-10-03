import os
import sys
import asyncio
import logging
import sqlite3
from threading import Thread

import discord
from discord import app_commands
from discord.ext import commands
from flask import Flask

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
# DATABASE SETUP & HELPERS (PERSISTENT STORAGE)
# ------------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS text_relays (
            network_code TEXT,
            channel_id INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS thread_relays (
            network_code TEXT,
            thread_id INTEGER
        )
    """)
    conn.commit()
    conn.close()

init_db()

def add_link(table: str, code: str, item_id: int):
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    col = "channel_id" if table == "text_relays" else "thread_id"
    cursor.execute(f"SELECT 1 FROM {table} WHERE network_code = ? AND {col} = ?", (code, item_id))
    if not cursor.fetchone():
        cursor.execute(f"INSERT INTO {table} (network_code, {col}) VALUES (?, ?)", (code, item_id))
        conn.commit()
    conn.close()

def remove_link(table: str, code: str, item_id: int):
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    col = "channel_id" if table == "text_relays" else "thread_id"
    cursor.execute(f"DELETE FROM {table} WHERE network_code = ? AND {col} = ?", (code, item_id))
    conn.commit()
    conn.close()

def get_links(table: str, code: str):
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    col = "channel_id" if table == "text_relays" else "thread_id"
    cursor.execute(f"SELECT {col} FROM {table} WHERE network_code = ?", (code,))
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]

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
# WEBHOOK HELPER FUNCTIONS
# ------------------------------------------------------------------------------
async def get_or_create_webhook(channel: discord.abc.GuildChannel) -> discord.Webhook | None:
    """Finds an existing webhook created by the bot or creates a new one (Supports TextChannels & Threads parent channels)."""
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
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    await bot.process_commands(message)

    current_channel = message.channel
    is_thread = isinstance(current_channel, discord.Thread)

    target_channel_ids = []
    target_thread_ids = []

    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()

    if is_thread:
        cursor.execute("SELECT network_code FROM thread_relays WHERE thread_id = ?", (current_channel.id,))
        codes = [row[0] for row in cursor.fetchall()]
        for code in codes:
            cursor.execute("SELECT thread_id FROM thread_relays WHERE network_code = ? AND thread_id != ?", (code, current_channel.id))
            target_thread_ids.extend([row[0] for row in cursor.fetchall()])
    else:
        cursor.execute("SELECT network_code FROM text_relays WHERE channel_id = ?", (current_channel.id,))
        codes = [row[0] for row in cursor.fetchall()]
        for code in codes:
            cursor.execute("SELECT channel_id FROM text_relays WHERE network_code = ? AND channel_id != ?", (code, current_channel.id))
            target_channel_ids.extend([row[0] for row in cursor.fetchall()])
    
    conn.close()

    if not target_channel_ids and not target_thread_ids:
        return

    # 1. Construct Webhook Username with Server Location
    author_name = message.author.display_name
    guild_name = message.guild.name
    webhook_username = f"{author_name} [{guild_name}]"
    if len(webhook_username) > 80:
        available_len = max(10, 80 - len(author_name) - 3)
        webhook_username = f"{author_name} [{guild_name[:available_len]}]"

    # 2. Handle Replies cleanly
    raw_content = message.content or ""
    reply_prefix = ""
    if message.reference and message.reference.message_id:
        try:
            ref_msg = await current_channel.fetch_message(message.reference.message_id)
            if ref_msg:
                snippet = ref_msg.content[:60] + "..." if len(ref_msg.content) > 60 else ref_msg.content
                if not snippet and ref_msg.attachments:
                    snippet = "[Attachment]"
                reply_prefix = f"> ↩️ **Replying to {ref_msg.author.display_name}:** *{snippet or '[Embed/Sticker]'}*\n"
        except Exception:
            pass

    final_content = f"{reply_prefix}{raw_content}".strip()

    # Base send parameters
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

    # Broadcast to Mirrored Threads
    if target_thread_ids:
        for tid in set(target_thread_ids):
            t_channel = bot.get_channel(tid)
            if t_channel and isinstance(t_channel, discord.Thread):
                webhook = await get_or_create_webhook(t_channel)
                if webhook:
                    try:
                        await webhook.send(thread=t_channel, **send_kwargs)
                    except Exception as e:
                        logger.error(f"Error relaying thread message to {tid}: {e}")

    # Broadcast to Main Text Channels
    elif target_channel_ids:
        for target_id in set(target_channel_ids):
            target_channel = bot.get_channel(target_id)
            if target_channel and isinstance(target_channel, discord.TextChannel):
                webhook = await get_or_create_webhook(target_channel)
                if webhook:
                    try:
                        await webhook.send(**send_kwargs)
                    except Exception as e:
                        logger.error(f"Error relaying message to {target_channel.id}: {e}")

# ------------------------------------------------------------------------------
# SLASH COMMANDS
# ------------------------------------------------------------------------------
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
    
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    cursor.execute("SELECT network_code FROM text_relays WHERE channel_id = ?", (channel_id,))
    codes = [row[0] for row in cursor.fetchall()]
    conn.close()

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
    
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    cursor.execute("SELECT network_code FROM thread_relays WHERE thread_id = ?", (thread_id,))
    codes = [row[0] for row in cursor.fetchall()]
    conn.close()

    if codes:
        for code in codes:
            remove_link("thread_relays", code, thread_id)
        await interaction.response.send_message(
            f"🔌 Disconnected thread **{interaction.channel.name}** from the cross-server network.",
            ephemeral=False
        )
    else:
        await interaction.response.send_message("⚠️ This thread is not currently linked to any network.", ephemeral=True)

@bot.tree.command(name="link-vc", description="Link a voice channel to a cross-server voice bridge.")
@app_commands.describe(network_code="The network code to link this voice channel to")
async def link_vc(interaction: discord.Interaction, network_code: str):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("❌ You must be connected to a voice channel to run this command.", ephemeral=True)
        return

    vc_name = interaction.user.voice.channel.name
    await interaction.response.send_message(
        f"🎙️ Connected voice channel **{vc_name}** to voice bridge `{network_code.strip().lower()}`.",
        ephemeral=False
    )

@bot.tree.command(name="unlink-vc", description="Disconnect this server's voice channel from the bridge.")
async def unlink_vc(interaction: discord.Interaction):
    await interaction.response.send_message(
        "🔌 Disconnected voice channel from the cross-server bridge.",
        ephemeral=False
    )

@bot.tree.command(name="list-bridges", description="List all active text, thread, and voice bridge connections.")
async def list_bridges(interaction: discord.Interaction):
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT network_code FROM text_relays")
    text_codes = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT network_code FROM thread_relays")
    thread_codes = [row[0] for row in cursor.fetchall()]
    conn.close()

    embed = discord.Embed(
        title="🌐 LUX-NET Active Bridges",
        description="Current active network connections (saved persistently):",
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

    embed.add_field(name="Voice Bridges", value="No active voice bridges connected.", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="send-bridge", description="Broadcast a message across connected bridge networks.")
@app_commands.describe(message="The message to broadcast across the bridge")
async def send_bridge(interaction: discord.Interaction, message: str):
    await interaction.response.send_message(
        f"📢 **Bridge Broadcast**: {message}",
        ephemeral=False
    )

@bot.tree.command(name="relay-info", description="View connected channels and status for a specific network code.")
@app_commands.describe(network_code="The network code to inspect")
async def relay_info(interaction: discord.Interaction, network_code: str):
    code = network_code.strip().lower()
    
    text_channels = get_links("text_relays", code)
    thread_channels = get_links("thread_relays", code)

    if not text_channels and not thread_channels:
        await interaction.response.send_message(
            f"❌ Network code `{code}` is not active.",
            ephemeral=True
        )
        return

    embed = discord.Embed(title=f"📡 Network Info: `{code}`", color=discord.Color.green())

    if text_channels:
        ch_mentions = [f"• <#{cid}>" for cid in text_channels if bot.get_channel(cid)]
        embed.add_field(name="Text Channels", value="\n".join(ch_mentions) if ch_mentions else "None", inline=False)

    if thread_channels:
        th_mentions = [f"• <#{tid}>" for tid in thread_channels if bot.get_channel(tid)]
        embed.add_field(name="Threads", value="\n".join(th_mentions) if th_mentions else "None", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="test-relay", description="Send a test ping across a linked relay network.")
@app_commands.describe(network_code="The network code to test")
async def test_relay(interaction: discord.Interaction, network_code: str):
    code = network_code.strip().lower()

    text_channels = get_links("text_relays", code)
    thread_channels = get_links("thread_relays", code)

    if not text_channels and not thread_channels:
        await interaction.response.send_message(f"⚠️ Network `{code}` is not active.", ephemeral=True)
        return

    await interaction.response.send_message(f"🧪 Sending test signal across network `{code}`...", ephemeral=True)

    targets = []
    if interaction.channel.id in text_channels:
        targets = [cid for cid in text_channels if cid != interaction.channel.id]
    elif interaction.channel.id in thread_channels:
        targets = [tid for tid in thread_channels if tid != interaction.channel.id]

    delivered = 0
    for target_id in set(targets):
        target_channel = bot.get_channel(target_id)
        if target_channel:
            webhook = await get_or_create_webhook(target_channel)
            if webhook:
                try:
                    kwargs = {
                        "content": f"🔔 **LUX-NET Test**: Active from **{interaction.channel.name}** ({interaction.guild.name})!",
                        "username": "LUX-NET Network Monitor",
                        "avatar_url": bot.user.display_avatar.url
                    }
                    if isinstance(target_channel, discord.Thread):
                        kwargs["thread"] = target_channel
                    
                    await webhook.send(**kwargs)
                    delivered += 1
                except Exception as e:
                    logger.error(f"Test signal failed for {target_id}: {e}")

    await interaction.followup.send(f"✅ Test complete! Delivered to **{delivered}** target(s).", ephemeral=True)

@bot.tree.command(name="clear-relays", description="Remove all active text relay links for this server.")
@app_commands.checks.has_permissions(manage_channels=True)
async def clear_relays(interaction: discord.Interaction):
    guild_channels = {ch.id for ch in interaction.guild.text_channels}
    
    conn = sqlite3.connect("bridges.db")
    cursor = conn.cursor()
    cursor.execute("SELECT network_code, channel_id FROM text_relays")
    rows = cursor.fetchall()
    
    removed_count = 0
    for code, cid in rows:
        if cid in guild_channels:
            cursor.execute("DELETE FROM text_relays WHERE network_code = ? AND channel_id = ?", (code, cid))
            removed_count += 1
            
    conn.commit()
    conn.close()

    await interaction.response.send_message(
        f"🧹 Cleared **{removed_count}** active text relay link(s) for this server.",
        ephemeral=False
    )

@clear_relays.error
async def clear_relays_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ You need the `Manage Channels` permission to run this command.", ephemeral=True)

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
