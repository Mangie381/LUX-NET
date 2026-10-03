import os
import sys
import asyncio
import logging
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

# Start Flask in a background thread
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
async def get_or_create_webhook(channel: discord.TextChannel) -> discord.Webhook | None:
    """Finds an existing webhook created by the bot or creates a new one."""
    if not isinstance(channel, discord.TextChannel):
        return None

    try:
        webhooks = await channel.webhooks()
        for wh in webhooks:
            if wh.user == bot.user:
                return wh
        return await channel.create_webhook(name="LUX-NET Relay Bridge")
    except discord.Forbidden:
        logger.error(f"Missing 'Manage Webhooks' permission in channel #{channel.name} (ID: {channel.id})")
        return None
    except Exception as e:
        logger.error(f"Failed to fetch/create webhook in channel #{channel.name}: {e}")
        return None

# ------------------------------------------------------------------------------
# BOT EVENTS
# ------------------------------------------------------------------------------
@bot.event
async def on_ready():
    logger.info(f"Connected to Discord as {bot.user} in {len(bot.guilds)} servers")

    # 1. Clear duplicate guild-scoped commands on connected servers
    for guild in bot.guilds:
        try:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
            logger.info(f"Cleared duplicate guild commands from {guild.name}")
        except Exception as e:
            logger.error(f"Could not clear guild commands in {guild.name}: {e}")

    # 2. Sync global commands cleanly
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync global slash commands: {e}")

@bot.event
async def on_message(message: discord.Message):
    # Prevent bot from relaying its own messages or messages from other bots
    if message.author.bot:
        return

    # Process standard prefix commands if any exist
    await bot.process_commands(message)

    # Ignore direct messages
    if not message.guild or not isinstance(message.channel, discord.TextChannel):
        return

    # MAPPED TARGET CHANNELS LOOKUP
    # Insert or query your target bridged text channel objects here
    target_channels = []

    for target_channel in target_channels:
        webhook = await get_or_create_webhook(target_channel)
        if not webhook:
            continue

        # Dynamic parameter building
        send_kwargs = {
            "username": message.author.display_name,
            "avatar_url": message.author.display_avatar.url,
            "allowed_mentions": discord.AllowedMentions.none(),
        }

        # 1. Content
        if message.content:
            send_kwargs["content"] = message.content

        # 2. Embeds (Only add if non-empty to avoid len(None) TypeError)
        if message.embeds:
            send_kwargs["embeds"] = message.embeds

        # 3. Attachments (Re-upload so files render across servers)
        files = []
        if message.attachments:
            for attachment in message.attachments:
                try:
                    file = await attachment.to_file()
                    files.append(file)
                except Exception as e:
                    logger.error(f"Failed to copy attachment {attachment.filename}: {e}")

        if files:
            send_kwargs["files"] = files

        # 4. Stickers and edge cases
        if "content" not in send_kwargs and "embeds" not in send_kwargs and "files" not in send_kwargs:
            if message.stickers:
                send_kwargs["content"] = f"*[Sticker: {message.stickers[0].name}]*"
            else:
                continue

        try:
            await webhook.send(**send_kwargs)
        except discord.HTTPException as e:
            logger.error(f"HTTP Error sending webhook to channel {target_channel.id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error sending webhook to channel {target_channel.id}: {e}")

# ------------------------------------------------------------------------------
# SLASH COMMANDS
# ------------------------------------------------------------------------------
@bot.tree.command(name="ping", description="Check the bot's latency.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(f"Pong! 🏓 `{latency}ms`", ephemeral=True)

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
    main()
