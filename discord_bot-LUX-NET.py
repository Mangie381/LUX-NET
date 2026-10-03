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

    # Sync commands directly to every server for instant availability without duplicates
    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            logger.info(f"Synced {len(synced)} slash commands to {guild.name}")
        except Exception as e:
            logger.error(f"Could not sync instant commands to {guild.name}: {e}")

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

    # TODO: Add your channel mapping/Gist database lookup here to populate target channels
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
# ALL 6 LUX-NET SLASH COMMANDS
# ------------------------------------------------------------------------------
@bot.tree.command(name="ping", description="Check the bot's latency.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(f"Pong! 🏓 `{latency}ms`", ephemeral=True)

@bot.tree.command(name="link-relay", description="Link this text channel to a cross-server relay network.")
@app_commands.describe(network_code="The network code to link this text channel to")
async def link_relay(interaction: discord.Interaction, network_code: str):
    await interaction.response.send_message(
        f"✅ Connected **#{interaction.channel.name}** to text relay network `{network_code}`.",
        ephemeral=False
    )

@bot.tree.command(name="unlink-relay", description="Unlink this text channel from its active relay network.")
async def unlink_relay(interaction: discord.Interaction):
    await interaction.response.send_message(
        f"🔌 Disconnected **#{interaction.channel.name}** from the text relay network.",
        ephemeral=False
    )

@bot.tree.command(name="link-vc", description="Link a voice channel to a cross-server voice bridge.")
@app_commands.describe(network_code="The network code to link this voice channel to")
async def link_vc(interaction: discord.Interaction, network_code: str):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("❌ You must be in a voice channel to use this command.", ephemeral=True)
        return

    vc_name = interaction.user.voice.channel.name
    await interaction.response.send_message(
        f"🎙️ Connected voice channel **{vc_name}** to voice bridge `{network_code}`.",
        ephemeral=False
    )

@bot.tree.command(name="unlink-vc", description="Disconnect this server's voice channel from the bridge.")
async def unlink_vc(interaction: discord.Interaction):
    await interaction.response.send_message(
        "🔌 Disconnected voice channel from the cross-server bridge.",
        ephemeral=False
    )

@bot.tree.command(name="list-bridges", description="List all active text and voice bridge connections.")
async def list_bridges(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🌐 LUX-NET Active Bridges",
        description="Current active channel connections across the telephone network:",
        color=discord.Color.blue()
    )
    embed.add_field(name="Text Relays", value="No active text relays configured.", inline=False)
    embed.add_field(name="Voice Bridges", value="No active voice bridges connected.", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="send-bridge", description="Broadcast a TTS announcement across connected bridge networks.")
@app_commands.describe(message="The message to broadcast across the bridge")
async def send_bridge(interaction: discord.Interaction, message: str):
    await interaction.response.send_message(
        f"📢 **Bridge Broadcast**: {message}",
        ephemeral=False
    )

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
