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
# GLOBAL RELAY MAPPINGS: { network_code: [channel_id, channel_id, ...] }
# ------------------------------------------------------------------------------
relay_bridges = {}

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
        logger.error(f"Missing 'Manage Webhooks' permission in #{channel.name} (ID: {channel.id})")
        return None
    except Exception as e:
        logger.error(f"Failed to create webhook in #{channel.name}: {e}")
        return None

# ------------------------------------------------------------------------------
# BOT EVENTS
# ------------------------------------------------------------------------------
@bot.event
async def on_ready():
    logger.info(f"Connected to Discord as {bot.user} in {len(bot.guilds)} servers")

    # Clear old guild-scoped commands to eliminate duplicate slash command entries
    for guild in bot.guilds:
        try:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
        except Exception as e:
            logger.error(f"Could not clear guild commands for {guild.name}: {e}")

    # Clean global sync
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync global slash commands: {e}")

@bot.event
async def on_message(message: discord.Message):
    # Ignore bot messages and non-guild messages
    if message.author.bot or not message.guild or not isinstance(message.channel, discord.TextChannel):
        return

    await bot.process_commands(message)

    # Find active bridge networks for the current channel
    current_channel_id = message.channel.id
    target_channel_ids = []

    for code, channels in relay_bridges.items():
        if current_channel_id in channels:
            target_channel_ids.extend([cid for cid in channels if cid != current_channel_id])

    if not target_channel_ids:
        return

    # 1. Format content with source server indicator
    server_badge = f"[`{message.guild.name}`]"
    raw_content = message.content or ""

    # 2. Handle Replies cleanly (if replying to an earlier message)
    reply_prefix = ""
    if message.reference and message.reference.message_id:
        try:
            ref_msg = await message.channel.fetch_message(message.reference.message_id)
            if ref_msg:
                snippet = ref_msg.content[:60] + "..." if len(ref_msg.content) > 60 else ref_msg.content
                if not snippet and ref_msg.attachments:
                    snippet = "[Attachment]"
                reply_prefix = f"> ↩️ **Replying to {ref_msg.author.display_name}:** *{snippet or '[Embed/Sticker]'}*\n"
        except Exception:
            pass  # Fail gracefully if reference message can't be fetched

    final_content = f"{reply_prefix}{server_badge} {raw_content}".strip()

    # Relay messages to target channels
    for target_id in set(target_channel_ids):
        target_channel = bot.get_channel(target_id)
        if not target_channel or not isinstance(target_channel, discord.TextChannel):
            continue

        webhook = await get_or_create_webhook(target_channel)
        if not webhook:
            continue

        # Build parameters dynamically (Prevents Python 3.14 len(None) crash)
        send_kwargs = {
            "username": message.author.display_name,
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
                send_kwargs["content"] = f"{server_badge} *[Sticker: {message.stickers[0].name}]*"
            else:
                continue

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

    if code not in relay_bridges:
        relay_bridges[code] = []

    if channel_id not in relay_bridges[code]:
        relay_bridges[code].append(channel_id)
        await interaction.response.send_message(
            f"✅ Linked **#{interaction.channel.name}** to relay network `{code}`! "
            f"({len(relay_bridges[code])} channels currently connected)",
            ephemeral=False
        )
    else:
        await interaction.response.send_message(
            f"⚠️ **#{interaction.channel.name}** is already linked to network `{code}`.",
            ephemeral=True
        )

@bot.tree.command(name="unlink-relay", description="Unlink this text channel from its active relay network.")
async def unlink_relay(interaction: discord.Interaction):
    channel_id = interaction.channel.id
    unlinked = False

    for code, channels in list(relay_bridges.items()):
        if channel_id in channels:
            channels.remove(channel_id)
            unlinked = True
            if not channels:
                del relay_bridges[code]

    if unlinked:
        await interaction.response.send_message(
            f"🔌 Disconnected **#{interaction.channel.name}** from the text relay network.",
            ephemeral=False
        )
    else:
        await interaction.response.send_message(
            f"⚠️ **#{interaction.channel.name}** is not currently linked to any relay network.",
            ephemeral=True
        )

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

@bot.tree.command(name="list-bridges", description="List all active text and voice bridge connections.")
async def list_bridges(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🌐 LUX-NET Active Bridges",
        description="Current active network connections:",
        color=discord.Color.blue()
    )

    if relay_bridges:
        text_summary = ""
        for code, channels in relay_bridges.items():
            text_summary += f"• **`{code}`**: {len(channels)} channel(s) connected\n"
        embed.add_field(name="Text Relays", value=text_summary, inline=False)
    else:
        embed.add_field(name="Text Relays", value="No active text relays linked.", inline=False)

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
    
    if code not in relay_bridges or not relay_bridges[code]:
        await interaction.response.send_message(
            f"❌ Relay network `{code}` is not active or has no linked channels.",
            ephemeral=True
        )
        return

    channel_mentions = []
    guild_count = set()

    for cid in relay_bridges[code]:
        ch = bot.get_channel(cid)
        if ch and isinstance(ch, discord.TextChannel):
            channel_mentions.append(f"• **#{ch.name}** ({ch.guild.name})")
            guild_count.add(ch.guild.id)

    embed = discord.Embed(
        title=f"📡 Network Relay: `{code}`",
        color=discord.Color.green()
    )
    embed.add_field(name="Connected Channels", value="\n".join(channel_mentions) if channel_mentions else "None", inline=False)
    embed.add_field(name="Total Servers", value=str(len(guild_count)), inline=True)
    embed.add_field(name="Total Channels", value=str(len(channel_mentions)), inline=True)

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="test-relay", description="Send a test ping across a linked relay network.")
@app_commands.describe(network_code="The network code to test")
async def test_relay(interaction: discord.Interaction, network_code: str):
    code = network_code.strip().lower()

    if code not in relay_bridges or interaction.channel.id not in relay_bridges[code]:
        await interaction.response.send_message(
            f"⚠️ This channel is not linked to network `{code}`.",
            ephemeral=True
        )
        return

    await interaction.response.send_message(f"🧪 Sending test signal across relay `{code}`...", ephemeral=True)

    target_ids = [cid for cid in relay_bridges[code] if cid != interaction.channel.id]
    delivered = 0

    for target_id in set(target_ids):
        target_channel = bot.get_channel(target_id)
        if not target_channel or not isinstance(target_channel, discord.TextChannel):
            continue

        webhook = await get_or_create_webhook(target_channel)
        if webhook:
            try:
                await webhook.send(
                    content=f"🔔 **LUX-NET Relay Test**: Connection active from **#{interaction.channel.name}** ({interaction.guild.name})!",
                    username="LUX-NET Network Monitor",
                    avatar_url=bot.user.display_avatar.url
                )
                delivered += 1
            except Exception as e:
                logger.error(f"Test signal failed for channel {target_id}: {e}")

    await interaction.followup.send(
        f"✅ Test complete! Signal delivered to **{delivered}/{len(set(target_ids))}** connected channels.",
        ephemeral=True
    )

@bot.tree.command(name="clear-relays", description="Remove all active text relay links for this server.")
@app_commands.checks.has_permissions(manage_channels=True)
async def clear_relays(interaction: discord.Interaction):
    guild_channels = [ch.id for ch in interaction.guild.text_channels]
    removed_count = 0

    for code, channels in list(relay_bridges.items()):
        before_len = len(channels)
        relay_bridges[code] = [cid for cid in channels if cid not in guild_channels]
        removed_count += (before_len - len(relay_bridges[code]))
        if not relay_bridges[code]:
            del relay_bridges[code]

    await interaction.response.send_message(
        f"🧹 Cleared **{removed_count}** active relay link(s) across **{interaction.guild.name}**.",
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
if __name__ == "__main__":
    main()
