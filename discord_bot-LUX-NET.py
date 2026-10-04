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
        col = "channel_id" if table == "text_relays" else ("thread_id" if table == "thread_relays" else "vc_id")
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
        col = "channel_id" if table == "text_relays" else ("thread_id" if table == "thread_relays" else "vc_id")
        supabase.table(table).delete().eq("network_code", code).eq(col, item_id).execute()
    except Exception as e:
        logger.error(f"Failed to remove link from Supabase: {e}")

def get_links(table: str, code: str):
    try:
        col = "channel_id" if table == "text_relays" else ("thread_id" if table == "thread_relays" else "vc_id")
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
# BOT EVENTS (TEXT, THREADS, & VOICE STATE BRIDGE)
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
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    # Check Code-based VC bridges or Direct ID VC bridges when someone joins a VC
    if after.channel:
        joined_vc_id = after.channel.id
        
        codes_to_check = []
        try:
            # 1. Check code-based VC table
            res = supabase.table("vc_code_relays").select("network_code").eq("vc_id", joined_vc_id).execute()
            codes_to_check.extend([row["network_code"] for row in res.data])

            # 2. Check direct bidirectional VC pairs table
            direct_res = supabase.table("vc_direct_relays").select("vc_b").eq("vc_a", joined_vc_id).execute()
            codes_to_check.extend([row["vc_b"] for row in direct_res.data])
            direct_res_rev = supabase.table("vc_direct_relays").select("vc_a").eq("vc_b", joined_vc_id).execute()
            codes_to_check.extend([row["vc_a"] for row in direct_res_rev.data])
        except Exception as e:
            logger.error(f"Error checking VC bridge database: {e}")


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
# TEXT, THREAD, & VOICE BRIDGE SLASH COMMANDS
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

# ------------------------------------------------------------------------------
# NEW VOICE CHANNEL BRIDGING COMMANDS (CODE-BASED & DIRECT ID)
# ------------------------------------------------------------------------------
@bot.tree.command(name="link-vc-code", description="Link your current voice channel to a multi-VC bridge network code.")
@app_commands.describe(network_code="Shared code name to bridge multiple voice channels together")
async def link_vc_code(interaction: discord.Interaction, network_code: str):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("❌ You must be connected to a voice channel to use this command.", ephemeral=True)
        return

    vc = interaction.user.voice.channel
    code = network_code.strip().lower()

    try:
        add_link("vc_code_relays", code, vc.id)
        linked_vcs = get_links("vc_code_relays", code)
        await interaction.response.send_message(
            f"🔊 Successfully bridged voice channel **{vc.name}** to multi-VC network `{code}`! ({len(linked_vcs)} connected VCs)",
            ephemeral=False
        )
    except Exception as e:
        await interaction.response.send_message(f"❌ Error linking voice channel: {e}", ephemeral=True)

@bot.tree.command(name="unlink-vc-code", description="Unlink this voice channel from its active multi-VC network code.")
async def unlink_vc_code(interaction: discord.Interaction):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("❌ You must be in a voice channel to run this command.", ephemeral=True)
        return

    vc = interaction.user.voice.channel
    try:
        res = supabase.table("vc_code_relays").select("network_code").eq("vc_id", vc.id).execute()
        codes = [row["network_code"] for row in res.data]

        if codes:
            for code in codes:
                remove_link("vc_code_relays", code, vc.id)
            await interaction.response.send_message(f"🔌 Disconnected voice channel **{vc.name}** from multi-VC network.", ephemeral=False)
        else:
            await interaction.response.send_message("⚠️ This voice channel is not linked to any code-based VC network.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking voice channel: {e}", ephemeral=True)

@bot.tree.command(name="link-vc-direct", description="Directly bridge your current voice channel to another specific Voice Channel ID.")
@app_commands.describe(target_vc_id="The exact Discord Voice Channel ID of the other server/channel to connect with")
async def link_vc_direct(interaction: discord.Interaction, target_vc_id: str):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("❌ You must be connected to a voice channel to use this command.", ephemeral=True)
        return

    current_vc = interaction.user.voice.channel
    try:
        target_id = int(target_vc_id.strip())
    except ValueError:
        await interaction.response.send_message("❌ Invalid target Voice Channel ID format. Must be numeric numbers.", ephemeral=True)
        return

    try:
        # Check if direct link entry already exists bidirectionally
        existing = (
            supabase.table("vc_direct_relays")
            .select("*")
            .or_(f"and(vc_a.eq.{current_vc.id},vc_b.eq.{target_id}),and(vc_a.eq.{target_id},vc_b.eq.{current_vc.id})")
            .execute()
        )

        if not existing.data:
            supabase.table("vc_direct_relays").insert({"vc_a": current_vc.id, "vc_b": target_id}).execute()

        await interaction.response.send_message(
            f"🔗 Successfully established a direct bridge between **{current_vc.name}** and Target VC ID `{target_id}`!",
            ephemeral=False
        )
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed to create direct VC link: {e}", ephemeral=True)

@bot.tree.command(name="unlink-vc-direct", description="Remove a direct ID bridge from your voice channel.")
@app_commands.describe(target_vc_id="The exact target Voice Channel ID to disconnect from")
async def unlink_vc_direct(interaction: discord.Interaction, target_vc_id: str):
    if not interaction.user.voice or not interaction.user.voice.channel:
        await interaction.response.send_message("❌ You must be connected to a voice channel to use this command.", ephemeral=True)
        return

    current_vc = interaction.user.voice.channel
    try:
        target_id = int(target_vc_id.strip())
    except ValueError:
        await interaction.response.send_message("❌ Invalid target Voice Channel ID format.", ephemeral=True)
        return

    try:
        supabase.table("vc_direct_relays").delete().or_(
            f"and(vc_a.eq.{current_vc.id},vc_b.eq.{target_id}),and(vc_a.eq.{target_id},vc_b.eq.{current_vc.id})"
        ).execute()

        await interaction.response.send_message(f"🔌 Removed direct bridge between your voice channel and ID `{target_id}`.", ephemeral=False)
    except Exception as e:
        await interaction.response.send_message(f"❌ Error unlinking direct VC bridge: {e}", ephemeral=True)

# ------------------------------------------------------------------------------
# INTERACTIVE YOUTUBE & SEARCH COMMANDS
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

@bot.tree.command(name="play", description="Search YouTube and select the exact video from a dropdown list.")
@app_commands.describe(search="Search keywords for the video")
async def play(interaction: discord.Interaction, search: str):
    await interaction.response.defer(ephemeral=True)

    try:
        search_opts = {
            'extract_flat': True,
            'default_search': 'ytsearch5',
            'quiet': True,
        }
        
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(search_opts).extract_info(search, download=False))
        
        entries = data.get('entries', [])
        if not entries:
            await interaction.followup.send(f"⚠️ No YouTube videos found for `{search}`.", ephemeral=True)
            return

        view = YouTubeSelectView(entries)
        await interaction.followup.send("🔍 **Select the correct video below:**", view=view, ephemeral=True)

    except Exception as e:
        logger.error(f"Search/Selection error: {e}")
        await interaction.followup.send(f"❌ An error occurred while searching YouTube: {e}", ephemeral=True)

@bot.tree.command(name="search", description="Perform a fast, reliable web search via DuckDuckGo.")
@app_commands.describe(query="What would you like to search for?")
async def search(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    try:
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=5, backend="lite"):
                results.append(r)

        if not results:
            await interaction.followup.send(f"⚠️ No results found for `{query}`. Try using simpler keywords.")
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
