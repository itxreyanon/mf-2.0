import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict
import aiohttp
import html
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, CallbackQuery
from collections import defaultdict
from aiogram.exceptions import TelegramBadRequest

# Import the now ASYNC custom db modules
from db import (
    set_token, get_tokens, set_current_account, get_current_account, delete_token,
    set_user_filters, get_user_filters, set_spam_filter, get_spam_filter,
    is_already_sent, add_sent_id, toggle_token_status, get_active_tokens,
    get_token_status, set_account_active, get_info_card,
    set_individual_spam_filter, get_individual_spam_filter, get_all_spam_filters,
    list_all_collections, get_collection_summary, connect_to_collection,
    rename_user_collection, transfer_to_user, get_current_collection_info, 
    has_valid_access as db_has_valid_access
)
from lounge import send_lounge, send_lounge_all_tokens
from chatroom import send_message_to_everyone, send_message_to_everyone_all_tokens
from unsubscribe import unsubscribe_everyone
from filters import meeff_filter_command, set_account_filter, get_meeff_filter_main_keyboard, set_filter
from allcountry import run_all_countries
from signup import signup_command, signup_callback_handler, signup_message_handler, signup_settings_command
from friend_requests import run_requests, process_all_tokens, user_states, stop_markup
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# Configuration constants
API_TOKEN = "7916536914:AAHwtvO8hfGl2U4xcfM1fAjMLNypPFEW5JQ"
ADMIN_USER_IDS = {7405203657, 7725409374, 7691399254, 7795345443}
TEMP_PASSWORD = "11223344"
TARGET_CHANNEL_ID = -1002610862940

# Global state
password_access: Dict[int, datetime] = {}
db_operation_states: Dict[int, Dict[str, str]] = defaultdict(dict)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
router = Router()
dp = Dispatcher()

# --- Utility Functions ---
def is_admin(user_id: int) -> bool:
    """Check if the user is an admin."""
    return user_id in ADMIN_USER_IDS

async def has_valid_access(user_id: int) -> bool:
    """Verify if the user has valid access (admin or temporary password)."""
    if is_admin(user_id):
        return True
    # Check for temporary password access
    if user_id in password_access and password_access[user_id] > datetime.now():
        return True
    # Check for persistent access via an existing database collection
    return await db_has_valid_access(user_id)

# --- Inline Keyboard Menus ---
async def get_settings_menu(user_id: int) -> InlineKeyboardMarkup:
    """Generate the settings menu. Must be async to fetch spam filter status."""
    spam_filters = await get_all_spam_filters(user_id)
    any_spam_on = any(spam_filters.values())
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Manage Accounts", callback_data="manage_accounts"),
            InlineKeyboardButton(text="Meeff Filters", callback_data="show_filters")
        ],
        [InlineKeyboardButton(text=f"Spam Filters: {'ON' if any_spam_on else 'OFF'}", callback_data="spam_filter_menu")],
        [InlineKeyboardButton(text="DB Settings", callback_data="db_settings")],
        [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
    ])

def get_db_settings_menu() -> InlineKeyboardMarkup:
    """Generate the database settings menu."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Connect DB", callback_data="db_connect"),
            InlineKeyboardButton(text="Rename DB", callback_data="db_rename")
        ],
        [
            InlineKeyboardButton(text="View DB", callback_data="db_view"),
            InlineKeyboardButton(text="Transfer DB", callback_data="db_transfer")
        ],
        [InlineKeyboardButton(text="Back", callback_data="settings_menu")]
    ])

def get_unsubscribe_menu() -> InlineKeyboardMarkup:
    """Generate the unsubscribe options menu."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Unsubscribe Current", callback_data="unsub_current"),
            InlineKeyboardButton(text="Unsubscribe All", callback_data="unsub_all")
        ],
        [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
    ])

async def get_spam_filter_menu(user_id: int) -> InlineKeyboardMarkup:
    """Generate the spam filter settings menu. Must be async."""
    spam_filters = await get_all_spam_filters(user_id)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Chatroom: {'ON' if spam_filters['chatroom'] else 'OFF'}", callback_data="toggle_spam_chatroom")],
        [InlineKeyboardButton(text=f"Requests: {'ON' if spam_filters['request'] else 'OFF'}", callback_data="toggle_spam_request")],
        [InlineKeyboardButton(text=f"Lounge: {'ON' if spam_filters['lounge'] else 'OFF'}", callback_data="toggle_spam_lounge")],
        [
            InlineKeyboardButton(text="Toggle All", callback_data="toggle_spam_all"),
            InlineKeyboardButton(text="Back", callback_data="settings_menu")
        ]
    ])

def get_account_view_menu(account_idx: int) -> InlineKeyboardMarkup:
    """Generate the account view menu."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Delete Account", callback_data=f"confirm_delete_{account_idx}"),
            InlineKeyboardButton(text="Back", callback_data="manage_accounts")
        ]
    ])

def get_confirmation_menu(action_type: str) -> InlineKeyboardMarkup:
    """Generate a confirmation menu for actions."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Yes", callback_data=f"confirm_{action_type}"),
            InlineKeyboardButton(text="Cancel", callback_data="back_to_menu")
        ]
    ])

# --- Predefined Keyboards ---
start_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Send Request", callback_data="send_request_menu"), InlineKeyboardButton(text="All Countries", callback_data="all_countries")]])
send_request_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Start Request", callback_data="start"), InlineKeyboardButton(text="Request All", callback_data="start_all")], [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]])
back_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back", callback_data="back_to_menu")]])

# --- Command Handlers ---
@router.message(Command("password"))
async def password_command(message: Message) -> None:
    """Handle the /password command to grant temporary access."""
    user_id = message.chat.id
    try:
        provided_password = message.text.split(maxsplit=1)[1]
        if provided_password == TEMP_PASSWORD:
            password_access[user_id] = datetime.now() + timedelta(hours=1)
            await message.reply("Access granted for one hour.")
        else:
            await message.reply("Incorrect password.")
    except IndexError:
        await message.reply("Usage: /password <password>")
    finally:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            logger.error(f"Failed to delete password message: {e}")

@router.message(Command("start"))
async def start_command(message: Message) -> None:
    """Handle the /start command to display the bot dashboard."""
    user_id = message.chat.id
    if not await has_valid_access(user_id):
        return await message.reply("You are not authorized to use this bot. Use /password to get access.")
    
    state = user_states.setdefault(user_id, {})
    status = await message.reply(
        "<b>Meeff Bot Dashboard</b>\n\nChoose an option below to get started:",
        reply_markup=start_markup, parse_mode="HTML"
    )
    state["status_message_id"] = status.message_id
    state["pinned_message_id"] = None

@router.message(Command("signup"))
async def signup_cmd(message: Message) -> None:
    if not await has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await signup_command(message)

@router.message(Command("signup_settings"))
async def signup_settings_cmd(message: Message) -> None:
    if not await has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await signup_settings_command(message)

@router.message(Command("signin"))
async def signin_cmd(message: Message) -> None:
    if not await has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    from signup import user_signup_states, BACK_TO_SIGNUP
    user_signup_states[message.from_user.id] = {"stage": "signin_email"}
    await message.reply("<b>Sign In</b>\n\nPlease enter your email address:", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")

@router.message(Command("skip"))
async def skip_command(message: Message) -> None:
    if not await has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await message.reply("<b>Unsubscribe Options</b>\nChoose accounts to unsubscribe:", reply_markup=get_unsubscribe_menu(), parse_mode="HTML")

@router.message(Command("send_lounge_all"))
async def send_lounge_all(message: Message) -> None:
    user_id = message.chat.id
    if not await has_valid_access(user_id): return await message.reply("You are not authorized.")
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: return await message.reply("<b>Usage:</b> <code>/send_lounge_all &lt;message&gt;</code>", parse_mode="HTML")
    
    custom_message = parts[1]
    active_tokens_data = await get_active_tokens(user_id)
    if not active_tokens_data: return await message.reply("No active tokens found.")

    spam_enabled = await get_individual_spam_filter(user_id, "lounge")
    status = await message.reply(f"<b>Starting Lounge AIO</b>\nTokens: {len(active_tokens_data)}\nSpam Filter: {'ON' if spam_enabled else 'OFF'}", parse_mode="HTML")

    try:
        await send_lounge_all_tokens(active_tokens_data, custom_message, status, bot, user_id, spam_enabled, user_id)
    except Exception as e:
        await status.edit_text(f"An error occurred: {e}")
        logger.error(f"Error in /send_lounge_all", exc_info=True)

@router.message(Command("lounge"))
async def lounge_command(message: Message) -> None:
    user_id = message.chat.id
    if not await has_valid_access(user_id): return await message.reply("You are not authorized.")

    token = await get_current_account(user_id)
    if not token: return await message.reply("No active account selected.")
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: return await message.reply("<b>Usage:</b> <code>/lounge &lt;message&gt;</code>", parse_mode="HTML")
    
    custom_message = parts[1]
    spam_enabled = await get_individual_spam_filter(user_id, "lounge")
    status_message = await message.reply(f"<b>Starting Lounge Messaging...</b>\nSpam Filter: {'ON' if spam_enabled else 'OFF'}", parse_mode="HTML")

    try:
        await send_lounge(token, custom_message, status_message, bot, user_id, spam_enabled, user_id)
    except Exception as e:
        await status_message.edit_text(f"An error occurred: {e}")
        logger.error(f"Error in /lounge", exc_info=True)

@router.message(Command("chatroom"))
async def send_to_all_command(message: Message) -> None:
    user_id = message.chat.id
    if not await has_valid_access(user_id): return await message.reply("You are not authorized.")

    token = await get_current_account(user_id)
    if not token: return await message.reply("No active account selected.")

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: return await message.reply("<b>Usage:</b> <code>/chatroom &lt;message&gt;</code>", parse_mode="HTML")

    custom_message = parts[1]
    spam_enabled = await get_individual_spam_filter(user_id, "chatroom")
    status_message = await message.reply(f"<b>Starting Chatroom Messaging...</b>\nSpam Filter: {'ON' if spam_enabled else 'OFF'}", parse_mode="HTML")

    try:
        total, sent, filtered = await send_message_to_everyone(token, custom_message, user_id, spam_enabled, user_id)
        await status_message.edit_text(
            f"<b>Chatroom Complete</b>\n- Total: <code>{total}</code>\n- Sent: <code>{sent}</code>\n- Filtered: <code>{filtered}</code>",
            parse_mode="HTML")
    except Exception as e:
        await status_message.edit_text(f"An error occurred: {e}")
        logger.error(f"Error in /chatroom", exc_info=True)

@router.message(Command("send_chat_all"))
async def send_chat_all(message: Message) -> None:
    user_id = message.chat.id
    if not await has_valid_access(user_id): return await message.reply("You are not authorized.")
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: return await message.reply("<b>Usage:</b> <code>/send_chat_all &lt;message&gt;</code>", parse_mode="HTML")
    
    custom_message = parts[1]
    active_tokens = await get_active_tokens(user_id)
    if not active_tokens: return await message.reply("No active tokens found.")

    tokens = [t["token"] for t in active_tokens]
    token_names = {t["token"]: t.get("name", "N/A") for t in active_tokens}
    spam_enabled = await get_individual_spam_filter(user_id, "chatroom")
    status = await message.reply(f"<b>Starting Chatroom AIO</b>\nTokens: {len(tokens)}\nSpam Filter: {'ON' if spam_enabled else 'OFF'}", parse_mode="HTML")

    try:
        await send_message_to_everyone_all_tokens(tokens, custom_message, status, bot, user_id, spam_enabled, token_names, False, user_id)
    except Exception as e:
        await status.edit_text(f"An error occurred: {e}")
        logger.error(f"Error in /send_chat_all", exc_info=True)

@router.message(Command("invoke"))
async def invoke_command(message: Message) -> None:
    user_id = message.chat.id
    if not await has_valid_access(user_id): return await message.reply("You are not authorized.")
    
    tokens = await get_tokens(user_id)
    if not tokens: return await message.reply("No tokens found to check.")

    status_msg = await message.reply(f"<b>Checking {len(tokens)} Accounts...</b>", parse_mode="HTML")
    disabled_accounts, working_accounts = [], []
    url = "https://api.meeff.com/facetalk/vibemeet/history/count/v1"

    async with aiohttp.ClientSession() as session:
        tasks = []
        for token_obj in tokens:
            async def check(t_obj):
                headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'meeff-access-token': t_obj["token"]}
                try:
                    async with session.get(url, params={'locale': "en"}, headers=headers, timeout=10) as resp:
                        result = await resp.json(content_type=None)
                        return "disabled" if result.get("errorCode") == "AuthRequired" else "working"
                except Exception as e:
                    logger.error(f"Error checking token {t_obj.get('name')}: {e}")
                    return "disabled"
            tasks.append(check(token_obj))
        
        results = await asyncio.gather(*tasks)
        for token_obj, status in zip(tokens, results):
            (disabled_accounts if status == "disabled" else working_accounts).append(token_obj)

    if disabled_accounts:
        await asyncio.gather(*(delete_token(user_id, t["token"]) for t in disabled_accounts))
        removed_names = "\n".join([f"- <code>{html.escape(acc['name'])}</code>" for acc in disabled_accounts])
        await status_msg.edit_text(
            f"<b>Account Cleanup Complete</b>\n- Working: <code>{len(working_accounts)}</code>\n- Removed: <code>{len(disabled_accounts)}</code>\n\n<b>Removed:</b>\n{removed_names}",
            parse_mode="HTML")
    else:
        await status_msg.edit_text(f"<b>All {len(working_accounts)} accounts are working.</b>", parse_mode="HTML")

@router.message(Command("settings"))
async def settings_command(message: Message) -> None:
    if not await has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await message.reply("<b>Settings Menu</b>\n\nChoose an option:", reply_markup=await get_settings_menu(message.chat.id), parse_mode="HTML")

@router.message(Command("add"))
async def add_person_command(message: Message) -> None:
    user_id = message.chat.id
    if not await has_valid_access(user_id): return await message.reply("You are not authorized.")

    args = message.text.strip().split()
    if len(args) < 2: return await message.reply("Usage: /add <person_id>")

    token = await get_current_account(user_id)
    if not token: return await message.reply("No active account selected.")

    person_id = args[1]
    url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={person_id}&isOkay=1"
    
    device_info = get_or_create_device_info_for_token(user_id, token)
    headers = get_headers_with_device_info({"meeff-access-token": token}, device_info)
    
    try:
        async with aiohttp.ClientSession() as session, session.get(url, headers=headers) as response:
            data = await response.json()
            if data.get("errorCode") == "LikeExceeded": await message.reply("Daily like limit reached.")
            elif data.get("errorCode"): await message.reply(f"Failed: {data.get('errorMessage', 'Unknown error')}")
            else: await message.reply(f"Successfully sent request to ID: {person_id}")
    except Exception as e:
        logger.error(f"Error adding person by ID: {e}")
        await message.reply("An error occurred.")

# --- Generic Message Handler ---
@router.message()
async def handle_message(message: Message) -> None:
    """Handle incoming messages for token addition or database operations."""
    if not message.text or message.text.startswith("/") or message.from_user.is_bot: return
    user_id = message.from_user.id

    if await signup_message_handler(message): return

    state = db_operation_states.get(user_id)
    if state:
        operation = state.get("operation")
        text = message.text.strip()
        msg_to_edit = await message.reply("Processing...")
        success, msg = False, "Invalid operation"

        if operation == "connect_db":
            collection_name = f"user_{text}" if not text.startswith("user_") else text
            success, msg = await connect_to_collection(collection_name, user_id)
        elif operation == "rename_db":
            success, msg = await rename_user_collection(user_id, text)
        elif operation == "transfer_db":
            try: success, msg = await transfer_to_user(user_id, int(text))
            except ValueError: msg = "Invalid user ID."

        await msg_to_edit.edit_text(f"<b>Result:</b> {msg}", parse_mode="HTML")
        db_operation_states.pop(user_id, None)
        return

    if not await has_valid_access(user_id): return await message.reply("You are not authorized.")

    token_data = message.text.strip().split(" ", 1)
    token = token_data[0]
    if len(token) < 100: return await message.reply("Invalid token format.")

    verification_msg = await message.reply("<b>Verifying Token...</b>", parse_mode="HTML")
    url = "https://api.meeff.com/facetalk/vibemeet/history/count/v1"
    
    device_info = get_or_create_device_info_for_token(user_id, token)
    headers = get_headers_with_device_info({'User-Agent': "okhttp/5.0.0-alpha.14", 'meeff-access-token': token}, device_info)

    try:
        async with aiohttp.ClientSession() as session, session.get(url, params={'locale': "en"}, headers=headers) as resp:
            result = await resp.json(content_type=None)
            if result.get("errorCode") == "AuthRequired":
                return await verification_msg.edit_text("<b>Token is invalid or expired.</b>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error verifying token: {e}")
        return await verification_msg.edit_text("<b>Token verification failed.</b>", parse_mode="HTML")

    current_tokens = await get_tokens(user_id)
    account_name = token_data[1] if len(token_data) > 1 else f"Account {len(current_tokens) + 1}"
    await set_token(user_id, token, account_name)
    
    await verification_msg.edit_text(f"<b>Token saved as '<code>{html.escape(account_name)}</code>'.</b>", parse_mode="HTML")

async def show_manage_accounts_menu(callback_query: CallbackQuery) -> None:
    """Display the manage accounts menu. Now fully async."""
    user_id = callback_query.from_user.id
    tokens = await get_tokens(user_id)
    current_token = await get_current_account(user_id)

    if not tokens:
        return await callback_query.message.edit_text("<b>No Accounts Found</b>\nSend a token to add one.", reply_markup=back_markup, parse_mode="HTML")

    buttons = []
    for i, tok in enumerate(tokens):
        is_active = tok.get("active", True)
        is_current = "🔹" if tok['token'] == current_token else "▫️"
        account_name_display = f"{is_current} {html.escape(tok['name'][:15])}"
        buttons.append([
            InlineKeyboardButton(text=account_name_display, callback_data=f"set_account_{i}"),
            InlineKeyboardButton(text="ON" if is_active else "OFF", callback_data=f"toggle_status_{i}"),
            InlineKeyboardButton(text="View", callback_data=f"view_account_{i}")
        ])
    buttons.append([InlineKeyboardButton(text="Back", callback_data="settings_menu")])

    message_text = f"<b>Manage Accounts</b>\n\nSelect an account to make it current."
    try:
        await callback_query.message.edit_text(text=message_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): await callback_query.answer()
        else: logger.error(f"Error in show_manage_accounts_menu: {e}")

# --- Callback Query Handler ---
@router.callback_query()
async def callback_handler(callback_query: CallbackQuery) -> None:
    """Handle all callback queries from inline keyboards."""
    user_id = callback_query.from_user.id
    data = callback_query.data

    if await signup_callback_handler(callback_query): return
    if not await has_valid_access(user_id): return await callback_query.answer("You are not authorized.")

    state = user_states.setdefault(user_id, {})
    
    # --- DB Settings ---
    if data == "db_settings":
        current_info = await get_current_collection_info(user_id)
        info_text = "<b>Database Settings</b>\n"
        if current_info["exists"]:
            summary = current_info["summary"]
            info_text += f"DB Name: <code>{html.escape(current_info['collection_name'])}</code>\nAccounts: <code>{summary.get('tokens_count', 0)}</code>"
        else: info_text += "No database found for your account."
        await callback_query.message.edit_text(info_text, reply_markup=get_db_settings_menu(), parse_mode="HTML")
    elif data == "db_connect":
        db_operation_states[user_id] = {"operation": "connect_db"}
        await callback_query.message.edit_text("<b>Connect DB:</b> Enter the source user ID to connect to:", parse_mode="HTML")
    elif data == "db_rename":
        db_operation_states[user_id] = {"operation": "rename_db"}
        await callback_query.message.edit_text("<b>Rename DB:</b> Enter the new name (without 'user_'):", parse_mode="HTML")
    elif data == "db_view":
        collections = await list_all_collections()
        if not collections: return await callback_query.message.edit_text("<b>No Collections Found.</b>", reply_markup=get_db_settings_menu(), parse_mode="HTML")
        view_text = "<b>All Database Collections (Top 10)</b>\n\n"
        for i, col in enumerate(collections[:10], 1):
            summary = col["summary"]
            created_str = summary.get("created_at").strftime("%Y-%m-%d") if summary.get("created_at") else "N/A"
            view_text += f"<b>{i}.</b> <code>{html.escape(col['user_id'])}</code> ({summary.get('tokens_count', 0)} accs) - <i>{created_str}</i>\n"
        await callback_query.message.edit_text(view_text, reply_markup=get_db_settings_menu(), parse_mode="HTML")
    elif data == "db_transfer":
        db_operation_states[user_id] = {"operation": "transfer_db"}
        await callback_query.message.edit_text("<b>Transfer DB:</b> Enter the target Telegram user ID:", parse_mode="HTML")

    # --- Unsubscribe ---
    elif data == "unsub_current":
        await callback_query.message.edit_text("<b>Confirm:</b> Unsubscribe current account from all chats?", reply_markup=get_confirmation_menu("unsub_current"), parse_mode="HTML")
    elif data == "unsub_all":
        count = len(await get_active_tokens(user_id))
        await callback_query.message.edit_text(f"<b>Confirm:</b> Unsubscribe all {count} active accounts?", reply_markup=get_confirmation_menu("unsub_all"), parse_mode="HTML")
    elif data == "confirm_unsub_current":
        token = await get_current_account(user_id)
        if not token: return await callback_query.message.edit_text("No active account selected.", reply_markup=back_markup, parse_mode="HTML")
        msg = await callback_query.message.edit_text("<b>Unsubscribing Current Account...</b>", parse_mode="HTML")
        await unsubscribe_everyone(token, status_message=msg, bot=bot, chat_id=user_id, user_id=user_id)
    elif data == "confirm_unsub_all":
        active_tokens = await get_active_tokens(user_id)
        if not active_tokens: return await callback_query.message.edit_text("No active accounts found.", reply_markup=back_markup, parse_mode="HTML")
        msg = await callback_query.message.edit_text(f"<b>Unsubscribing {len(active_tokens)} Accounts...</b>", parse_mode="HTML")
        # This can be slow, might be better to run them in parallel with asyncio.gather
        for i, token_obj in enumerate(active_tokens, 1):
            await msg.edit_text(f"Processing {i}/{len(active_tokens)}: {html.escape(token_obj['name'])}", parse_mode="HTML")
            await unsubscribe_everyone(token_obj["token"], user_id=user_id)
        await msg.edit_text(f"<b>Unsubscribe Complete</b>", parse_mode="HTML")

    # --- Menus & Navigation ---
    elif data == "send_request_menu":
        await callback_query.message.edit_text("<b>Send Request Options</b>", reply_markup=send_request_markup, parse_mode="HTML")
    elif data == "settings_menu":
        await callback_query.message.edit_text("<b>Settings Menu</b>", reply_markup=await get_settings_menu(user_id), parse_mode="HTML")
    elif data == "show_filters":
        await callback_query.message.edit_text("<b>Filter Settings</b>", reply_markup=await get_meeff_filter_main_keyboard(user_id), parse_mode="HTML")
    elif data == "manage_accounts":
        await show_manage_accounts_menu(callback_query)
    elif data == "back_to_menu":
        await callback_query.message.edit_text("<b>Meeff Bot Dashboard</b>", reply_markup=start_markup, parse_mode="HTML")

    # --- Filters ---
    elif data in ("toggle_request_filter", "meeff_filter_main") or data.startswith(("account_filter_", "account_gender_", "account_age_", "account_nationality_")):
        await set_account_filter(callback_query) # This function must also be async now

    # --- Account Management ---
    elif data.startswith("view_account_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            token_obj = tokens[idx]
            token = token_obj["token"]
            info_card = await get_info_card(user_id, token)
            is_current = (await get_current_account(user_id)) == token
            details = f"<b>Name:</b> <code>{html.escape(token_obj.get('name', 'N/A'))}</code>\n" \
                      f"<b>Status:</b> {'Active' if token_obj.get('active', True) else 'Inactive'}\n" \
                      f"<b>Current:</b> {'Yes' if is_current else 'No'}\n\n"
            details += f"<b>Profile Info:</b>\n{info_card}" if info_card else "No profile card found."
            await callback_query.message.edit_text(details, reply_markup=get_account_view_menu(idx), parse_mode="HTML", disable_web_page_preview=True)
    elif data.startswith("confirm_delete_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            name = tokens[idx]["name"]
            await callback_query.message.edit_text(f"<b>Confirm:</b> Delete <code>{html.escape(name)}</code>?", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Yes, Delete", callback_data=f"delete_account_{idx}"), InlineKeyboardButton(text="Cancel", callback_data="manage_accounts")]]), parse_mode="HTML")
    elif data.startswith("delete_account_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            await delete_token(user_id, tokens[idx]["token"])
            await callback_query.answer(f"Deleted {tokens[idx]['name']}")
            await show_manage_accounts_menu(callback_query)
    elif data.startswith("toggle_status_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            await toggle_token_status(user_id, tokens[idx]["token"])
            await callback_query.answer(f"Toggled status for {tokens[idx]['name']}")
            await show_manage_accounts_menu(callback_query)
    elif data.startswith("set_account_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            await set_current_account(user_id, tokens[idx]["token"])
            await callback_query.answer(f"Set {tokens[idx]['name']} as current")
            await show_manage_accounts_menu(callback_query)

    # --- Spam Filter ---
    elif data == "spam_filter_menu":
        await callback_query.message.edit_text("<b>Spam Filter Settings</b>", reply_markup=await get_spam_filter_menu(user_id), parse_mode="HTML")
    elif data.startswith("toggle_spam_"):
        filter_type = data.split("_")[-1]
        if filter_type == "all":
            current_status = any((await get_all_spam_filters(user_id)).values())
            new_status = not current_status
            await asyncio.gather(*(set_individual_spam_filter(user_id, ft, new_status) for ft in ["chatroom", "request", "lounge"]))
            await callback_query.answer(f"All filters set to {'ON' if new_status else 'OFF'}")
        elif filter_type in ["chatroom", "request", "lounge"]:
            current_status = await get_individual_spam_filter(user_id, filter_type)
            await set_individual_spam_filter(user_id, filter_type, not current_status)
            await callback_query.answer(f"{filter_type.capitalize()} filter {'disabled' if current_status else 'enabled'}")
        
        # Refresh the menu
        await callback_query.message.edit_text("<b>Spam Filter Settings</b>", reply_markup=await get_spam_filter_menu(user_id), parse_mode="HTML")

    # --- Main Operations ---
    elif data == "start":
        if state.get("running"): return await callback_query.answer("Already running!")
        state["running"] = True
        msg = await callback_query.message.edit_text("<b>Initializing Requests...</b>", reply_markup=stop_markup, parse_mode="HTML")
        state.update({"status_message_id": msg.message_id, "pinned_message_id": msg.message_id})
        await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        asyncio.create_task(run_requests(user_id, bot, TARGET_CHANNEL_ID))
        await callback_query.answer("Requests started!")
    elif data == "start_all":
        if state.get("running"): return await callback_query.answer("Already running!")
        tokens = await get_active_tokens(user_id)
        if not tokens: return await callback_query.answer("No active tokens found.", show_alert=True)
        state["running"] = True
        msg = await callback_query.message.edit_text(f"<b>Starting Multi-Account Requests ({len(tokens)})...</b>", reply_markup=stop_markup, parse_mode="HTML")
        state.update({"status_message_id": msg.message_id, "pinned_message_id": msg.message_id})
        await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        asyncio.create_task(process_all_tokens(user_id, tokens, bot, TARGET_CHANNEL_ID))
        await callback_query.answer("Multi-account requests started!")
    elif data == "stop":
        if not state.get("running"): return await callback_query.answer("Not running!")
        state["running"], state["stopped"] = False, True
        await callback_query.message.edit_text(f"<b>Requests Stopping...</b>", parse_mode="HTML")
        await callback_query.answer("Stopping process.")
        if state.get("pinned_message_id"):
            try: await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
            except Exception: pass
    elif data == "all_countries":
        if state.get("running"): return await callback_query.answer("Already running!")
        state["running"] = True
        msg = await callback_query.message.edit_text("<b>Starting All Countries...</b>", reply_markup=stop_markup, parse_mode="HTML")
        state.update({"status_message_id": msg.message_id, "pinned_message_id": msg.message_id})
        await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        asyncio.create_task(run_all_countries(user_id, state, bot, get_current_account))
        await callback_query.answer("All Countries feature started!")

async def set_bot_commands() -> None:
    """Set the bot's command menu."""
    commands = [
        BotCommand(command="start", description="Start the bot & show dashboard"),
        BotCommand(command="settings", description="Manage accounts, filters, and DB"),
        BotCommand(command="lounge", description="Send msg to lounge (current acc)"),
        BotCommand(command="send_lounge_all", description="Send lounge msg (all accs)"),
        BotCommand(command="chatroom", description="Send msg to chats (current acc)"),
        BotCommand(command="send_chat_all", description="Send msg to chats (all accs)"),
        BotCommand(command="invoke", description="Remove disabled/invalid accounts"),
        BotCommand(command="skip", description="Unsubscribe from chats"),
        BotCommand(command="add", description="Add a user by their ID"),
        BotCommand(command="signup", description="Create a new Meeff account"),
        BotCommand(command="password", description="Enter password for temporary access")
    ]
    await bot.set_my_commands(commands)

async def main() -> None:
    """Main function to start the bot."""
    try:
        await set_bot_commands()
        dp.include_router(router)
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Failed to start bot:", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
