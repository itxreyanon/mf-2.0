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

# Import custom modules from the new async db.py
from db import (
    set_token, get_tokens, set_current_account, get_current_account, delete_token,
    set_user_filters, get_user_filters, set_spam_filter, get_spam_filter,
    is_already_sent, add_sent_id, toggle_token_status, get_active_tokens,
    get_token_status, set_account_active, get_info_card,
    set_individual_spam_filter, get_individual_spam_filter, get_all_spam_filters,
    list_all_collections, get_collection_summary, connect_to_collection,
    rename_user_collection, transfer_to_user, get_current_collection_info
)
# Make sure these other local modules are compatible if they also perform I/O
from lounge import send_lounge, send_lounge_all_tokens
from chatroom import send_message_to_everyone, send_message_to_everyone_all_tokens
from unsubscribe import unsubscribe_everyone
from filters import meeff_filter_command, set_account_filter, get_meeff_filter_main_keyboard, set_filter
from allcountry import run_all_countries
from signup import signup_command, signup_callback_handler, signup_message_handler, signup_settings_command
from friend_requests import run_requests, process_all_tokens, user_states, stop_markup
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# --- Configuration & Setup ---
API_TOKEN = "7916536914:AAHwtvO8hfGl2U4xcfM1fAjMLNypPFEW5JQ"
ADMIN_USER_IDS = {7405203657, 7725409374, 7691399254, 7795345443}
TEMP_PASSWORD = "11223344"
TARGET_CHANNEL_ID = -1002610862940

password_access: Dict[int, datetime] = {}
db_operation_states: Dict[int, Dict[str, str]] = defaultdict(dict)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
router = Router()
dp = Dispatcher()

# --- Utility & Keyboards ---
def is_admin(user_id: int) -> bool: return user_id in ADMIN_USER_IDS
def has_valid_access(user_id: int) -> bool:
    if is_admin(user_id): return True
    return user_id in password_access and password_access[user_id] > datetime.now()

async def get_settings_menu(user_id: int) -> InlineKeyboardMarkup:
    spam_filters = await get_all_spam_filters(user_id)
    any_spam_on = any(spam_filters.values())
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Manage Accounts", callback_data="manage_accounts"), InlineKeyboardButton(text="Meeff Filters", callback_data="show_filters")],
        [InlineKeyboardButton(text=f"Spam Filters: {'ON' if any_spam_on else 'OFF'}", callback_data="spam_filter_menu")],
        [InlineKeyboardButton(text="DB Settings", callback_data="db_settings"), InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
    ])

def get_db_settings_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Connect DB", callback_data="db_connect"), InlineKeyboardButton(text="Rename DB", callback_data="db_rename")], [InlineKeyboardButton(text="View DB", callback_data="db_view"), InlineKeyboardButton(text="Transfer DB", callback_data="db_transfer")], [InlineKeyboardButton(text="Back", callback_data="settings_menu")]])

def get_unsubscribe_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Unsubscribe Current", callback_data="unsub_current"), InlineKeyboardButton(text="Unsubscribe All", callback_data="unsub_all")], [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]])

async def get_spam_filter_menu(user_id: int) -> InlineKeyboardMarkup:
    spam_filters = await get_all_spam_filters(user_id)
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"Chatroom: {'ON' if spam_filters['chatroom'] else 'OFF'}", callback_data="toggle_spam_chatroom")], [InlineKeyboardButton(text=f"Requests: {'ON' if spam_filters['request'] else 'OFF'}", callback_data="toggle_spam_request")], [InlineKeyboardButton(text=f"Lounge: {'ON' if spam_filters['lounge'] else 'OFF'}", callback_data="toggle_spam_lounge")], [InlineKeyboardButton(text="Toggle All", callback_data="toggle_spam_all"), InlineKeyboardButton(text="Back", callback_data="settings_menu")]])

def get_account_view_menu(account_idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Delete Account", callback_data=f"confirm_delete_{account_idx}"), InlineKeyboardButton(text="Back", callback_data="manage_accounts")]])

def get_confirmation_menu(action_type: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Yes", callback_data=f"confirm_{action_type}"), InlineKeyboardButton(text="Cancel", callback_data="back_to_menu")]])

start_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Current Request", callback_data="send_request_menu"), InlineKeyboardButton(text="Request All", callback_data="start_all")]])
send_request_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Start Request", callback_data="start"), InlineKeyboardButton(text="All Countries", callback_data="all_countries")], [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]])
back_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back", callback_data="back_to_menu")]])

# --- Command Handlers ---
@router.message(Command("password"))
async def password_command(message: Message):
    try:
        if message.text.split()[1] == TEMP_PASSWORD:
            password_access[message.chat.id] = datetime.now() + timedelta(hours=1)
            await message.reply("Access granted for one hour.")
        else:
            await message.reply("Incorrect password.")
    except IndexError: await message.reply("Usage: /password <password>")
    finally:
        try: await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e: logger.error(f"Failed to delete password message: {e}")

@router.message(Command("start"))
async def start_command(message: Message):
    if not has_valid_access(message.chat.id): return await message.reply("You are not authorized. Use /password to get access.")
    state = user_states.setdefault(message.chat.id, {})
    status = await message.reply("<b>Meeff Bot Dashboard</b>...", reply_markup=start_markup, parse_mode="HTML")
    state.update({"status_message_id": status.message_id, "pinned_message_id": None})

@router.message(Command("signup"))
async def signup_cmd(message: Message):
    if not has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await signup_command(message)

@router.message(Command("signup_settings"))
async def signup_settings_cmd(message: Message):
    if not has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await signup_settings_command(message)

@router.message(Command("signin"))
async def signin_cmd(message: Message):
    if not has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    from signup import user_signup_states, BACK_TO_SIGNUP
    user_signup_states[message.from_user.id] = {"stage": "signin_email"}
    await message.reply("<b>Sign In</b>\nPlease enter your email address:", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")

@router.message(Command("skip"))
async def skip_command(message: Message):
    if not has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await message.reply("<b>Unsubscribe Options</b>...", reply_markup=get_unsubscribe_menu(), parse_mode="HTML")

@router.message(Command("send_lounge_all"))
async def send_lounge_all(message: Message):
    user_id = message.chat.id
    if not has_valid_access(user_id): return await message.reply("You are not authorized.")
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2: return await message.reply("<b>Usage</b>\n<code>/send_lounge_all &lt;message&gt;</code>", parse_mode="HTML")
    
    custom_message = parts[1]
    active_tokens_data = await get_active_tokens(user_id)
    if not active_tokens_data: return await message.reply("No active tokens found.")

    spam_enabled = await get_individual_spam_filter(user_id, "lounge")
    status = await message.reply(f"<b>Starting Lounge Messages</b> for {len(active_tokens_data)} accounts...", parse_mode="HTML")
    # FIX: Added 'user_id' as the last argument
    await send_lounge_all_tokens(active_tokens_data, custom_message, status, bot, user_id, spam_enabled, user_id)

@router.message(Command("lounge"))
async def lounge_command(message: Message):
    user_id = message.chat.id
    if not has_valid_access(user_id): return await message.reply("You are not authorized.")
    token = await get_current_account(user_id)
    if not token: return await message.reply("No active account found.")
    
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2: return await message.reply("<b>Usage</b>\n<code>/lounge &lt;message&gt;</code>", parse_mode="HTML")
    
    custom_message = parts[1]
    spam_enabled = await get_individual_spam_filter(user_id, "lounge")
    status_message = await message.reply(f"<b>Starting Lounge Messaging...</b>", parse_mode="HTML")
    # FIX: Added 'user_id' as the last argument
    await send_lounge(token, custom_message, status_message, bot, user_id, spam_enabled, user_id)


@router.message(Command("chatroom"))
async def send_to_all_command(message: Message):
    user_id = message.chat.id
    if not has_valid_access(user_id): return await message.reply("You are not authorized.")
    token = await get_current_account(user_id)
    if not token: return await message.reply("No active account found.")

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2: return await message.reply("<b>Usage</b>\n<code>/chatroom &lt;message&gt;</code>", parse_mode="HTML")
    
    custom_message = parts[1]
    spam_enabled = await get_individual_spam_filter(user_id, "chatroom")
    status_message = await message.reply("<b>Starting Chatroom Messages...</b>", parse_mode="HTML")

    # --- FIX: Create sent_ids and lock for the single-token function call ---
    sent_ids = await is_already_sent(user_id, "chatroom", None, bulk=True) if spam_enabled else set()
    sent_ids_lock = asyncio.Lock()
    
    total, sent, filtered = await send_message_to_everyone(
        token, custom_message, user_id, spam_enabled, user_id,
        sent_ids, sent_ids_lock
    )
    await status_message.edit_text(f"<b>Complete</b>\nTotal: {total}, Sent: {sent}, Filtered: {filtered}", parse_mode="HTML")

@router.message(Command("send_chat_all"))
async def send_chat_all(message: Message):
    user_id = message.chat.id
    if not has_valid_access(user_id): return await message.reply("You are not authorized.")
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2: return await message.reply("<b>Usage</b>\n<code>/send_chat_all &lt;message&gt;</code>", parse_mode="HTML")

    custom_message = parts[1]
    active_tokens = await get_active_tokens(user_id)
    if not active_tokens: return await message.reply("No active tokens found.")

    tokens = [t["token"] for t in active_tokens]
    token_names = {t["token"]: t["name"] for t in active_tokens}
    spam_enabled = await get_individual_spam_filter(user_id, "chatroom")
    status = await message.reply(f"<b>Starting Multi-Account Chatroom ({len(tokens)})...</b>", parse_mode="HTML")
    
    # --- FIX: Changed use_in_memory_deduplication from False to True to enable spam filter ---
    await send_message_to_everyone_all_tokens(
        tokens, custom_message, status, bot, user_id, spam_enabled, token_names, True, user_id
    )

@router.message(Command("invoke"))
async def invoke_command(message: Message):
    user_id = message.chat.id
    if not has_valid_access(user_id): return await message.reply("You are not authorized.")
    tokens = await get_tokens(user_id)
    if not tokens: return await message.reply("No tokens found.")

    status_msg = await message.reply("<b>Checking Account Status...</b>", parse_mode="HTML")
    disabled_accounts, working_accounts = [], []
    async with aiohttp.ClientSession() as session:
        for token_obj in tokens:
            headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'meeff-access-token': token_obj["token"]}
            try:
                async with session.get("https://api.meeff.com/facetalk/vibemeet/history/count/v1", params={'locale': "en"}, headers=headers) as resp:
                    if (await resp.json(content_type=None)).get("errorCode") == "AuthRequired": disabled_accounts.append(token_obj)
                    else: working_accounts.append(token_obj)
            except Exception as e:
                logger.error(f"Error checking token {token_obj.get('name')}: {e}")
                disabled_accounts.append(token_obj)

    if disabled_accounts:
        for token_obj in disabled_accounts: await delete_token(user_id, token_obj["token"])
        removed_names = "\n".join([f"â€¢ {html.escape(acc['name'])}" for acc in disabled_accounts])
        await status_msg.edit_text(f"<b>Cleanup Complete</b>\nWorking: {len(working_accounts)}\nRemoved: {len(disabled_accounts)}\n\n<b>Removed accounts:</b>\n{removed_names}", parse_mode="HTML")
    else:
        await status_msg.edit_text(f"<b>All Accounts Working ({len(working_accounts)} total).</b>", parse_mode="HTML")

@router.message(Command("settings"))
async def settings_command(message: Message):
    if not has_valid_access(message.chat.id): return await message.reply("You are not authorized.")
    await message.reply("<b>Settings Menu</b>", reply_markup=await get_settings_menu(message.chat.id), parse_mode="HTML")

@router.message(Command("add"))
async def add_person_command(message: Message):
    user_id = message.chat.id
    if not has_valid_access(user_id): return await message.reply("You are not authorized.")
    args = message.text.strip().split()
    if len(args) < 2: return await message.reply("Usage: /add <person_id>")
    
    token = await get_current_account(user_id)
    if not token: return await message.reply("No active account found.")

    person_id = args[1]
    url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={person_id}&isOkay=1"
    device_info = await get_or_create_device_info_for_token(user_id, token)
    headers = get_headers_with_device_info({"meeff-access-token": token}, device_info)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                if data.get("errorCode"): await message.reply(f"Failed: {data.get('errorMessage', 'Unknown error')}")
                else: await message.reply(f"Successfully added person with ID: {person_id}")
    except Exception as e:
        logger.error(f"Error adding person by ID: {e}")
        await message.reply("An error occurred.")

@router.message()
async def handle_new_token(message: Message):
    if message.text and message.text.startswith("/"): return
    user_id = message.from_user.id
    if message.from_user.is_bot: return
    if await signup_message_handler(message): return

    state = db_operation_states.get(user_id)
    if state:
        operation, text = state.get("operation"), message.text.strip()
        msg = await message.reply("<b>Processing...</b>", parse_mode="HTML")
        success, result_msg = False, "Invalid operation."
        if operation == "connect_db":
            collection_name = f"user_{text}" if not text.startswith("user_") else text
            success, result_msg = await connect_to_collection(collection_name, user_id)
        elif operation == "rename_db":
            success, result_msg = await rename_user_collection(user_id, text)
        elif operation == "transfer_db":
            try: success, result_msg = await transfer_to_user(user_id, int(text))
            except ValueError: result_msg = "Invalid user ID."
        await msg.edit_text(f"<b>{'Success' if success else 'Failed'}</b>: {result_msg}", parse_mode="HTML")
        db_operation_states.pop(user_id, None)
        return

    if not has_valid_access(user_id): return await message.reply("You are not authorized.")

    if message.text:
        token_data = message.text.strip().split(" ", 1)
        token = token_data[0]
        if len(token) < 100: return await message.reply("Invalid token format.")

        verification_msg = await message.reply("<b>Verifying Token...</b>", parse_mode="HTML")
        device_info = await get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info({'User-Agent': "okhttp/5.0.0-alpha.14", 'meeff-access-token': token}, device_info)
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get("https://api.meeff.com/facetalk/vibemeet/history/count/v1", params={'locale': "en"}, headers=headers) as resp:
                    if (await resp.json(content_type=None)).get("errorCode") == "AuthRequired":
                        return await verification_msg.edit_text("<b>Invalid Token</b>.", parse_mode="HTML")
            except Exception as e:
                logger.error(f"Error verifying token: {e}")
                return await verification_msg.edit_text("<b>Verification Error</b>.", parse_mode="HTML")

        account_name = token_data[1] if len(token_data) > 1 else f"Account {len(await get_tokens(user_id)) + 1}"
        await set_token(user_id, token, account_name) # Fixed: Only pass 3 arguments, not 4
        await verification_msg.edit_text(f"<b>Token Verified</b> and saved as '<code>{html.escape(account_name)}</code>'.", parse_mode="HTML")

async def show_manage_accounts_menu(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    tokens = await get_tokens(user_id)
    current_token = await get_current_account(user_id)

    if not tokens:
        return await callback_query.message.edit_text("<b>No Accounts Found</b>...", reply_markup=back_markup, parse_mode="HTML")

    buttons = []
    for i, tok in enumerate(tokens):
        is_current = "🔹" if tok['token'] == current_token else "▫️"
        buttons.append([InlineKeyboardButton(text=f"{is_current} {html.escape(tok['name'][:15])}", callback_data=f"set_account_{i}"), InlineKeyboardButton(text="ON" if tok.get('active', True) else "OFF", callback_data=f"toggle_status_{i}"), InlineKeyboardButton(text="View", callback_data=f"view_account_{i}")])
    buttons.append([InlineKeyboardButton(text="Back", callback_data="settings_menu")])
    
    try:
        await callback_query.message.edit_text(f"<b>Manage Accounts</b>\nCurrently selected: {'Yes' if current_token else 'No'}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" not in e.message: logger.error(f"Error editing message: {e}")
        await callback_query.answer()

@router.callback_query()
async def callback_handler(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    if await signup_callback_handler(callback_query): return
    if not has_valid_access(user_id): return await callback_query.answer("You are not authorized.")
    
    state = user_states.setdefault(user_id, {})
    
    if data == "db_settings":
        current_info = await get_current_collection_info(user_id)
        info_text = f"<b>DB:</b> <code>{html.escape(current_info['collection_name'])}</code>\nAccounts: {current_info['summary'].get('tokens_count', 0)}" if current_info["exists"] else "No database found."
        await callback_query.message.edit_text(f"<b>Database Settings</b>\n{info_text}", reply_markup=get_db_settings_menu(), parse_mode="HTML")
    elif data in ("db_connect", "db_rename", "db_transfer"):
        prompts = {"db_connect": "Enter collection name", "db_rename": "Enter new name", "db_transfer": "Enter target Telegram user ID"}
        db_operation_states[user_id] = {"operation": data}
        await callback_query.message.edit_text(f"<b>{prompts[data]}:</b>", parse_mode="HTML")
    elif data == "db_view":
        collections = await list_all_collections()
        text = "\n\n".join([f"<b>{i}.</b> <code>{html.escape(c['collection_name'])}</code>\n  Accounts: {c['summary'].get('tokens_count', 0)}" for i, c in enumerate(collections[:10], 1)]) or "No Collections Found."
        await callback_query.message.edit_text(text, reply_markup=get_db_settings_menu(), parse_mode="HTML")
    elif data in ("unsub_current", "unsub_all"):
        confirm_text, count = ("current account", 1) if data == "unsub_current" else (f"all {len(await get_active_tokens(user_id))} active accounts", -1)
        await callback_query.message.edit_text(f"<b>Confirm:</b> Unsubscribe {confirm_text}?", reply_markup=get_confirmation_menu(data), parse_mode="HTML")
    elif data == "confirm_unsub_current":
        token = await get_current_account(user_id)
        if not token: return await callback_query.message.edit_text("No active account found.", reply_markup=back_markup, parse_mode="HTML")
        msg = await callback_query.message.edit_text("<b>Unsubscribing Current Account...</b>", parse_mode="HTML")
        await unsubscribe_everyone(token, status_message=msg, bot=bot, chat_id=user_id, user_id=user_id)
    elif data == "confirm_unsub_all":
        active_tokens = await get_active_tokens(user_id)
        if not active_tokens: return await callback_query.message.edit_text("No active accounts found.", reply_markup=back_markup, parse_mode="HTML")
        msg = await callback_query.message.edit_text(f"<b>Unsubscribing All Accounts ({len(active_tokens)})...</b>", parse_mode="HTML")
        for i, token_obj in enumerate(active_tokens, 1):
            await msg.edit_text(f"Processing account {i}/{len(active_tokens)}: {html.escape(token_obj['name'])}", parse_mode="HTML")
            await unsubscribe_everyone(token_obj["token"], user_id=user_id)
        await msg.edit_text(f"<b>Unsubscribe Complete</b>\nSuccessfully unsubscribed {len(active_tokens)} accounts.", parse_mode="HTML")
    elif data == "send_request_menu":
        await callback_query.message.edit_text("<b>Send Request Options</b>", reply_markup=send_request_markup, parse_mode="HTML")
    elif data == "settings_menu":
        await callback_query.message.edit_text("<b>Settings Menu</b>", reply_markup=await get_settings_menu(user_id), parse_mode="HTML")
    elif data == "show_filters":
        await callback_query.message.edit_text("<b>Filter Settings</b>", reply_markup=await get_meeff_filter_main_keyboard(user_id), parse_mode="HTML")
    elif data in ("toggle_request_filter", "meeff_filter_main") or data.startswith(("account_filter_", "account_gender_", "account_age_", "account_nationality_")):
        await set_account_filter(callback_query)
    elif data == "manage_accounts":
        await show_manage_accounts_menu(callback_query)
    elif data.startswith("view_account_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            token_obj = tokens[idx]
            info_card = await get_info_card(user_id, token_obj['token'])
            details = f"<b>Name:</b> <code>{html.escape(token_obj.get('name', 'N/A'))}</code>\n<b>Status:</b> {'Active' if token_obj.get('active', True) else 'Inactive'}\n\n"
            details += info_card if info_card else "No profile card found."
            await callback_query.message.edit_text(details, reply_markup=get_account_view_menu(idx), parse_mode="HTML", disable_web_page_preview=True)
    elif data.startswith("confirm_delete_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            await callback_query.message.edit_text(f"<b>Confirm Deletion</b> of <code>{html.escape(tokens[idx]['name'])}</code>?", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Yes, Delete", callback_data=f"delete_account_{idx}"), InlineKeyboardButton(text="Cancel", callback_data="manage_accounts")]]), parse_mode="HTML")
    elif data.startswith("toggle_status_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            await toggle_token_status(user_id, tokens[idx]["token"])
            await show_manage_accounts_menu(callback_query)
    elif data == "spam_filter_menu":
        await callback_query.message.edit_text("<b>Spam Filter Settings</b>", reply_markup=await get_spam_filter_menu(user_id), parse_mode="HTML")
    elif data.startswith("toggle_spam_"):
        filter_type = data.split("_")[-1]
        if filter_type == "all":
            new_status = not any((await get_all_spam_filters(user_id)).values())
            for ft in ["chatroom", "request", "lounge"]: await set_individual_spam_filter(user_id, ft, new_status)
        else:
            new_status = not await get_individual_spam_filter(user_id, filter_type)
            await set_individual_spam_filter(user_id, filter_type, new_status)
        await callback_handler(callback_query.model_copy(update={'data': 'spam_filter_menu'}))
    elif data.startswith("set_account_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            await set_current_account(user_id, tokens[idx]["token"])
            await show_manage_accounts_menu(callback_query)
    elif data.startswith("delete_account_"):
        idx = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        if 0 <= idx < len(tokens):
            await delete_token(user_id, tokens[idx]["token"])
            await callback_query.message.edit_text(f"<b>Account Deleted.</b>", reply_markup=back_markup, parse_mode="HTML")
    elif data == "back_to_menu":
        await callback_query.message.edit_text("<b>Meeff Bot Dashboard</b>", reply_markup=start_markup, parse_mode="HTML")
    elif data in ("start", "start_all", "stop", "all_countries"):
        if data in ("start", "start_all", "all_countries") and state.get("running"): return await callback_query.answer("A process is already running!")
        if data == "stop" and not state.get("running"): return await callback_query.answer("No process is running!")
        
        if data == "start":
            msg = await callback_query.message.edit_text("<b>Initializing Requests...</b>", reply_markup=stop_markup, parse_mode="HTML")
            state.update({"running": True, "status_message_id": msg.message_id, "pinned_message_id": msg.message_id})
            await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
            asyncio.create_task(run_requests(user_id, bot, TARGET_CHANNEL_ID))
        elif data == "start_all":
            tokens = await get_active_tokens(user_id)
            if not tokens: return await callback_query.answer("No active tokens found.", show_alert=True)
            msg = await callback_query.message.edit_text(f"<b>Starting Multi-Account Requests ({len(tokens)})...</b>", reply_markup=stop_markup, parse_mode="HTML")
            state.update({"running": True, "status_message_id": msg.message_id, "pinned_message_id": msg.message_id})
            await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
            asyncio.create_task(process_all_tokens(user_id, tokens, bot, TARGET_CHANNEL_ID))
        elif data == "stop":
            state.update({"running": False, "stopped": True})
            await callback_query.message.edit_text(f"<b>Requests Stopped.</b>", reply_markup=start_markup, parse_mode="HTML")
            if state.get("pinned_message_id"): await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        elif data == "all_countries":
            msg = await callback_query.message.edit_text("<b>Starting All Countries Feature...</b>", reply_markup=stop_markup, parse_mode="HTML")
            state.update({"running": True, "status_message_id": msg.message_id, "pinned_message_id": msg.message_id, "stop_markup": stop_markup})
            await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
            asyncio.create_task(run_all_countries(user_id, state, bot, get_current_account))

async def set_bot_commands():
    commands = [BotCommand(command=c, description=d) for c, d in [
        ("start", "Start the bot"), ("lounge", "Send message in the lounge"),
        ("send_lounge_all", "Send lounge message to all accounts"), ("chatroom", "Send message in chatrooms"),
        ("send_chat_all", "Send chatroom message to all accounts"), ("invoke", "Remove disabled accounts"),
        ("skip", "Unsubscribe from chats"), ("settings", "Bot settings"),
        ("add", "Add a person by ID"), ("signup", "Create a Meeff account"),
        ("password", "Enter password for access")]]
    await bot.set_my_commands(commands)

async def main():
    try:
        await set_bot_commands()
        dp.include_router(router)
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
