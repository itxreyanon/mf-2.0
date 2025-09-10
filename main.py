import asyncio
import logging
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import aiohttp
import html
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, CallbackQuery
from collections import defaultdict
from aiogram.exceptions import TelegramBadRequest
# Import custom modules
from db import (
    set_token, get_tokens, set_current_account, get_current_account, delete_token,
    set_user_filters, get_user_filters, set_spam_filter, get_spam_filter,
    is_already_sent, add_sent_id, toggle_token_status, get_active_tokens,
    get_token_status, set_account_active, get_info_card,
    set_individual_spam_filter, get_individual_spam_filter, get_all_spam_filters,
    list_all_collections, get_collection_summary, connect_to_collection,
    rename_user_collection, transfer_to_user, get_current_collection_info
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

# Performance optimizations
MAX_CONCURRENT_OPERATIONS = 50
DB_CONNECTION_POOL_SIZE = 20
HTTP_SESSION_POOL = None
THREAD_POOL = ThreadPoolExecutor(max_workers=10)

# User isolation - use WeakValueDictionary to prevent memory leaks
user_sessions: Dict[int, weakref.WeakSet] = defaultdict(lambda: weakref.WeakSet())
user_locks: Dict[int, asyncio.Lock] = {}

def get_user_lock(user_id: int) -> asyncio.Lock:
    """Get or create a lock for a specific user to prevent race conditions."""
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]

async def get_http_session() -> aiohttp.ClientSession:
    """Get a shared HTTP session with connection pooling."""
    global HTTP_SESSION_POOL
    if HTTP_SESSION_POOL is None or HTTP_SESSION_POOL.closed:
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        HTTP_SESSION_POOL = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
    return HTTP_SESSION_POOL

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

# Utility Functions
def is_admin(user_id: int) -> bool:
    """Check if the user is an admin."""
    return user_id in ADMIN_USER_IDS

def has_valid_access(user_id: int) -> bool:
    """Verify if the user has valid access (admin or temporary password)."""
    if is_admin(user_id):
        return True
    return user_id in password_access and password_access[user_id] > datetime.now()

# Inline Keyboard Menus
def get_settings_menu(user_id: int) -> InlineKeyboardMarkup:
    """Generate the settings menu with spam filter status."""
    spam_filters = get_all_spam_filters(user_id)
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

def get_spam_filter_menu(user_id: int) -> InlineKeyboardMarkup:
    """Generate the spam filter settings menu."""
    spam_filters = get_all_spam_filters(user_id)
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

# Predefined Keyboards
start_markup = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Send Request", callback_data="send_request_menu"),
        InlineKeyboardButton(text="All Countries", callback_data="all_countries")
    ]
])

send_request_markup = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Start Request", callback_data="start"),
        InlineKeyboardButton(text="Request All", callback_data="start_all")
    ],
    [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
])

back_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
])

# Command Handlers
@router.message(Command("password"))
async def password_command(message: Message) -> None:
    """Handle the /password command to grant temporary access."""
    user_id = message.chat.id
    try:
        provided_password = message.text.split()[1]
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
    if not has_valid_access(user_id):
        await message.reply("You are not authorized to use this bot. Use /password to get access.")
        return
    state = user_states.setdefault(user_id, {})
    status = await message.reply(
        "<b>Meeff Bot Dashboard</b>\n\nChoose an option below to get started:",
        reply_markup=start_markup,
        parse_mode="HTML"
    )
    state["status_message_id"] = status.message_id
    state["pinned_message_id"] = None

@router.message(Command("signup"))
async def signup_cmd(message: Message) -> None:
    """Handle the /signup command."""
    if not has_valid_access(message.chat.id):
        await message.reply("You are not authorized.")
        return
    await signup_command(message)

@router.message(Command("signup_settings"))
async def signup_settings_cmd(message: Message) -> None:
    """Handle the /signup_settings command."""
    if not has_valid_access(message.chat.id):
        await message.reply("You are not authorized.")
        return
    await signup_settings_command(message)

@router.message(Command("signin"))
async def signin_cmd(message: Message) -> None:
    """Handle the /signin command."""
    if not has_valid_access(message.chat.id):
        await message.reply("You are not authorized.")
        return
    from signup import user_signup_states, BACK_TO_SIGNUP
    user_signup_states[message.from_user.id] = {"stage": "signin_email"}
    await message.reply(
        "<b>Sign In</b>\n\nPlease enter your email address:",
        reply_markup=BACK_TO_SIGNUP,
        parse_mode="HTML"
    )

@router.message(Command("skip"))
async def skip_command(message: Message) -> None:
    """Handle the /skip command to show unsubscribe options."""
    if not has_valid_access(message.chat.id):
        await message.reply("You are not authorized.")
        return
    await message.reply(
        "<b>Unsubscribe Options</b>\n\nChoose which accounts to unsubscribe from chatrooms:",
        reply_markup=get_unsubscribe_menu(),
        parse_mode="HTML"
    )

@router.message(Command("send_lounge_all"))
async def send_lounge_all(message: Message) -> None:
    """Handle the /send_lounge_all command to send lounge messages for all accounts."""
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.reply("<b>Usage</b>\n\n<code>/send_lounge_all &lt;message&gt;</code>", parse_mode="HTML")
        return

    custom_message = parts[1]
    active_tokens_data = get_active_tokens(user_id)
    if not active_tokens_data:
        await message.reply("No active tokens found.")
        return

    spam_enabled = get_individual_spam_filter(user_id, "lounge")
    status_text = (
        f"<b>Starting Lounge Messages</b>\n\nActive tokens: {len(active_tokens_data)}\n"
        f"Message: <code>{html.escape(custom_message[:50])}...</code>\nSpam filter: {'ON' if spam_enabled else 'OFF'}"
    )
    status = await message.reply(status_text, parse_mode="HTML")

    try:
        await send_lounge_all_tokens(active_tokens_data, custom_message, status, bot, user_id, spam_enabled, user_id)
    except Exception as e:
        await status.edit_text(f"Error sending lounge messages: {str(e)}")
        logger.error(f"Error in /send_lounge_all: {str(e)}")

@router.message(Command("lounge"))
async def lounge_command(message: Message) -> None:
    """Handle the /lounge command to send a message to the lounge."""
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized.")
        return

    token = get_current_account(user_id)
    if not token:
        await message.reply("No active account found.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.reply("<b>Usage</b>\n\n<code>/lounge &lt;message&gt;</code>", parse_mode="HTML")
        return

    custom_message = parts[1]
    spam_enabled = get_individual_spam_filter(user_id, "lounge")
    status_text = (
        f"<b>Starting Lounge Messaging</b>\n\nMessage: <code>{html.escape(custom_message[:50])}...</code>\n"
        f"Spam filter: {'ON' if spam_enabled else 'OFF'}"
    )
    status_message = await message.reply(status_text, parse_mode="HTML")

    try:
        await send_lounge(token, custom_message, status_message, bot, user_id, spam_enabled)
    except Exception as e:
        await status_message.edit_text(f"Error sending lounge messages: {str(e)}")
        logger.error(f"Error in /lounge: {str(e)}")

@router.message(Command("chatroom"))
async def send_to_all_command(message: Message) -> None:
    """Handle the /chatroom command to send messages to all chatrooms."""
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized.")
        return

    token = get_current_account(user_id)
    if not token:
        await message.reply("No active account found.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.reply("<b>Usage</b>\n\n<code>/chatroom &lt;message&gt;</code>", parse_mode="HTML")
        return

    custom_message = parts[1]
    spam_enabled = get_individual_spam_filter(user_id, "chatroom")
    status_text = (
        f"<b>Starting Chatroom Messages</b>\n\nMessage: <code>{html.escape(custom_message[:50])}...</code>\n"
        f"Spam filter: {'ON' if spam_enabled else 'OFF'}\n\nInitializing..."
    )
    status_message = await message.reply(status_text, parse_mode="HTML")

    try:
        total, sent, filtered = await send_message_to_everyone(token, custom_message, user_id, spam_enabled, user_id)
        await status_message.edit_text(
            f"<b>Chatroom Messages Complete</b>\n\n<b>Results:</b>\n• Total chatrooms: <code>{total}</code>\n"
            f"• Messages sent: <code>{sent}</code>\n• Filtered: <code>{filtered}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await status_message.edit_text(f"<b>Error</b>\n\nFailed to send messages: {str(e)[:200]}", parse_mode="HTML")
        logger.error(f"Error in /chatroom: {str(e)}")

@router.message(Command("send_chat_all"))
async def send_chat_all(message: Message) -> None:
    """Handle the /send_chat_all command to send messages to all chatrooms for all accounts."""
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.reply("<b>Usage</b>\n\n<code>/send_chat_all &lt;message&gt;</code>", parse_mode="HTML")
        return

    custom_message = parts[1]
    active_tokens = get_active_tokens(user_id)
    if not active_tokens:
        await message.reply("No active tokens found.")
        return

    tokens = [t["token"] for t in active_tokens]
    token_names = {t["token"]: t["name"] for t in active_tokens}
    spam_enabled = get_individual_spam_filter(user_id, "chatroom")
    status_text = (
        f"<b>Starting Multi-Account Chatroom</b>\n\nActive tokens: <code>{len(tokens)}</code>\n"
        f"Message: <code>{html.escape(custom_message[:50])}...</code>\nSpam filter: {'ON' if spam_enabled else 'OFF'}\n\nInitializing..."
    )
    status = await message.reply(status_text, parse_mode="HTML")

    try:
        await send_message_to_everyone_all_tokens(tokens, custom_message, status, bot, user_id, spam_enabled, token_names, False, user_id)
    except Exception as e:
        await status.edit_text(f"<b>Error</b>\n\nFailed to send messages: {str(e)[:200]}", parse_mode="HTML")
        logger.error(f"Error in /send_chat_all: {str(e)}")

@router.message(Command("invoke"))
async def invoke_command(message: Message) -> None:
    """Handle the /invoke command to remove disabled accounts."""
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized.")
        return

    tokens = get_tokens(user_id)
    if not tokens:
        await message.reply("No tokens found.")
        return

    status_msg = await message.reply("<b>Checking Account Status</b>...", parse_mode="HTML")
    disabled_accounts, working_accounts = [], []
    url = "https://api.meeff.com/facetalk/vibemeet/history/count/v1"
    params = {'locale': "en"}

    async with aiohttp.ClientSession() as session:
        for token_obj in tokens:
            headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'meeff-access-token': token_obj["token"]}
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    result = await resp.json(content_type=None)
                    if result.get("errorCode") == "AuthRequired":
                        disabled_accounts.append(token_obj)
                    else:
                        working_accounts.append(token_obj)
            except Exception as e:
                logger.error(f"Error checking token {token_obj.get('name')}: {e}")
                disabled_accounts.append(token_obj)

    if disabled_accounts:
        for token_obj in disabled_accounts:
            delete_token(user_id, token_obj["token"])
        await status_msg.edit_text(
            f"<b>Account Cleanup Complete</b>\n\nWorking: <code>{len(working_accounts)}</code>\n"
            f"Removed: <code>{len(disabled_accounts)}</code>\n\n<b>Removed accounts:</b>\n" +
            "\n".join([f"• {html.escape(acc['name'])}" for acc in disabled_accounts]),
            parse_mode="HTML"
        )
    else:
        await status_msg.edit_text(f"<b>All Accounts Working</b> ({len(working_accounts)} total).", parse_mode="HTML")

@router.message(Command("settings"))
async def settings_command(message: Message) -> None:
    """Handle the /settings command to show the settings menu."""
    if not has_valid_access(message.chat.id):
        await message.reply("You are not authorized.")
        return
    await message.reply(
        "<b>Settings Menu</b>\n\nChoose an option:",
        reply_markup=get_settings_menu(message.chat.id),
        parse_mode="HTML"
    )

@router.message(Command("add"))
async def add_person_command(message: Message) -> None:
    """Handle the /add command to add a person by ID."""
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized.")
        return

    args = message.text.strip().split()
    if len(args) < 2:
        await message.reply("Usage: /add <person_id>")
        return

    token = get_current_account(user_id)
    if not token:
        await message.reply("No active account found.")
        return

    person_id = args[1]
    url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={person_id}&isOkay=1"
    
    # Get device info for this token
    device_info = get_or_create_device_info_for_token(user_id, token)
    base_headers = {"meeff-access-token": token}
    headers = get_headers_with_device_info(base_headers, device_info)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                if data.get("errorCode") == "LikeExceeded":
                    await message.reply("Daily like limit reached.")
                elif data.get("errorCode"):
                    await message.reply(f"Failed: {data.get('errorMessage', 'Unknown error')}")
                else:
                    await message.reply(f"Successfully added person with ID: {person_id}")
    except Exception as e:
        logger.error(f"Error adding person by ID: {e}")
        await message.reply("An error occurred.")

@router.message()
async def handle_new_token(message: Message) -> None:
    """Handle incoming messages for token addition or database operations."""
    user_id = message.from_user.id
    
    # Use user-specific lock to prevent race conditions
    async with get_user_lock(user_id):
        await _handle_new_token_internal(message)

async def _handle_new_token_internal(message: Message) -> None:
    """Internal token handling with user isolation."""
    if message.text and message.text.startswith("/"):
        return
    user_id = message.from_user.id
    if message.from_user.is_bot:
        return

    if await signup_message_handler(message):
        return

    state = db_operation_states.get(user_id)
    if state:
        operation = state.get("operation")
        text = message.text.strip()

        if operation == "connect_db":
            collection_name = f"user_{text}" if not text.startswith("user_") else text
            processing_msg = await message.reply("<b>Connecting to DB</b>...", parse_mode="HTML")
            success, msg = connect_to_collection(collection_name, user_id)
            await processing_msg.edit_text(f"<b>{'Success' if success else 'Failed'}</b>: {msg}", parse_mode="HTML")
        elif operation == "rename_db":
            processing_msg = await message.reply("<b>Renaming DB</b>...", parse_mode="HTML")
            success, msg = rename_user_collection(user_id, text)
            await processing_msg.edit_text(f"<b>{'Success' if success else 'Failed'}</b>: {msg}", parse_mode="HTML")
        elif operation == "transfer_db":
            try:
                target_user_id = int(text)
                processing_msg = await message.reply("<b>Transferring DB</b>...", parse_mode="HTML")
                success, msg = transfer_to_user(user_id, target_user_id)
                await processing_msg.edit_text(f"<b>{'Success' if success else 'Failed'}</b>: {msg}", parse_mode="HTML")
            except ValueError:
                await message.reply("Invalid user ID.")
        
        db_operation_states.pop(user_id, None)
        return

    if not has_valid_access(user_id):
        await message.reply("You are not authorized.")
        return

    if message.text:
        token_data = message.text.strip().split(" ", 1)
        token = token_data[0]
        if len(token) < 100:
            await message.reply("Invalid token format.")
            return

        # Verify token asynchronously without blocking other users
        verification_msg = await message.reply("<b>Verifying Token</b>...", parse_mode="HTML")
        
        # Run token verification in background
        is_valid = await asyncio.create_task(_verify_token_async(token, user_id))
        
        if not is_valid:
            await verification_msg.edit_text("<b>Invalid Token</b>.", parse_mode="HTML")
            return

        account_name = token_data[1] if len(token_data) > 1 else f"Account {len(get_tokens(user_id)) + 1}"
        
        # Run database operation in thread pool to avoid blocking
        await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            set_token, 
            user_id, 
            token, 
            account_name
        )
        
        await verification_msg.edit_text(
            f"<b>Token Verified</b> and saved as '<code>{html.escape(account_name)}</code>'.",
            parse_mode="HTML"
        )
    else:
        await message.reply("Please provide a token.")

async def _verify_token_async(token: str, user_id: int) -> bool:
    """Verify token asynchronously with connection pooling."""
    try:
        session = await get_http_session()
        url = "https://api.meeff.com/facetalk/vibemeet/history/count/v1"
        params = {'locale': "en"}
        
        # Get or create device info for this token
        device_info = get_or_create_device_info_for_token(user_id, token)
        base_headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'meeff-access-token': token}
        headers = get_headers_with_device_info(base_headers, device_info)

        async with session.get(url, params=params, headers=headers) as resp:
            result = await resp.json(content_type=None)
            return result.get("errorCode") != "AuthRequired"
    except Exception as e:
        logger.error(f"Error verifying token: {e}")
        return False

async def show_manage_accounts_menu(callback_query: CallbackQuery) -> None:
    """Display the manage accounts menu."""
    user_id = callback_query.from_user.id
    
    # Use user lock and run DB operations in thread pool
    async with get_user_lock(user_id):
        # Run database operations in thread pool to avoid blocking
        tokens, current_token = await asyncio.gather(
            asyncio.get_event_loop().run_in_executor(THREAD_POOL, get_tokens, user_id),
            asyncio.get_event_loop().run_in_executor(THREAD_POOL, get_current_account, user_id)
        )

    if not tokens:
        await callback_query.message.edit_text(
            "<b>No Accounts Found</b>\n\nSend a token to add an account.",
            reply_markup=back_markup,
            parse_mode="HTML"
        )
        return

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

    current_text = "A current account is set." if current_token else "No current account is set."
    message_text = f"<b>Manage Accounts</b>\n\n{current_text}"
    reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons)

    # --- START OF FIX ---
    try:
        await callback_query.message.edit_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
    except TelegramBadRequest as e:
        if "message is not modified" in e.message:
            # This error is safe to ignore, it just means the menu is already up-to-date.
            await callback_query.answer() # Acknowledge the button press without an alert
            logger.info("Message not modified, skipping edit.")
        else:
            # If it's a different error, we should still see it.
            logger.error(f"Error editing message: {e}")
            raise


@router.callback_query()
async def callback_handler(callback_query: CallbackQuery) -> None:
    """Handle callback queries from inline keyboards."""
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    # Use user-specific lock for callback handling
    async with get_user_lock(user_id):
        await _callback_handler_internal(callback_query)

async def _callback_handler_internal(callback_query: CallbackQuery) -> None:
    """Internal callback handler with user isolation."""
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    if await signup_callback_handler(callback_query):
        return

    if not has_valid_access(user_id):
        await callback_query.answer("You are not authorized.")
        return

    state = user_states.setdefault(user_id, {})

    if data == "db_settings":
        # Run DB operation in thread pool
        current_info = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            get_current_collection_info, 
            user_id
        )
        info_text = "<b>Database Settings</b>\n\n"
        if current_info["exists"]:
            summary = current_info["summary"]
            info_text += (
                f"<b>Current DB:</b> <code>{html.escape(current_info['collection_name'])}</code>\n"
                f"Accounts: <code>{summary.get('tokens_count', 0)}</code>\n"
            )
        else:
            info_text += "No database found for your account.\n\n"
        await callback_query.message.edit_text(
            info_text + "Choose an option:",
            reply_markup=get_db_settings_menu(),
            parse_mode="HTML"
        )
    elif data == "db_connect":
        db_operation_states[user_id] = {"operation": "connect_db"}
        await callback_query.message.edit_text(
            "<b>Connect to Database</b>\n\nEnter the collection name:",
            parse_mode="HTML"
        )
    elif data == "db_rename":
        db_operation_states[user_id] = {"operation": "rename_db"}
        await callback_query.message.edit_text(
            "<b>Rename Database</b>\n\nEnter the new name:",
            parse_mode="HTML"
        )
    elif data == "db_view":
        # Run DB operation in thread pool
        collections = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            list_all_collections
        )
        if not collections:
            await callback_query.message.edit_text(
                "<b>No Collections Found.</b>",
                reply_markup=get_db_settings_menu(),
                parse_mode="HTML"
            )
            return
        view_text = "<b>All Database Collections</b>\n\n"
        for i, col in enumerate(collections[:10], 1):
            summary = col["summary"]
            created_str = summary.get("created_at").strftime("%Y-%m-%d") if summary.get("created_at") else "N/A"
            view_text += (
                f"<b>{i}.</b> <code>{html.escape(col['collection_name'])}</code>\n"
                f"    Accounts: {summary.get('tokens_count', 0)} | Created: {created_str}\n\n"
            )
        await callback_query.message.edit_text(
            view_text,
            reply_markup=get_db_settings_menu(),
            parse_mode="HTML"
        )
    elif data == "db_transfer":
        db_operation_states[user_id] = {"operation": "transfer_db"}
        await callback_query.message.edit_text(
            "<b>Transfer Database</b>\n\nEnter the target Telegram user ID:",
            parse_mode="HTML"
        )
    elif data == "unsub_current":
        await callback_query.message.edit_text(
            "<b>Confirm:</b> Unsubscribe current account from all chatrooms?",
            reply_markup=get_confirmation_menu("unsub_current"),
            parse_mode="HTML"
        )
    elif data == "unsub_all":
        count = len(get_active_tokens(user_id))
        await callback_query.message.edit_text(
            f"<b>Confirm:</b> Unsubscribe all {count} active accounts?",
            reply_markup=get_confirmation_menu("unsub_all"),
            parse_mode="HTML"
        )
    elif data == "confirm_unsub_current":
        token = get_current_account(user_id)
        if not token:
            await callback_query.message.edit_text(
                "No active account found.",
                reply_markup=back_markup,
                parse_mode="HTML"
            )
            return
        msg = await callback_query.message.edit_text(
            "<b>Unsubscribing Current Account</b>...",
            parse_mode="HTML"
        )
        await unsubscribe_everyone(token, status_message=msg, bot=bot, chat_id=user_id, user_id=user_id)
    elif data == "confirm_unsub_all":
        active_tokens = get_active_tokens(user_id)
        if not active_tokens:
            await callback_query.message.edit_text(
                "No active accounts found.",
                reply_markup=back_markup,
                parse_mode="HTML"
            )
            return
        msg = await callback_query.message.edit_text(
            f"<b>Unsubscribing All Accounts</b> ({len(active_tokens)})...",
            parse_mode="HTML"
        )
        total = 0
        for i, token_obj in enumerate(active_tokens, 1):
            await msg.edit_text(
                f"Processing account {i}/{len(active_tokens)}: {html.escape(token_obj['name'])}",
                parse_mode="HTML"
            )
            await unsubscribe_everyone(token_obj["token"], user_id=user_id)
            total += 1
        await msg.edit_text(
            f"<b>Unsubscribe Complete</b>\nSuccessfully unsubscribed {total} accounts.",
            parse_mode="HTML"
        )
    elif data == "send_request_menu":
        await callback_query.message.edit_text(
            "<b>Send Request Options</b>\n\nChoose your request type:",
            reply_markup=send_request_markup,
            parse_mode="HTML"
        )
    elif data == "settings_menu":
        await callback_query.message.edit_text(
            "<b>Settings Menu</b>",
            reply_markup=get_settings_menu(user_id),
            parse_mode="HTML"
        )
    elif data == "show_filters":
        await callback_query.message.edit_text(
            "<b>Filter Settings</b>\n\nConfigure your search preferences:",
            reply_markup=get_meeff_filter_main_keyboard(user_id),
            parse_mode="HTML"
        )
    elif data in ("toggle_request_filter", "meeff_filter_main") or data.startswith(("account_filter_", "account_gender_", "account_age_", "account_nationality_")):
        await set_account_filter(callback_query)
    elif data == "manage_accounts":
        await show_manage_accounts_menu(callback_query)
    elif data.startswith("view_account_"):
        idx = int(data.split("_")[-1])
        # Run DB operation in thread pool
        tokens = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            get_tokens, 
            user_id
        )
        if 0 <= idx < len(tokens):
            token, token_obj = tokens[idx]["token"], tokens[idx]
            info_card = get_info_card(user_id, token)
            status_summary = (
                f"<b>Account Status</b>\n━━━━━━━━━━━━━━━━━━━━\n<b>Name:</b> <code>{html.escape(token_obj.get('name', 'N/A'))}</code>\n"
                f"<b>Status:</b> {'Active' if token_obj.get('active', True) else 'Inactive'}\n"
                f"<b>Current Account:</b> {'Yes' if get_current_account(user_id) == token else 'No'}\n\n"
            )
            if info_card:
                full_details = status_summary + "<b>Profile Information:</b>\n" + info_card
                await callback_query.message.edit_text(
                    full_details,
                    reply_markup=get_account_view_menu(idx),
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
            else:
                fallback_details = (
                    f"<b>Account Details</b>\n━━━━━━━━━━━━━━━━━━━━\n<b>Name:</b> <code>{html.escape(token_obj.get('name', 'N/A'))}</code>\n"
                    f"<b>Email:</b> <code>{html.escape(token_obj.get('email', 'N/A'))}</code>\n<b>Token:</b> <code>{html.escape(token)}</code>\n"
                    f"<b>Status:</b> {'Active' if token_obj.get('active', True) else 'Inactive'}\n"
                    f"<b>Current Account:</b> {'Yes' if get_current_account(user_id) == token else 'No'}\n\nNo profile card found."
                )
                await callback_query.message.edit_text(
                    fallback_details,
                    reply_markup=get_account_view_menu(idx),
                    parse_mode="HTML"
                )
        else:
            await callback_query.answer("Invalid account selected.")
    elif data.startswith("confirm_delete_"):
        idx = int(data.split("_")[-1])
        # Run DB operation in thread pool
        tokens = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            get_tokens, 
            user_id
        )
        if 0 <= idx < len(tokens):
            name = tokens[idx]["name"]
            buttons = [
                [
                    InlineKeyboardButton(text="Yes, Delete", callback_data=f"delete_account_{idx}"),
                    InlineKeyboardButton(text="Cancel", callback_data="manage_accounts")
                ]
            ]
            await callback_query.message.edit_text(
                f"<b>Confirm Deletion</b>\n\nDelete account <code>{html.escape(name)}</code>?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                parse_mode="HTML"
            )
        else:
            await callback_query.answer("Invalid account selected.")
    elif data.startswith("toggle_status_"):
        idx = int(data.split("_")[-1])
        # Run DB operation in thread pool
        tokens = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            get_tokens, 
            user_id
        )
        if 0 <= idx < len(tokens):
            # Run DB operation in thread pool
            await asyncio.get_event_loop().run_in_executor(
                THREAD_POOL, 
                toggle_token_status, 
                user_id, 
                tokens[idx]["token"]
            )
            await callback_query.answer(f"Toggled status for {html.escape(tokens[idx]['name'])}")
            await show_manage_accounts_menu(callback_query)
        else:
            await callback_query.answer("Invalid account selected.")
    elif data == "spam_filter_menu":
        await callback_query.message.edit_text(
            "<b>Spam Filter Settings</b>\n\nControl filters for each feature.",
            reply_markup=get_spam_filter_menu(user_id),
            parse_mode="HTML"
        )
    elif data.startswith("toggle_spam_"):
        filter_type = data.split("_")[-1]
        if filter_type == "all":
            new_status = not any(get_all_spam_filters(user_id).values())
            for ft in ["chatroom", "request", "lounge"]:
                set_individual_spam_filter(user_id, ft, new_status)
            await callback_query.answer(f"All spam filters turned {'ON' if new_status else 'OFF'}")
        elif filter_type in ["chatroom", "request", "lounge"]:
            new_status = not get_individual_spam_filter(user_id, filter_type)
            set_individual_spam_filter(user_id, filter_type, new_status)
            await callback_query.answer(f"{filter_type.capitalize()} spam filter {'enabled' if new_status else 'disabled'}")

        new_callback_query = callback_query.model_copy(update={'data': 'spam_filter_menu'})
        await callback_handler(new_callback_query)
    elif data.startswith("set_account_"):
        idx = int(data.split("_")[-1])
        # Run DB operation in thread pool
        tokens = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            get_tokens, 
            user_id
        )
        if 0 <= idx < len(tokens):
            # Run DB operation in thread pool
            await asyncio.get_event_loop().run_in_executor(
                THREAD_POOL, 
                set_current_account, 
                user_id, 
                tokens[idx]["token"]
            )
            await callback_query.answer(f"Set {html.escape(tokens[idx]['name'])} as current account")
            await show_manage_accounts_menu(callback_query)
        else:
            await callback_query.answer("Invalid account selected.")
    elif data.startswith("delete_account_"):
        idx = int(data.split("_")[-1])
        # Run DB operation in thread pool
        tokens = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            get_tokens, 
            user_id
        )
        if 0 <= idx < len(tokens):
            name = tokens[idx]["name"]
            # Run DB operation in thread pool
            await asyncio.get_event_loop().run_in_executor(
                THREAD_POOL, 
                delete_token, 
                user_id, 
                tokens[idx]["token"]
            )
            await callback_query.message.edit_text(
                f"<b>Account Deleted:</b> <code>{html.escape(name)}</code>",
                reply_markup=back_markup,
                parse_mode="HTML"
            )
        else:
            await callback_query.answer("Invalid account selected.")
    elif data == "back_to_menu":
        await callback_query.message.edit_text(
            "<b>Meeff Bot Dashboard</b>",
            reply_markup=start_markup,
            parse_mode="HTML"
        )
    elif data == "start":
        if state.get("running"):
            await callback_query.answer("Requests are already running!")
            return
        state["running"] = True
        msg = await callback_query.message.edit_text(
            "<b>Initializing Requests</b>...",
            reply_markup=stop_markup,
            parse_mode="HTML"
        )
        state.update({"status_message_id": msg.message_id, "pinned_message_id": msg.message_id})
        await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        asyncio.create_task(run_requests(user_id, bot, TARGET_CHANNEL_ID))
        await callback_query.answer("Requests started!")
    elif data == "start_all":
        if state.get("running"):
            await callback_query.answer("Another request is already running!")
            return
        # Run DB operation in thread pool
        tokens = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL, 
            get_active_tokens, 
            user_id
        )
        if not tokens:
            await callback_query.answer("No active tokens found.", show_alert=True)
            return
        state["running"] = True
        msg = await callback_query.message.edit_text(
            f"<b>Starting Multi-Account Requests</b> ({len(tokens)})...",
            reply_markup=stop_markup,
            parse_mode="HTML"
        )
        state.update({"status_message_id": msg.message_id, "pinned_message_id": msg.message_id})
        await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        asyncio.create_task(process_all_tokens(user_id, tokens, bot, TARGET_CHANNEL_ID))
        await callback_query.answer("Multi-account processing started!")
    elif data == "stop":
        if not state.get("running"):
            await callback_query.answer("Requests are not running!")
            return
        state["running"], state["stopped"] = False, True
        await callback_query.message.edit_text(
            f"<b>Requests Stopped.</b>\nTotal Added: <code>{state.get('total_added_friends', 0)}</code>",
            reply_markup=start_markup,
            parse_mode="HTML"
        )
        await callback_query.answer("Requests stopped.")
        if state.get("pinned_message_id"):
            await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
    elif data == "all_countries":
        if state.get("running"):
            await callback_query.answer("Another process is already running!")
            return
        state["running"] = True
        msg = await callback_query.message.edit_text(
            "<b>Starting All Countries Feature</b>...",
            reply_markup=stop_markup,
            parse_mode="HTML"
        )
        state.update({"status_message_id": msg.message_id, "pinned_message_id": msg.message_id, "stop_markup": stop_markup})
        await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        asyncio.create_task(run_all_countries(user_id, state, bot, get_current_account))
        await callback_query.answer("All Countries feature started!")

async def set_bot_commands() -> None:
    """Set the bot's command menu."""
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="lounge", description="Send message in the lounge"),
        BotCommand(command="send_lounge_all", description="Send lounge message to all accounts"),
        BotCommand(command="chatroom", description="Send message in chatrooms"),
        BotCommand(command="send_chat_all", description="Send chatroom message to all accounts"),
        BotCommand(command="invoke", description="Remove disabled accounts"),
        BotCommand(command="skip", description="Unsubscribe from chats"),
        BotCommand(command="settings", description="Bot settings"),
        BotCommand(command="add", description="Add a person by ID"),
        BotCommand(command="signup", description="Create a Meeff account"),
        BotCommand(command="password", description="Enter password for access")
    ]
    await bot.set_my_commands(commands)

async def main() -> None:
    """Main function to start the bot."""
    try:
        # Initialize HTTP session pool
        await get_http_session()
        
        await set_bot_commands()
        dp.include_router(router)
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
    finally:
        # Cleanup resources
        if HTTP_SESSION_POOL and not HTTP_SESSION_POOL.closed:
            await HTTP_SESSION_POOL.close()
        THREAD_POOL.shutdown(wait=True)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
