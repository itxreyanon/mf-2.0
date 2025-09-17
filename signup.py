import aiohttp
import json
import random
import itertools
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dateutil import parser
from aiogram.exceptions import TelegramBadRequest
from device_info import get_or_create_device_info_for_email, get_api_payload_with_device_info
from db import set_token, set_info_card, set_signup_config, get_signup_config, set_user_filters
from filters import get_nationality_keyboard

# Logging configuration
logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_BIOS = [
    "Love traveling and meeting new people!",
    "Coffee lover and adventure seeker",
    "Passionate about music and good vibes",
    "Foodie exploring new cuisines",
    "Fitness enthusiast and nature lover",
]
DEFAULT_PHOTOS = (
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/"
    "20250616052423006_profile-1.0-bd262b27-1916-4bd3-9f1d-0e7fdba35268.jpg|"
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/"
    "20250616052438006_profile-1.0-349bf38c-4555-40cc-a322-e61afe15aa35.jpg"
)

# Global state
user_signup_states: Dict[int, Dict] = {}

# Inline Keyboard Menus
SIGNUP_MENU = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Sign Up", callback_data="signup_go"),
        InlineKeyboardButton(text="Sign In", callback_data="signin_go")
    ],
    [
        InlineKeyboardButton(text="Signup Config", callback_data="signup_settings")
    ],
    [InlineKeyboardButton(text="Back to Main Menu", callback_data="back_to_menu")]
])

VERIFY_ALL_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Verify All Emails", callback_data="verify_accounts")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

RETRY_VERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Retry Pending Verification", callback_data="retry_pending")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

BACK_TO_SIGNUP = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

BACK_TO_CONFIG = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Back", callback_data="signup_settings")]
])

DONE_PHOTOS = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Done", callback_data="signup_photos_done")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

CONFIG_MENU = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Change Email", callback_data="change_email")],
    [InlineKeyboardButton(text="Auto Signup: Turn OFF", callback_data="toggle_auto_signup")],
    [InlineKeyboardButton(text="Setup Signup Details", callback_data="setup_signup_config")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

FILTER_NATIONALITY_KB = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="All Countries", callback_data="signup_filter_nationality_all")],
    [
        InlineKeyboardButton(text="🇷🇺 RU", callback_data="signup_filter_nationality_RU"),
        InlineKeyboardButton(text="🇺🇦 UA", callback_data="signup_filter_nationality_UA"),
        InlineKeyboardButton(text="🇧🇾 BY", callback_data="signup_filter_nationality_BY"),
        InlineKeyboardButton(text="🇮🇷 IR", callback_data="signup_filter_nationality_IR"),
        InlineKeyboardButton(text="🇵🇭 PH", callback_data="signup_filter_nationality_PH")
    ],
    [
        InlineKeyboardButton(text="🇵🇰 PK", callback_data="signup_filter_nationality_PK"),
        InlineKeyboardButton(text="🇺🇸 US", callback_data="signup_filter_nationality_US"),
        InlineKeyboardButton(text="🇮🇳 IN", callback_data="signup_filter_nationality_IN"),
        InlineKeyboardButton(text="🇩🇪 DE", callback_data="signup_filter_nationality_DE"),
        InlineKeyboardButton(text="🇫🇷 FR", callback_data="signup_filter_nationality_FR")
    ],
    [
        InlineKeyboardButton(text="🇧🇷 BR", callback_data="signup_filter_nationality_BR"),
        InlineKeyboardButton(text="🇨🇳 CN", callback_data="signup_filter_nationality_CN"),
        InlineKeyboardButton(text="🇯🇵 JP", callback_data="signup_filter_nationality_JP"),
        InlineKeyboardButton(text="🇰🇷 KR", callback_data="signup_filter_nationality_KR"),
        InlineKeyboardButton(text="🇨🇦 CA", callback_data="signup_filter_nationality_CA")
    ],
    [
        InlineKeyboardButton(text="🇦🇺 AU", callback_data="signup_filter_nationality_AU"),
        InlineKeyboardButton(text="🇮🇹 IT", callback_data="signup_filter_nationality_IT"),
        InlineKeyboardButton(text="🇪🇸 ES", callback_data="signup_filter_nationality_ES"),
        InlineKeyboardButton(text="🇿🇦 ZA", callback_data="signup_filter_nationality_ZA"),
        InlineKeyboardButton(text="🇹🇷 TR", callback_data="signup_filter_nationality_TR")
    ],
    [InlineKeyboardButton(text="Back", callback_data="signup_photos_done")]
])

def format_user_with_nationality(user: Dict) -> str:
    """Format user information into a displayable string with nationality and last active time."""
    def time_ago(dt_str: Optional[str]) -> str:
        if not dt_str:
            return "N/A"
        try:
            dt = parser.isoparse(dt_str)
            now = datetime.now(timezone.utc)
            diff = now - dt
            minutes = int(diff.total_seconds() // 60)
            if minutes < 1:
                return "just now"
            if minutes < 60:
                return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24:
                return f"{hours} hr ago"
            days = hours // 24
            return f"{days} day(s) ago"
        except Exception as e:
            logger.error(f"Error parsing date {dt_str}: {e}")
            return "unknown"

    last_active = time_ago(user.get("recentAt"))
    card = (
        f"<b>📱 Account Information</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>👤 Name:</b> {user.get('name', 'N/A')}\n"
        f"<b>🆔 ID:</b> <code>{user.get('_id', 'N/A')}</code>\n"
        f"<b>📝 Bio:</b> {user.get('description', 'N/A')}\n"
        f"<b>🎂 Birth Year:</b> {user.get('birthYear', 'N/A')}\n"
        f"<b>🌍 Country:</b> {user.get('nationalityCode', 'N/A')}\n"
        f"<b>📱 Platform:</b> {user.get('platform', 'N/A')}\n"
        f"<b>⭐ Score:</b> {user.get('profileScore', 'N/A')}\n"
        f"<b>📍 Distance:</b> {user.get('distance', 'N/A')} km\n"
        f"<b>🗣️ Languages:</b> {', '.join(user.get('languageCodes', [])) or 'N/A'}\n"
        f"<b>🕐 Last Active:</b> {last_active}\n"
    )

    if user.get('photoUrls'):
        card += f"<b>📸 Photos:</b> " + ' '.join([f"<a href='{url}'>📷</a>" for url in user.get('photoUrls', [])])
    
    if "email" in user:
        card += f"\n\n<b>📧 Email:</b> <code>{user['email']}</code>"
    if "password" in user:
        card += f"\n<b>🔐 Password:</b> <code>{user['password']}</code>"
    if "token" in user:
        card += f"\n<b>🔑 Token:</b> <code>{user['token']}</code>"
    
    return card

def generate_email_variations(base_email: str, count: int = 50) -> List[str]:
    """Generate variations of an email address by adding dots to the username."""
    if '@' not in base_email:
        return []
    username, domain = base_email.split('@', 1)
    variations = {base_email}
    for i in range(1, len(username)):
        for positions in itertools.combinations(range(1, len(username)), i):
            if len(variations) >= count:
                return list(variations)
            new_username = list(username)
            for pos in reversed(positions):
                new_username.insert(pos, '.')
            variations.add(''.join(new_username) + '@' + domain)
    return list(variations)

async def count_available_emails(base_email: str, pending_emails: List[str], max_check: int = 50) -> int:
    """Count available email variations for the base email."""
    variations = generate_email_variations(base_email, max_check)
    available_count = 0
    for email in variations:
        if email in pending_emails:
            is_available, _ = await check_email_exists(email)
            if is_available:
                available_count += 1
        elif len(pending_emails) + available_count < max_check:
            is_available, _ = await check_email_exists(email)
            if is_available:
                available_count += 1
        await asyncio.sleep(0.1)  # Small delay to avoid overwhelming API
    return available_count

def get_random_bio() -> str:
    """Return a random bio from the default bios list."""
    return random.choice(DEFAULT_BIOS)

async def check_email_exists(email: str) -> Tuple[bool, str]:
    """Check if an email is available for signup."""
    url = "https://api.meeff.com/user/checkEmail/v1"
    payload = {"email": email, "locale": "en"}
    headers = {
        'User-Agent': "okhttp/5.0.0-alpha.14",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json; charset=utf-8"
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                resp_json = await response.json()
                if response.status == 406 or resp_json.get("errorMessage") == "This email is already in use.":
                    return False, resp_json.get("errorMessage", "This email is already in use.")
                return True, ""
        except Exception as e:
            logger.error(f"Error checking email {email}: {e}")
            return False, "Failed to check email availability."

async def select_available_emails(base_email: str, num_accounts: int, pending_emails: List[str]) -> List[str]:
    """Select available email variations, prioritizing pending emails."""
    available_emails = []
    # First, check pending emails for availability
    for email in pending_emails:
        if len(available_emails) >= num_accounts:
            break
        is_available, _ = await check_email_exists(email)
        if is_available:
            available_emails.append(email)
        await asyncio.sleep(0.1)  # Small delay to avoid overwhelming API
    
    # If more emails are needed, generate new variations
    if len(available_emails) < num_accounts:
        email_variations = generate_email_variations(base_email, num_accounts * 10)
        # Exclude pending emails to avoid duplicates
        email_variations = [e for e in email_variations if e not in pending_emails]
        for email in email_variations:
            if len(available_emails) >= num_accounts:
                break
            is_available, _ = await check_email_exists(email)
            if is_available:
                available_emails.append(email)
            await asyncio.sleep(0.1)  # Small delay to avoid overwhelming API
    
    return available_emails

async def show_signup_preview(message: Message, user_id: int, state: Dict) -> None:
    """Show a preview of the signup configuration with exact emails to be used."""
    config = await get_signup_config(user_id) or {}
    if not all(k in config for k in ['email', 'password', 'gender', 'birth_year', 'nationality']):
        await message.edit_text(
            "<b>Configuration Incomplete</b>\n\nYou must set up all details in 'Signup Config' first.",
            reply_markup=SIGNUP_MENU,
            parse_mode="HTML"
        )
        return
    num_accounts = state.get('num_accounts', 1)
    pending_emails = [acc['email'] for acc in state.get('pending_accounts', [])]
    available_emails = await select_available_emails(config.get("email", ""), num_accounts, pending_emails)
    state["selected_emails"] = available_emails  # Store selected emails for creation
    filter_nat = state.get('filter_nationality', 'All Countries')
    email_list = '\n'.join([f"{i+1}. {email}{' (Pending)' if email in pending_emails else ''}" for i, email in enumerate(available_emails)]) if available_emails else "No available emails found!"
    preview_text = (
        f"<b>Signup Preview</b>\n\n"
        f"<b>Name:</b> {state.get('name', 'N/A')}\n"
        f"<b>Photos:</b> {len(state.get('photos', []))} uploaded\n"
        f"<b>Number of Accounts:</b> {num_accounts}\n"
        f"<b>Gender:</b> {config.get('gender', 'N/A')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'N/A')}\n"
        f"<b>Nationality:</b> {config.get('nationality', 'N/A')}\n"
        f"<b>Filter Nationality:</b> {filter_nat}\n\n"
        f"<b>Emails to be Used:</b>\n{email_list}\n\n"
        f"<b>Ready to create {len(available_emails)} of {num_accounts} requested account{'s' if num_accounts > 1 else ''}?</b>"
    )
    confirm_text = f"Create {len(available_emails)} Account{'s' if len(available_emails) != 1 else ''}"
    menu = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=confirm_text, callback_data="create_accounts_confirm")],
        [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
    ])
    try:
        await message.edit_text(preview_text, reply_markup=menu, parse_mode="HTML")
    except TelegramBadRequest as e:
        logger.warning(f"TelegramBadRequest in show_signup_preview: {e}")
        await message.answer(preview_text, reply_markup=menu, parse_mode="HTML")
    user_signup_states[user_id] = state

async def signup_settings_command(message: Message, is_callback: bool = False) -> None:
    """Display and manage signup configuration settings."""
    user_id = message.chat.id
    config = await get_signup_config(user_id) or {}
    auto_signup_status = config.get('auto_signup', False)
    base_email = config.get('email', '')
    pending_emails = [acc['email'] for acc in user_signup_states.get(user_id, {}).get('pending_accounts', [])]
    available_email_count = await count_available_emails(base_email, pending_emails, max_check=50) if base_email else 0
    config_text = (
        "<b>Signup Configuration</b>\n\nSet default values and enable Auto Signup.\n\n"
        f"<b>Email:</b> <code>{base_email or 'Not set'}</code>\n"
        f"<b>Available Email Variations:</b> {available_email_count}\n"
        f"<b>Password:</b> <code>{'*' * len(config.get('password', '')) if config.get('password') else 'Not set'}</code>\n"
        f"<b>Gender:</b> {config.get('gender', 'Not set')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'Not set')}\n"
        f"<b>Nationality:</b> {config.get('nationality', 'Not set')}\n"
        f"<b>Auto Signup:</b> {'ON' if auto_signup_status else 'OFF'}\n\n"
        "Turn <b>Auto Signup ON</b> to use these settings automatically."
    )
    menu = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Change Email", callback_data="change_email")],
        [InlineKeyboardButton(text=f"Auto Signup: {'Turn OFF' if auto_signup_status else 'Turn ON'}", callback_data="toggle_auto_signup")],
        [InlineKeyboardButton(text="Setup Signup Details", callback_data="setup_signup_config")],
        [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
    ])
    try:
        if is_callback:
            await message.edit_text(config_text, reply_markup=menu, parse_mode="HTML")
        else:
            await message.answer(config_text, reply_markup=menu, parse_mode="HTML")
    except TelegramBadRequest as e:
        logger.warning(f"TelegramBadRequest in signup_settings_command: {e}")
        await message.answer(config_text, reply_markup=menu, parse_mode="HTML")

async def signup_command(message: Message) -> None:
    """Handle the /signup command to initiate account creation."""
    user_signup_states[message.chat.id] = {"stage": "menu"}
    await message.answer(
        "<b>Account Creation</b>\n\nChoose an option:",
        reply_markup=SIGNUP_MENU,
        parse_mode="HTML"
    )

async def signup_callback_handler(callback: CallbackQuery) -> bool:
    """Handle callback queries for signup-related actions."""
    user_id = callback.from_user.id
    state = user_signup_states.get(user_id, {})
    data = callback.data

    try:
        if data == "signup_settings":
            await signup_settings_command(callback.message, is_callback=True)
        elif data == "change_email":
            state["stage"] = "change_email"
            user_signup_states[user_id] = state
            await callback.message.edit_text(
                "<b>Change Email</b>\n\nEnter your new base Gmail address (e.g., yourname@gmail.com).",
                reply_markup=BACK_TO_CONFIG,
                parse_mode="HTML"
            )
        elif data == "toggle_auto_signup":
            config = await get_signup_config(user_id) or {}
            config['auto_signup'] = not config.get('auto_signup', False)
            await set_signup_config(user_id, config)
            await callback.answer(f"Auto Signup turned {'ON' if config['auto_signup'] else 'OFF'}")
            await signup_settings_command(callback.message, is_callback=True)
        elif data == "setup_signup_config":
            state["stage"] = "config_email"
            user_signup_states[user_id] = state
            await callback.message.edit_text(
                "<b>Setup Email</b>\n\nEnter your base Gmail address (e.g., yourname@gmail.com).",
                reply_markup=BACK_TO_CONFIG,
                parse_mode="HTML"
            )
        elif data == "signup_go":
            config = await get_signup_config(user_id) or {}
            if not all(k in config for k in ['email', 'password', 'gender', 'birth_year', 'nationality']):
                await callback.message.edit_text(
                    "<b>Configuration Incomplete</b>\n\nPlease set up all details in <b>Signup Config</b> first.",
                    reply_markup=SIGNUP_MENU,
                    parse_mode="HTML"
                )
            else:
                state["stage"] = "ask_num_accounts"
                user_signup_states[user_id] = state
                await callback.message.edit_text(
                    "<b>Account Creation</b>\n\nEnter the number of accounts to create (1-10):",
                    reply_markup=BACK_TO_SIGNUP,
                    parse_mode="HTML"
                )
        elif data == "signup_photos_done":
            state["stage"] = "ask_filter_nationality"
            await callback.message.edit_text(
                "<b>Select Filter Nationality</b>\n\nChoose the nationality filter for requests:",
                reply_markup=FILTER_NATIONALITY_KB,
                parse_mode="HTML"
            )
        elif data.startswith("signup_filter_nationality_"):
            code = data.split("_")[-1] if len(data.split("_")) > 3 else ""
            state["filter_nationality"] = code if code != "all" else ""
            await show_signup_preview(callback.message, user_id, state)
        elif data == "create_accounts_confirm":
            config = await get_signup_config(user_id) or {}
            num_accounts = state.get("num_accounts", 1)
            selected_emails = state.get("selected_emails", [])
            if not selected_emails:
                await callback.message.edit_text(
                    "<b>No Available Emails</b>\n\nNo valid email variations found. Please try a different base email in Signup Config.",
                    reply_markup=SIGNUP_MENU,
                    parse_mode="HTML"
                )
                return True
            await callback.message.edit_text("<b>Creating Accounts...</b>\n\nProgress: 0%", parse_mode="HTML")
            created_accounts = []
            for i, email in enumerate(selected_emails[:num_accounts]):
                acc_state = {
                    "email": email,
                    "password": config.get("password"),
                    "name": state.get('name', 'User'),
                    "gender": config.get("gender"),
                    "desc": get_random_bio(),
                    "photos": state.get("photos", []),
                    "birth_year": config.get("birth_year", 2000),
                    "nationality": config.get("nationality", "US")
                }
                res = await try_signup(acc_state, user_id)
                if res.get("user", {}).get("_id"):
                    created_accounts.append({
                        "email": email,
                        "name": acc_state["name"],
                        "password": config.get("password")
                    })
                await asyncio.sleep(1)  # Delay to avoid rate-limiting
                progress = int((i + 1) / len(selected_emails[:num_accounts]) * 100)
                try:
                    await callback.message.edit_text(
                        f"<b>Creating Accounts...</b>\n\nProgress: {progress}%",
                        parse_mode="HTML"
                    )
                except TelegramBadRequest as e:
                    logger.warning(f"TelegramBadRequest during progress update: {e}")
                    await callback.message.answer(
                        f"<b>Creating Accounts...</b>\n\nProgress: {progress}%",
                        parse_mode="HTML"
                    )
            state["created_accounts"] = created_accounts
            state["verified_accounts"] = []
            state["pending_accounts"] = created_accounts.copy()
            result_text = (
                f"<b>Account Creation Results</b>\n\n"
                f"<b>Created:</b> {len(created_accounts)} of {num_accounts} requested account{'s' if num_accounts != 1 else ''}\n\n"
            )
            if created_accounts:
                result_text += "<b>Created Accounts:</b>\n" + '\n'.join([
                    f"• {a['name']} - <code>{a['email']}</code>" for a in created_accounts
                ])
            result_text += "\n\nPlease verify all emails, then click the button below."
            try:
                await callback.message.edit_text(
                    result_text,
                    reply_markup=VERIFY_ALL_BUTTON,
                    parse_mode="HTML"
                )
            except TelegramBadRequest as e:
                logger.warning(f"TelegramBadRequest in create_accounts_confirm: {e}")
                await callback.message.answer(
                    result_text,
                    reply_markup=VERIFY_ALL_BUTTON,
                    parse_mode="HTML"
                )
        elif data == "verify_accounts" or data == "retry_pending":
            pending = state.get("pending_accounts", [])
            if not pending:
                try:
                    await callback.message.edit_text(
                        "<b>No Pending Accounts</b>\n\nAll accounts are either verified or none were created.",
                        reply_markup=SIGNUP_MENU,
                        parse_mode="HTML"
                    )
                except TelegramBadRequest as e:
                    logger.warning(f"TelegramBadRequest in verify_accounts: {e}")
                    await callback.message.answer(
                        "<b>No Pending Accounts</b>\n\nAll accounts are either verified or none were created.",
                        reply_markup=SIGNUP_MENU,
                        parse_mode="HTML"
                    )
                return True
            await callback.message.edit_text("<b>Verifying Accounts...</b>\n\nProgress: 0%", parse_mode="HTML")
            verified = []
            new_pending = []
            filter_nat = state.get("filter_nationality", "")
            for i, acc in enumerate(pending):
                res = await try_signin(acc["email"], acc["password"], user_id)
                if res.get("accessToken") and res.get("user"):
                    token = res["accessToken"]
                    await set_token(user_id, token, acc["name"], acc["email"])
                    await set_user_filters(user_id, token, {"filterNationalityCode": filter_nat})
                    res["user"].update({
                        "email": acc["email"],
                        "password": acc["password"],
                        "token": token
                    })
                    await set_info_card(user_id, token, format_user_with_nationality(res["user"]), acc["email"])
                    verified.append(acc)
                else:
                    error_msg = res.get("errorMessage", "Unknown error")
                    if "too frequent" in error_msg.lower():
                        new_pending.append(acc)
                        logger.warning(f"Rate-limited for {acc['email']}: {error_msg}")
                    else:
                        new_pending.append(acc)
                await asyncio.sleep(2)  # Delay to avoid rate-limiting
                progress = int((i + 1) / len(pending) * 100)
                try:
                    await callback.message.edit_text(
                        f"<b>Verifying Accounts...</b>\n\nProgress: {progress}%",
                        parse_mode="HTML"
                    )
                except TelegramBadRequest as e:
                    logger.warning(f"TelegramBadRequest during verification progress: {e}")
                    await callback.message.answer(
                        f"<b>Verifying Accounts...</b>\n\nProgress: {progress}%",
                        parse_mode="HTML"
                    )
            state["verified_accounts"] = verified
            state["pending_accounts"] = new_pending
            if not new_pending:
                result_text = (
                    f"<b>Verification Results</b>\n\n"
                    f"<b>Verified:</b> {len(verified)} account{'s' if len(verified) != 1 else ''}\n\n"
                    "All accounts have been successfully verified and saved."
                )
                reply_markup = SIGNUP_MENU
            else:
                result_text = (
                    f"<b>Verification Results</b>\n\n"
                    f"<b>Verified:</b> {len(verified)} account{'s' if len(verified) != 1 else ''}\n"
                    f"<b>Pending Verification:</b> {len(new_pending)} account{'s' if len(new_pending) != 1 else ''}\n\n"
                    "<b>Pending Accounts:</b>\n" + '\n'.join([f"• <code>{a['email']}</code>" for a in new_pending]) +
                    "\n\nPlease verify these emails, then retry."
                )
                if any("too frequent" in res.get("errorMessage", "").lower() for acc in new_pending):
                    result_text += "\n\n<b>Note:</b> Some accounts could not be verified due to frequent login attempts. Please wait and retry."
                reply_markup = RETRY_VERIFY_BUTTON
            try:
                await callback.message.edit_text(
                    result_text,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
            except TelegramBadRequest as e:
                logger.warning(f"TelegramBadRequest in verification result: {e}")
                await callback.message.answer(
                    result_text,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
        elif data == "signup_menu":
            state["stage"] = "menu"
            try:
                await callback.message.edit_text(
                    "<b>Account Creation</b>\n\nChoose an option:",
                    reply_markup=SIGNUP_MENU,
                    parse_mode="HTML"
                )
            except TelegramBadRequest as e:
                logger.warning(f"TelegramBadRequest in signup_menu: {e}")
                await callback.message.answer(
                    "<b>Account Creation</b>\n\nChoose an option:",
                    reply_markup=SIGNUP_MENU,
                    parse_mode="HTML"
                )
        elif data == "signin_go":
            state["stage"] = "signin_email"
            try:
                await callback.message.edit_text(
                    "<b>Sign In</b>\n\nEnter your email address:",
                    reply_markup=BACK_TO_SIGNUP,
                    parse_mode="HTML"
                )
            except TelegramBadRequest as e:
                logger.warning(f"TelegramBadRequest in signin_go: {e}")
                await callback.message.answer(
                    "<b>Sign In</b>\n\nEnter your email address:",
                    reply_markup=BACK_TO_SIGNUP,
                    parse_mode="HTML"
                )
        else:
            await callback.answer()
            return False
        
        user_signup_states[user_id] = state
        await callback.answer()
        return True
    except TelegramBadRequest as e:
        logger.error(f"TelegramBadRequest in signup_callback_handler: {e}")
        try:
            await callback.message.answer(
                "<b>Error</b>\n\nAction took too long. Please try again.",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        except Exception as e2:
            logger.error(f"Failed to send error message: {e2}")
        return True

async def signup_message_handler(message: Message) -> bool:
    """Handle messages during the signup process."""
    user_id = message.from_user.id
    if user_id not in user_signup_states:
        return False
    state = user_signup_states.get(user_id, {})
    stage = state.get("stage", "")
    text = message.text.strip() if message.text else ""

    if stage == "change_email":
        if '@' not in text:
            await message.answer("Invalid Email. Please try again:", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
            return True
        config = await get_signup_config(user_id) or {}
        config["email"] = text
        await set_signup_config(user_id, config)
        state["stage"] = "menu"
        await message.answer("<b>Email Updated!</b>", parse_mode="HTML")
        await signup_settings_command(message)
    elif stage.startswith("config_"):
        config = await get_signup_config(user_id) or {}
        if stage == "config_email":
            if '@' not in text:
                await message.answer("Invalid Email. Please try again:", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
                return True
            config["email"] = text
            state["stage"] = "config_password"
            await message.answer("<b>Setup Password</b>\nEnter the password:", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
        elif stage == "config_password":
            config["password"] = text
            state["stage"] = "config_gender"
            await message.answer("<b>Setup Gender</b>\nEnter gender (M/F):", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
        elif stage == "config_gender":
            if text.upper() not in ("M", "F"):
                await message.answer("Invalid. Please enter M or F:", parse_mode="HTML")
                return True
            config["gender"] = text.upper()
            state["stage"] = "config_birth_year"
            await message.answer("<b>Setup Birth Year</b>\nEnter birth year (e.g., 2000):", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
        elif stage == "config_birth_year":
            try:
                year = int(text)
                if not 1950 <= year <= 2010:
                    raise ValueError()
                config["birth_year"] = year
                state["stage"] = "config_nationality"
                await message.answer(
                    "<b>Setup Nationality</b>\nEnter a 2-letter code (e.g., US, UK):",
                    reply_markup=BACK_TO_CONFIG,
                    parse_mode="HTML"
                )
            except ValueError:
                await message.answer("Invalid Year (1950-2010). Please try again:", parse_mode="HTML")
                return True
        elif stage == "config_nationality":
            if len(text) != 2:
                await message.answer("Invalid. Please enter a 2-letter code:", parse_mode="HTML")
                return True
            config["nationality"] = text.upper()
            state["stage"] = "menu"
            await message.answer("<b>Configuration Saved!</b>", parse_mode="HTML")
            await signup_settings_command(message)
        await set_signup_config(user_id, config)
    elif stage == "ask_num_accounts":
        try:
            num = int(text)
            if not 1 <= num <= 10:
                raise ValueError()
            state["num_accounts"] = num
            state["stage"] = "ask_name"
            await message.answer(
                "<b>Display Name</b>\nEnter the display name for the account(s):",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer("Invalid number (1-10). Please try again:", parse_mode="HTML")
            return True
    elif stage == "ask_name":
        state["name"] = text
        state["stage"] = "ask_photos"
        state["photos"] = []
        state["last_photo_message_id"] = None
        await message.answer(
            "<b>Profile Photos</b>\n\nSend up to 6 photos. Click 'Done' when finished.",
            reply_markup=DONE_PHOTOS,
            parse_mode="HTML"
        )
    elif stage == "ask_photos":
        if message.content_type != "photo":
            await message.answer("Please send a photo or click 'Done'.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
            return True
        if len(state.get("photos", [])) >= 6:
            await message.answer("Photo limit reached (6). Click Done.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
            return True
        photo_url = await upload_tg_photo(message)
        if photo_url:
            if "photos" not in state:
                state["photos"] = []
            state["photos"].append(photo_url)
            # Delete the previous photo message to keep the chat clean
            if state.get("last_photo_message_id"):
                try:
                    await message.bot.delete_message(chat_id=user_id, message_id=state["last_photo_message_id"])
                except Exception as e:
                    logger.warning(f"Failed to delete previous photo message: {e}")
            # Send a new message with the updated count and Done button
            new_message = await message.answer(
                f"<b>Profile Photos</b>\n\nPhoto uploaded ({len(state['photos'])}/6). Send another or click 'Done'.",
                reply_markup=DONE_PHOTOS,
                parse_mode="HTML"
            )
            state["last_photo_message_id"] = new_message.message_id
        else:
            await message.answer("Upload Failed. Please try again.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
    elif stage == "signin_email":
        state["signin_email"] = text
        state["stage"] = "signin_password"
        await message.answer(
            "<b>Password</b>\nEnter your password:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
    elif stage == "signin_password":
        msg = await message.answer("<b>Signing In</b>...", parse_mode="HTML")
        res = await try_signin(state["signin_email"], text, user_id)
        if res.get("accessToken")
