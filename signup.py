import asyncio
import aiohttp
import json
import random
import itertools
import logging
from typing import Dict, List, Optional, Tuple

from aiogram import Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dateutil import parser
from datetime import datetime, timezone

# (Assume these are your custom modules, they must be present)
from device_info import get_or_create_device_info_for_email, get_api_payload_with_device_info
from db import set_token, set_info_card, set_signup_config, get_signup_config, set_user_filters
from filters import get_nationality_keyboard

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BIOS = [
    "Love traveling and meeting new people!", "Coffee lover and adventure seeker",
    "Passionate about music and good vibes", "Foodie exploring new cuisines",
    "Fitness enthusiast and nature lover",
]
DEFAULT_PHOTOS = (
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/"
    "20250616052423006_profile-1.0-bd262b27-1916-4bd3-9f1d-0e7fdba35268.jpg|"
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/"
    "20250616052438006_profile-1.0-349bf38c-4555-40cc-a322-e61afe15aa35.jpg"
)
RATE_LIMIT_DELAY = 5  # Delay in seconds between verification attempts

# --- Global State ---
user_signup_states: Dict[int, Dict] = {}

# --- Inline Keyboard Menus ---
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
    [InlineKeyboardButton(text="✅ Verify All Emails", callback_data="verify_accounts")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

RETRY_VERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🔁 Retry Pending Verification", callback_data="retry_pending")],
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

# --- Background Task Handlers (NEW & REFACTORED) ---

async def run_account_creation_task(user_id: int, state: Dict, message: Message, bot: Bot):
    """
    (BACKGROUND TASK) Creates accounts one by one with progress updates.
    """
    config = await get_signup_config(user_id) or {}
    selected_emails = state.get("selected_emails", [])
    total_to_create = len(selected_emails)

    created_accounts = []
    failed_accounts = []

    for i, email in enumerate(selected_emails):
        try:
            await message.edit_text(f"🔄 **Creating Accounts...**\n\nProgress: {i + 1}/{total_to_create}", parse_mode="HTML")
        except Exception: # Message might not have changed, ignore
            pass
            
        acc_state = {
            "email": email, "password": config.get("password"),
            "name": state.get('name', 'User'), "gender": config.get("gender"),
            "desc": random.choice(DEFAULT_BIOS), "photos": state.get("photos", []),
            "birth_year": config.get("birth_year", 2000), "nationality": config.get("nationality", "US")
        }
        res = await try_signup(acc_state, user_id)
        if res.get("user", {}).get("_id"):
            created_accounts.append({"email": email, "name": acc_state["name"], "password": config.get("password")})
        else:
            failed_accounts.append(email)

    state["created_accounts"] = created_accounts
    state["verified_accounts"] = []
    state["pending_accounts"] = created_accounts.copy()

    # Format final message
    result_text = f"✅ **Account Creation Complete**\n\n"
    result_text += f"**Succeeded:** {len(created_accounts)}\n"
    result_text += f"**Failed:** {len(failed_accounts)}\n\n"
    if created_accounts:
        result_text += "Please verify all emails, then click the button below."
        reply_markup = VERIFY_ALL_BUTTON
    else:
        result_text += "No accounts were created. Please check your config and try again."
        reply_markup = SIGNUP_MENU
        
    await message.edit_text(result_text, reply_markup=reply_markup, parse_mode="HTML")
    user_signup_states[user_id] = state


async def run_account_verification_task(user_id: int, state: Dict, message: Message, bot: Bot):
    """
    (BACKGROUND TASK) Verifies accounts with delays to prevent rate-limiting.
    """
    pending = state.get("pending_accounts", [])
    total_to_verify = len(pending)
    verified = state.get("verified_accounts", [])
    new_pending = []
    filter_nat = state.get("filter_nationality", "")

    for i, acc in enumerate(pending):
        try:
            await message.edit_text(f"🔄 **Verifying Accounts...**\n\nProgress: {i + 1}/{total_to_verify}", parse_mode="HTML")
        except Exception: # Message might not have changed, ignore
            pass
            
        res = await try_signin(acc["email"], acc["password"], user_id)
        if res.get("accessToken") and res.get("user"):
            token = res["accessToken"]
            await set_token(user_id, token, acc["name"], acc["email"])
            await set_user_filters(user_id, token, {"filterNationalityCode": filter_nat})
            res["user"].update({"email": acc["email"], "password": acc["password"], "token": token})
            await set_info_card(user_id, token, format_user_with_nationality(res["user"]), acc["email"])
            verified.append(acc)
        else:
            new_pending.append(acc)
        
        # CRITICAL: Wait to avoid rate-limiting, but not after the last item.
        if i < total_to_verify - 1:
            await asyncio.sleep(RATE_LIMIT_DELAY)

    state["verified_accounts"] = verified
    state["pending_accounts"] = new_pending
    user_signup_states[user_id] = state

    # Format final message
    if not new_pending:
        result_text = (f"🎉 **All Accounts Verified!**\n\n"
                       f"Successfully verified and saved {len(verified)} accounts.")
        reply_markup = SIGNUP_MENU
    else:
        pending_list = '\n'.join([f"• <code>{a['email']}</code>" for a in new_pending])
        result_text = (f"⚠️ **Verification Incomplete**\n\n"
                       f"**Verified:** {len(verified)}\n"
                       f"**Pending:** {len(new_pending)}\n\n"
                       f"Please check the following accounts and retry:\n{pending_list}")
        reply_markup = RETRY_VERIFY_BUTTON

    await message.edit_text(result_text, reply_markup=reply_markup, parse_mode="HTML")

# --- Main Bot Handlers ---

async def signup_command(message: Message) -> None:
    """Handle the /signup command to initiate account creation."""
    user_signup_states[message.chat.id] = {"stage": "menu"}
    await message.answer(
        "<b>Account Creation</b>\n\nChoose an option:",
        reply_markup=SIGNUP_MENU,
        parse_mode="HTML"
    )

async def signup_callback_handler(callback: CallbackQuery, bot: Bot) -> bool:
    """Handle callback queries for signup-related actions."""
    user_id = callback.from_user.id
    state = user_signup_states.get(user_id, {})
    data = callback.data
    
    # --- START OF MODIFIED SECTIONS ---
    if data == "create_accounts_confirm":
        await callback.answer("✅ Starting account creation in the background...")
        message = await callback.message.edit_text("🔄 Preparing to create accounts...", parse_mode="HTML")
        # Run the slow process in the background
        asyncio.create_task(run_account_creation_task(user_id, state, message, bot))
        return True

    elif data == "verify_accounts" or data == "retry_pending":
        pending = state.get("pending_accounts", [])
        if not pending:
            await callback.answer("No pending accounts to verify.", show_alert=True)
            return True
        await callback.answer("✅ Starting verification in the background...")
        message = await callback.message.edit_text("🔄 Preparing to verify accounts...", parse_mode="HTML")
        # Run the slow process in the background
        asyncio.create_task(run_account_verification_task(user_id, state, message, bot))
        return True
    # --- END OF MODIFIED SECTIONS ---

    if data == "signup_settings":
        await signup_settings_command(callback.message, is_callback=True)
    elif data == "change_email":
        state["stage"] = "config_email"
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
        state["filter_nationality"] = code if code.lower() != "all" else ""
        await show_signup_preview(callback.message, user_id, state)
    elif data == "signup_menu":
        state["stage"] = "menu"
        await callback.message.edit_text(
            "<b>Account Creation</b>\n\nChoose an option:",
            reply_markup=SIGNUP_MENU,
            parse_mode="HTML"
        )
    elif data == "signin_go":
        state["stage"] = "signin_email"
        await callback.message.edit_text(
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

async def signup_message_handler(message: Message, bot: Bot) -> bool:
    """Handle messages during the signup process."""
    user_id = message.from_user.id
    if user_id not in user_signup_states:
        return False
    state = user_signup_states.get(user_id, {})
    stage = state.get("stage", "")
    text = message.text.strip() if message.text else ""

    if stage.startswith("config_"):
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
            if not 1 <= num <= 30: # Increased limit
                raise ValueError()
            state["num_accounts"] = num
            state["stage"] = "ask_name"
            await message.answer(
                "<b>Display Name</b>\nEnter the display name for the account(s):",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer("Invalid number (1-30). Please try again:", parse_mode="HTML")
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
            if state.get("last_photo_message_id"):
                try:
                    await bot.delete_message(chat_id=user_id, message_id=state["last_photo_message_id"])
                except Exception as e:
                    logger.warning(f"Failed to delete previous photo message: {e}")
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
        if res.get("accessToken") and res.get("user"):
            creds = {"email": state["signin_email"], "password": text}
            await store_token_and_show_card(msg, res, creds, bot)
        else:
            error_msg = res.get("errorMessage", "Unknown error.")
            await msg.edit_text(
                f"<b>Sign In Failed</b>\n\nError: {error_msg}",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        state["stage"] = "menu"
    else:
        return False
    
    user_signup_states[user_id] = state
    return True

# --- All Original Helper Functions ---

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
            if minutes < 1: return "just now"
            if minutes < 60: return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24: return f"{hours} hr ago"
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
    if "email" in user: card += f"\n\n<b>📧 Email:</b> <code>{user['email']}</code>"
    if "password" in user: card += f"\n<b>🔐 Password:</b> <code>{user['password']}</code>"
    if "token" in user: card += f"\n<b>🔑 Token:</b> <code>{user['token']}</code>"
    return card

def generate_email_variations(base_email: str, count: int = 50) -> List[str]:
    """Generate variations of an email address by adding dots to the username."""
    if '@' not in base_email: return []
    username, domain = base_email.split('@', 1)
    variations = {base_email}
    for i in range(1, len(username)):
        for positions in itertools.combinations(range(1, len(username)), i):
            if len(variations) >= count: return list(variations)
            new_username = list(username)
            for pos in reversed(positions):
                new_username.insert(pos, '.')
            variations.add(''.join(new_username) + '@' + domain)
    return list(variations)

async def check_email_exists(email: str) -> Tuple[bool, str]:
    """Check if an email is available for signup."""
    url = "https://api.meeff.com/user/checkEmail/v1"
    payload = {"email": email, "locale": "en"}
    headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'Accept-Encoding': "gzip", 'Content-Type': "application/json; charset=utf-8"}
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
    for email in pending_emails:
        if len(available_emails) >= num_accounts: break
        is_available, _ = await check_email_exists(email)
        if is_available: available_emails.append(email)
    if len(available_emails) < num_accounts:
        email_variations = generate_email_variations(base_email, num_accounts * 10)
        email_variations = [e for e in email_variations if e not in pending_emails]
        for email in email_variations:
            if len(available_emails) >= num_accounts: break
            is_available, _ = await check_email_exists(email)
            if is_available: available_emails.append(email)
    return available_emails

async def show_signup_preview(message: Message, user_id: int, state: Dict) -> None:
    """Show a preview of the signup configuration with exact emails to be used."""
    config = await get_signup_config(user_id) or {}
    if not all(k in config for k in ['email', 'password', 'gender', 'birth_year', 'nationality']):
        await message.edit_text("<b>Configuration Incomplete</b>\n\nYou must set up all details in 'Signup Config' first.", reply_markup=SIGNUP_MENU, parse_mode="HTML")
        return
    num_accounts = state.get('num_accounts', 1)
    pending_emails = [acc['email'] for acc in state.get('pending_accounts', [])]
    await message.edit_text("<i>Checking email availability...</i>", parse_mode="HTML")
    available_emails = await select_available_emails(config.get("email", ""), num_accounts, pending_emails)
    state["selected_emails"] = available_emails
    filter_nat = state.get('filter_nationality', 'All Countries')
    email_list = '\n'.join([f"{i+1}. <code>{email}</code>" for i, email in enumerate(available_emails)]) if available_emails else "No available emails found!"
    preview_text = (
        f"<b>Signup Preview</b>\n\n"
        f"<b>Name:</b> {state.get('name', 'N/A')}\n"
        f"<b>Photos:</b> {len(state.get('photos', []))} uploaded\n"
        f"<b>Number of Accounts:</b> {num_accounts}\n"
        f"<b>Gender:</b> {config.get('gender', 'N/A')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'N/A')}\n"
        f"<b>Nationality:</b> {config.get('nationality', 'N/A')}\n"
        f"<b>Filter Nationality:</b> {filter_nat}\n\n"
        f"<b>Emails to be Used ({len(available_emails)}):</b>\n{email_list}\n\n"
        f"Ready to create {len(available_emails)} account(s)?"
    )
    confirm_text = f"Create {len(available_emails)} Account{'s' if len(available_emails) != 1 else ''}"
    menu = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=confirm_text, callback_data="create_accounts_confirm")], [InlineKeyboardButton(text="Back", callback_data="signup_menu")]])
    await message.edit_text(preview_text, reply_markup=menu, parse_mode="HTML")
    user_signup_states[user_id] = state

async def signup_settings_command(message: Message, is_callback: bool = False) -> None:
    """Display and manage signup configuration settings."""
    user_id = message.chat.id
    config = await get_signup_config(user_id) or {}
    auto_signup_status = config.get('auto_signup', False)
    config_text = (
        "<b>Signup Configuration</b>\n\nSet default values and enable Auto Signup.\n\n"
        f"<b>Email:</b> <code>{config.get('email', 'Not set')}</code>\n"
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
    except Exception as e:
        logger.error(f"Error displaying signup settings: {e}")

async def upload_tg_photo(message: Message) -> Optional[str]:
    """Upload a Telegram photo to Meeff's server."""
    try:
        file = await message.bot.get_file(message.photo[-1].file_id)
        # Note: The token is exposed here. For production, consider a safer method.
        file_url = f"https://api.telegram.org/file/bot{message.bot.token}/{file.file_path}"
        async with aiohttp.ClientSession() as session:
            async with session.get(file_url) as resp:
                if resp.status != 200: return None
                return await meeff_upload_image(await resp.read())
    except Exception as e:
        logger.error(f"Error uploading Telegram photo: {e}")
        return None

async def meeff_upload_image(img_bytes: bytes) -> Optional[str]:
    """Upload an image to Meeff's S3 storage."""
    url = "https://api.meeff.com/api/upload/v1"
    payload = {"category": "profile", "count": 1, "locale": "en"}
    headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'Accept-Encoding': "gzip", 'Content-Type': "application/json; charset=utf-8"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json.dumps(payload), headers=headers) as resp:
                resp_json = await resp.json()
                data = resp_json.get("data", {})
                upload_info = data.get("uploadImageInfoList", [{}])[0]
                upload_url = data.get("Host")
                if not (upload_info and upload_url): return None
                fields = {k: upload_info.get(k) or data.get(k) for k in ["X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "Policy", "X-Amz-Signature"]}
                fields.update({k: data.get(k) for k in ["acl", "Content-Type", "x-amz-meta-uuid"]})
                fields["key"] = upload_info.get("key")
                if any(v is None for v in fields.values()): return None
                form = aiohttp.FormData()
                for k, v in fields.items():
                    form.add_field(k, v)
                form.add_field('file', img_bytes, filename='photo.jpg', content_type='image/jpeg')
                async with session.post(upload_url, data=form) as s3resp:
                    return upload_info.get("uploadImagePath") if s3resp.status in (200, 204) else None
    except Exception as e:
        logger.error(f"Error uploading image to Meeff: {e}")
        return None

async def try_signup(state: Dict, telegram_user_id: int) -> Dict:
    """Attempt to sign up a new user, using device info from the DB."""
    url = "https://api.meeff.com/user/register/email/v4"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, state["email"])
    logger.warning(f"SIGN UP using Device ID: {device_info.get('device_unique_id')} for email {state['email']}")
    base_payload = {
        "providerId": state["email"], "providerToken": state["password"], "name": state["name"], "gender": state["gender"],
        "birthYear": state.get("birth_year", 2004), "nationalityCode": state.get("nationality", "US"),
        "description": state["desc"], "photos": "|".join(state.get("photos", [])) or DEFAULT_PHOTOS, "locale": "en", "color": "777777",
        "birthMonth": 3, "birthDay": 1, "languages": "en,es,fr", "levels": "5,1,1",
        "purpose": "PB000000,PB000001", "purposeEtcDetail": "", "interest": "IS000001,IS000002,IS000003,IS000004",
    }
    payload = get_api_payload_with_device_info(base_payload, device_info)
    headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'Content-Type': "application/json; charset=utf-8"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                return await response.json()
    except Exception as e:
        logger.error(f"Error during signup: {e}")
        return {"errorMessage": "Failed to register account."}

async def try_signin(email: str, password: str, telegram_user_id: int) -> Dict:
    """Attempt to sign in, using device info from the DB."""
    url = "https://api.meeff.com/user/login/v4"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, email)
    logger.warning(f"SIGN IN using Device ID: {device_info.get('device_unique_id')} for email {email}")
    base_payload = {"provider": "email", "providerId": email, "providerToken": password, "locale": "en"}
    payload = get_api_payload_with_device_info(base_payload, device_info)
    headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'Content-Type': "application/json; charset=utf-8"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                resp_json = await response.json()
                if response.status != 200:
                    logger.error(f"Signin failed for {email}: Status {response.status}, Error: {resp_json.get('errorMessage', 'Unknown')}")
                return resp_json
    except Exception as e:
        logger.error(f"Error during signin for {email}: {e}")
        return {"errorMessage": "Failed to sign in."}

async def store_token_and_show_card(msg_obj: Message, login_result: Dict, creds: Dict, bot: Bot) -> None:
    """Store the access token and display the user card."""
    access_token = login_result.get("accessToken")
    user_data = login_result.get("user")
    if access_token and user_data:
        user_id = msg_obj.chat.id
        await set_token(user_id, access_token, user_data.get("name", creds.get("email")), creds.get("email"))
        user_data.update({"email": creds.get("email"), "password": creds.get("password"), "token": access_token})
        text = format_user_with_nationality(user_data)
        await set_info_card(user_id, access_token, text, creds.get("email"))
        await msg_obj.edit_text(
            "<b>Account Signed In & Saved!</b>\n\n" + text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    else:
        error_msg = login_result.get("errorMessage", "Token or user data not received.")
        await msg_obj.edit_text(
            f"<b>Error</b>\n\nFailed to save account: {error_msg}",
            parse_mode="HTML"
        )
