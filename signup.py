import aiohttp
import json
import random
import itertools
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dateutil import parser

# --- IMPORT THE NEW DEVICE INFO MODULE ---
from device_info import get_or_create_device_info_for_email, get_api_payload_with_device_info

# --- IMPORT THE NOW-ASYNC DB FUNCTIONS ---
from db import (
    set_token,
    set_info_card,
    set_signup_config,
    get_signup_config
)

# Logging configuration
logger = logging.getLogger(__name__)

# --- Configuration constants ---
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

# Global state
user_signup_states: Dict[int, Dict] = {}

# --- Inline Keyboard Menus ---
SIGNUP_MENU = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Sign Up", callback_data="signup_go"),
        InlineKeyboardButton(text="Sign In", callback_data="signin_go")
    ],
    [
        InlineKeyboardButton(text="Multi Signup", callback_data="multi_signup_go"),
        InlineKeyboardButton(text="Signup Config", callback_data="signup_settings")
    ],
    [InlineKeyboardButton(text="Back to Main Menu", callback_data="back_to_menu")]
])
MULTI_SIGNUP_CONFIRM = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Create 5 Accounts", callback_data="multi_signup_confirm")], [InlineKeyboardButton(text="Back", callback_data="signup_menu")]])
VERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Verify Email", callback_data="signup_verify")], [InlineKeyboardButton(text="Back", callback_data="signup_menu")]])
MULTI_VERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Verify All Emails", callback_data="multi_signup_verify")], [InlineKeyboardButton(text="Back", callback_data="signup_menu")]])
BACK_TO_SIGNUP = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back", callback_data="signup_menu")]])
BACK_TO_CONFIG = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back", callback_data="signup_settings")]])
DONE_PHOTOS = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Done", callback_data="signup_photos_done")], [InlineKeyboardButton(text="Back", callback_data="signup_menu")]])
MULTI_DONE_PHOTOS = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Done", callback_data="multi_signup_photos_done")], [InlineKeyboardButton(text="Back", callback_data="signup_menu")]])

# --- Helper Functions ---
def format_user_with_nationality(user: Dict) -> str:
    """Format user information into a displayable string."""
    def time_ago(dt_str: Optional[str]) -> str:
        if not dt_str: return "N/A"
        try:
            dt = parser.isoparse(dt_str)
            diff = datetime.now(timezone.utc) - dt
            minutes = int(diff.total_seconds() // 60)
            if minutes < 1: return "just now"
            if minutes < 60: return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24: return f"{hours} hr ago"
            days = hours // 24
            return f"{days} day(s) ago"
        except Exception: return "unknown"

    card = (
        f"<b>📱 Account Information</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>👤 Name:</b> {user.get('name', 'N/A')}\n"
        f"<b>🆔 ID:</b> <code>{user.get('_id', 'N/A')}</code>\n"
        f"<b>📝 Bio:</b> {user.get('description', 'N/A')}\n"
        f"<b>🎂 Birth Year:</b> {user.get('birthYear', 'N/A')}\n"
        f"<b>🌍 Country:</b> {user.get('nationalityCode', 'N/A')}\n"
        f"<b>🗣️ Languages:</b> {', '.join(user.get('languageCodes', [])) or 'N/A'}\n"
        f"<b>🕐 Last Active:</b> {time_ago(user.get('recentAt'))}\n"
    )
    if user.get('photoUrls'):
        card += f"<b>📸 Photos:</b> " + ' '.join([f"<a href='{url}'>📷</a>" for url in user.get('photoUrls', [])])
    if "email" in user: card += f"\n\n<b>📧 Email:</b> <code>{user['email']}</code>"
    if "password" in user: card += f"\n<b>🔐 Password:</b> <code>{user['password']}</code>"
    if "token" in user: card += f"\n<b>🔑 Token:</b> <code>{user['token']}</code>"
    return card

def generate_email_variations(base_email: str, count: int = 50) -> List[str]:
    """Generate variations of a Gmail address by adding dots."""
    if '@' not in base_email: return []
    username, domain = base_email.split('@', 1)
    variations = {base_email}
    for i in range(1, len(username)):
        for positions in itertools.combinations(range(1, len(username)), i):
            if len(variations) >= count: return list(variations)
            new_username_list = list(username)
            for pos in reversed(positions):
                new_username_list.insert(pos, '.')
            variations.add(''.join(new_username_list) + '@' + domain)
    return list(variations)

# --- API Interaction ---
async def check_email_exists(session: aiohttp.ClientSession, email: str) -> Tuple[bool, str]:
    """Check if an email is available for signup using a shared session."""
    url = "https://api.meeff.com/user/checkEmail/v1"
    payload = {"email": email, "locale": "en"}
    headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'Content-Type': "application/json; charset=utf-8"}
    try:
        async with session.post(url, json=payload, headers=headers) as response:
            resp_json = await response.json()
            if response.status == 406 or resp_json.get("errorCode") == "AlreadyInUse":
                return False, resp_json.get("errorMessage", "This email is already in use.")
            return True, ""
    except Exception as e:
        logger.error(f"Error checking email {email}: {e}")
        return False, "Failed to check email availability."

async def try_signup(state: Dict, telegram_user_id: int) -> Dict:
    """Attempt to sign up a new user."""
    url = "https://api.meeff.com/user/register/email/v4"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, state["email"])
    base_payload = {
        "providerId": state["email"], "providerToken": state["password"], "name": state["name"],
        "gender": state["gender"], "birthYear": state.get("birth_year", 2004),
        "nationalityCode": state.get("nationality", "US"), "description": state["desc"],
        "photos": "|".join(state.get("photos", [])) or DEFAULT_PHOTOS, "locale": "en",
        "color": "777777", "birthMonth": 3, "birthDay": 1, "languages": "en,es,fr",
        "levels": "5,1,1", "purpose": "PB000000,PB000001", "purposeEtcDetail": "",
        "interest": "IS000001,IS000002,IS000003,IS000004",
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
    """Attempt to sign in a user."""
    url = "https://api.meeff.com/user/login/v4"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, email)
    base_payload = {"provider": "email", "providerId": email, "providerToken": password, "locale": "en"}
    payload = get_api_payload_with_device_info(base_payload, device_info)
    headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'Content-Type': "application/json; charset=utf-8"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                return await response.json()
    except Exception as e:
        logger.error(f"Error during signin: {e}")
        return {"errorMessage": "Failed to sign in."}

# --- Command and Callback Handlers ---
async def signup_command(message: Message) -> None:
    """Handle the /signup command to initiate account creation."""
    user_signup_states[message.chat.id] = {"stage": "menu"}
    await message.answer("<b>Account Creation</b>\n\nChoose an option:", reply_markup=SIGNUP_MENU, parse_mode="HTML")

async def signup_settings_command(message: Message, is_callback: bool = False) -> None:
    """Display and manage signup configuration settings."""
    user_id = message.chat.id
    config = await get_signup_config(user_id) or {}
    auto_signup_status = config.get('auto_signup', False)
    config_text = (
        "<b>Signup Configuration</b>\n\n"
        f"<b>Email:</b> <code>{config.get('email', 'Not set')}</code>\n"
        f"<b>Password:</b> <code>{'*' * len(config.get('password', '')) if config.get('password') else 'Not set'}</code>\n"
        f"<b>Gender:</b> {config.get('gender', 'Not set')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'Not set')}\n"
        f"<b>Nationality:</b> {config.get('nationality', 'Not set')}\n"
        f"<b>Auto Signup:</b> {'ON' if auto_signup_status else 'OFF'}"
    )
    menu = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Auto Signup: {'Turn OFF' if auto_signup_status else 'Turn ON'}", callback_data="toggle_auto_signup")],
        [InlineKeyboardButton(text="Setup Details", callback_data="setup_signup_config")],
        [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
    ])
    if is_callback:
        await message.edit_text(config_text, reply_markup=menu, parse_mode="HTML")
    else:
        await message.answer(config_text, reply_markup=menu, parse_mode="HTML")

async def signup_callback_handler(callback: CallbackQuery) -> bool:
    """Handle callback queries for all signup-related actions."""
    user_id = callback.from_user.id
    state = user_signup_states.get(user_id, {})
    data = callback.data

    # Main menu navigation
    if data == "signup_menu":
        state["stage"] = "menu"
        await callback.message.edit_text("<b>Account Creation</b>", reply_markup=SIGNUP_MENU, parse_mode="HTML")
    elif data == "signup_go":
        config = await get_signup_config(user_id) or {}
        if config.get('auto_signup') and all(k in config for k in ['email', 'password', 'gender', 'birth_year', 'nationality']):
            state["stage"] = "auto_signup_ask_name"
            await callback.message.edit_text("<b>Auto Signup</b>\nEnter the display name:", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
        else:
            state["stage"] = "ask_email"
            await callback.message.edit_text("<b>Manual Signup</b>\nEnter your email address:", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    elif data == "signin_go":
        state["stage"] = "signin_email"
        await callback.message.edit_text("<b>Sign In</b>\nEnter your email:", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    
    # Signup Config
    elif data == "signup_settings":
        await signup_settings_command(callback.message, is_callback=True)
    elif data == "toggle_auto_signup":
        config = await get_signup_config(user_id) or {}
        config['auto_signup'] = not config.get('auto_signup', False)
        await set_signup_config(user_id, config)
        await callback.answer(f"Auto Signup turned {'ON' if config['auto_signup'] else 'OFF'}")
        await signup_settings_command(callback.message, is_callback=True)
    elif data == "setup_signup_config":
        state["stage"] = "config_email"
        await callback.message.edit_text("<b>Setup Email:</b>\nEnter base Gmail (e.g., yourname@gmail.com):", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")

    # Multi-Signup Flow
    elif data == "multi_signup_go":
        config = await get_signup_config(user_id) or {}
        if not all(k in config for k in ['email', 'password', 'gender', 'birth_year', 'nationality']):
            await callback.message.edit_text("<b>Config Incomplete</b>\nSet up all details in 'Signup Config' first.", reply_markup=SIGNUP_MENU, parse_mode="HTML")
        else:
            state["stage"] = "multi_ask_name"
            await callback.message.edit_text("<b>Multi Signup:</b>\nEnter the base name for accounts (e.g., 'David'):", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    elif data == "multi_signup_photos_done":
        await show_multi_signup_preview(callback.message, user_id, state)
    elif data == "multi_signup_confirm":
        await handle_multi_signup_creation(callback, user_id, state)
    elif data == "multi_signup_verify":
        await handle_multi_signup_verification(callback, user_id, state)

    # Single Signup Flow
    elif data == "signup_photos_done":
        await handle_single_signup_creation(callback, user_id, state)
    elif data == "signup_verify":
        await handle_single_signup_verification(callback, user_id, state)
    
    else: # Not a signup callback
        return False
    
    user_signup_states[user_id] = state
    await callback.answer()
    return True

async def signup_message_handler(message: Message) -> bool:
    """Handle text messages during the signup process."""
    user_id = message.from_user.id
    if user_id not in user_signup_states: return False
    
    state = user_signup_states[user_id]
    stage = state.get("stage", "")
    text = message.text.strip() if message.text else ""
    
    # --- Config Setup ---
    if stage.startswith("config_"):
        await handle_config_messages(message, user_id, state, stage, text)
    # --- Photo Uploads ---
    elif "ask_photos" in stage:
        await handle_photo_upload(message, state, stage)
    # --- Text Inputs ---
    elif stage == "ask_email":
        async with aiohttp.ClientSession() as session:
            ok, msg = await check_email_exists(session, text)
        if not ok:
            await message.answer(f"<b>Email Error:</b> {msg}", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
        else:
            state.update({"email": text, "stage": "ask_password"})
            await message.answer("<b>Password:</b> Enter a secure password:", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    elif stage == "ask_password":
        state.update({"password": text, "stage": "ask_name"})
        await message.answer("<b>Display Name:</b>", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    elif stage == "ask_name":
        state.update({"name": text, "stage": "ask_gender"})
        await message.answer("<b>Gender (M/F):</b>", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    elif stage == "ask_gender":
        if text.upper() not in ("M", "F"):
            await message.answer("Invalid. Please enter M or F:")
        else:
            state.update({"gender": text.upper(), "stage": "ask_desc"})
            await message.answer("<b>Profile Bio:</b>", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    elif stage == "ask_desc":
        state.update({"desc": text, "stage": "ask_photos", "photos": []})
        await message.answer("<b>Profile Photos:</b>\nSend up to 6. Click 'Done' when finished.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
    elif stage == "auto_signup_ask_name":
        state.update({"name": text, "desc": random.choice(DEFAULT_BIOS), "stage": "auto_signup_ask_photos", "photos": []})
        await message.answer("<b>Profile Photos:</b>\nSend up to 6. Click 'Done' when finished.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
    elif stage == "multi_ask_name":
        state.update({"multi_name": text, "stage": "multi_ask_photos", "multi_photos": []})
        await message.answer("<b>Profile Photos:</b>\nSend up to 6 for all accounts.", reply_markup=MULTI_DONE_PHOTOS, parse_mode="HTML")
    # --- Sign In ---
    elif stage == "signin_email":
        state.update({"signin_email": text, "stage": "signin_password"})
        await message.answer("<b>Password:</b>", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
    elif stage == "signin_password":
        msg = await message.answer("<b>Signing In...</b>", parse_mode="HTML")
        res = await try_signin(state["signin_email"], text, user_id)
        if res.get("accessToken"):
            await store_token_and_show_card(msg, res, {"email": state["signin_email"], "password": text})
        else:
            await msg.edit_text(f"<b>Sign In Failed:</b> {res.get('errorMessage', 'Unknown error.')}", reply_markup=SIGNUP_MENU, parse_mode="HTML")
        state["stage"] = "menu"
    else:
        return False

    user_signup_states[user_id] = state
    return True

# --- Helper functions for handlers ---
async def show_multi_signup_preview(message: Message, user_id: int, state: Dict):
    config = await get_signup_config(user_id) or {}
    email_variations = generate_email_variations(config.get("email", ""), 5)
    preview_text = (
        f"<b>Multi Signup Preview</b>\n\n"
        f"<b>Base Name:</b> {state.get('multi_name', 'N/A')}\n"
        f"<b>Photos:</b> {len(state.get('multi_photos', []))} uploaded\n"
        f"<b>Gender:</b> {config.get('gender', 'N/A')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'N/A')}\n"
        f"<b>Nationality:</b> {config.get('nationality', 'N/A')}\n\n"
        f"<b>Example Emails:</b>\n" + '\n'.join([f"• <code>{email}</code>" for email in email_variations])
    )
    await message.edit_text(preview_text, reply_markup=MULTI_SIGNUP_CONFIRM, parse_mode="HTML")

async def handle_multi_signup_creation(callback: CallbackQuery, user_id: int, state: Dict):
    await callback.message.edit_text("<b>Creating 5 Accounts...</b> This may take a moment.", parse_mode="HTML")
    config = await get_signup_config(user_id)
    email_variations = generate_email_variations(config["email"], 50)
    created_accounts, email_idx = [], 0

    async with aiohttp.ClientSession() as session:
        while len(created_accounts) < 5 and email_idx < len(email_variations):
            email = email_variations[email_idx]
            email_idx += 1
            is_available, _ = await check_email_exists(session, email)
            if not is_available: continue
            
            acc_state = {
                "email": email, "password": config["password"], "name": state['multi_name'],
                "gender": config["gender"], "desc": random.choice(DEFAULT_BIOS),
                "photos": state.get("multi_photos", []), "birth_year": config["birth_year"],
                "nationality": config["nationality"]
            }
            res = await try_signup(acc_state, user_id)
            if res.get("user", {}).get("_id"):
                created_accounts.append({"email": email, "name": acc_state["name"], "password": config["password"]})

    state["created_accounts"] = created_accounts
    result_text = f"<b>Multi Signup Results</b>\nCreated: <b>{len(created_accounts)}</b> accounts\n\n"
    if created_accounts:
        result_text += '\n'.join([f"• {a['name']} - <code>{a['email']}</code>" for a in created_accounts])
    result_text += "\n\nPlease verify all emails, then click below."
    await callback.message.edit_text(result_text, reply_markup=MULTI_VERIFY_BUTTON, parse_mode="HTML")

async def handle_multi_signup_verification(callback: CallbackQuery, user_id: int, state: Dict):
    created_accounts = state.get("created_accounts", [])
    if not created_accounts: return await callback.answer("No accounts to verify.", show_alert=True)
    
    await callback.message.edit_text("<b>Verifying Accounts...</b>", parse_mode="HTML")
    verified, failed = [], []
    
    async def verify_account(acc):
        res = await try_signin(acc["email"], acc["password"], user_id)
        if res.get("accessToken"):
            await set_token(user_id, res["accessToken"], acc["name"], acc["email"])
            user_data = res.get("user", {})
            user_data.update({"email": acc["email"], "password": acc["password"], "token": res["accessToken"]})
            await set_info_card(user_id, res["accessToken"], format_user_with_nationality(user_data), acc["email"])
            return "verified", acc
        return "failed", acc

    results = await asyncio.gather(*(verify_account(acc) for acc in created_accounts))
    for status, acc in results:
        (verified if status == "verified" else failed).append(acc)

    result_text = f"<b>Multi Signup Complete!</b>\nVerified & Saved: <b>{len(verified)}</b>\nPending: <b>{len(failed)}</b>\n"
    await callback.message.edit_text(result_text, reply_markup=SIGNUP_MENU, parse_mode="HTML")

async def handle_single_signup_creation(callback: CallbackQuery, user_id: int, state: Dict):
    is_auto = state.get("stage") == "auto_signup_ask_photos"
    msg = await callback.message.edit_text("Creating Account...", parse_mode="HTML")
    
    if is_auto:
        config = await get_signup_config(user_id)
        async with aiohttp.ClientSession() as session:
            available_email = next((e for e in generate_email_variations(config["email"]) if (await check_email_exists(session, e))[0]), None)
        
        if not available_email:
            return await msg.edit_text("<b>Signup Failed:</b> Could not find an available email variation.", reply_markup=SIGNUP_MENU, parse_mode="HTML")
        
        state.update({"email": available_email, "password": config["password"], "gender": config["gender"], "birth_year": config["birth_year"], "nationality": config["nationality"]})

    res = await try_signup(state, user_id)
    if res.get("user", {}).get("_id"):
        state["creds"] = {"email": state["email"], "password": state["password"], "name": state["name"]}
        state["stage"] = "await_verify"
        await msg.edit_text("<b>Account Created!</b> Please verify your email, then click below.", reply_markup=VERIFY_BUTTON, parse_mode="HTML")
    else:
        state["stage"] = "menu"
        await msg.edit_text(f"<b>Signup Failed:</b> {res.get('errorMessage', 'Registration failed.')}", reply_markup=SIGNUP_MENU, parse_mode="HTML")

async def handle_single_signup_verification(callback: CallbackQuery, user_id: int, state: Dict):
    creds = state.get("creds")
    if not creds: return await callback.answer("Signup info lost. Please start over.", show_alert=True)
    
    await callback.message.edit_text("<b>Verifying Account...</b>", parse_mode="HTML")
    res = await try_signin(creds['email'], creds['password'], user_id)
    if res.get("accessToken"):
        await store_token_and_show_card(callback.message, res, creds)
    else:
        await callback.message.edit_text(f"<b>Login Failed:</b> {res.get('errorMessage', 'Verification likely pending')}", reply_markup=VERIFY_BUTTON, parse_mode="HTML")

async def handle_config_messages(message: Message, user_id: int, state: Dict, stage: str, text: str):
    config = await get_signup_config(user_id) or {}
    next_stage, prompt = None, None
    
    if stage == "config_email":
        if '@' not in text: return await message.answer("Invalid Email. Try again:", reply_markup=BACK_TO_CONFIG)
        config["email"] = text
        next_stage, prompt = "config_password", "<b>Password:</b>"
    elif stage == "config_password":
        config["password"] = text
        next_stage, prompt = "config_gender", "<b>Gender (M/F):</b>"
    elif stage == "config_gender":
        if text.upper() not in ("M", "F"): return await message.answer("Invalid. Please enter M or F:")
        config["gender"] = text.upper()
        next_stage, prompt = "config_birth_year", "<b>Birth Year (e.g., 2000):</b>"
    elif stage == "config_birth_year":
        try:
            year = int(text)
            if not 1950 <= year <= 2010: raise ValueError
            config["birth_year"] = year
            next_stage, prompt = "config_nationality", "<b>Nationality (2-letter code, e.g., US):</b>"
        except ValueError: return await message.answer("Invalid Year (1950-2010). Try again:")
    elif stage == "config_nationality":
        if len(text) != 2: return await message.answer("Invalid. Enter a 2-letter code:")
        config["nationality"] = text.upper()
        state["stage"] = "menu"
        await set_signup_config(user_id, config)
        await message.answer("<b>Configuration Saved!</b>", parse_mode="HTML")
        await signup_settings_command(message) # Show updated menu
        return

    state["stage"] = next_stage
    await set_signup_config(user_id, config)
    await message.answer(prompt, reply_markup=BACK_TO_CONFIG, parse_mode="HTML")

async def handle_photo_upload(message: Message, state: Dict, stage: str):
    if message.content_type != "photo": return await message.answer("Please send a photo or click 'Done'.")
    
    photo_key = "multi_photos" if stage == "multi_ask_photos" else "photos"
    if len(state.get(photo_key, [])) >= 6: return await message.answer("Max 6 photos. Click 'Done'.")
    
    photo_url = await upload_tg_photo(message)
    if photo_url:
        state.setdefault(photo_key, []).append(photo_url)
        await message.answer(f"Photo uploaded ({len(state[photo_key])}/6).")
    else:
        await message.answer("Upload Failed. Please try again.")

async def upload_tg_photo(message: Message) -> Optional[str]:
    """Upload a Telegram photo to Meeff's server."""
    try:
        file = await message.bot.get_file(message.photo[-1].file_id)
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
    headers = {'User-Agent': "okhttp/5.0.0-alpha.14", 'Content-Type': "application/json; charset=utf-8"}
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
                for k, v in fields.items(): form.add_field(k, v)
                form.add_field('file', img_bytes, filename='photo.jpg', content_type='image/jpeg')
                
                async with session.post(upload_url, data=form) as s3resp:
                    return upload_info.get("uploadImagePath") if s3resp.status in (200, 204) else None
    except Exception as e:
        logger.error(f"Error uploading image to Meeff: {e}")
        return None

async def store_token_and_show_card(msg_obj: Message, login_result: Dict, creds: Dict):
    """Store the access token and display the user card."""
    access_token = login_result.get("accessToken")
    user_data = login_result.get("user")
    if access_token and user_data:
        user_id = msg_obj.chat.id
        name = user_data.get("name", creds.get("name", creds.get("email")))
        await set_token(user_id, access_token, name, creds.get("email"))
        
        user_data.update({"email": creds.get("email"), "password": creds.get("password"), "token": access_token})
        text = format_user_with_nationality(user_data)
        
        await set_info_card(user_id, access_token, text, creds.get("email"))
        await msg_obj.edit_text("<b>Account Saved!</b>\n\n" + text, parse_mode="HTML", disable_web_page_preview=True)
    else:
        await msg_obj.edit_text("<b>Error:</b> Token not received, failed to save account.", parse_mode="HTML")
