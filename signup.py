import asyncio
import aiohttp
import json
import random
import itertools
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dateutil import parser
import logging
from device_info import get_or_create_device_info_for_email, get_api_payload_with_device_info
from db import (
    set_token,
    set_info_card,
    set_signup_config,
    get_signup_config,
    get_used_email_variations,
    add_used_email_variation
)

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


def get_nationality_keyboard() -> InlineKeyboardMarkup:
    """Creates an inline keyboard for selecting nationality during multi-signup."""
    countries = [
        ("RU", "🇷🇺"), ("UA", "🇺🇦"), ("BY", "🇧🇾"), ("IR", "🇮🇷"), ("PH", "🇵🇭"),
        ("PK", "🇵🇰"), ("US", "🇺🇸"), ("IN", "🇮🇳"), ("DE", "🇩🇪"), ("FR", "🇫🇷"),
        ("BR", "🇧🇷"), ("CN", "🇨🇳"), ("JP", "🇯🇵"), ("KR", "🇰🇷"), ("CA", "🇨🇦"),
        ("AU", "🇦🇺"), ("IT", "🇮🇹"), ("ES", "🇪🇸"), ("ZA", "🇿🇦"), ("TR", "🇹🇷")
    ]
    keyboard = []
    # "All Nationalities" button that selects a default (US)
    keyboard.append([InlineKeyboardButton(text="🌎 All Nationalities (Default)", callback_data="multi_nationality_US")])

    NATIONALITIES_PER_ROW = 5
    row = []
    for code, flag in countries:
        row.append(InlineKeyboardButton(
            text=f"{flag} {code}",
            callback_data=f"multi_nationality_{code}"
        ))
        if len(row) == NATIONALITIES_PER_ROW:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton(text="Back", callback_data="signup_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_multi_signup_confirm_keyboard(count: int) -> InlineKeyboardMarkup:
    """Generates the confirmation keyboard for multi-signup with a dynamic account count."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Create {count} Accounts", callback_data="multi_signup_confirm")],
        [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
    ])


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

VERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Verify Email", callback_data="signup_verify")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

REVERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Re-verify Pending Accounts", callback_data="reverify_pending")],
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

MULTI_DONE_PHOTOS = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Done", callback_data="multi_signup_photos_done")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
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
                if response.status == 406 or resp_json.get("errorCode") == "AlreadyInUse":
                    return False, resp_json.get("errorMessage", "This email is already in use.")
                return True, ""
        except Exception as e:
            logger.error(f"Error checking email {email}: {e}")
            return False, "Failed to check email availability."

async def show_multi_signup_preview(message: Message, user_id: int, state: Dict) -> None:
    """Show a preview of the multi-signup configuration."""
    config = await get_signup_config(user_id) or {}
    count = state.get("multi_count", 1)
    names = state.get("multi_names", ['N/A'])
    email_variations = generate_email_variations(config.get("email", ""), count)
    preview_text = (
        f"<b>Multi Signup Preview ({count} Accounts)</b>\n\n"
        f"<b>Names:</b> {', '.join(names)}\n"
        f"<b>Nationality:</b> {state.get('multi_nationality', 'N/A')}\n"
        f"<b>Photos:</b> {len(state.get('multi_photos', []))} uploaded\n"
        f"<b>Gender:</b> {config.get('gender', 'N/A')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'N/A')}\n\n"
        f"<b>Example Email Variations:</b>\n" +
        '\n'.join([f"• {email}" for email in email_variations[:5]]) +
        f"\n\nConfirm to create these {count} accounts."
    )
    await message.edit_text(preview_text, reply_markup=get_multi_signup_confirm_keyboard(count), parse_mode="HTML")

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
        "Turn <b>Auto Signup ON</b> to use these settings automatically for single signups."
    )
    menu = InlineKeyboardMarkup(inline_keyboard=[
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
    config = await get_signup_config(user_id) or {}

    if data == "signup_settings":
        await signup_settings_command(callback.message, is_callback=True)
    elif data == "toggle_auto_signup":
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
        if not all(k in config for k in ['email', 'password', 'gender', 'birth_year']):
            await callback.message.edit_text(
                "<b>Configuration Incomplete</b>\n\nPlease set up Email, Password, Gender, and Birth Year in <b>Signup Config</b> first.",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        else:
            state["stage"] = "multi_ask_count"
            user_signup_states[user_id] = state
            await callback.message.edit_text(
                "<b>Multi Signup (Step 1/4)</b>\n\nEnter the number of accounts you want to create (e.g., 10).",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
    elif data.startswith("multi_nationality_"):
        code = data.split("_")[-1]
        state["multi_nationality"] = code
        state["stage"] = "multi_ask_names"
        user_signup_states[user_id] = state
        await callback.message.edit_text(
            (
                f"<b>Multi Signup (Step 3/4)</b>\n\n"
                f"Nationality set to: <b>{code}</b>\n\n"
                "Enter the name(s) for the accounts:\n"
                "• For one name on all accounts, just type the name (e.g., <code>David</code>).\n"
                "• For different names, type names separated by commas (e.g., <code>David, Mike, John</code>)."
            ),
            parse_mode="HTML"
        )
    elif data == "multi_signup_photos_done":
        await show_multi_signup_preview(callback.message, user_id, state)
    elif data == "multi_signup_confirm":
        await callback.message.edit_text("<b>Creating Accounts</b>...", parse_mode="HTML")
        
        count = state.get("multi_count", 1)
        names = state.get('multi_names', [])
        nationality = state.get('multi_nationality', 'US')
        photos = state.get('multi_photos', [])
        
        # New robust logic to check for available emails
        base_email = config.get("email")
        if not base_email:
            await callback.message.edit_text("<b>Error:</b> Base email not configured.", reply_markup=SIGNUP_MENU, parse_mode="HTML")
            return True

        email_variations = generate_email_variations(base_email, count * 5)
        used_emails = await get_used_email_variations(user_id, base_email)
        
        available_emails = []
        for email in email_variations:
            if email in used_emails:
                continue
            is_available, _ = await check_email_exists(email)
            if is_available:
                available_emails.append(email)
            if len(available_emails) >= count:
                break
        
        if not available_emails:
            await callback.message.edit_text("<b>Error:</b> Could not find any available email variations.", reply_markup=SIGNUP_MENU, parse_mode="HTML")
            return True
        
        created_accounts = []
        for i, email in enumerate(available_emails):
            current_name = names[i % len(names)].strip()
            acc_state = {
                "email": email,
                "password": config.get("password"),
                "name": current_name,
                "gender": config.get("gender"),
                "desc": get_random_bio(),
                "photos": photos,
                "birth_year": config.get("birth_year", 2000),
                "nationality": nationality
            }
            res = await try_signup(acc_state, user_id)
            if res.get("user", {}).get("_id"):
                await add_used_email_variation(user_id, base_email, email)
                created_accounts.append({
                    "email": email,
                    "name": current_name,
                    "password": config.get("password")
                })
        
        pending_accounts = []
        verified_accounts = []
        for acc in created_accounts:
            res = await try_signin(acc["email"], acc["password"], user_id)
            if res.get("accessToken"):
                await set_token(user_id, res["accessToken"], acc["name"], acc["email"])
                if res.get("user"):
                    res["user"].update({
                        "email": acc["email"],
                        "password": acc["password"],
                        "token": res["accessToken"]
                    })
                    await set_info_card(user_id, res["accessToken"], format_user_with_nationality(res["user"]), acc["email"])
                verified_accounts.append(acc)
            else:
                pending_accounts.append(acc)

        # Update persistent storage with pending accounts
        current_config = await get_signup_config(user_id) or {}
        current_config['pending_accounts'] = pending_accounts
        await set_signup_config(user_id, current_config)

        result_text = (
            f"<b>Multi Signup Results</b>\n\n"
            f"<b>Created:</b> {len(created_accounts)} of {count} accounts\n"
            f"<b>Verified & Saved:</b> {len(verified_accounts)}\n"
            f"<b>Pending Verification:</b> {len(pending_accounts)}\n\n"
        )
        if verified_accounts:
            result_text += "<b>Added:</b>\n" + '\n'.join([f"• {a['name']}" for a in verified_accounts])
        if pending_accounts:
            result_text += "\n<b>Still Pending:</b>\n" + '\n'.join([f"• <code>{a['email']}</code>" for a in pending_accounts])
            await callback.message.edit_text(result_text, reply_markup=REVERIFY_BUTTON, parse_mode="HTML")
        else:
            await callback.message.edit_text(result_text, reply_markup=SIGNUP_MENU, parse_mode="HTML")
    elif data == "reverify_pending":
        await callback.message.edit_text("<b>Re-verifying Accounts</b>...", parse_mode="HTML")
        config = await get_signup_config(user_id) or {}
        pending_accounts = config.get("pending_accounts", [])
        
        if not pending_accounts:
            await callback.answer("No accounts to re-verify.", show_alert=True)
            await callback.message.edit_text("<b>All accounts verified!</b>", reply_markup=SIGNUP_MENU, parse_mode="HTML")
            return True

        new_pending, verified_count = [], 0
        for acc in pending_accounts:
            res = await try_signin(acc["email"], acc["password"], user_id)
            if res.get("accessToken"):
                await set_token(user_id, res["accessToken"], acc["name"], acc["email"])
                if res.get("user"):
                    res["user"].update({"email": acc["email"], "password": acc["password"], "token": res["accessToken"]})
                    await set_info_card(user_id, res["accessToken"], format_user_with_nationality(res["user"]), acc["email"])
                verified_count += 1
            else:
                new_pending.append(acc)
        
        config['pending_accounts'] = new_pending
        await set_signup_config(user_id, config)
        
        result_text = (
            f"<b>Re-verification Complete!</b>\n\n"
            f"<b>Successfully Verified:</b> {verified_count}\n"
            f"<b>Still Pending:</b> {len(new_pending)}\n"
        )
        if new_pending:
            result_text += "\n<b>Pending Accounts:</b>\n" + '\n'.join([f"• <code>{a['email']}</code>" for a in new_pending])
            await callback.message.edit_text(result_text, reply_markup=REVERIFY_BUTTON, parse_mode="HTML")
        else:
            result_text += "\n\n<b>All pending accounts have been verified!</b>"
            await callback.message.edit_text(result_text, reply_markup=SIGNUP_MENU, parse_mode="HTML")
    elif data == "signin_go":
        state["stage"] = "signin_email"
        await callback.message.edit_text(
            "<b>Sign In</b>\n\nEnter your email address:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
    elif data == "signup_verify":
        creds = state.get("creds")
        if not creds:
            await callback.answer("No signup info. Please start over.", show_alert=True)
            return True
        await callback.message.edit_text("<b>Verifying Account</b>...", parse_mode="HTML")
        res = await try_signin(creds['email'], creds['password'], user_id)
        if res.get("accessToken"):
            await store_token_and_show_card(callback.message, res, creds)
        else:
            await callback.message.edit_text(
                f"<b>Login Failed</b>\n\nError: {res.get('errorMessage', 'Verification likely pending')}",
                reply_markup=VERIFY_BUTTON,
                parse_mode="HTML"
            )
    elif data == "signup_photos_done":
        is_auto = state.get("stage") == "auto_signup_ask_photos"
        msg = await callback.message.edit_text(
            "<b>Auto Signup</b>\n\nFinding available email and creating account..." if is_auto else "<b>Creating Account</b>...",
            parse_mode="HTML"
        )
        if is_auto:
            available_email = None
            if config.get("email"):
                async def find_available():
                    for email in generate_email_variations(config.get("email"), 50):
                        if (await check_email_exists(email))[0]:
                            return email
                    return None
                available_email = await find_available()

            if not available_email:
                await msg.edit_text(
                    "<b>Signup Failed</b>\n\nCould not find an available email variation.",
                    reply_markup=SIGNUP_MENU,
                    parse_mode="HTML"
                )
                return True
            state.update({
                "email": available_email,
                "password": config.get("password"),
                "gender": config.get("gender"),
                "birth_year": config.get("birth_year"),
                "nationality": config.get("nationality")
            })
        
        res = await try_signup(state, user_id)
        if res.get("user", {}).get("_id"):
            state["creds"] = {"email": state["email"], "password": state["password"], "name": state["name"]}
            state["stage"] = "await_verify"
            await msg.edit_text(
                "<b>Account Created!</b>\n\nPlease verify your email, then click the button below.",
                reply_markup=VERIFY_BUTTON,
                parse_mode="HTML"
            )
        else:
            state["stage"] = "menu"
            await msg.edit_text(
                f"<b>Signup Failed</b>\n\nError: {res.get('errorMessage', 'Registration failed.')}",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
    else:
        await callback.answer()
        return False
    
    user_signup_states[user_id] = state
    await callback.answer()
    return True

async def signup_message_handler(message: Message) -> bool:
    """Handle messages during the signup process."""
    user_id = message.from_user.id
    if user_id not in user_signup_states:
        return False
    state = user_signup_states[user_id]
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
    elif stage in ["ask_email", "ask_password", "ask_name", "ask_gender", "ask_desc"]:
        if stage == "ask_email":
            ok, msg = await check_email_exists(text)
            if not ok:
                await message.answer(f"<b>Email Error</b>\n\n{msg}", reply_markup=BACK_TO_SIGNUP, parse_mode="HTML")
                return True
            state["email"] = text
            state["stage"] = "ask_password"
            await message.answer(
                "<b>Password Setup</b>\nEnter a secure password:",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
        elif stage == "ask_password":
            state["password"] = text
            state["stage"] = "ask_name"
            await message.answer(
                "<b>Display Name</b>\nEnter your display name:",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
        elif stage == "ask_name":
            state["name"] = text
            state["stage"] = "ask_gender"
            await message.answer(
                "<b>Gender Selection</b>\nEnter your gender (M/F):",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
        elif stage == "ask_gender":
            if text.upper() not in ("M", "F"):
                await message.answer("Invalid. Please enter M or F:", parse_mode="HTML")
                return True
            state["gender"] = text.upper()
            state["stage"] = "ask_desc"
            await message.answer(
                "<b>Profile Description</b>\nEnter your profile bio:",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
        elif stage == "ask_desc":
            state["desc"] = text
            state["stage"] = "ask_photos"
            state["photos"] = []
            await message.answer(
                "<b>Profile Photos</b>\n\nSend up to 6 photos. Click 'Done' when finished.",
                reply_markup=DONE_PHOTOS,
                parse_mode="HTML"
            )
    elif stage == "multi_ask_count":
        try:
            count = int(text)
            if not 1 <= count <= 25:
                raise ValueError()
            state["multi_count"] = count
            state["stage"] = "multi_select_nationality"
            await message.answer(
                "<b>Multi Signup (Step 2/4)</b>\n\nSelect the nationality for all accounts:",
                reply_markup=get_nationality_keyboard(),
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer("Invalid number. Please enter a number between 1 and 25.", parse_mode="HTML")
            return True
    elif stage == "multi_ask_names":
        count = state.get("multi_count", 1)
        if ',' in text:
            names = [name.strip() for name in text.split(',') if name.strip()]
            if not names:
                await message.answer("Invalid input. Please enter at least one name.", parse_mode="HTML")
                return True
            state["multi_names"] = names
        else:
            single_name = text.strip()
            if not single_name:
                await message.answer("Name cannot be empty. Please try again.", parse_mode="HTML")
                return True
            state["multi_names"] = [single_name]
        
        state["multi_photos"] = []
        state["stage"] = "multi_ask_photos"
        await message.answer(
            f"<b>Multi Signup (Step 4/4)</b>\n\nSend up to 6 photos to be used for all {count} accounts. Click 'Done' when finished.",
            reply_markup=MULTI_DONE_PHOTOS,
            parse_mode="HTML"
        )
    elif stage in ["ask_photos", "auto_signup_ask_photos", "multi_ask_photos"]:
        if message.content_type != "photo":
            await message.answer("Please send a photo or click 'Done'.", parse_mode="HTML")
            return True
        
        photo_key = "photos"
        done_markup = DONE_PHOTOS
        if stage == "multi_ask_photos":
            photo_key = "multi_photos"
            done_markup = MULTI_DONE_PHOTOS
        
        if len(state.get(photo_key, [])) >= 6:
            await message.answer("Photo limit reached (6). Click Done.", parse_mode="HTML")
            return True
        photo_url = await upload_tg_photo(message)
        if photo_url:
            if photo_key not in state:
                state[photo_key] = []
            state[photo_key].append(photo_url)
            await message.answer(f"Photo uploaded ({len(state[photo_key])}/6).", reply_markup=done_markup, parse_mode="HTML")
        else:
            await message.answer("Upload Failed. Please try again.", parse_mode="HTML")
    elif stage == "auto_signup_ask_name":
        state["name"] = text
        state["desc"] = get_random_bio()
        state["photos"] = []
        state["stage"] = "auto_signup_ask_photos"
        await message.answer(
            "<b>Profile Photos</b>\n\nSend up to 6 photos. Click 'Done' when finished.",
            reply_markup=DONE_PHOTOS,
            parse_mode="HTML"
        )
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
        if res.get("accessToken"):
            await store_token_and_show_card(msg, res, {"email": state["signin_email"], "password": text})
        else:
            await msg.edit_text(
                f"<b>Sign In Failed</b>\n\nError: {res.get('errorMessage', 'Unknown error.')}",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        state["stage"] = "menu"
    else:
        return False
    
    user_signup_states[user_id] = state
    return True

async def upload_tg_photo(message: Message) -> Optional[str]:
    """Upload a Telegram photo to Meeff's server."""
    try:
        file = await message.bot.get_file(message.photo[-1].file_id)
        file_url = f"https://api.telegram.org/file/bot{message.bot.token}/{file.file_path}"
        async with aiohttp.ClientSession() as session:
            async with session.get(file_url) as resp:
                if resp.status != 200:
                    return None
                return await meeff_upload_image(await resp.read())
    except Exception as e:
        logger.error(f"Error uploading Telegram photo: {e}")
        return None

async def meeff_upload_image(img_bytes: bytes) -> Optional[str]:
    """Upload an image to Meeff's S3 storage."""
    url = "https://api.meeff.com/api/upload/v1"
    payload = {"category": "profile", "count": 1, "locale": "en"}
    headers = {
        'User-Agent': "okhttp/5.0.0-alpha.14",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json; charset=utf-8"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json.dumps(payload), headers=headers) as resp:
                resp_json = await resp.json()
                data = resp_json.get("data", {})
                upload_info = data.get("uploadImageInfoList", [{}])[0]
                upload_url = data.get("Host")
                if not (upload_info and upload_url):
                    return None
                fields = {
                    k: upload_info.get(k) or data.get(k)
                    for k in ["X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "Policy", "X-Amz-Signature"]
                }
                fields.update({
                    k: data.get(k)
                    for k in ["acl", "Content-Type", "x-amz-meta-uuid"]
                })
                fields["key"] = upload_info.get("key")
                if any(v is None for v in fields.values()):
                    return None
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
    """Attempt to sign in, using device info from the DB."""
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


async def store_token_and_show_card(msg_obj: Message, login_result: Dict, creds: Dict) -> None:
    """Store the access token and display the user card."""
    access_token = login_result.get("accessToken")
    user_data = login_result.get("user")
    if access_token and user_data:
        user_id = msg_obj.chat.id
        await set_token(user_id, access_token, user_data.get("name", creds.get("email")), creds.get("email"))
        user_data.update({
            "email": creds.get("email"),
            "password": creds.get("password"),
            "token": access_token
        })
        text = format_user_with_nationality(user_data)
        await set_info_card(user_id, access_token, text, creds.get("email"))
        await msg_obj.edit_text(
            "<b>Account Signed In & Saved!</b>\n\n" + text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=SIGNUP_MENU
        )
    else:
        await msg_obj.edit_text(
            "<b>Error</b>\n\nToken not received, failed to save account.",
            parse_mode="HTML"
        )
