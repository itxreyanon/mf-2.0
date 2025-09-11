import asyncio
import aiohttp
from aiogram import types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Import the now ASYNC db functions
from db import get_user_filters, set_user_filters, get_tokens
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# Global state for filter settings (remains synchronous as it's in-memory)
user_filter_states = {}

async def get_meeff_filter_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Main Meeff Filter menu. Now async to fetch tokens."""
    # ASYNC CHANGE: Await the database call
    tokens = await get_tokens(user_id)
    
    filter_enabled = user_filter_states.get(user_id, {}).get('request_filter_enabled', True)
    filter_status = "✅ Enabled" if filter_enabled else "❌ Disabled"
    
    keyboard = [[InlineKeyboardButton(text=f"Request Filter: {filter_status}", callback_data="toggle_request_filter")]]
    
    ACCOUNTS_PER_ROW = 2
    row = []
    for i, token_data in enumerate(tokens):
        account_name = token_data.get('name', f'Account {i+1}')
        # OPTIMIZATION: Filters are already part of the token data from the DB
        filters = token_data.get('filters', {})
        nationality = filters.get('filterNationalityCode', '')
        display_text = f"{account_name} ({nationality})" if nationality else account_name
        
        row.append(InlineKeyboardButton(text=display_text, callback_data=f"account_filter_{i}"))
        
        if len(row) == ACCOUNTS_PER_ROW:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="settings_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_account_filter_keyboard(account_index: int) -> InlineKeyboardMarkup:
    """Filter options for a specific account."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚻 Gender", callback_data=f"account_filter_gender_{account_index}"),
            InlineKeyboardButton(text="🎂 Age", callback_data=f"account_filter_age_{account_index}"),
            InlineKeyboardButton(text="🌍 Nationality", callback_data=f"account_filter_nationality_{account_index}")
        ],
        [InlineKeyboardButton(text="⬅️ Back to Accounts", callback_data="meeff_filter_main")]
    ])

def get_gender_keyboard(account_index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="All", callback_data=f"account_gender_all_{account_index}")],
        [InlineKeyboardButton(text="Male", callback_data=f"account_gender_male_{account_index}")],
        [InlineKeyboardButton(text="Female", callback_data=f"account_gender_female_{account_index}")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data=f"account_filter_back_{account_index}")]
    ])

def get_age_keyboard(account_index: int) -> InlineKeyboardMarkup:
    keyboard = []
    ages = list(range(18, 41))
    for i in range(0, len(ages), 5):
        row = [InlineKeyboardButton(text=str(age), callback_data=f"account_age_{age}_{account_index}") for age in ages[i:i+5]]
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data=f"account_filter_back_{account_index}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_nationality_keyboard(account_index: int) -> InlineKeyboardMarkup:
    countries = [
        ("RU", "🇷🇺"), ("UA", "🇺🇦"), ("BY", "🇧🇾"), ("IR", "🇮🇷"), ("PH", "🇵🇭"),
        ("PK", "🇵🇰"), ("US", "🇺🇸"), ("IN", "🇮🇳"), ("DE", "🇩🇪"), ("FR", "🇫🇷"),
        ("BR", "🇧🇷"), ("CN", "🇨🇳"), ("JP", "🇯🇵"), ("KR", "🇰🇷"), ("CA", "🇨🇦"),
    ]
    keyboard = [[InlineKeyboardButton(text="All Countries", callback_data=f"account_nationality_all_{account_index}")]]
    
    NATIONALITIES_PER_ROW = 5
    for i in range(0, len(countries), NATIONALITIES_PER_ROW):
        row = [InlineKeyboardButton(text=f"{flag} {code}", callback_data=f"account_nationality_{code}_{account_index}") for code, flag in countries[i:i+NATIONALITIES_PER_ROW]]
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data=f"account_filter_back_{account_index}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def apply_filter_for_account(token: str, user_id: int):
    """Apply stored filters for a specific account. Now async."""
    try:
        # ASYNC CHANGE: Await the database call
        user_filters = await get_user_filters(user_id, token) or {}
        
        filter_data = {
            "filterGenderType": user_filters.get("filterGenderType", 7),
            "filterBirthYearFrom": user_filters.get("filterBirthYearFrom", 1980),
            "filterBirthYearTo": 2006,
            "filterDistance": 510,
            "filterLanguageCodes": user_filters.get("filterLanguageCodes", ""),
            "filterNationalityBlock": 0,
            "filterNationalityCode": user_filters.get("filterNationalityCode", ""),
            "locale": "en"
        }
        
        url = "https://api.meeff.com/user/updateFilter/v1"
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info({
            'User-Agent': "okhttp/4.12.0",
            'meeff-access-token': token,
            'content-type': "application/json; charset=utf-8"
        }, device_info)
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=filter_data, headers=headers) as response:
                if response.status == 200:
                    logging.info(f"Filter applied successfully for token: {token[:10]}...")
                    return True
                else:
                    logging.warning(f"Failed to apply filter for token: {token[:10]}... Status: {response.status}")
                    return False
    except Exception as e:
        logging.error(f"Error applying filter: {e}", exc_info=True)
        return False

async def set_account_filter(callback_query: types.CallbackQuery):
    """Handle account-specific filter settings. Now fully async."""
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    # ASYNC CHANGE: Await the database call
    tokens = await get_tokens(user_id)
    
    # --- Main Menu and Toggling ---
    if data == "toggle_request_filter":
        user_filter_states.setdefault(user_id, {})
        current_status = user_filter_states[user_id].get('request_filter_enabled', True)
        user_filter_states[user_id]['request_filter_enabled'] = not current_status
        await callback_query.message.edit_reply_markup(reply_markup=await get_meeff_filter_main_keyboard(user_id))
        await callback_query.answer(f"Request filter {'enabled' if not current_status else 'disabled'}")
        return

    if data == "meeff_filter_main":
        await callback_query.message.edit_text("🎛️ <b>Meeff Filter Settings</b>", reply_markup=await get_meeff_filter_main_keyboard(user_id), parse_mode="HTML")
        await callback_query.answer()
        return

    # --- Navigation to Filter Sub-menus ---
    data_parts = data.split('_')
    action_type = f"{data_parts[0]}_{data_parts[1]}"

    if action_type == "account_filter":
        try:
            account_index = int(data_parts[2])
            if account_index < len(tokens):
                account_name = tokens[account_index].get('name', f'Account {account_index + 1}')
                target_menu = data_parts[2] if len(data_parts) > 3 else 'main'
                
                if target_menu == "gender":
                    await callback_query.message.edit_text("🚻 <b>Select Gender Filter:</b>", reply_markup=get_gender_keyboard(account_index), parse_mode="HTML")
                elif target_menu == "age":
                    await callback_query.message.edit_text("🎂 <b>Select Age Filter:</b>", reply_markup=get_age_keyboard(account_index), parse_mode="HTML")
                elif target_menu == "nationality":
                    await callback_query.message.edit_text("🌍 <b>Select Nationality Filter:</b>", reply_markup=get_nationality_keyboard(account_index), parse_mode="HTML")
                elif target_menu == "back": # back from a sub-menu to the account menu
                    await callback_query.message.edit_text(f"🎛️ <b>Settings for {account_name}</b>", reply_markup=get_account_filter_keyboard(account_index), parse_mode="HTML")
                else: # main account menu
                    await callback_query.message.edit_text(f"🎛️ <b>Settings for {account_name}</b>", reply_markup=get_account_filter_keyboard(account_index), parse_mode="HTML")
            await callback_query.answer()
        except (ValueError, IndexError):
            pass # Ignore malformed callbacks
        return

    # --- Applying Filter Selections ---
    try:
        filter_category = data_parts[1]
        value = data_parts[2]
        account_index = int(data_parts[3])
    except (ValueError, IndexError):
        await callback_query.answer("Invalid action.")
        return

    if account_index < len(tokens):
        token_data = tokens[account_index]
        token = token_data['token']
        account_name = token_data.get('name', f'Account {account_index + 1}')
        
        # ASYNC CHANGE: Await the database call
        user_filters = await get_user_filters(user_id, token) or {}
        
        # Update filter based on category
        if filter_category == "gender":
            gender_map = {"male": 6, "female": 5, "all": 7}
            user_filters["filterGenderType"] = gender_map.get(value, 7)
            display_value = value.capitalize()
        elif filter_category == "age":
            user_filters["filterBirthYearFrom"] = datetime.now().year - int(value)
            display_value = value
        elif filter_category == "nationality":
            user_filters["filterNationalityCode"] = "" if value == "all" else value
            display_value = "All Countries" if value == "all" else value.upper()
        else:
            return # Unknown filter category
            
        # ASYNC CHANGE: Await the database call
        await set_user_filters(user_id, token, user_filters)
        await apply_filter_for_account(token, user_id)
        
        await callback_query.message.edit_text(
            f"✅ <b>Filter updated for {account_name}</b>\n\n{filter_category.capitalize()} set to: <b>{display_value}</b>",
            reply_markup=get_account_filter_keyboard(account_index), parse_mode="HTML")
    await callback_query.answer()

async def meeff_filter_command(message: types.Message):
    """Main command to show Meeff Filter settings."""
    await message.answer(
        "🎛️ <b>Meeff Filter Settings</b>\n\nConfigure filters for each account:",
        reply_markup=await get_meeff_filter_main_keyboard(message.from_user.id),
        parse_mode="HTML")

def is_request_filter_enabled(user_id: int) -> bool:
    """Check if request filter is enabled for a user."""
    return user_filter_states.get(user_id, {}).get('request_filter_enabled', True)

# --- Legacy function redirects for backward compatibility ---
async def set_filter(callback_query: types.CallbackQuery):
    return await set_account_filter(callback_query)

async def filter_command(message: types.Message):
    return await meeff_filter_command(message)
