from aiogram import types
import requests
import json
from db import get_current_account, get_user_filters, set_user_filters, get_tokens
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import aiohttp

def get_filter_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Gender", callback_data="filter_gender")],
        [InlineKeyboardButton(text="Age", callback_data="filter_age")],
        [InlineKeyboardButton(text="Nationality", callback_data="filter_nationality")],
        [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
    ])
    return keyboard

def get_gender_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="All Gender", callback_data="filter_gender_all")],
        [InlineKeyboardButton(text="Male", callback_data="filter_gender_male")],
        [InlineKeyboardButton(text="Female", callback_data="filter_gender_female")],
        [InlineKeyboardButton(text="Back", callback_data="filter_back")]
    ])
    return keyboard

def get_age_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(age), callback_data=f"filter_age_{age}") for age in range(18, 41)],
        [InlineKeyboardButton(text="Back", callback_data="filter_back")]
    ])
    return keyboard

def get_nationality_keyboard():
    countries = [
        ("RU", "🇷🇺"), ("UA", "🇺🇦"), ("BY", "🇧🇾"), ("IR", "🇮🇷"), ("PH", "🇵🇭"),
        ("PK", "🇵🇰"), ("US", "🇺🇸"), ("IN", "🇮🇳"), ("DE", "🇩🇪"), ("FR", "🇫🇷"),
        ("BR", "🇧🇷"), ("CN", "🇨🇳"), ("JP", "🇯🇵"), ("KR", "🇰🇷"), ("CA", "🇨🇦"),
        ("AU", "🇦🇺"), ("IT", "🇮🇹"), ("ES", "🇪🇸"), ("ZA", "🇿🇦")
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="All Countries", callback_data="filter_nationality_all")],
        *[[InlineKeyboardButton(text=f"{flag} {country}", callback_data=f"filter_nationality_{country}")] for country, flag in countries],
        [InlineKeyboardButton(text="Back", callback_data="filter_back")]
    ])
    return keyboard

def get_account_filter_keyboard(user_id):
    """Get keyboard for selecting account to set filters"""
    tokens = get_tokens(user_id)
    if not tokens:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_menu")]
        ])
    
    keyboard_buttons = []
    for token_data in tokens:
        name = token_data.get("name", "Unknown Account")
        # Truncate long names
        display_name = name[:20] + "..." if len(name) > 20 else name
        callback_data = f"account_filter_{tokens.index(token_data)}"
        keyboard_buttons.append([InlineKeyboardButton(text=f"🌍 {display_name}", callback_data=callback_data)])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

def get_nationality_keyboard_for_account(account_index):
    """Get nationality keyboard for specific account"""
    countries = [
        ("RU", "🇷🇺"), ("UA", "🇺🇦"), ("BY", "🇧🇾"), ("IR", "🇮🇷"), ("PH", "🇵🇭"),
        ("PK", "🇵🇰"), ("US", "🇺🇸"), ("IN", "🇮🇳"), ("DE", "🇩🇪"), ("FR", "🇫🇷"),
        ("BR", "🇧🇷"), ("CN", "🇨🇳"), ("JP", "🇯🇵"), ("KR", "🇰🇷"), ("CA", "🇨🇦"),
        ("AU", "🇦🇺"), ("IT", "🇮🇹"), ("ES", "🇪🇸"), ("ZA", "🇿🇦")
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌍 All Countries", callback_data=f"account_nationality_{account_index}_all")],
        *[[InlineKeyboardButton(text=f"{flag} {country}", callback_data=f"account_nationality_{account_index}_{country}")] for country, flag in countries],
        [InlineKeyboardButton(text="🔙 Back", callback_data="account_filters_menu")]
    ])
    return keyboard

async def apply_nationality_filter(token, nationality_code=""):
    """Apply nationality filter using the account's token"""
    url = "https://api.meeff.com/user/updateFilter/v1"
    headers = {
        'User-Agent': "okhttp/4.12.0",
        'Accept-Encoding': "gzip",
        'meeff-access-token': token,
        'content-type': "application/json; charset=utf-8"
    }
    
    filter_data = {
        "filterGenderType": 7,  # All genders
        "filterBirthYearFrom": 1979,
        "filterBirthYearTo": 2006,
        "filterDistance": 510,
        "filterLanguageCodes": "",
        "filterNationalityBlock": 0,
        "filterNationalityCode": nationality_code,
        "locale": "en"
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=filter_data, headers=headers) as response:
                if response.status == 200:
                    return True, "Filter applied successfully"
                else:
                    return False, f"Failed to apply filter: {response.status}"
        except Exception as e:
            return False, f"Error applying filter: {str(e)}"

async def set_filter(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    token = get_current_account(user_id)
    
    if not token:
        await callback_query.message.edit_text("No active account found. Please set an account before updating filters.")
        return
    
    # Retrieve stored user filters
    user_filters = get_user_filters(user_id, token) or {}

    # Filter data to be updated
    filter_data = {
        "filterGenderType": user_filters.get("filterGenderType", 7),
        "filterBirthYearFrom": user_filters.get("filterBirthYearFrom", 1979),
        "filterBirthYearTo": 2006,      # Default value
        "filterDistance": 510,          # Default value
        "filterLanguageCodes": user_filters.get("filterLanguageCodes", ""),
        "filterNationalityBlock": user_filters.get("filterNationalityBlock", 0),
        "filterNationalityCode": user_filters.get("filterNationalityCode", ""),
        "locale": "en"  # Ensure locale is always included
    }

    if callback_query.data == "filter_gender":
        await callback_query.message.edit_text("Select Gender:", reply_markup=get_gender_keyboard())
        return

    if callback_query.data.startswith("filter_gender_"):
        gender = callback_query.data.split("_")[-1]
        if gender == "male":
            filter_data["filterGenderType"] = 6
        elif gender == "female":
            filter_data["filterGenderType"] = 5
        elif gender == "all":
            filter_data["filterGenderType"] = 7
        message = f"Filter updated: Gender set to {gender.capitalize()}"
    
    elif callback_query.data == "filter_age":
        await callback_query.message.edit_text("Select Age:", reply_markup=get_age_keyboard())
        return

    elif callback_query.data.startswith("filter_age_"):
        age = int(callback_query.data.split("_")[-1])
        current_year = 2024  # Current year
        filter_data["filterBirthYearFrom"] = current_year - age
        filter_data["filterBirthYearTo"] = 2006
        message = f"Filter updated: Age set to {age}"
    
    elif callback_query.data == "filter_nationality":
        await callback_query.message.edit_text("Select Nationality:", reply_markup=get_nationality_keyboard())
        return

    elif callback_query.data.startswith("filter_nationality_"):
        nationality = callback_query.data.split("_")[-1]
        if nationality == "all":
            filter_data["filterNationalityCode"] = ""
        else:
            filter_data["filterNationalityCode"] = nationality
        message = f"Filter updated: Nationality set to {nationality}"

    elif callback_query.data == "filter_back":
        # Return to main filter menu
        await callback_query.message.edit_text(
            "Set your filter preferences:",
            reply_markup=get_filter_keyboard()
        )
        return

    # Update user filters in storage
    set_user_filters(user_id, token, filter_data)

    url = "https://api.meeff.com/user/updateFilter/v1"
    headers = {
        'User-Agent': "okhttp/4.12.0",
        'Accept-Encoding': "gzip",
        'meeff-access-token': token,
        'content-type': "application/json; charset=utf-8"
    }

    print(f"Updating filters with data: {filter_data}")  # Debug statement
    response = requests.post(url, data=json.dumps(filter_data), headers=headers)
    if response.status_code == 200:
        await callback_query.message.edit_text(message)
    else:
        await callback_query.message.edit_text(f"Failed to update filter. Response: {response.text}")

async def filter_command(message: types.Message):
    await message.answer("Set your filter preferences:", reply_markup=get_filter_keyboard())

async def account_filters_command(message: types.Message):
    """Command to show account filter selection"""
    user_id = message.from_user.id
    await message.answer(
        "🌍 <b>Account Nationality Filters</b>\n\n"
        "Select an account to set its nationality filter:",
        reply_markup=get_account_filter_keyboard(user_id),
        parse_mode="HTML"
    )

async def handle_account_filter_callback(callback_query: types.CallbackQuery):
    """Handle account filter callbacks"""
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    if data == "account_filters_menu":
        await callback_query.message.edit_text(
            "🌍 <b>Account Nationality Filters</b>\n\n"
            "Select an account to set its nationality filter:",
            reply_markup=get_account_filter_keyboard(user_id),
            parse_mode="HTML"
        )
        await callback_query.answer()
        return True
    
    if data.startswith("account_filter_"):
        account_index = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if account_index < len(tokens):
            account_name = tokens[account_index].get("name", "Unknown Account")
            await callback_query.message.edit_text(
                f"🌍 <b>Set Nationality Filter</b>\n\n"
                f"Account: <b>{account_name}</b>\n\n"
                f"Select nationality filter for this account:",
                reply_markup=get_nationality_keyboard_for_account(account_index),
                parse_mode="HTML"
            )
        await callback_query.answer()
        return True
    
    if data.startswith("account_nationality_"):
        parts = data.split("_")
        account_index = int(parts[2])
        nationality = parts[3] if len(parts) > 3 else "all"
        
        tokens = get_tokens(user_id)
        if account_index < len(tokens):
            token_data = tokens[account_index]
            token = token_data["token"]
            account_name = token_data.get("name", "Unknown Account")
            
            # Apply the nationality filter
            nationality_code = "" if nationality == "all" else nationality
            success, message = await apply_nationality_filter(token, nationality_code)
            
            if success:
                # Store the filter in the token data
                current_filters = token_data.get("filters", {})
                current_filters["filterNationalityCode"] = nationality_code
                set_user_filters(user_id, token, current_filters)
                
                nationality_display = "All Countries" if nationality == "all" else nationality
                await callback_query.message.edit_text(
                    f"✅ <b>Filter Updated</b>\n\n"
                    f"Account: <b>{account_name}</b>\n"
                    f"Nationality: <b>{nationality_display}</b>\n\n"
                    f"Filter has been applied successfully!",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔙 Back to Accounts", callback_data="account_filters_menu")]
                    ]),
                    parse_mode="HTML"
                )
            else:
                await callback_query.message.edit_text(
                    f"❌ <b>Filter Update Failed</b>\n\n"
                    f"Account: <b>{account_name}</b>\n"
                    f"Error: {message}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔙 Back", callback_data="account_filters_menu")]
                    ]),
                    parse_mode="HTML"
                )
        
        await callback_query.answer()
        return True
    
    return False