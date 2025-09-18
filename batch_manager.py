import asyncio
from typing import Dict, List, Optional
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from db import get_tokens, set_account_active, get_active_tokens, set_user_filters, get_user_filters
from filters import apply_filter_for_account
import math

# Batch management state
user_batch_states: Dict[int, Dict] = {}

ACCOUNTS_PER_BATCH = 12

def get_batch_number(account_index: int) -> int:
    """Get batch number for an account (1-based)"""
    return (account_index // ACCOUNTS_PER_BATCH) + 1

def get_accounts_in_batch(tokens: List[Dict], batch_number: int) -> List[Dict]:
    """Get all accounts in a specific batch"""
    start_idx = (batch_number - 1) * ACCOUNTS_PER_BATCH
    end_idx = start_idx + ACCOUNTS_PER_BATCH
    return tokens[start_idx:end_idx]

def get_total_batches(tokens: List[Dict]) -> int:
    """Get total number of batches"""
    return math.ceil(len(tokens) / ACCOUNTS_PER_BATCH) if tokens else 0

async def get_batch_management_menu(user_id: int, current_page: int = 1) -> InlineKeyboardMarkup:
    """Main batch management menu with pagination"""
    tokens = await get_tokens(user_id)
    total_batches = get_total_batches(tokens)
    
    if total_batches == 0:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="No Accounts Found", callback_data="dummy")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="settings_menu")]
        ])
    
    keyboard = []
    
    # Show 5 batches per page
    batches_per_page = 5
    start_batch = (current_page - 1) * batches_per_page + 1
    end_batch = min(start_batch + batches_per_page - 1, total_batches)
    
    # Batch buttons
    for batch_num in range(start_batch, end_batch + 1):
        batch_accounts = get_accounts_in_batch(tokens, batch_num)
        active_count = sum(1 for acc in batch_accounts if acc.get('active', True))
        total_count = len(batch_accounts)
        
        # Get nationality filter for this batch (from first account)
        nationality = ""
        if batch_accounts:
            first_token = batch_accounts[0]['token']
            filters = await get_user_filters(user_id, first_token) or {}
            nationality = filters.get('filterNationalityCode', '')
        
        nationality_display = f" ({nationality})" if nationality else ""
        status_text = f"Batch {batch_num} - {active_count}/{total_count}{nationality_display}"
        
        keyboard.append([
            InlineKeyboardButton(
                text=status_text,
                callback_data=f"batch_manage_{batch_num}"
            )
        ])
    
    # Navigation buttons
    nav_buttons = []
    if current_page > 1:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"batch_page_{current_page-1}"))
    
    total_pages = math.ceil(total_batches / batches_per_page)
    if current_page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"batch_page_{current_page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Global controls
    keyboard.append([
        InlineKeyboardButton(text="🔛 All Batches ON", callback_data="batch_all_on"),
        InlineKeyboardButton(text="🔴 All Batches OFF", callback_data="batch_all_off")
    ])
    
    keyboard.append([
        InlineKeyboardButton(text="🔙 Back", callback_data="settings_menu")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def get_single_batch_menu(user_id: int, batch_number: int) -> InlineKeyboardMarkup:
    """Menu for managing a single batch"""
    tokens = await get_tokens(user_id)
    batch_accounts = get_accounts_in_batch(tokens, batch_number)
    
    if not batch_accounts:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="batch_management")]
        ])
    
    active_count = sum(1 for acc in batch_accounts if acc.get('active', True))
    total_count = len(batch_accounts)
    
    # Get current nationality filter
    first_token = batch_accounts[0]['token']
    filters = await get_user_filters(user_id, first_token) or {}
    current_nationality = filters.get('filterNationalityCode', 'All Countries')
    if current_nationality == '':
        current_nationality = 'All Countries'
    
    keyboard = [
        [InlineKeyboardButton(
            text=f"📊 Batch {batch_number} Status: {active_count}/{total_count} Active",
            callback_data="dummy"
        )],
        [
            InlineKeyboardButton(text="🔛 Turn All ON", callback_data=f"batch_{batch_number}_all_on"),
            InlineKeyboardButton(text="🔴 Turn All OFF", callback_data=f"batch_{batch_number}_all_off")
        ],
        [InlineKeyboardButton(
            text=f"🌍 Nationality: {current_nationality}",
            callback_data=f"batch_{batch_number}_nationality"
        )],
        [InlineKeyboardButton(text="👥 View Accounts", callback_data=f"batch_{batch_number}_view")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="batch_management")]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def get_batch_accounts_view(user_id: int, batch_number: int) -> InlineKeyboardMarkup:
    """View individual accounts in a batch"""
    tokens = await get_tokens(user_id)
    batch_accounts = get_accounts_in_batch(tokens, batch_number)
    
    keyboard = []
    
    for i, account in enumerate(batch_accounts):
        status_emoji = "✅" if account.get('active', True) else "❌"
        account_name = account.get('name', f'Account {i+1}')[:15]
        
        keyboard.append([
            InlineKeyboardButton(
                text=f"{status_emoji} {account_name}",
                callback_data=f"batch_account_toggle_{batch_number}_{i}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton(text="🔙 Back", callback_data=f"batch_manage_{batch_number}")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_batch_nationality_keyboard(batch_number: int) -> InlineKeyboardMarkup:
    """Nationality selection for batch"""
    countries = [
        ("", "🌍 All Countries"),
        ("RU", "🇷🇺 Russia"), ("UA", "🇺🇦 Ukraine"), ("BY", "🇧🇾 Belarus"),
        ("IR", "🇮🇷 Iran"), ("PH", "🇵🇭 Philippines"), ("PK", "🇵🇰 Pakistan"),
        ("US", "🇺🇸 USA"), ("IN", "🇮🇳 India"), ("DE", "🇩🇪 Germany"),
        ("FR", "🇫🇷 France"), ("BR", "🇧🇷 Brazil"), ("CN", "🇨🇳 China"),
        ("JP", "🇯🇵 Japan"), ("KR", "🇰🇷 Korea"), ("CA", "🇨🇦 Canada"),
        ("AU", "🇦🇺 Australia"), ("IT", "🇮🇹 Italy"), ("ES", "🇪🇸 Spain"),
        ("ZA", "🇿🇦 South Africa"), ("TR", "🇹🇷 Turkey")
    ]
    
    keyboard = []
    
    # Add countries in rows of 2
    for i in range(0, len(countries), 2):
        row = []
        for j in range(2):
            if i + j < len(countries):
                code, name = countries[i + j]
                row.append(InlineKeyboardButton(
                    text=name,
                    callback_data=f"batch_{batch_number}_set_nationality_{code}"
                ))
        keyboard.append(row)
    
    keyboard.append([
        InlineKeyboardButton(text="🔙 Back", callback_data=f"batch_manage_{batch_number}")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def apply_batch_nationality_filter(user_id: int, batch_number: int, nationality_code: str):
    """Apply nationality filter to all accounts in a batch"""
    tokens = await get_tokens(user_id)
    batch_accounts = get_accounts_in_batch(tokens, batch_number)
    
    success_count = 0
    for account in batch_accounts:
        token = account['token']
        
        # Update filters in database
        current_filters = await get_user_filters(user_id, token) or {}
        current_filters['filterNationalityCode'] = nationality_code
        await set_user_filters(user_id, token, current_filters)
        
        # Apply filter if account is active
        if account.get('active', True):
            success = await apply_filter_for_account(token, user_id)
            if success:
                success_count += 1
    
    return success_count, len(batch_accounts)

async def toggle_batch_status(user_id: int, batch_number: int, status: bool):
    """Turn all accounts in a batch on or off"""
    tokens = await get_tokens(user_id)
    batch_accounts = get_accounts_in_batch(tokens, batch_number)
    
    for account in batch_accounts:
        await set_account_active(user_id, account['token'], status)
    
    return len(batch_accounts)

async def toggle_all_batches_status(user_id: int, status: bool):
    """Turn all accounts in all batches on or off"""
    tokens = await get_tokens(user_id)
    
    for account in tokens:
        await set_account_active(user_id, account['token'], status)
    
    return len(tokens)

async def handle_batch_callback(callback_query: CallbackQuery) -> bool:
    """Handle all batch management callbacks"""
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    if data == "batch_management":
        await callback_query.message.edit_text(
            "🗂️ <b>Batch Management</b>\n\n"
            f"Manage your accounts in batches of {ACCOUNTS_PER_BATCH}.\n"
            "Each batch can have its own nationality filter and on/off status.",
            reply_markup=await get_batch_management_menu(user_id),
            parse_mode="HTML"
        )
        await callback_query.answer()
        return True
    
    elif data.startswith("batch_page_"):
        page = int(data.split("_")[-1])
        await callback_query.message.edit_text(
            "🗂️ <b>Batch Management</b>\n\n"
            f"Manage your accounts in batches of {ACCOUNTS_PER_BATCH}.\n"
            "Each batch can have its own nationality filter and on/off status.",
            reply_markup=await get_batch_management_menu(user_id, page),
            parse_mode="HTML"
        )
        await callback_query.answer()
        return True
    
    elif data.startswith("batch_manage_"):
        batch_number = int(data.split("_")[-1])
        tokens = await get_tokens(user_id)
        batch_accounts = get_accounts_in_batch(tokens, batch_number)
        
        await callback_query.message.edit_text(
            f"🗂️ <b>Batch {batch_number} Management</b>\n\n"
            f"Managing {len(batch_accounts)} accounts in this batch.\n"
            "You can control all accounts together or individually.",
            reply_markup=await get_single_batch_menu(user_id, batch_number),
            parse_mode="HTML"
        )
        await callback_query.answer()
        return True
    
    elif data.startswith("batch_") and "_all_on" in data:
        batch_number = int(data.split("_")[1])
        count = await toggle_batch_status(user_id, batch_number, True)
        await callback_query.answer(f"✅ Turned ON {count} accounts in Batch {batch_number}")
        
        # Refresh the menu
        await callback_query.message.edit_reply_markup(
            reply_markup=await get_single_batch_menu(user_id, batch_number)
        )
        return True
    
    elif data.startswith("batch_") and "_all_off" in data:
        batch_number = int(data.split("_")[1])
        count = await toggle_batch_status(user_id, batch_number, False)
        await callback_query.answer(f"❌ Turned OFF {count} accounts in Batch {batch_number}")
        
        # Refresh the menu
        await callback_query.message.edit_reply_markup(
            reply_markup=await get_single_batch_menu(user_id, batch_number)
        )
        return True
    
    elif data == "batch_all_on":
        count = await toggle_all_batches_status(user_id, True)
        await callback_query.answer(f"✅ Turned ON all {count} accounts")
        
        # Refresh the menu
        await callback_query.message.edit_reply_markup(
            reply_markup=await get_batch_management_menu(user_id)
        )
        return True
    
    elif data == "batch_all_off":
        count = await toggle_all_batches_status(user_id, False)
        await callback_query.answer(f"❌ Turned OFF all {count} accounts")
        
        # Refresh the menu
        await callback_query.message.edit_reply_markup(
            reply_markup=await get_batch_management_menu(user_id)
        )
        return True
    
    elif data.startswith("batch_") and "_nationality" in data:
        batch_number = int(data.split("_")[1])
        await callback_query.message.edit_text(
            f"🌍 <b>Set Nationality Filter for Batch {batch_number}</b>\n\n"
            "Choose a nationality filter that will be applied to all accounts in this batch:",
            reply_markup=get_batch_nationality_keyboard(batch_number),
            parse_mode="HTML"
        )
        await callback_query.answer()
        return True
    
    elif data.startswith("batch_") and "_set_nationality_" in data:
        parts = data.split("_")
        batch_number = int(parts[1])
        nationality_code = parts[4] if len(parts) > 4 else ""
        
        # Show processing message
        await callback_query.message.edit_text(
            f"⏳ <b>Applying Nationality Filter...</b>\n\n"
            f"Setting nationality filter for Batch {batch_number}...",
            parse_mode="HTML"
        )
        
        success_count, total_count = await apply_batch_nationality_filter(user_id, batch_number, nationality_code)
        nationality_display = nationality_code.upper() if nationality_code else "All Countries"
        
        await callback_query.answer(f"✅ Applied {nationality_display} filter to Batch {batch_number}")
        
        # Go back to batch menu
        tokens = await get_tokens(user_id)
        batch_accounts = get_accounts_in_batch(tokens, batch_number)
        
        await callback_query.message.edit_text(
            f"🗂️ <b>Batch {batch_number} Management</b>\n\n"
            f"Managing {len(batch_accounts)} accounts in this batch.\n"
            f"✅ Nationality filter applied: {nationality_display}",
            reply_markup=await get_single_batch_menu(user_id, batch_number),
            parse_mode="HTML"
        )
        return True
    
    elif data.startswith("batch_") and "_view" in data:
        batch_number = int(data.split("_")[1])
        await callback_query.message.edit_text(
            f"👥 <b>Batch {batch_number} Accounts</b>\n\n"
            "Click on any account to toggle its status:",
            reply_markup=await get_batch_accounts_view(user_id, batch_number),
            parse_mode="HTML"
        )
        await callback_query.answer()
        return True
    
    elif data.startswith("batch_account_toggle_"):
        parts = data.split("_")
        batch_number = int(parts[3])
        account_index = int(parts[4])
        
        tokens = await get_tokens(user_id)
        batch_accounts = get_accounts_in_batch(tokens, batch_number)
        
        if account_index < len(batch_accounts):
            account = batch_accounts[account_index]
            current_status = account.get('active', True)
            new_status = not current_status
            
            await set_account_active(user_id, account['token'], new_status)
            
            status_text = "ON" if new_status else "OFF"
            await callback_query.answer(f"✅ {account.get('name', 'Account')} turned {status_text}")
            
            # Refresh the view
            await callback_query.message.edit_reply_markup(
                reply_markup=await get_batch_accounts_view(user_id, batch_number)
            )
        
        return True
    
    return False

async def auto_assign_new_account_to_batch(user_id: int, token: str):
    """Automatically assign a new account to the appropriate batch"""
    tokens = await get_tokens(user_id)
    
    # Find the account that was just added
    account_index = None
    for i, account in enumerate(tokens):
        if account['token'] == token:
            account_index = i
            break
    
    if account_index is not None:
        batch_number = get_batch_number(account_index)
        return batch_number
    
    return None