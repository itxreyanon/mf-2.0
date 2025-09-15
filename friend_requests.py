import asyncio
import aiohttp
import logging
import html
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db import get_individual_spam_filter, bulk_add_sent_ids, get_active_tokens, get_current_account, get_already_sent_ids
from filters import apply_filter_for_account, is_request_filter_enabled
from collections import defaultdict
from dateutil import parser
from datetime import datetime, timezone

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ✅ Speed configuration
PER_USER_DELAY = 0.5      # Delay between sending requests to individual users
PER_BATCH_DELAY = 1       # Delay between fetching new batches of users
EMPTY_BATCH_DELAY = 2     # Delay after receiving an empty batch
PER_ERROR_DELAY = 5       # Delay after a network or API error


# Global state variables for friend requests
user_states = defaultdict(lambda: {
    "running": False,
    "status_message_id": None,
    "pinned_message_id": None,
    "total_added_friends": 0,
    "batch_index": 0,
    "stopped": False,
})

# Inline keyboards for friend request operations
stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Stop Requests", callback_data="stop")]
])

async def fetch_users(session, token):
    """Fetch users from the API for friend requests."""
    url = "https://api.meeff.com/user/explore/v2?lng=-112.0613784790039&unreachableUserIds=&lat=33.437198638916016&locale=en"
    headers = {
        'User-Agent': "okhttp/4.12.0",
        'X-Device-Info': "iPhone15Pro-iOS17.5.1-6.6.2",
        'meeff-access-token': token
    }
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 429:
                logging.error("Request limit exceeded while fetching users.")
                return None  # Special value for rate limiting
            if response.status != 200:
                logging.error(f"Failed to fetch users: {response.status}")
                return []
            return (await response.json()).get("users", [])
    except Exception as e:
        logging.error(f"Fetch users failed: {e}")
        return []

def format_user(user):
    """Formats user data into a readable HTML string."""
    def time_ago(dt_str):
        if not dt_str: return "N/A"
        try:
            # Ensure the datetime object is timezone-aware (UTC)
            dt = parser.isoparse(dt_str).astimezone(timezone.utc)
            now = datetime.now(timezone.utc)
            minutes = int((now - dt).total_seconds() // 60)
            if minutes < 1: return "just now"
            if minutes < 60: return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24: return f"{hours} hr ago"
            return f"{hours // 24} day(s) ago"
        except Exception:
            return "unknown"

    last_active = time_ago(user.get("recentAt"))
    height_raw = str(user.get('height', 'N/A'))
    height = f"{height_raw.replace('|', ' ')}" if '|' in height_raw else height_raw
    
    return (
        f"<b>Name:</b> {html.escape(user.get('name', 'N/A'))}\n"
        f"<b>ID:</b> <code>{html.escape(user.get('_id', 'N/A'))}</code>\n"
        f"<b>Nationality:</b> {html.escape(user.get('nationalityCode', 'N/A'))}\n"
        f"<b>Height:</b> {html.escape(height)}\n"
        f"<b>Description:</b> {html.escape(user.get('description', 'N/A'))}\n"
        f"<b>Birth Year:</b> {user.get('birthYear', 'N/A')}\n"
        f"<b>Last Active:</b> {last_active}"
    )

async def process_users(session, users, token, user_id, bot, token_name, already_sent_ids, lock):
    """Process a batch of users, sending friend requests and handling spam filters atomically."""
    state = user_states[user_id]
    added_count = 0
    filtered_count = 0
    limit_reached = False
    
    is_spam_filter_enabled = await get_individual_spam_filter(user_id, "request")
    ids_to_persist = []

    for user in users:
        if not state["running"]:
            break

        user_id_to_check = user["_id"]

        # Atomically check and claim user if spam filter is on
        if is_spam_filter_enabled:
            async with lock:
                if user_id_to_check in already_sent_ids:
                    filtered_count += 1
                    continue
                # Claim this ID for the entire session
                already_sent_ids.add(user_id_to_check)
        
        # Send friend request
        url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id_to_check}&isOkay=1"
        headers = {"meeff-access-token": token, "Connection": "keep-alive"}

        try:
            async with session.get(url, headers=headers) as response:
                data = await response.json()

                if data.get("errorCode") == "LikeExceeded":
                    logging.info(f"Daily like limit reached for {token_name}.")
                    limit_reached = True
                    break

                if is_spam_filter_enabled:
                    ids_to_persist.append(user_id_to_check)

                details = format_user(user)
                await bot.send_message(chat_id=user_id, text=details, parse_mode="HTML", disable_web_page_preview=True)
                
                added_count += 1
                state["total_added_friends"] += 1
                await asyncio.sleep(PER_USER_DELAY)
        
        except Exception as e:
            logging.error(f"Error processing user with {token_name}: {e}")
            await asyncio.sleep(PER_ERROR_DELAY)
    
    # Persist all successfully sent IDs to the database in one go
    if is_spam_filter_enabled and ids_to_persist:
        await bulk_add_sent_ids(user_id, "request", ids_to_persist)

    return limit_reached, added_count, filtered_count


async def run_requests(user_id, bot, target_channel_id):
    """Main function to run the request process for a single token."""
    state = user_states[user_id]
    state.update({"total_added_friends": 0, "batch_index": 0, "running": True, "stopped": False})
    
    token = await get_current_account(user_id)
    if not token:
        await bot.edit_message_text(chat_id=user_id, message_id=state["status_message_id"], text="No active account found.")
        state["running"] = False
        return

    tokens = await get_active_tokens(user_id)
    token_name = next((t.get("name", "Default") for t in tokens if t["token"] == token), "Default")
    
    # Pre-load sent IDs from DB once
    already_sent_ids = await get_already_sent_ids(user_id, "request")
    lock = asyncio.Lock() # For consistency with multi-token logic

    async with aiohttp.ClientSession() as session:
        while state["running"]:
            try:
                # --- THIS IS THE FIX (await removed) ---
                if is_request_filter_enabled(user_id):
                    await apply_filter_for_account(token, user_id)
                    await asyncio.sleep(1)
                
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=state["status_message_id"],
                    text=f"{token_name}: Requests sent: {state['total_added_friends']}",
                    reply_markup=stop_markup
                )

                users = await fetch_users(session, token)
                state["batch_index"] += 1
                
                if not users:
                    logging.info(f"No users found for batch {state['batch_index']}.")
                    if state["batch_index"] > 10:
                        await bot.edit_message_text(
                            chat_id=user_id, message_id=state["status_message_id"],
                            text=f"{token_name}: No more users found. Total: {state['total_added_friends']}"
                        )
                        state["running"] = False
                        break
                    await asyncio.sleep(EMPTY_BATCH_DELAY)
                    continue
                
                limit_reached, _, _ = await process_users(session, users, token, user_id, bot, token_name, already_sent_ids, lock)
                if limit_reached:
                    state["running"] = False
                    break
                
                await asyncio.sleep(PER_BATCH_DELAY)
            
            except Exception as e:
                logging.error(f"Error during processing: {e}")
                await asyncio.sleep(PER_ERROR_DELAY)

    if state.get("pinned_message_id"):
        try: await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        except Exception: pass
    
    status = "Stopped" if state.get("stopped") else "Completed"
    await bot.send_message(user_id, f"✅ {status}! Total Added: {state.get('total_added_friends', 0)}")


async def process_all_tokens(user_id, tokens, bot, target_channel_id):
    """Process friend requests for all tokens concurrently with a shared spam filter list."""
    state = user_states[user_id]
    state.update({"total_added_friends": 0, "running": True, "stopped": False})

    status_message = await bot.send_message(chat_id=user_id, text="🔄 <b>AIO Starting...</b>", parse_mode="HTML", reply_markup=stop_markup)
    state["status_message_id"] = status_message.message_id
    try:
        await bot.pin_chat_message(chat_id=user_id, message_id=status_message.message_id, disable_notification=True)
        state["pinned_message_id"] = status_message.message_id
    except Exception as e:
        logging.error(f"Failed to pin message: {e}")

    token_status = {token_obj.get("name", f"Account {i+1}"): {"added": 0, "filtered": 0, "status": "Queued"} for i, token_obj in enumerate(tokens)}
    
    # Pre-load all sent IDs once and share across all workers
    session_sent_ids = await get_already_sent_ids(user_id, "request")
    lock = asyncio.Lock()

    async def _worker(token_obj):
        name = token_obj.get("name")
        token = token_obj["token"]
        empty_batches = 0
        
        async with aiohttp.ClientSession() as session:
            while state["running"]:
                try:
                    # --- THIS IS THE FIX (await removed) ---
                    if is_request_filter_enabled(user_id):
                        await apply_filter_for_account(token, user_id)
                        await asyncio.sleep(1)

                    users = await fetch_users(session, token)
                    
                    if users is None: # Rate limited
                        token_status[name]["status"] = "Rate limited"
                        return
                    
                    if not users or len(users) < 5:
                        empty_batches += 1
                        token_status[name]["status"] = f"Waiting ({empty_batches}/10)"
                        await asyncio.sleep(EMPTY_BATCH_DELAY)
                        if empty_batches >= 10:
                            token_status[name]["status"] = "No users"
                            return
                        continue
                    
                    empty_batches = 0
                    token_status[name]["status"] = "Processing"
                    
                    limit_reached, batch_added, batch_filtered = await process_users(session, users, token, user_id, bot, name, session_sent_ids, lock)
                    
                    token_status[name]["added"] += batch_added
                    token_status[name]["filtered"] += batch_filtered
                    
                    if limit_reached:
                        token_status[name]["status"] = "Limit Full"
                        return
                        
                    await asyncio.sleep(PER_BATCH_DELAY)

                except Exception as e:
                    logging.error(f"Error processing {name}: {e}")
                    token_status[name]["status"] = "Retrying..."
                    await asyncio.sleep(PER_ERROR_DELAY)
        
        token_status[name]["status"] = "Stopped"

    async def _refresh_ui():
        last_message = ""
        while state["running"]:
            total_added_now = sum(status["added"] for status in token_status.values())
            header = f"🔄 <b>AIO Requests</b> | <b>Added:</b> {total_added_now}"
            
            lines = [header, "", "<pre>Account   │Added │Filter│Status      </pre>"]
            for name, status in token_status.items():
                display = name[:10] + '…' if len(name) > 10 else name.ljust(10)
                lines.append(f"<pre>{display} │{status['added']:>5} │{status['filtered']:>6}│{status['status']:<10}</pre>")

            current_message = "\n".join(lines)
            if current_message != last_message:
                try:
                    await bot.edit_message_text(
                        chat_id=user_id, message_id=state["status_message_id"],
                        text=current_message, parse_mode="HTML", reply_markup=stop_markup
                    )
                    last_message = current_message
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logging.error(f"Status update failed: {e}")
            await asyncio.sleep(1)

    # Start UI updater and workers
    ui_task = asyncio.create_task(_refresh_ui())
    worker_tasks = [asyncio.create_task(_worker(token_obj)) for token_obj in tokens]
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Clean up
    state["running"] = False
    await asyncio.sleep(1.1) # Allow final UI update
    ui_task.cancel()
    if state.get("pinned_message_id"):
        try: await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        except Exception: pass

    # Final Status UI
    total_added = sum(status["added"] for status in token_status.values())
    completion_status = "⚠️ Process Stopped" if state.get("stopped") else "✅ AIO Requests Completed"
    final_header = f"<b>{completion_status}</b> | <b>Total Added:</b> {total_added}"
    
    final_lines = [final_header, "", "<pre>Account   │Added │Filter│Status      </pre>"]
    for name, status in token_status.items():
        display = name[:10] + '…' if len(name) > 10 else name.ljust(10)
        final_lines.append(f"<pre>{display} │{status['added']:>5} │{status['filtered']:>6}│{status['status']}</pre>")

    await bot.edit_message_text(
        chat_id=user_id, message_id=state["status_message_id"],
        text="\n".join(final_lines), parse_mode="HTML"
    )
