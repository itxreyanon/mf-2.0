import asyncio
import aiohttp
import logging
import html
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
# Import the now ASYNC db functions
from db import get_individual_spam_filter, bulk_add_sent_ids, get_active_tokens, get_current_account, get_already_sent_ids
from filters import apply_filter_for_account, is_request_filter_enabled
from collections import defaultdict
from dateutil import parser
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Speed configuration
PER_USER_DELAY = 0.5      # Delay between individual requests
PER_BATCH_DELAY = 1.0     # Delay between fetching new batches
EMPTY_BATCH_DELAY = 2.0   # Delay after fetching an empty batch
PER_ERROR_DELAY = 5.0     # Delay after a network or API error

# Global state variables for friend requests
user_states = defaultdict(lambda: {
    "running": False, "status_message_id": None, "pinned_message_id": None,
    "total_added_friends": 0, "batch_index": 0
})

# Inline keyboards for friend request operations
stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Stop Requests", callback_data="stop")]
])

async def fetch_users(session, token, user_id):
    """Fetch users from the API for friend requests."""
    url = "https://api.meeff.com/user/explore/v2?lng=-112.0613784790039&unreachableUserIds=&lat=33.437198638916016&locale=en"
    
    # Use the correct device info for the specific token and user
    device_info = get_or_create_device_info_for_token(user_id, token)
    base_headers = {'User-Agent': "okhttp/4.12.0", 'meeff-access-token': token}
    headers = get_headers_with_device_info(base_headers, device_info)
    
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status == 429:
                logging.warning(f"Rate limit hit for token {token[:10]}...")
                return None
            response.raise_for_status() # Raise an error for bad statuses (4xx or 5xx)
            data = await response.json()
            return data.get("users", [])
    except Exception as e:
        logging.error(f"Fetch users failed: {e}")
        return []

def format_user(user):
    def time_ago(dt_str):
        if not dt_str: return "N/A"
        try:
            dt = parser.isoparse(dt_str).replace(tzinfo=None)
            from datetime import datetime
            diff = datetime.utcnow() - dt
            minutes, seconds = divmod(diff.total_seconds(), 60)
            hours, minutes = divmod(minutes, 60)
            days, hours = divmod(hours, 24)
            if days > 0: return f"{int(days)}d ago"
            if hours > 0: return f"{int(hours)}h ago"
            if minutes > 0: return f"{int(minutes)}m ago"
            return "just now"
        except Exception: return "unknown"
    
    height_str = str(user.get('height', 'N/A'))
    height = f"{h[0].strip()} {h[1].strip()}" if (h := height_str.split('|', 1)) and len(h) == 2 else height_str

    return (
        f"<b>Name:</b> {html.escape(user.get('name', 'N/A'))}\n"
        f"<b>ID:</b> <code>{html.escape(user.get('_id', 'N/A'))}</code>\n"
        f"<b>Nationality:</b> {html.escape(user.get('nationalityCode', 'N/A'))}\n"
        f"<b>Height:</b> {html.escape(height)}\n"
        f"<b>Description:</b> {html.escape(user.get('description', 'N/A'))[:100]}\n"
        f"<b>Birth Year:</b> {user.get('birthYear', 'N/A')}\n"
        f"<b>Last Active:</b> {time_ago(user.get('recentAt'))}\n"
        f"<b>Photos:</b> " + ' '.join([f"<a href='{html.escape(url)}'>🖼️</a>" for url in user.get('photoUrls', [])])
    )

async def process_users(session, users, token, user_id, bot, token_status, session_sent_ids, lock):
    """Process a batch of users and send friend requests."""
    state = user_states[user_id]
    limit_reached = False
    
    # PERFORMANCE: Collect successful IDs for a single bulk DB write later
    successfully_added_user_ids = []

    is_spam_filter_enabled = await get_individual_spam_filter(user_id, "request")

    for user in users:
        if not state["running"]: break
        user_id_to_add = user["_id"]

        if is_spam_filter_enabled:
            is_duplicate = False
            async with lock:
                if user_id_to_add in session_sent_ids:
                    is_duplicate = True
                else: # Claim this user ID to prevent other workers from processing it
                    session_sent_ids.add(user_id_to_add)
            if is_duplicate:
                token_status['filtered'] += 1
                continue
        
        url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id_to_add}&isOkay=1"
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info({"meeff-access-token": token}, device_info)
        
        try:
            async with session.get(url, headers=headers, timeout=7) as response:
                data = await response.json()
                if data.get("errorCode") == "LikeExceeded":
                    logging.info(f"Daily like limit reached for {token_status['name']}.")
                    token_status['status'] = "Limit Full"
                    limit_reached = True
                    break

                if response.status == 200:
                    details = format_user(user)
                    await bot.send_message(chat_id=user_id, text=details, parse_mode="HTML", disable_web_page_preview=True)
                    
                    token_status['added'] += 1
                    state["total_added_friends"] += 1
                    
                    if is_spam_filter_enabled:
                        successfully_added_user_ids.append(user_id_to_add)

                    await asyncio.sleep(PER_USER_DELAY)
                else:
                    logging.warning(f"Failed to add {user_id_to_add}. Status: {response.status}, Msg: {data.get('errorMessage')}")

        except Exception as e:
            logging.error(f"Error processing user {user_id_to_add} with {token_status['name']}: {e}")
            await asyncio.sleep(1)

    # PERFORMANCE: Perform a single bulk database write for all successful requests in this batch.
    if is_spam_filter_enabled and successfully_added_user_ids:
        await bulk_add_sent_ids(user_id, "request", successfully_added_user_ids)

    return limit_reached

async def run_requests(user_id, bot, target_channel_id):
    """Main function to run the request process for a single token."""
    state = user_states[user_id]
    state.update({"running": True, "stopped": False, "total_added_friends": 0, "batch_index": 0})
    
    async with aiohttp.ClientSession() as session:
        while state["running"]:
            try:
                token = await get_current_account(user_id)
                if not token:
                    await bot.edit_message_text(chat_id=user_id, message_id=state["status_message_id"], text="No active account found.", reply_markup=None)
                    break

                if await is_request_filter_enabled(user_id):
                    await apply_filter_for_account(token, user_id)
                    await asyncio.sleep(1)

                tokens = await get_active_tokens(user_id)
                token_name = next((t.get("name", "Default") for t in tokens if t["token"] == token), "Default")
                token_status = {'name': token_name, 'added': 0, 'filtered': 0, 'status': 'Processing'}

                users = await fetch_users(session, token, user_id)
                state["batch_index"] += 1
                
                if users is None or not users:
                    logging.info(f"No users found for batch {state['batch_index']}.")
                    await asyncio.sleep(EMPTY_BATCH_DELAY)
                    if state["batch_index"] > 10: # Stop after 10 empty batches
                        await bot.edit_message_text(chat_id=user_id, message_id=state["status_message_id"], text=f"{token_name}: No more users found. Total: {state['total_added_friends']}", reply_markup=None)
                        break
                    continue
                
                session_sent_ids = await get_already_sent_ids(user_id, "request")
                lock = asyncio.Lock()
                limit_reached = await process_users(session, users, token, user_id, bot, token_status, session_sent_ids, lock)
                
                if limit_reached: break
                await bot.edit_message_text(chat_id=user_id, message_id=state["status_message_id"], text=f"{token_name}: Requests sent: {state['total_added_friends']}", reply_markup=stop_markup)
                await asyncio.sleep(PER_BATCH_DELAY)
                
            except Exception as e:
                logging.error(f"Error during run_requests: {e}", exc_info=True)
                await asyncio.sleep(PER_ERROR_DELAY)
                 
    state["running"] = False
    if state.get("pinned_message_id"):
        try: await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        except Exception: pass
    
    await bot.send_message(user_id, f"✅ Process finished! Total Added: {state.get('total_added_friends', 0)}")


async def process_all_tokens(user_id, tokens, bot, target_channel_id):
    """Process friend requests for all tokens concurrently."""
    state = user_states[user_id]
    state.update({"running": True, "stopped": False, "total_added_friends": 0})

    status_message = await bot.send_message(chat_id=user_id, text="🔄 <b>AIO Starting...</b>", parse_mode="HTML", reply_markup=stop_markup)
    state["status_message_id"] = status_message.message_id
    try:
        await bot.pin_chat_message(chat_id=user_id, message_id=status_message.message_id, disable_notification=True)
        state["pinned_message_id"] = status_message.message_id
    except Exception as e:
        logging.error(f"Failed to pin message: {e}")

    token_status = {t["token"]: {'name': t.get("name", f"Account_{i+1}"), 'added': 0, 'filtered': 0, 'status': 'Queued'} for i, t in enumerate(tokens)}
    
    session_sent_ids = await get_already_sent_ids(user_id, "request")
    lock = asyncio.Lock()

    async def _worker(token_obj):
        token = token_obj["token"]
        worker_status = token_status[token]
        worker_status['status'] = "Processing"
        empty_batches = 0

        async with aiohttp.ClientSession() as session:
            while state["running"]:
                try:
                    if await is_request_filter_enabled(user_id):
                        await apply_filter_for_account(token, user_id)
                        await asyncio.sleep(1)

                    users = await fetch_users(session, token, user_id)
                    
                    if users is None: # Rate limit
                        worker_status['status'] = "Rate Limited"
                        await asyncio.sleep(30)
                        continue
                        
                    if not users:
                        empty_batches += 1
                        worker_status['status'] = f"Waiting ({empty_batches}/10)"
                        if empty_batches >= 10:
                            worker_status['status'] = "No users"
                            return
                        await asyncio.sleep(EMPTY_BATCH_DELAY)
                        continue
                    
                    empty_batches = 0
                    limit_reached = await process_users(session, users, token, user_id, bot, worker_status, session_sent_ids, lock)
                    if limit_reached:
                        worker_status['status'] = "Limit Full"
                        return
                    await asyncio.sleep(PER_BATCH_DELAY)

                except Exception as e:
                    logging.error(f"Error in worker {worker_status['name']}: {e}", exc_info=True)
                    worker_status['status'] = "Retrying..."
                    await asyncio.sleep(PER_ERROR_DELAY)
        worker_status['status'] = "Stopped"

    async def _refresh_ui():
        last_message = ""
        while state["running"]:
            total_added_now = sum(data['added'] for data in token_status.values())
            header = f"🔄 <b>AIO Requests</b> | Added: <b>{total_added_now}</b>"
            lines = [header, "", "<pre>Account    │ Added │ Filter │ Status</pre>"]
            for data in token_status.values():
                display = data['name'][:10].ljust(10)
                lines.append(f"<pre>{display} │ {data['added']:>5} │ {data['filtered']:>6} │ {data['status']}</pre>")
            
            current_message = "\n".join(lines)
            if current_message != last_message:
                try:
                    await bot.edit_message_text(chat_id=user_id, message_id=state["status_message_id"], text=current_message, parse_mode="HTML", reply_markup=stop_markup)
                    last_message = current_message
                except TelegramBadRequest as e:
                    if "message is not modified" not in str(e):
                        logging.error(f"Status update failed: {e}")
                except Exception as e:
                    logging.error(f"UI updater exception: {e}")
            await asyncio.sleep(1.5)

    # Staggered start for workers
    ui_task = asyncio.create_task(_refresh_ui())
    worker_tasks = []
    for token_obj in tokens:
        if not state['running']: break
        token_status[token_obj['token']]['status'] = 'Starting...'
        task = asyncio.create_task(_worker(token_obj))
        worker_tasks.append(task)
        await asyncio.sleep(3) # The 3-second delay between starts

    await asyncio.gather(*worker_tasks)

    # Clean up
    state["running"] = False
    await asyncio.sleep(1.6) # Allow final UI update
    ui_task.cancel()
    if state.get("pinned_message_id"):
        try: await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        except Exception: pass

    # Final Status UI
    total_added = state["total_added_friends"]
    total_filtered = sum(data['filtered'] for data in token_status.values())
    completion_status = "⚠️ Process Stopped by User" if state.get("stopped") else "✅ AIO Requests Completed"
    final_header = f"<b>{completion_status}</b> | Total Added: <b>{total_added}</b>"
    final_lines = [final_header, "", "<pre>Account    │ Added │ Filter │ Status</pre>"]
    for data in token_status.values():
        display = data['name'][:10].ljust(10)
        final_lines.append(f"<pre>{display} │ {data['added']:>5} │ {data['filtered']:>6} │ {data['status']}</pre>")
    
    try:
        await bot.edit_message_text(chat_id=user_id, message_id=state["status_message_id"], text="\n".join(final_lines), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Final status update failed: {e}")

    await bot.send_message(user_id, f"<b>Process Finished!</b>\n- Total Added: {total_added}\n- Total Filtered: {total_filtered}", parse_mode="HTML")
