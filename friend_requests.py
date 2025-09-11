import asyncio
import aiohttp
import logging
import html
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db import get_individual_spam_filter, is_already_sent, add_sent_id, get_active_tokens, get_current_account, get_already_sent_ids # Import the new function
from filters import apply_filter_for_account, is_request_filter_enabled
from collections import defaultdict
import time
from dateutil import parser
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ✅ Speed configuration
PER_USER_DELAY = 1   # Reduced from 2 to half a second
PER_BATCH_DELAY = 1.5     # Reduced from 2 to 1 second
EMPTY_BATCH_DELAY = 2     # Delay after empty batch
PER_ERROR_DELAY = 5       # Delay after errors


# Global state variables for friend requests
user_states = defaultdict(lambda: {
    "running": False,
    "status_message_id": None,
    "pinned_message_id": None,
    "total_added_friends": 0,
    "batch_index": 0
})

# Inline keyboards for friend request operations
stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Stop Requests", callback_data="stop")]
])

async def fetch_users(session, token):
    """Fetch users from the API for friend requests"""
    url = "https://api.meeff.com/user/explore/v2?lng=-112.0613784790039&unreachableUserIds=&lat=33.437198638916016&locale=en"
    
    # This function needs user_id to get device info, but it's not passed
    # We'll use a generic device info for now, but ideally should be refactored
    from device_info import generate_device_info
    device_info = generate_device_info()
    
    base_headers = {
        'User-Agent': "okhttp/4.12.0",
        'meeff-access-token': token
    }
    headers = get_headers_with_device_info(base_headers, device_info)
    
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 429:
                logging.error("Request limit exceeded while fetching users.")
                return None
            if response.status != 200:
                logging.error(f"Failed to fetch users: {response.status}")
                return []
            return (await response.json()).get("users", [])
    except Exception as e:
        logging.error(f"Fetch users failed: {e}")
        return []

def format_user(user):
    def time_ago(dt_str):
        if not dt_str:
            return "N/A"
        try:
            dt = parser.isoparse(dt_str)
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            diff = now - dt
            minutes = int(diff.total_seconds() // 60)
            if minutes < 1:
                return "just now"
            elif minutes < 60:
                return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24:
                return f"{hours} hr ago"
            days = hours // 24
            return f"{days} day(s) ago"
        except Exception:
            return "unknown"
    last_active = time_ago(user.get("recentAt"))
    nationality = html.escape(user.get('nationalityCode', 'N/A'))
    height = html.escape(str(user.get('height', 'N/A')))
    if "|" in height:
        height_val, height_unit = height.split("|", 1)
        height = f"{height_val.strip()} {height_unit.strip()}"
    return (
        f"<b>Name:</b> {html.escape(user.get('name', 'N/A'))}\n"
        f"<b>ID:</b> <code>{html.escape(user.get('_id', 'N/A'))}</code>\n"
        f"<b>Nationality:</b> {nationality}\n"
        f"<b>Height:</b> {height}\n"
        f"<b>Description:</b> {html.escape(user.get('description', 'N/A'))}\n"
        f"<b>Birth Year:</b> {html.escape(str(user.get('birthYear', 'N/A')))}\n"
        f"<b>Platform:</b> {html.escape(user.get('platform', 'N/A'))}\n"
        f"<b>Profile Score:</b> {html.escape(str(user.get('profileScore', 'N/A')))}\n"
        f"<b>Distance:</b> {html.escape(str(user.get('distance', 'N/A')))} km\n"
        f"<b>Language Codes:</b> {html.escape(', '.join(user.get('languageCodes', [])))}\n"
        f"<b>Last Active:</b> {last_active}\n"
        "Photos: " + ' '.join([f"<a href='{html.escape(url)}'>Photo</a>" for url in user.get('photoUrls', [])])
    )

def format_time_used(start_time, end_time):
    delta = end_time - start_time
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

async def process_users(session, users, token, user_id, bot, target_channel_id, token_name=None, token_status=None, session_sent_ids=None, lock=None):
    """Process a batch of users and send friend requests."""
    state = user_states[user_id]
    added_count = 0
    filtered_count = 0
    limit_reached = False
    
    is_spam_filter_enabled = get_individual_spam_filter(user_id, "request")

    # Use the shared session list if available (for multi-token runs), otherwise fetch from DB
    already_sent_ids = session_sent_ids if session_sent_ids is not None else set()
    if is_spam_filter_enabled and session_sent_ids is None:
        already_sent_ids = get_already_sent_ids(user_id, "request")

    for user in users:
        if not state["running"]:
            break

        user_id_to_check = user["_id"]

        # If spam filter is enabled, perform an atomic check-and-set to prevent race conditions
        if is_spam_filter_enabled:
            is_duplicate = False
            if lock:  # This indicates a multi-token run
                async with lock:
                    if user_id_to_check in already_sent_ids:
                        is_duplicate = True
                    else:
                        # Add to the session set immediately while locked to claim this user
                        already_sent_ids.add(user_id_to_check)
            else:  # This is a single-token run
                if user_id_to_check in already_sent_ids:
                    is_duplicate = True
                else:
                    already_sent_ids.add(user_id_to_check)
            
            if is_duplicate:
                filtered_count += 1
                if token_status and token_name in token_status:
                    current = token_status[token_name]
                    token_status[token_name] = (current[0], current[1] + 1, current[2])
                continue

        # Send friend request
        url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id_to_check}&isOkay=1"
        
        # Use generic device info since we don't have user_id context here
        from device_info import generate_device_info
        device_info = generate_device_info()
        base_headers = {"meeff-access-token": token, "Connection": "keep-alive"}
        headers = get_headers_with_device_info(base_headers, device_info)

        try:
            async with session.get(url, headers=headers) as response:
                data = await response.json()

                if data.get("errorCode") == "LikeExceeded":
                    logging.info(f"Daily like limit reached for {token_name}.")
                    if token_status and token_name in token_status:
                        token_status[token_name] = (token_status[token_name][0], token_status[token_name][1], "Limit Full")
                    limit_reached = True
                    break

                # If spam filter is on, add this user ID to the permanent database record
                if is_spam_filter_enabled:
                    add_sent_id(user_id, "request", user_id_to_check)

                # Format and send user details
                details = format_user(user)
                await bot.send_message(chat_id=user_id, text=details, parse_mode="HTML")
                
                added_count += 1
                state["total_added_friends"] += 1

                if token_status and token_name in token_status:
                    current = token_status[token_name]
                    token_status[token_name] = (current[0] + 1, current[1], "Processing")
                else:
                    if state["running"] and state["status_message_id"]:
                        try:
                            await bot.edit_message_text(
                                chat_id=user_id,
                                message_id=state["status_message_id"],
                                text=f"{token_name}: Friend request sending: {state['total_added_friends']}",
                                reply_markup=stop_markup
                            )
                        except Exception as e:
                            if "message is not modified" not in str(e):
                                logging.error(f"Error updating status message: {e}")

                await asyncio.sleep(PER_USER_DELAY)
                
        except Exception as e:
            logging.error(f"Error processing user with {token_name}: {e}")
            await asyncio.sleep(1)

    return limit_reached, added_count, filtered_count


async def run_requests(user_id, bot, target_channel_id):
    """Main function to run the request process for a single token"""
    state = user_states[user_id]
    state["total_added_friends"] = 0
    state["batch_index"] = 0
    state["running"] = True
    
    async with aiohttp.ClientSession() as session:
        while state["running"]:
            try:
                token = get_current_account(user_id)
                if not token:
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=state["status_message_id"],
                        text="No active account found.",
                        reply_markup=None
                    )
                    state["running"] = False
                    return

                if is_request_filter_enabled(user_id):
                    await apply_filter_for_account(token, user_id)
                    await asyncio.sleep(1)

                tokens = get_active_tokens(user_id)
                token_name = next((t.get("name", "Default") for t in tokens if t["token"] == token), "Default")

                try:
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=state["status_message_id"],
                        text=f"{token_name}: Friend request sending: {state['total_added_friends']}",
                        reply_markup=stop_markup
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logging.error(f"Error updating status message: {e}")

                users = await fetch_users(session, token)
                state["batch_index"] += 1
                
                if not users or len(users) == 0:
                    logging.info(f"No users found for batch {state['batch_index']}.")
                    await asyncio.sleep(EMPTY_BATCH_DELAY)
                    
                    if state["batch_index"] > 10:
                        await bot.edit_message_text(
                            chat_id=user_id,
                            message_id=state["status_message_id"],
                            text=f"{token_name}: No more users found. Total: {state['total_added_friends']}",
                            reply_markup=None
                        )
                        state["running"] = False
                        break
                    continue
                
                limit_reached, _, _ = await process_users(session, users, token, user_id, bot, target_channel_id, token_name=token_name)
                if limit_reached:
                    state["running"] = False
                    break
                        
                await asyncio.sleep(PER_BATCH_DELAY)
                
            except Exception as e:
                logging.error(f"Error during processing: {e}")
                await asyncio.sleep(PER_ERROR_DELAY)
                 
        if state.get("pinned_message_id"):
            try:
                await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
            except Exception: pass
        
        await bot.send_message(user_id, f"✅ All done! Total Added: {state.get('total_added_friends', 0)}")


async def process_all_tokens(user_id, tokens, bot, target_channel_id):
    """Process friend requests for all tokens concurrently with a shared spam filter list."""
    state = user_states[user_id]
    state["total_added_friends"] = 0
    state["running"] = True
    state["stopped"] = False

    if not state.get("status_message_id"):
        status_message = await bot.send_message(
            chat_id=user_id, text="🔄 <b>AIO Starting</b>", parse_mode="HTML", reply_markup=stop_markup
        )
        state["status_message_id"] = status_message.message_id
        try:
            await bot.pin_chat_message(chat_id=user_id, message_id=status_message.message_id, disable_notification=True)
            state["pinned_message_id"] = status_message.message_id
        except Exception as e:
            logging.error(f"Failed to pin message: {e}")

    token_status = {}
    
    session_sent_ids = get_already_sent_ids(user_id, "request")
    lock = asyncio.Lock()

    async def _worker(token_obj, idx, shared_sent_ids, shared_lock):
        token = token_obj["token"]
        name = token_obj.get("name", f"Account {idx}")
        worker_status = token_status[token]
        
        # Set status to processing once the worker actually starts
        worker_status['status'] = "Processing"
        
        empty_batches = 0

        try:
            async with aiohttp.ClientSession() as session:
                while state["running"]:
                    try:
                        if is_request_filter_enabled(user_id):
                            await apply_filter_for_account(token, user_id)
                            await asyncio.sleep(1)

                        users = await fetch_users(session, token)
                        
                        if users is None:
                            worker_status['status'] = "Rate limited"
                            return worker_status['added']
                            
                        if not users or len(users) < 5:
                            empty_batches += 1
                            worker_status['status'] = f"Waiting ({empty_batches}/10)"
                            await asyncio.sleep(EMPTY_BATCH_DELAY)
                            if empty_batches >= 10:
                                worker_status['status'] = "No users"
                                return worker_status['added']
                            continue
                        
                        empty_batches = 0
                        
                        limit_reached, batch_added, batch_filtered = await process_users(
                            session, users, token, user_id, bot, target_channel_id, 
                            token_name=name, token_status=token_status,
                            session_sent_ids=shared_sent_ids, lock=shared_lock
                        )
                        
                        worker_status['added'] += batch_added
                        worker_status['filtered'] += batch_filtered
                        
                        if limit_reached:
                            worker_status['status'] = "Limit Full"
                            return worker_status['added']
                            
                        await asyncio.sleep(PER_BATCH_DELAY)

                    except Exception as e:
                        logging.error(f"Error processing {name}: {e}")
                        worker_status['status'] = "Retrying..."
                        await asyncio.sleep(PER_ERROR_DELAY)

                worker_status['status'] = "Stopped"
                return worker_status['added']

        except Exception as e:
            logging.error(f"Worker failed for {name}: {e}")
            worker_status['status'] = f"Failed: {str(e)[:20]}..."
            return worker_status['added']

    async def _refresh_ui():
        last_message = ""
        update_count = 0
        update_interval = 1
        force_update_interval = 3

        while state["running"]:
            try:
                total_added_now = sum(data['added'] for data in token_status.values())
                header = f"🔄 <b>AIO Requests </b> | <b> Added:</b> {total_added_now}"
                
                lines = [
                    header,
                    "",
                    "<pre>Account    │Added │Filter│Status      </pre>"
                ]

                for token, data in token_status.items():
                    name = data['name']
                    added = data['added']
                    filtered = data['filtered']
                    status = data['status']
                    
                    display = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
                    lines.append(f"<pre>{display} │{added:>5} │{filtered:>6}│{status.ljust(10)}</pre>")

                spinners = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                spinner = spinners[update_count % len(spinners)]
                lines.append(f"\n{spinner} <i>Processing...</i>")

                current_message = "\n".join(lines)
                update_count += 1
                force_update = (update_count % force_update_interval == 0)

                if current_message != last_message or force_update:
                    try:
                        await bot.edit_message_text(
                            chat_id=user_id,
                            message_id=state["status_message_id"],
                            text=current_message,
                            parse_mode="HTML",
                            reply_markup=stop_markup
                        )
                        last_message = current_message
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logging.error(f"Status update failed: {e}")

            except Exception as e:
                logging.error(f"UI updater exception: {e}")

            await asyncio.sleep(update_interval)

    # Initialize all statuses to 'Queued'
    for idx, token_obj in enumerate(tokens, 1):
        token = token_obj["token"]
        name = token_obj.get("name", f"Account {idx}")
        token_status[token] = {'name': name, 'added': 0, 'filtered': 0, 'status': 'Queued'}

    # Start the UI updater task so it can show the staggered start
    ui_task = asyncio.create_task(_refresh_ui())
    await asyncio.sleep(0.1) # Give UI a moment to post the initial message

    # --- NEW: Staggered start for each worker task ---
    worker_tasks = []
    for idx, token_obj in enumerate(tokens, 1):
        # Update the status to 'Starting...' so the user sees the change in the UI
        token_status[token_obj['token']]['status'] = 'Starting...'
        await asyncio.sleep(0.5) # Allow the UI a moment to update

        # Create the task for the current token
        task = asyncio.create_task(_worker(token_obj, idx, session_sent_ids, lock))
        worker_tasks.append(task)
        
        # If this is not the last token, wait before starting the next one
        if idx < len(tokens):
            await asyncio.sleep(3) # The 3-second delay between starts

    # Wait for all the started tasks to complete
    results = await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Clean up after tasks are done
    state["running"] = False
    await asyncio.sleep(1.1)
    ui_task.cancel()
    try:
        await ui_task
    except asyncio.CancelledError:
        pass

    if state.get("pinned_message_id"):
        try:
            await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        except Exception: pass

    # Final Status UI
    total_added = sum(result for result in results if isinstance(result, int))
    
    was_stopped = state.get("stopped", False)
    completion_status = "⚠️ Process Stopped" if was_stopped else "✅ AIO Friend Requests Completed"
    final_header = f"<b>{completion_status}</b> | <b>Total Added:</b> {total_added}"
    
    final_lines = [
        final_header,
        "",
        "<pre>Account    │Added │Filter│Status       </pre>"
    ]
    
    for token, data in token_status.items():
        name = data['name']
        added = data['added']
        filtered = data['filtered']
        status = data['status']
        
        display = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
        final_lines.append(f"<pre>{display} │{added:>5} │{filtered:>6}│{status.ljust(11)}</pre>")

    try:
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=state["status_message_id"],
            text="\n".join(final_lines),
            parse_mode="HTML"
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logging.error(f"Final status update failed: {e}")

    total_filtered_final = sum(data['filtered'] for data in token_status.values())
    
    if state.get("stopped", False):
        await bot.send_message(
            user_id,
            f"⚠️ Process stopped!\nTotal Added: {total_added}\nTotal Filtered: {total_filtered_final}"
        )
    else:
        await bot.send_message(
            user_id,
            f"✅ AIO requests completed!\nTotal Added: {total_added}\nTotal Filtered: {total_filtered_final}"
        )
