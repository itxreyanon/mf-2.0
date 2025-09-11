import asyncio
import aiohttp
import logging
from typing import List, Dict
from aiogram import types
from aiogram.exceptions import TelegramBadRequest

# Import the now ASYNC db functions
from db import bulk_add_sent_ids, is_already_sent
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

LOUNGE_URL = "https://api.meeff.com/lounge/dashboard/v1"
CHATROOM_URL = "https://api.meeff.com/chatroom/open/v2"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
BASE_HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8"
}

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def fetch_lounge_users(session: aiohttp.ClientSession, token: str, user_id: int) -> List[Dict]:
    """Fetch users from lounge using a shared session."""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    device_info = get_or_create_device_info_for_token(user_id, token)
    final_headers = get_headers_with_device_info(headers, device_info)
    
    try:
        async with session.get(LOUNGE_URL, params={'locale': "en"}, headers=final_headers, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("both", [])
    except Exception as e:
        logger.error(f"Error fetching lounge users for token {token[:10]}: {e}")
        return []

async def open_chatroom(session: aiohttp.ClientSession, token: str, target_user_id: str, telegram_user_id: int) -> str:
    """Open chatroom with a user using a shared session."""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    device_info = get_or_create_device_info_for_token(telegram_user_id, token)
    final_headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"waitingRoomId": target_user_id, "locale": "en"}
    
    try:
        async with session.post(CHATROOM_URL, json=payload, headers=final_headers, timeout=10) as response:
            if response.status == 412:
                logger.info(f"User {target_user_id} has disabled chat.")
                return None
            response.raise_for_status()
            data = await response.json()
            return data.get("chatRoom", {}).get("_id")
    except Exception as e:
        logger.error(f"Error opening chatroom with {target_user_id}: {e}")
        return None

async def send_lounge_message(session: aiohttp.ClientSession, token: str, chatroom_id: str, message: str, user_id: int) -> bool:
    """Send message to a chatroom using a shared session."""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    device_info = get_or_create_device_info_for_token(user_id, token)
    final_headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"chatRoomId": chatroom_id, "message": message, "locale": "en"}
    
    try:
        async with session.post(SEND_MESSAGE_URL, json=payload, headers=final_headers, timeout=10) as response:
            return response.status == 200
    except Exception as e:
        logger.error(f"Error sending message to {chatroom_id}: {e}")
        return False

async def send_lounge(token: str, message: str, status_message: types.Message, bot, chat_id: int, spam_enabled: bool, user_id: int):
    total_sent = 0
    total_filtered = 0

    async def update_status(msg: str):
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=status_message.message_id, text=msg, parse_mode="HTML")
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                logger.warning(f"Failed to update status: {e}")

    try:
        await update_status("⏳ Fetching lounge users...")
        # NETWORK EFFICIENCY: Create one session for the entire task
        async with aiohttp.ClientSession() as session:
            all_users = await fetch_lounge_users(session, token, user_id)
            if not all_users:
                return await update_status(f"⚠️ No users found in lounge.")

            if spam_enabled:
                user_ids = [u["user"]["_id"] for u in all_users if u.get("user", {}).get("_id")]
                # ASYNC CHANGE: Await the database call
                existing_ids = await is_already_sent(chat_id, "lounge", user_ids, bulk=True)
                users_to_process = [u for u in all_users if u.get("user", {}).get("_id") and u["user"]["_id"] not in existing_ids]
                total_filtered = len(all_users) - len(users_to_process)
            else:
                users_to_process = all_users

            if not users_to_process:
                return await update_status(f"✅ Lounge complete. All {len(all_users)} users were already contacted.")

            sent_in_batch = 0
            successfully_sent_ids = []
            total_to_process = len(users_to_process)
            
            for i, user_data in enumerate(users_to_process, 1):
                await update_status(f"🚀 Processing {i}/{total_to_process}...\nSent: {sent_in_batch} | Filtered: {total_filtered}")
                target_user_id = user_data.get("user", {}).get("_id")
                if not target_user_id:
                    continue

                chatroom_id = await open_chatroom(session, token, target_user_id, user_id)
                if chatroom_id:
                    if await send_lounge_message(session, token, chatroom_id, message, user_id):
                        sent_in_batch += 1
                        if spam_enabled:
                            successfully_sent_ids.append(target_user_id)
                await asyncio.sleep(0.5) # Small delay between users

            total_sent += sent_in_batch
            if spam_enabled and successfully_sent_ids:
                # ASYNC CHANGE: Await the database call
                await bulk_add_sent_ids(chat_id, "lounge", successfully_sent_ids)
        
        await update_status(f"✅ Lounge complete!\nSent: {total_sent} | Filtered: {total_filtered}")

    except Exception as e:
        logger.error(f"Lounge process failed: {e}", exc_info=True)
        await update_status(f"❌ An error occurred: {e}")

async def send_lounge_all_tokens(tokens_data: List[Dict], message: str, status_message: types.Message, bot, chat_id: int, spam_enabled: bool, user_id: int):
    token_status = {td['token']: {'name': td.get("name", f"Acc_{i+1}"), 'sent': 0, 'filtered': 0, 'status': 'Queued'} for i, td in enumerate(tokens_data)}
    
    # ASYNC CHANGE: Await the database call
    sent_ids = await is_already_sent(chat_id, "lounge", None, bulk=True) if spam_enabled else set()
    processing_ids = set()
    lock = asyncio.Lock()
    is_running = True

    async def _worker(session: aiohttp.ClientSession, token_data: Dict):
        token = token_data["token"]
        status_entry = token_status[token]
        status_entry['status'] = "Fetching"
        
        users = await fetch_lounge_users(session, token, user_id)
        if not users:
            status_entry['status'] = "No users"
            return

        successfully_sent_ids = []
        for u in users:
            if not is_running: break
            uid = u.get("user", {}).get("_id")
            if not uid: continue
            
            is_duplicate = False
            async with lock:
                if uid in sent_ids or uid in processing_ids:
                    is_duplicate = True
                else:
                    processing_ids.add(uid)
            
            if is_duplicate:
                status_entry['filtered'] += 1
                continue

            status_entry['status'] = "Opening chat"
            room_id = await open_chatroom(session, token, uid, user_id)
            if room_id:
                status_entry['status'] = "Sending"
                if await send_lounge_message(session, token, room_id, message, user_id):
                    status_entry['sent'] += 1
                    if spam_enabled:
                        successfully_sent_ids.append(uid)
            
            async with lock:
                processing_ids.discard(uid)
            
            await asyncio.sleep(0.2)
        
        if spam_enabled and successfully_sent_ids:
            # ASYNC CHANGE: Await the database call
            await bulk_add_sent_ids(chat_id, "lounge", successfully_sent_ids)
        
        status_entry['status'] = "Done"

    async def _refresh_ui():
        last_message = ""
        while is_running:
            lines = ["🧾 <b>AIO Lounge Status</b>", "<pre>Account    │ Sent │ Filter │ Status</pre>"]
            for data in token_status.values():
                display_name = data['name'][:10].ljust(10)
                lines.append(f"<pre>{display_name} │ {data['sent']:>4} │ {data['filtered']:>6} │ {data['status']}</pre>")
            
            current_message = "\n".join(lines)
            if current_message != last_message:
                try:
                    await bot.edit_message_text(chat_id=chat_id, message_id=status_message.message_id, text=current_message, parse_mode="HTML")
                    last_message = current_message
                except TelegramBadRequest as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"UI refresh failed: {e}")
            await asyncio.sleep(1.5)

    # NETWORK EFFICIENCY: Create one session for all workers
    async with aiohttp.ClientSession() as session:
        ui_task = asyncio.create_task(_refresh_ui())
        worker_tasks = [asyncio.create_task(_worker(session, td)) for td in tokens_data]
        await asyncio.gather(*worker_tasks)
    
    is_running = False
    await asyncio.sleep(1.6) # Allow final UI update
    ui_task.cancel()

    # Final Summary
    total_sent = sum(d['sent'] for d in token_status.values())
    total_filtered = sum(d['filtered'] for d in token_status.values())
    lines = [f"✅ <b>AIO Lounge Completed</b>\nTotal Sent: {total_sent}", "<pre>Account    │ Sent │ Filter │ Status</pre>"]
    for data in token_status.values():
        display_name = data['name'][:10].ljust(10)
        lines.append(f"<pre>{display_name} │ {data['sent']:>4} │ {data['filtered']:>6} │ {data['status']}</pre>")
    
    await bot.edit_message_text(chat_id=chat_id, message_id=status_message.message_id, text="\n".join(lines), parse_mode="HTML")
