import asyncio
import aiohttp
import logging
from typing import List, Dict, Set
from aiogram import types
from db import bulk_add_sent_ids, is_already_sent

LOUNGE_URL = "https://api.meeff.com/lounge/dashboard/v1"
CHATROOM_URL = "https://api.meeff.com/chatroom/open/v2"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8",
    'X-Device-Info': "iPhone15Pro-iOS17.5.1-6.6.2"
}

# Configure logging
logger = logging.getLogger(__name__)

async def fetch_lounge_users(session: aiohttp.ClientSession, token: str) -> List[Dict]:
    """Fetch users from lounge with a persistent session."""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    try:
        async with session.get(LOUNGE_URL, params={'locale': "en"}, headers=headers, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Failed to fetch lounge users (Status: {response.status})")
                return []
            data = await response.json()
            return data.get("both", [])
    except Exception as e:
        logger.error(f"Error fetching lounge users: {str(e)}")
        return []

async def open_chatroom_and_send(
    session: aiohttp.ClientSession, token: str, user_id: str, message: str
) -> bool:
    """Atomically opens a chatroom and sends a message."""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # 1. Open Chatroom
    try:
        payload = {"waitingRoomId": user_id, "locale": "en"}
        async with session.post(CHATROOM_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status == 412:
                logger.info(f"User {user_id} has disabled chat.")
                return False
            if response.status != 200:
                logger.warning(f"Failed to open chatroom with {user_id} (Status: {response.status})")
                return False
            data = await response.json()
            chatroom_id = data.get("chatRoom", {}).get("_id")
    except Exception as e:
        logger.error(f"Error opening chatroom with {user_id}: {e}")
        return False

    if not chatroom_id:
        return False
        
    # 2. Send Message
    try:
        payload = {"chatRoomId": chatroom_id, "message": message, "locale": "en"}
        async with session.post(SEND_MESSAGE_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status == 200:
                logger.info(f"Sent message to {user_id}")
                return True
            logger.warning(f"Failed to send message to {user_id} (Status: {response.status})")
            return False
    except Exception as e:
        logger.error(f"Error sending message to {user_id}: {e}")
        return False

async def process_lounge_batch(
    session: aiohttp.ClientSession, token: str, users: List[Dict], message: str,
    sent_ids: Set[str], processing_ids: Set[str], lock: asyncio.Lock
) -> tuple[int, int]:
    """Processes a batch of users, preventing duplicate messages across all accounts."""
    tasks = []
    users_to_process = []
    filtered_count = 0
    
    # Atomically check and claim users to process
    async with lock:
        for user in users:
            user_id = user.get("user", {}).get("_id")
            if not user_id:
                continue
            if user_id not in sent_ids and user_id not in processing_ids:
                users_to_process.append(user)
                processing_ids.add(user_id) # Claim this user ID
            else:
                filtered_count += 1
    
    # Create concurrent tasks for the claimed users
    for user in users_to_process:
        user_id = user["user"]["_id"]
        tasks.append(open_chatroom_and_send(session, token, user_id, message))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successful_ids = []
    # Process results and release claims
    async with lock:
        for i, result in enumerate(results):
            user_id = users_to_process[i]["user"]["_id"]
            if result is True:
                successful_ids.append(user_id)
            processing_ids.discard(user_id) # Release claim
            
    return len(successful_ids), filtered_count, successful_ids


async def send_lounge(
    token: str, message: str, status_message: types.Message,
    bot, chat_id: int, spam_enabled: bool
) -> None:
    """Sends a message to all users in the lounge for a single account."""
    total_sent = total_filtered = 0
    sent_ids = await is_already_sent(chat_id, "lounge", None, bulk=True) if spam_enabled else set()
    processing_ids = set()
    lock = asyncio.Lock() # For consistency, though not strictly needed for single token

    async def update_status(msg: str):
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=status_message.message_id, text=msg, parse_mode="HTML")
        except Exception:
            pass # Ignore "message not modified" errors

    await update_status("⏳ <b>Lounge Messaging:</b> Starting...")
    async with aiohttp.ClientSession() as session:
        users = await fetch_lounge_users(session, token)
        if not users:
            return await update_status("⚠️ <b>Lounge Messaging:</b> No users found.")

        batch_sent, batch_filtered, successful_ids = await process_lounge_batch(
            session, token, users, message, sent_ids, processing_ids, lock
        )
        total_sent += batch_sent
        total_filtered += batch_filtered
        
        if spam_enabled and successful_ids:
            await bulk_add_sent_ids(chat_id, "lounge", successful_ids)
        
        await update_status(f"✅ <b>Lounge Completed</b>\nSent: {total_sent} | Filtered: {total_filtered}")

async def send_lounge_all_tokens(
    tokens_data: List[Dict], message: str, status_message: types.Message,
    bot, chat_id: int, spam_enabled: bool
) -> None:
    """Processes lounge messaging concurrently for all tokens with proper deduplication."""
    token_status = {td.get("name", f"Acc {i+1}"): {"sent": 0, "filtered": 0, "status": "Queued"} for i, td in enumerate(tokens_data)}
    
    # Shared state for all workers to prevent race conditions
    sent_ids = await is_already_sent(chat_id, "lounge", None, bulk=True) if spam_enabled else set()
    processing_ids = set()
    lock = asyncio.Lock()
    running = True

    async def _worker(token_data: Dict):
        name = token_data.get("name")
        token = token_data.get("token")
        
        async with aiohttp.ClientSession() as session:
            token_status[name]["status"] = "Fetching"
            users = await fetch_lounge_users(session, token)
            if not users:
                token_status[name]["status"] = "No users"
                return

            token_status[name]["status"] = "Processing"
            batch_sent, batch_filtered, successful_ids = await process_lounge_batch(
                session, token, users, message, sent_ids, processing_ids, lock
            )

            # Update master sent list and persist to DB
            async with lock:
                sent_ids.update(successful_ids)
                if spam_enabled and successful_ids:
                    await bulk_add_sent_ids(chat_id, "lounge", successful_ids)

            token_status[name].update({"sent": batch_sent, "filtered": batch_filtered, "status": "Done"})

    async def _refresh_ui():
        last_message = ""
        while running:
            lines = ["🧾 <b>AIO Lounge Status</b>", "<pre>Account   | Sent | Filtered | State</pre>"]
            for name, status in token_status.items():
                display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
                lines.append(f"<pre>{display_name}| {status['sent']:<4} | {status['filtered']:<8} | {status['status']}</pre>")
            
            current_message = "\n".join(lines)
            if current_message != last_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=status_message.message_id,
                        text=current_message, parse_mode="HTML"
                    )
                    last_message = current_message
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"UI refresh error: {e}")
            await asyncio.sleep(1)

    # Start UI and worker tasks
    ui_task = asyncio.create_task(_refresh_ui())
    worker_tasks = [asyncio.create_task(_worker(td)) for td in tokens_data]
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Cleanup
    running = False
    await asyncio.sleep(1.1) # Allow for a final UI update
    ui_task.cancel()

    # Final Summary
    total_sent = sum(s["sent"] for s in token_status.values())
    total_filtered = sum(s["filtered"] for s in token_status.values())
    
    final_lines = [f"✅ <b>AIO Lounge Completed</b> (Total Sent: {total_sent})", "<pre>Account   | Sent | Filtered | State</pre>"]
    for name, status in token_status.items():
        display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
        final_lines.append(f"<pre>{display_name}| {status['sent']:<4} | {status['filtered']:<8} | {status['status']}</pre>")
    
    await bot.edit_message_text(
        chat_id=chat_id, message_id=status_message.message_id,
        text="\n".join(final_lines), parse_mode="HTML"
    )
