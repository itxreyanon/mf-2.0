from db import bulk_add_sent_ids, is_already_sent
import asyncio
import aiohttp
import logging
from contextlib import asynccontextmanager
from typing import List, Dict
from aiogram import types
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
logger = logging.getLogger(__name__)

# Connection pool for better performance
_session_pool = None

@asynccontextmanager
async def get_session():
    """Get a shared session with connection pooling."""
    global _session_pool
    if _session_pool is None or _session_pool.closed:
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30
        )
        _session_pool = aiohttp.ClientSession(connector=connector)
    
    yield _session_pool

async def fetch_lounge_users(token: str, user_id: int = None) -> List[Dict]:
    """Fetch users from lounge with improved error handling"""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    async with get_session() as session:
        try:
            async with session.get(
                LOUNGE_URL, 
                params={'locale': "en"},
                headers=headers,
                timeout=10
            ) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch lounge users (Status: {response.status})")
                    return []
                data = await response.json()
                return data.get("both", [])
        except Exception as e:
            logger.error(f"Error fetching lounge users: {str(e)}")
            return []

async def open_chatroom(token: str, target_user_id: str, telegram_user_id: int = None) -> str:
    """Open chatroom with a user with retry logic"""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if telegram_user_id is provided
    if telegram_user_id:
        device_info = get_or_create_device_info_for_token(telegram_user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"waitingRoomId": target_user_id, "locale": "en"}
    
    async with get_session() as session:
        try:
            async with session.post(
                CHATROOM_URL,
                json=payload,
                headers=headers,
                timeout=10
            ) as response:
                if response.status == 412:
                    logger.info(f"User {target_user_id} has disabled chat")
                    return None
                elif response.status != 200:
                    logger.warning(f"Failed to open chatroom (Status: {response.status})")
                    return None
                data = await response.json()
                return data.get("chatRoom", {}).get("_id")
        except Exception as e:
            logger.error(f"Error opening chatroom: {str(e)}")
            return None

async def send_lounge_message(token: str, chatroom_id: str, message: str, user_id: int = None) -> bool:
    """Send message to a chatroom with error handling"""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {
        "chatRoomId": chatroom_id,
        "message": message,
        "locale": "en"
    }
    
    async with get_session() as session:
        try:
            async with session.post(
                SEND_MESSAGE_URL,
                json=payload,
                headers=headers,
                timeout=10
            ) as response:
                if response.status != 200:
                    logger.warning(f"Failed to send message (Status: {response.status})")
                    return False
                return True
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            return False

async def process_lounge_batch(
    token: str,
    users: List[Dict],
    message: str,
    chat_id: int,
    spam_enabled: bool,
    user_id: int = None
) -> int:
    """
    Process a batch of lounge users concurrently
    Returns number of successfully sent messages
    """
    sent_count = 0
    sent_ids = []
    
    # Filter users based on spam filter
    if spam_enabled:
        user_ids = [user["user"]["_id"] for user in users if user.get("user", {}).get("_id")]
        existing_ids = await is_already_sent(chat_id, "lounge", user_ids, bulk=True)
        users = [user for user in users 
                if user.get("user", {}).get("_id") 
                and user["user"]["_id"] not in existing_ids]
    
    # Process users concurrently
    tasks = []
    for user in users:
        user_id = user["user"]["_id"]
        tasks.append(process_single_lounge_user(
            token, user, message, chat_id, spam_enabled, user_id
        ))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    sent_count = sum(1 for result in results if result is True)
    
    # Bulk add sent IDs if spam filter enabled
    if spam_enabled:
        sent_ids = [user["user"]["_id"] for user, success in zip(users, results) 
                   if success is True]
        if sent_ids:
            await bulk_add_sent_ids(chat_id, "lounge", sent_ids)
    
    return sent_count

async def process_single_lounge_user(
    token: str,
    user: Dict,
    message: str,
    chat_id: int,
    spam_enabled: bool,
    user_id: int = None
) -> bool:
    """Process a single lounge user and return success status"""
    target_user_id = user["user"].get("_id")
    user_name = user["user"].get("name", "Unknown")
    
    if not target_user_id:
        logger.warning(f"User ID not found for user: {user}")
        return False
    
    # Open chatroom
    chatroom_id = await open_chatroom(token, target_user_id, user_id)
    if not chatroom_id:
        logger.warning(f"Failed to open chatroom with {user_name} ({target_user_id})")
        return False
    
    # Send message
    success = await send_lounge_message(token, chatroom_id, message, user_id)
    if success:
        logger.info(f"Sent message to {user_name} ({target_user_id})")
        return True
    return False


async def send_lounge(
    token: str, message: str, status_message: types.Message, 
    bot, chat_id: int, spam_enabled: bool, batch_size: int = 20, user_id: int = None
) -> None:
    total_sent = total_filtered = 0

    async def upd(msg: str):
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message.message_id,
            text=msg
        )

    try:
        await upd("⏳ loading…")
        while users := await fetch_lounge_users(token, user_id):
            # apply spam filter
            if not spam_enabled:
                filtered = sum(u.get("is_spam", False) for u in users)
                batch = [u for u in users if not u.get("is_spam", False)]
            else:
                filtered = 0
                batch = users

            total_filtered += filtered
            sent = await process_lounge_batch(
                token, batch, message, chat_id, spam_enabled, user_id
            )
            total_sent += sent

            await upd(
                f"🔍 {len(users)} users fetched | Sent: {total_sent} | Filtered: {total_filtered}"
            )
            await asyncio.sleep(2)

        await upd(f"⚠️ no users | Sent: {total_sent} | Filtered: {total_filtered}")

    except asyncio.CancelledError:
        await upd(f"🛑 cancelled | Sent: {total_sent} | Filtered: {total_filtered}")
        raise

    except Exception as e:
        logger.error(f"Lounge error: {e}")
        await upd(f"❌ {e} | Sent: {total_sent} | Filtered: {total_filtered}")

    else:
        await upd(f" lounge completed ✅ | Sent: {total_sent} | Filtered: {total_filtered}")

async def send_lounge_all_tokens(
    tokens_data: List[Dict],
    message: str,
    status_message: types.Message,
    bot,
    chat_id: int,
    spam_enabled: bool,
    user_id: int = None
) -> None:
    """
    Process lounge messaging for all tokens.
    Uses the original line-by-line alignment and displays account names.
    """
    logger.info(f"Spam filter enabled: {spam_enabled}")
    # MODIFIED: Use the robust token-as-key data structure to track all accounts
    token_status: Dict[str, Dict] = {}
    sent_ids = await is_already_sent(chat_id, "lounge", None, bulk=True) if spam_enabled else set()
    processing_ids = set()
    lock = asyncio.Lock()

    async def _worker(token_data: Dict, sent_ids: set):
        token = token_data["token"]
        status_entry = token_status[token]
        
        sent = 0
        filtered = 0
        successful_ids = []
        batch_count = 0

        # Get device info for this token
        device_info = get_or_create_device_info_for_token(user_id, token) if user_id else None
        session_headers = BASE_HEADERS.copy()
        session_headers['meeff-access-token'] = token
        if device_info:
            session_headers = get_headers_with_device_info(session_headers, device_info)
        
        async with get_session() as session:
            while True:
                batch_count += 1
                try:
                    users = await fetch_lounge_users(token, user_id)
                    if not users:
                        if batch_count == 1:
                            status_entry['status'] = "No users"
                        break

                    filtered_users = []
                    for u in users:
                        uid = u["user"].get("_id")
                        if not uid: continue
                        if not spam_enabled and u.get("user", {}).get("is_spam", False):
                            filtered += 1
                            continue
                        async with lock:
                            if uid not in sent_ids and uid not in processing_ids:
                                filtered_users.append(u)
                                processing_ids.add(uid)
                    
                    status_entry['filtered'] = filtered
                    total = len(filtered_users)
                    for idx, u in enumerate(filtered_users, start=1):
                        uid = u["user"]["_id"]
                        room = None
                        try:
                            async with session.post(CHATROOM_URL, json={"waitingRoomId": uid, "locale": "en"}, timeout=10) as r:
                                room = (await r.json()).get("chatRoom", {}).get("_id") if r.status == 200 else None
                        except Exception: pass

                        if room:
                            try:
                                async with session.post(SEND_MESSAGE_URL, json={"chatRoomId": room, "message": message, "locale": "en"}, timeout=10) as r2:
                                    if r2.status == 200:
                                        sent += 1
                                        successful_ids.append(uid)
                            except Exception: pass
                        
                        async with lock:
                            processing_ids.discard(uid)

                        status_entry['sent'] = sent
                        status_entry['status'] = f"Batch {batch_count}, {idx}/{total}"

                    await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"Token {status_entry['name']} error in batch {batch_count}: {e}")
                    break

            if spam_enabled and successful_ids:
                await bulk_add_sent_ids(chat_id, "lounge", successful_ids)
            
            if status_entry['status'] not in ["No users"]:
                 status_entry['status'] = "Done"

    async def _refresh():
        last_message = ""
        while any(d['status'] not in ("Done", "No users", "Fetch error") for d in token_status.values()):
            # MODIFIED: Use the original line-by-line <pre> method as requested
            lines = [
                "🧾 <b>Lounge Status</b>\n",
                "<pre>Account │Sent  │Filtered│Status</pre>",
            ]
            for status_dict in token_status.values():
                name = status_dict['name']
                s = status_dict['sent']
                f = status_dict['filtered']
                st = status_dict['status']
                lines.append(f"<pre>{name:<10} │{s:<5} │{f:<8} │{st}</pre>")
            
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
                        logger.error(f"Error updating status: {e}")
            await asyncio.sleep(1)

    # Initialize the data structure to track all accounts
    for idx, td in enumerate(tokens_data, start=1):
        token = td['token']
        name = td.get("name", f"Account {idx}")
        token_status[token] = {'name': name, 'sent': 0, 'filtered': 0, 'status': 'Queued'}

    # Spawn workers
    tasks = [asyncio.create_task(_worker(td, sent_ids)) for td in tokens_data]

    ui_task = asyncio.create_task(_refresh())
    await asyncio.gather(*tasks)
    await ui_task

    # Final summary using the original alignment method
    lines = [
        "✅ <b>AIO Lounge completed</b>\n",
        "<pre>Account │Sent  │Filtered│Status</pre>",
    ]
    for status_dict in token_status.values():
        name = status_dict['name']
        s = status_dict['sent']
        f = status_dict['filtered']
        lines.append(f"<pre>{name:<10} │{s:<5} │{f:<8} │Done</pre>")

    final_message = "\n".join(lines)

    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=status_message.message_id,
            text=final_message, parse_mode="HTML"
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Error in final status update: {e}")
