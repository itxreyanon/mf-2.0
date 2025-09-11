from db import bulk_add_sent_ids, is_already_sent
import asyncio
import aiohttp
import logging
from typing import List, Dict
from aiogram import types

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

async def fetch_lounge_users(token: str) -> List[Dict]:
    """Fetch users from lounge with improved error handling"""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    
    async with aiohttp.ClientSession() as session:
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

async def open_chatroom(token: str, user_id: str) -> str:
    """Open chatroom with a user with retry logic"""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    payload = {"waitingRoomId": user_id, "locale": "en"}
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                CHATROOM_URL,
                json=payload,
                headers=headers,
                timeout=10
            ) as response:
                if response.status == 412:
                    logger.info(f"User {user_id} has disabled chat")
                    return None
                elif response.status != 200:
                    logger.warning(f"Failed to open chatroom (Status: {response.status})")
                    return None
                data = await response.json()
                return data.get("chatRoom", {}).get("_id")
        except Exception as e:
            logger.error(f"Error opening chatroom: {str(e)}")
            return None

async def send_lounge_message(token: str, chatroom_id: str, message: str) -> bool:
    """Send message to a chatroom with error handling"""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    payload = {
        "chatRoomId": chatroom_id,
        "message": message,
        "locale": "en"
    }
    
    async with aiohttp.ClientSession() as session:
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
    spam_enabled: bool
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
            token, user, message, chat_id, spam_enabled
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
    spam_enabled: bool
) -> bool:
    """Process a single lounge user and return success status"""
    user_id = user["user"].get("_id")
    user_name = user["user"].get("name", "Unknown")
    
    if not user_id:
        logger.warning(f"User ID not found for user: {user}")
        return False
    
    # Open chatroom
    chatroom_id = await open_chatroom(token, user_id)
    if not chatroom_id:
        logger.warning(f"Failed to open chatroom with {user_name} ({user_id})")
        return False
    
    # Send message
    success = await send_lounge_message(token, chatroom_id, message)
    if success:
        logger.info(f"Sent message to {user_name} ({user_id})")
        return True
    return False


async def send_lounge(
    token: str, message: str, status_message: types.Message,
    bot, chat_id: int, spam_enabled: bool, batch_size: int = 20
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
        while users := await fetch_lounge_users(token):
            # apply spam filter
            if not spam_enabled:
                filtered = sum(u.get("is_spam", False) for u in users)
                batch = [u for u in users if not u.get("is_spam", False)]
            else:
                filtered = 0
                batch = users

            total_filtered += filtered
            sent = await process_lounge_batch(
                token, batch, message, chat_id, spam_enabled
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
    spam_enabled: bool
) -> None:
    """
    Process lounge messaging for all tokens, fetching all batches of users.
    Uses the same pagination logic as send_lounge.
    """
    logger.info(f"Spam filter enabled: {spam_enabled}")
    token_status: Dict[str, Tuple[int, int, str]] = {}
    sent_ids = await is_already_sent(chat_id, "lounge", None, bulk=True) if spam_enabled else set()
    processing_ids = set()
    lock = asyncio.Lock()

    async def _worker(token: str, tid: str, sent_ids: set):
        sent = filtered = 0
        successful_ids = []
        token_status[tid] = (sent, filtered, "Queued")
        batch_count = 0

        async with aiohttp.ClientSession(headers={**HEADERS, 'meeff-access-token': token}) as session:
            while True:
                batch_count += 1
                try:
                    users = await fetch_lounge_users(token)
                    if not users:
                        if batch_count == 1:
                            token_status[tid] = (sent, filtered, "No users")
                        break

                    logger.info(f"Token {tid} fetched {len(users)} users in batch {batch_count}")

                    # Single-pass filtering for is_spam and deduplication
                    filtered_users = []
                    for u in users:
                        uid = u["user"].get("_id")
                        if not uid:
                            continue
                        if not spam_enabled and u.get("user", {}).get("is_spam", False):
                            filtered += 1
                            continue
                        async with lock:
                            if uid not in sent_ids and uid not in processing_ids:
                                filtered_users.append(u)
                                processing_ids.add(uid)

                    total = len(filtered_users)
                    for idx, u in enumerate(filtered_users, start=1):
                        uid = u["user"]["_id"]
                        try:
                            async with session.post(
                                CHATROOM_URL,
                                json={"waitingRoomId": uid, "locale": "en"},
                                timeout=10
                            ) as r:
                                room = (await r.json()).get("chatRoom", {}).get("_id") if r.status == 200 else None
                        except Exception:
                            room = None

                        if room:
                            try:
                                async with session.post(
                                    SEND_MESSAGE_URL,
                                    json={"chatRoomId": room, "message": message, "locale": "en"},
                                    timeout=10
                                ) as r2:
                                    if r2.status == 200:
                                        sent += 1
                                        successful_ids.append(uid)
                                        logger.info(f"Token {tid} sent message to {uid}")
                            except Exception:
                                pass

                        async with lock:
                            processing_ids.discard(uid)
                        token_status[tid] = (sent, filtered, f"Batch {batch_count}, {idx}/{total}")

                    await asyncio.sleep(2)  # Match send_lounge delay

                except Exception as e:
                    logger.error(f"Token {tid} error in batch {batch_count}: {e}")
                    break

            # Save successful IDs
            if spam_enabled and successful_ids:
                await bulk_add_sent_ids(chat_id, "lounge", successful_ids)
                logger.info(f"Token {tid} saved {len(successful_ids)} IDs to database")

            token_status[tid] = (sent, filtered, "Done")

    async def _refresh():
        last_message = ""
        while any(st not in ("Done", "No users", "Fetch error") for _, (_, _, st) in token_status.items()):
            lines = [
                "🧾 <b>Lounge Status</b>\n",
                "<pre>ID  | Sent | Filtered | State</pre>",
            ]
            for tid, (s, f, st) in token_status.items():
                lines.append(f"<pre>{tid:<2} | {s:<4} | {f:<8} | {st}</pre>")
            
            current_message = "\n".join(lines)
            if current_message != last_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message.message_id,
                        text=current_message,
                        parse_mode="HTML"
                    )
                    last_message = current_message
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"Error updating status: {e}")
            await asyncio.sleep(1)

    # Spawn workers
    tasks = []
    for idx, td in enumerate(tokens_data, start=1):
        tid = str(td.get("id", idx))
        tasks.append(asyncio.create_task(_worker(td["token"], tid, sent_ids)))

    ui_task = asyncio.create_task(_refresh())
    await asyncio.gather(*tasks)
    await ui_task

    # Final summary
    lines = [
        "✅ <b>AIO Lounge completed</b>\n",
        "<pre>ID | Sent | Filtered | State</pre>",
    ]
    for tid, (s, f, _) in token_status.items():
        lines.append(f"<pre>{tid:<2} | {s:<4} | {f:<8} | Done</pre>")

    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message.message_id,
            text="\n".join(lines),
            parse_mode="HTML"
        )
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Error in final status update: {e}")
