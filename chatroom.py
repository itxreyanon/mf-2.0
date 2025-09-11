import aiohttp
import asyncio
import logging
from typing import List, Dict, Tuple
from aiogram import types
from aiogram.exceptions import TelegramBadRequest

# Import the now ASYNC db functions
from db import is_already_sent, bulk_add_sent_ids
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
BASE_HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8"
}
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- API Functions (Refactored to use a shared session) ---
async def fetch_chatrooms(session: aiohttp.ClientSession, token: str, from_date: str = None, user_id: int = None) -> Tuple[List, str]:
    params = {'locale': "en"}
    if from_date:
        params['fromDate'] = from_date
    
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    try:
        async with session.get(CHATROOM_URL, params=params, headers=headers, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("rooms", []), data.get("next")
    except Exception as e:
        logging.error(f"Error fetching chatrooms: {e}")
        return [], None

async def fetch_more_chatrooms(session: aiohttp.ClientSession, token: str, from_date: str, user_id: int = None) -> Tuple[List, str]:
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"fromDate": from_date, "locale": "en"}
    try:
        async with session.post(MORE_CHATROOMS_URL, json=payload, headers=headers, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("rooms", []), data.get("next")
    except Exception as e:
        logging.error(f"Error fetching more chatrooms: {e}")
        return [], None

async def send_message(session: aiohttp.ClientSession, token: str, chatroom_id: str, message: str, user_id: int = None) -> bool:
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"chatRoomId": chatroom_id, "message": message, "locale": "en"}
    try:
        async with session.post(SEND_MESSAGE_URL, json=payload, headers=headers, timeout=10) as response:
            return response.status == 200
    except Exception as e:
        logging.error(f"Error sending message to {chatroom_id}: {e}")
        return False

# --- Processing Logic ---
async def process_chatroom_batch(session: aiohttp.ClientSession, token: str, chatrooms: List[Dict], message: str, chat_id: int, spam_enabled: bool, sent_ids: set, sent_ids_lock: asyncio.Lock, user_id: int) -> Tuple[int, int, int]:
    filtered_rooms = []
    
    if spam_enabled:
        room_ids = {room.get('_id') for room in chatrooms if room.get('_id')}
        
        # In-memory check for multi-token runs
        if sent_ids is not None and sent_ids_lock is not None:
            async with sent_ids_lock:
                new_room_ids = room_ids - sent_ids
                filtered_rooms = [room for room in chatrooms if room.get('_id') in new_room_ids]
        # Database check for single-token runs
        else:
            # ASYNC CHANGE: Await the database call
            existing_ids = await is_already_sent(chat_id, "chatroom", list(room_ids), bulk=True)
            new_room_ids = room_ids - existing_ids
            filtered_rooms = [room for room in chatrooms if room.get('_id') in new_room_ids]
    else:
        filtered_rooms = chatrooms

    filtered_count = len(chatrooms) - len(filtered_rooms)
    if not filtered_rooms:
        return len(chatrooms), 0, filtered_count

    tasks = [send_message(session, token, room.get('_id'), message, user_id) for room in filtered_rooms]
    results = await asyncio.gather(*tasks)
    sent_count = sum(1 for res in results if res is True)

    if spam_enabled:
        sent_ids_batch = [room.get('_id') for room, success in zip(filtered_rooms, results) if success]
        if sent_ids_batch:
            # ASYNC CHANGE: Await the database call
            await bulk_add_sent_ids(chat_id, "chatroom", sent_ids_batch)
            if sent_ids is not None and sent_ids_lock is not None:
                async with sent_ids_lock:
                    sent_ids.update(sent_ids_batch)

    return len(chatrooms), sent_count, filtered_count

async def send_message_to_everyone(token: str, message: str, chat_id: int, spam_enabled: bool, user_id: int, sent_ids: set = None, sent_ids_lock: asyncio.Lock = None, status_entry: Dict = None) -> Tuple[int, int, int]:
    total_chatrooms, sent_count, filtered_count = 0, 0, 0
    from_date = None
    
    # NETWORK EFFICIENCY: Create one session for the entire task
    async with aiohttp.ClientSession() as session:
        while True:
            fetch_func = fetch_chatrooms if from_date is None else fetch_more_chatrooms
            rooms, next_from = await fetch_func(session, token, from_date, user_id)
            
            if not rooms:
                break
                
            batch_total, batch_sent, batch_filtered = await process_chatroom_batch(
                session, token, rooms, message, chat_id, spam_enabled, sent_ids, sent_ids_lock, user_id
            )
            total_chatrooms += batch_total
            sent_count += batch_sent
            filtered_count += batch_filtered
            
            if status_entry is not None:
                status_entry.update({'rooms': total_chatrooms, 'sent': sent_count, 'filtered': filtered_count, 'status': "Processing"})

            if not next_from:
                break
            from_date = next_from
            
    return total_chatrooms, sent_count, filtered_count

async def send_message_to_everyone_all_tokens(tokens: List[str], message: str, status_message: types.Message, bot, chat_id: int, spam_enabled: bool, token_names: Dict[str, str], use_in_memory_deduplication: bool, user_id: int):
    """Send messages to everyone for multiple tokens concurrently."""
    token_status = {token: {'name': token_names.get(token, token[:6]), 'rooms': 0, 'sent': 0, 'filtered': 0, 'status': "Queued"} for token in tokens}
    
    # Use a shared set for all workers to prevent sending to the same chatroom multiple times in one run
    sent_ids = set() if use_in_memory_deduplication and spam_enabled else None
    sent_ids_lock = asyncio.Lock() if sent_ids is not None else None
    
    running = True

    async def _worker(token: str):
        display_name = token_names.get(token, token[:6])
        status_entry = token_status[token]
        status_entry['status'] = "Processing"
        try:
            await send_message_to_everyone(
                token, message, chat_id, spam_enabled, user_id,
                sent_ids=sent_ids, sent_ids_lock=sent_ids_lock, status_entry=status_entry
            )
            status_entry['status'] = "Done"
        except Exception as e:
            logging.error(f"[{display_name}] worker failed: {e}", exc_info=True)
            status_entry['status'] = f"Failed"

    async def _refresh_ui():
        last_message = ""
        while running:
            lines = ["🔄 <b>Chatroom AIO Status</b>", "<pre>Account    │ Rooms │ Sent  │ Filter │ Status</pre>"]
            for status in token_status.values():
                display_name = status['name'][:10].ljust(10)
                lines.append(f"<pre>{display_name}│{status['rooms']:>5} │{status['sent']:>5} │{status['filtered']:>6} │{status['status']}</pre>")
            
            current_message = "\n".join(lines)
            if current_message != last_message:
                try:
                    await bot.edit_message_text(chat_id=chat_id, message_id=status_message.message_id, text=current_message, parse_mode="HTML")
                    last_message = current_message
                except TelegramBadRequest as e:
                    if "message is not modified" not in str(e):
                        logging.error(f"Error updating UI: {e}")
                except Exception as e:
                    logging.error(f"UI refresh failed: {e}")
            await asyncio.sleep(1.5)

    ui_task = asyncio.create_task(_refresh_ui())
    
    worker_tasks = [asyncio.create_task(_worker(token)) for token in tokens]
    await asyncio.gather(*worker_tasks)

    # Cleanup and final summary
    running = False
    await asyncio.sleep(1.6) # Allow one final UI update
    ui_task.cancel()

    successful_tokens = sum(1 for s in token_status.values() if s['status'] == "Done")
    success_rate = (successful_tokens / len(tokens)) * 100 if tokens else 0
    emoji = "✅" if success_rate > 90 else "⚠️" if success_rate > 70 else "❌"
    
    lines = [f"{emoji} <b>Chatroom AIO Completed</b> ({successful_tokens}/{len(tokens)} success)", "<pre>Account    │ Rooms │ Sent  │ Filter │ Status</pre>"]
    for status in token_status.values():
        display_name = status['name'][:10].ljust(10)
        lines.append(f"<pre>{display_name}│{status['rooms']:>5} │{status['sent']:>5} │{status['filtered']:>6} │{status['status']}</pre>")
    
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=status_message.message_id, text="\n".join(lines), parse_mode="HTML")
    except Exception as e:
        if "message is not modified" not in str(e):
            logging.error(f"Error in final status update: {e}")
