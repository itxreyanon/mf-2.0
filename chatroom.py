import aiohttp
import asyncio
import logging
from datetime import datetime
from db import is_already_sent, add_sent_id, bulk_add_sent_ids
from typing import List, Dict, Tuple
from aiogram import types
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
BASE_HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8"
}

async def fetch_chatrooms(session, token, from_date=None, user_id=None):
    params = {'locale': "en"}
    if from_date:
        params['fromDate'] = from_date
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    try:
        async with session.get(CHATROOM_URL, params=params, headers=headers, timeout=10) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch chatrooms: {response.status}")
                return [], None
            data = await response.json()
            return data.get("rooms", []), data.get("next")
    except Exception as e:
        logging.error(f"Error fetching chatrooms: {str(e)}")
        return [], None

async def fetch_more_chatrooms(session, token, from_date, user_id=None):
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"fromDate": from_date, "locale": "en"}
    try:
        async with session.post(MORE_CHATROOMS_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch more chatrooms: {response.status}")
                return [], None
            data = await response.json()
            return data.get("rooms", []), data.get("next")
    except Exception as e:
        logging.error(f"Error fetching more chatrooms: {str(e)}")
        return [], None

async def send_message(session, token, chatroom_id, message, user_id=None):
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"chatRoomId": chatroom_id, "message": message, "locale": "en"}
    try:
        async with session.post(SEND_MESSAGE_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status != 200:
                logging.error(f"Failed to send message to {chatroom_id}: {response.status}")
                return None
            return await response.json()
    except Exception as e:
        logging.error(f"Error sending message to {chatroom_id}: {str(e)}")
        return None

async def process_chatroom_batch(session, token, chatrooms, message, chat_id, spam_enabled, sent_ids=None, sent_ids_lock=None, user_id=None):
    sent_count = 0
    filtered_count = 0
    filtered_rooms = []
    if spam_enabled:
        room_ids = [room.get('_id') for room in chatrooms]
        if sent_ids is not None:
            async with sent_ids_lock:
                filtered_rooms = [room for room in chatrooms if room.get('_id') not in sent_ids]
            filtered_count = len(chatrooms) - len(filtered_rooms)
        else:
            existing_ids = await is_already_sent(chat_id, "chatroom", room_ids, bulk=True)
            filtered_rooms = [room for room in chatrooms if room.get('_id') not in existing_ids]
            filtered_count = len(chatrooms) - len(filtered_rooms)
    else:
        filtered_rooms = chatrooms
    tasks = [send_message(session, token, room.get('_id'), message, user_id) for room in filtered_rooms]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    sent_count = sum(1 for result in results if result is not None)
    if spam_enabled and filtered_rooms:
        sent_ids_batch = [room.get('_id') for room in filtered_rooms]
        await bulk_add_sent_ids(chat_id, "chatroom", sent_ids_batch)
        if sent_ids is not None:
            async with sent_ids_lock:
                sent_ids.update(sent_ids_batch)
    return len(chatrooms), sent_count, filtered_count

async def send_message_to_everyone(
    token, message, chat_id=None, spam_enabled=True, user_id=None,
    sent_ids=None, sent_ids_lock=None, status_entry=None):
    
    sent_count = 0
    total_chatrooms = 0
    filtered_count = 0
    from_date = None
    async with aiohttp.ClientSession() as session:
        while True:
            rooms, next_from = await (fetch_chatrooms(session, token, from_date, user_id)
                                      if from_date is None else
                                      fetch_more_chatrooms(session, token, from_date, user_id))
            if not rooms:
                break
            batch_total, batch_sent, batch_filtered = await process_chatroom_batch(
                session, token, rooms, message, chat_id, spam_enabled, sent_ids, sent_ids_lock, user_id
            )
            total_chatrooms += batch_total
            sent_count += batch_sent
            filtered_count += batch_filtered
            
            if status_entry is not None:
                status_entry['rooms'] = total_chatrooms
                status_entry['sent'] = sent_count
                status_entry['filtered'] = filtered_count
                status_entry['status'] = "Processing"

            if not next_from:
                break
            from_date = next_from
    return total_chatrooms, sent_count, filtered_count

# CORRECTED AND REFACTORED CODE

async def send_message_to_everyone_all_tokens(
    tokens: List[str],
    message: str,
    status_message: 'types.Message' = None,
    bot=None,
    chat_id: int = None,
    spam_enabled: bool = True,
    token_names: Dict[str, str] = None,
    use_in_memory_deduplication: bool = False,
    user_id: int = None
) -> None:
    """
    Send messages to everyone for multiple tokens concurrently.
    Correctly handles and displays accounts that have the same name.
    """
    token_status = {}
    sent_ids = set() if use_in_memory_deduplication and spam_enabled else None
    sent_ids_lock = asyncio.Lock() if sent_ids is not None else None
    
    # --- CHANGE: Added a reliable flag to control the UI loop ---
    running = True

    async def _worker(token: str):
        display_name = token_names.get(token, token[:6]) if token_names else token[:6]
        token_status[token] = {'name': display_name, 'rooms': 0, 'sent': 0, 'filtered': 0, 'status': "Processing"}

        try:
            rooms, sent, filtered = await send_message_to_everyone(
                token,
                message,
                chat_id=chat_id,
                spam_enabled=spam_enabled,
                user_id=user_id,
                sent_ids=sent_ids,
                sent_ids_lock=sent_ids_lock,
                status_entry=token_status[token] # Pass the dictionary for live updates
            )
            token_status[token]['status'] = "Done"
            return True
        except Exception as e:
            logging.error(f"[{display_name}] failed: {str(e)}")
            token_status[token]['status'] = f"Failed: {str(e)[:20]}..."
            return False

    async def _refresh_ui():
        last_message = ""
        while running: # --- CHANGE: Loop condition is now simpler and more reliable ---
            lines = [
                "🔄 <b>Chatroom AIO Status</b>\n",
                "<pre>Account   │Rooms │Sent  │Filter│Status</pre>"
            ]
            
            for status in token_status.values():
                name = status.get('name', 'N/A')
                rooms = status.get('rooms', 0)
                sent = status.get('sent', 0)
                filtered = status.get('filtered', 0)
                stat = status.get('status', 'Queued')
                
                display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
                lines.append(
                    f"<pre>{display_name}│{rooms:>5} │{sent:>5} │{filtered:>6}│{stat}</pre>"
                )
            
            current_message = "\n".join(lines)
            if current_message != last_message and bot and chat_id and status_message:
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
                        logging.error(f"Error updating status: {e}")
            await asyncio.sleep(1)

    # Initialize token status
    for token in tokens:
        display_name = token_names.get(token, token[:6]) if token_names else token[:6]
        token_status[token] = {'name': display_name, 'rooms': 0, 'sent': 0, 'filtered': 0, 'status': "Queued"}

    ui_task = asyncio.create_task(_refresh_ui()) if bot and chat_id and status_message else None
    
    worker_tasks = [asyncio.create_task(_worker(token)) for token in tokens]
    results = await asyncio.gather(*worker_tasks, return_exceptions=True)

    # --- CHANGE: Proper cleanup of the background UI task ---
    running = False
    if ui_task:
        await asyncio.sleep(1.1) # Allow one final UI update
        ui_task.cancel()
        try:
            await ui_task
        except asyncio.CancelledError:
            pass # Task cancellation is expected

    successful_tokens = sum(1 for result in results if result is True)
    grand_rooms = sum(status.get('rooms', 0) for status in token_status.values())
    grand_sent = sum(status.get('sent', 0) for status in token_status.values())
    grand_filtered = sum(status.get('filtered', 0) for status in token_status.values())

    logging.info(
        f"[AllTokens] Finished: {successful_tokens}/{len(tokens)} tokens succeeded. "
        f"Total Rooms={grand_rooms}, Total Sent={grand_sent}, Total Filtered={grand_filtered}"
    )

    if bot and chat_id and status_message:
        success_rate = (successful_tokens / len(tokens)) * 100 if len(tokens) > 0 else 0
        success_emoji = "✅" if success_rate > 90 else "⚠️" if success_rate > 70 else "❌"
        lines = [
            f"{success_emoji} <b>Chatroom AIO Completed</b> - {successful_tokens}/{len(tokens)} ({success_rate:.1f}%)\n",
            "<pre>Account   │Rooms │Sent  │Filter│Status</pre>"
        ]
        for status in token_status.values():
            name = status.get('name', 'N/A')
            rooms = status.get('rooms', 0)
            sent = status.get('sent', 0)
            filtered = status.get('filtered', 0)
            stat = status.get('status', 'Done')
            
            display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
            lines.append(
                f"<pre>{display_name}│{rooms:>5} │{sent:>5} │{filtered:>6}│{stat}</pre>"
            )
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message.message_id,
                text="\n".join(lines),
                parse_mode="HTML"
            )
        except Exception as e:
            if "message is not modified" not in str(e):
                logging.error(f"Error in final status update: {e}")
