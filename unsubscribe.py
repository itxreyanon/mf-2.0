import aiohttp
import asyncio
import logging
from typing import List, Tuple
from aiogram import types
from aiogram.exceptions import TelegramBadRequest

from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# --- Constants ---
UNSUBSCRIBE_URL = "https://api.meeff.com/chatroom/unsubscribe/v1"
CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
BASE_HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8"
}
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# --- API Functions (Refactored to use a shared session) ---
async def fetch_chatrooms_page(session: aiohttp.ClientSession, token: str, from_date: str = None, user_id: int = None) -> Tuple[List, str]:
    """Fetches a single page of chatrooms."""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    try:
        if from_date is None:
            # First page uses GET
            url = CHATROOM_URL
            params = {'locale': "en"}
            async with session.get(url, params=params, headers=headers, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("rooms", []), data.get("next")
        else:
            # Subsequent pages use POST
            url = MORE_CHATROOMS_URL
            payload = {"fromDate": from_date, "locale": "en"}
            async with session.post(url, json=payload, headers=headers, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("rooms", []), data.get("next")
    except Exception as e:
        logging.error(f"Failed to fetch chatrooms: {e}")
        return [], None

async def unsubscribe_from_chatroom(session: aiohttp.ClientSession, token: str, chatroom_id: str, user_id: int = None) -> bool:
    """Unsubscribes from a single chatroom and returns success status."""
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {"chatRoomId": chatroom_id, "locale": "en"}

    try:
        async with session.post(UNSUBSCRIBE_URL, json=payload, headers=headers, timeout=7) as response:
            if response.status == 200:
                return True
            logging.warning(f"Failed to unsubscribe from {chatroom_id}: Status {response.status}")
            return False
    except Exception as e:
        logging.error(f"Error unsubscribing from {chatroom_id}: {e}")
        return False

# --- Main Logic ---
async def unsubscribe_everyone(token: str, status_message: types.Message = None, bot=None, chat_id: int = None, user_id: int = None):
    """
    Fetches all chatrooms and unsubscribes from them concurrently in batches.
    """
    total_unsubscribed = 0
    from_date = None
    
    # NETWORK EFFICIENCY: Create one session for the entire unsubscribe process
    async with aiohttp.ClientSession() as session:
        while True:
            if status_message and bot:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message.message_id,
                        text=f"🔄 Unsubscribing... Total left: {total_unsubscribed}"
                    )
                except TelegramBadRequest:
                    pass # Ignore "message not modified" errors

            chatrooms, next_from_date = await fetch_chatrooms_page(session, token, from_date, user_id)
            
            if not chatrooms:
                logging.info("No more chatrooms found.")
                break

            # CONCURRENCY: Create unsubscribe tasks for the entire batch
            tasks = [
                unsubscribe_from_chatroom(session, token, chatroom["_id"], user_id)
                for chatroom in chatrooms if "_id" in chatroom
            ]
            
            results = await asyncio.gather(*tasks)
            
            # Count how many tasks in the batch were successful
            batch_unsubscribed_count = sum(1 for res in results if res is True)
            total_unsubscribed += batch_unsubscribed_count
            logging.info(f"Unsubscribed from {batch_unsubscribed_count} chatrooms in this batch.")

            if not next_from_date:
                break
            from_date = next_from_date
            
            await asyncio.sleep(1) # Small delay between fetching pages to be safe

    logging.info(f"Finished unsubscribing. Total chatrooms unsubscribed: {total_unsubscribed}")
    if status_message and bot:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message.message_id,
            text=f"✅ Finished! Total chatrooms unsubscribed: {total_unsubscribed}"
        )
