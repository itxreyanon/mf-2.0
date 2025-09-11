import aiohttp
import asyncio
import logging
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

UNSUBSCRIBE_URL = "https://api.meeff.com/chatroom/unsubscribe/v1"
CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
BASE_HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8"
}

async def fetch_chatrooms(token, from_date=None, user_id=None):
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    params = {'locale': "en"}
    if from_date:
        params['fromDate'] = from_date

    async with aiohttp.ClientSession() as session:
        async with session.get(CHATROOM_URL, params=params, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch chatrooms: {response.status}")
                return None, None
            data = await response.json()
            return data.get("rooms", []), data.get("next")

async def fetch_more_chatrooms(token, from_date, user_id=None):
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {
        "fromDate": from_date,
        "locale": "en"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(MORE_CHATROOMS_URL, json=payload, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch more chatrooms: {response.status}")
                return None, None
            data = await response.json()
            return data.get("rooms", []), data.get("next")

async def unsubscribe_chatroom(token, chatroom_id, user_id=None):
    headers = BASE_HEADERS.copy()
    headers['meeff-access-token'] = token
    
    # Get device info for this token if user_id is provided
    if user_id:
        device_info = get_or_create_device_info_for_token(user_id, token)
        headers = get_headers_with_device_info(headers, device_info)
    
    payload = {
        "chatRoomId": chatroom_id,
        "locale": "en"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(UNSUBSCRIBE_URL, json=payload, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Failed to unsubscribe chatroom: {response.status}")
                return None
            return await response.json()

async def unsubscribe_everyone(token, status_message=None, bot=None, chat_id=None, user_id=None):
    total_unsubscribed = 0
    from_date = None

    while True:
        chatrooms, next_from_date = await fetch_chatrooms(token, from_date, user_id) if from_date is None else await fetch_more_chatrooms(token, from_date, user_id)
        if not chatrooms:
            logging.info("No more chatrooms found.")
            break

        for chatroom in chatrooms:
            chatroom_id = chatroom["_id"]
            await unsubscribe_chatroom(token, chatroom_id, user_id)
            total_unsubscribed += 1
            if bot and chat_id and status_message:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message.message_id,
                    text=f"Total chatrooms unsubscribed: {total_unsubscribed}",
                )
            logging.info(f"Unsubscribed chatroom {chatroom_id}.")
            await asyncio.sleep(0.02)  # Avoid hitting API rate limits

        if not next_from_date:
            break
        from_date = next_from_date

    logging.info(f"Finished unsubscribing. Total chatrooms unsubscribed: {total_unsubscribed}")
    if bot and chat_id and status_message:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message.message_id,
            text=f"Finished unsubscribing. Total chatrooms unsubscribed: {total_unsubscribed}"
        )
