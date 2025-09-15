import asyncio
import aiohttp
import logging
import html
from aiogram import Bot
from aiogram.types import Message
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def post_to_lounge(session, token, message, user_id):
    """Helper function to post a single message to the lounge for a given token."""
    url = "https://api.meeff.com/lounge/post/v1"
    payload = {"message": message, "locale": "en"}
    
    # Get consistent device info for the token
    device_info = await get_or_create_device_info_for_token(user_id, token)
    base_headers = {
        'User-Agent': "okhttp/4.12.0",
        'meeff-access-token': token,
        'Content-Type': "application/json; charset=utf-8"
    }
    headers = get_headers_with_device_info(base_headers, device_info)
    
    try:
        # The 'await' here is critical. It executes the web request.
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status == 200:
                logging.info(f"Lounge post successful for token {token[:10]}...")
                return True, "Success"
            else:
                resp_json = await response.json()
                error_message = resp_json.get("errorMessage", f"HTTP {response.status}")
                logging.error(f"Failed to post to lounge for token {token[:10]...}: {error_message}")
                return False, error_message
    except Exception as e:
        logging.error(f"Exception posting to lounge for token {token[:10]...}: {e}")
        return False, str(e)

async def send_lounge(token: str, custom_message: str, status_message: Message, bot: Bot, user_id: int, spam_enabled: bool):
    """Handles the /lounge command for a single account."""
    async with aiohttp.ClientSession() as session:
        success, reason = await post_to_lounge(session, token, custom_message, user_id)
        
        if success:
            # The 'await' here executes the message edit.
            await status_message.edit_text("✅ <b>Lounge Message Sent!</b>", parse_mode="HTML")
        else:
            await status_message.edit_text(f"❌ <b>Failed:</b> {html.escape(reason)}", parse_mode="HTML")

async def send_lounge_all_tokens(active_tokens_data: list, custom_message: str, status: Message, bot: Bot, user_id: int, spam_enabled: bool):
    """Handles the /send_lounge_all command for multiple accounts."""
    success_count = 0
    failure_count = 0
    total_tokens = len(active_tokens_data)
    
    async with aiohttp.ClientSession() as session:
        for i, token_data in enumerate(active_tokens_data, 1):
            token = token_data['token']
            name = token_data.get('name', f"Account {i}")
            
            # The 'await' here executes the status update.
            await status.edit_text(
                f"🔄 <b>Processing:</b> {i}/{total_tokens}\n"
                f"<b>Account:</b> {html.escape(name)}",
                parse_mode="HTML"
            )
            
            success, reason = await post_to_lounge(session, token, custom_message, user_id)
            
            if success:
                success_count += 1
            else:
                failure_count += 1
            
            await asyncio.sleep(1) # Small delay between accounts

    final_message = (
        f"✅ <b>Lounge Messaging Complete!</b>\n\n"
        f"<b>Successful:</b> {success_count}\n"
        f"<b>Failed:</b> {failure_count}"
    )
    await status.edit_text(final_message, parse_mode="HTML")
