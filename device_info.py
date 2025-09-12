# device_info.py (Corrected)
import random
import string
from typing import Dict, Optional
from db import _get_user_collection, _ensure_user_collection_exists

def _sanitize_email_for_key(email: str) -> str:
    """Replaces characters that are invalid in MongoDB field names."""
    return email.replace('.', '_')

# --- All generate_* functions and constants remain the same ---
DEVICE_MODELS = ["iPhone16,2", "iPhone16,1", "iPhone15,5", "iPhone15,4", "iPhone15,3", "iPhone15,2", "iPhone14,8", "iPhone14,7", "iPhone14,6", "iPhone14,5", "iPhone14,4", "iPhone14,3", "iPhone14,2", "iPhone13,4", "iPhone13,3", "iPhone13,2", "iPhone13,1", "iPhone12,8", "iPhone12,5", "iPhone12,3", "iPhone12,1", "iPhone11,8", "iPhone11,6", "iPhone11,4", "iPhone11,2"]
DEVICE_NAMES = ["iPhone 15 Pro Max", "iPhone 15 Pro", "iPhone 15 Plus", "iPhone 15", "iPhone 14 Pro Max", "iPhone 14 Pro", "iPhone 14 Plus", "iPhone 14", "iPhone 13 Pro Max", "iPhone 13 Pro", "iPhone 13 mini", "iPhone 13", "iPhone 12 Pro Max", "iPhone 12 Pro", "iPhone 12 mini", "iPhone 12", "iPhone 11 Pro Max", "iPhone 11 Pro", "iPhone 11", "iPhone SE"]
IOS_VERSIONS = ["iOS 17.6.1", "iOS 17.5.1", "iOS 17.4.1", "iOS 17.3.1", "iOS 17.2.1", "iOS 17.1.2", "iOS 17.0.3", "iOS 16.7.8", "iOS 16.6.1", "iOS 16.5.1"]
APP_VERSIONS = ["6.6.2", "6.6.1", "6.6.0", "6.5.9", "6.5.8"]

def generate_device_unique_id() -> str:
    return ''.join(random.choices('0123456789abcdef', k=16))

def generate_push_token() -> str:
    chars = string.ascii_letters + string.digits + '_-'
    part1 = ''.join(random.choices(chars, k=11))
    part2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=70))
    return f"{part1}:{part2}"

def generate_device_info() -> Dict[str, str]:
    model = random.choice(DEVICE_MODELS)
    device_name = random.choice(DEVICE_NAMES)
    ios_version = random.choice(IOS_VERSIONS)
    app_version = random.choice(APP_VERSIONS)
    device_id = generate_device_unique_id()
    push_token = generate_push_token()
    gmt_offsets = ["-0800", "-0700", "-0600", "-0500", "-0400"]
    gmt_offset = random.choice(gmt_offsets)
    regions = ["US"]
    region = random.choice(regions)
    return {
        "device_model": model, "device_name": device_name, "ios_version": ios_version,
        "app_version": app_version, "device_unique_id": device_id, "push_token": push_token,
        "device_info_header": f"{device_name}-{ios_version}-{app_version}",
        "device_string": f"BRAND: Apple, MODEL: {model}, DEVICE: {device_name}, PRODUCT: {device_name.replace(' ', '')}",
        "os": ios_version, "platform": "ios", "device_language": "en", "device_region": region,
        "sim_region": region, "device_gmt_offset": gmt_offset, "device_rooted": 0, "device_emulator": 0
    }

async def store_device_info_for_email(telegram_user_id: int, email: str, device_info: Dict[str, str]) -> bool:
    """Store device info for a specific email (async)"""
    await _ensure_user_collection_exists(telegram_user_id)
    sanitized_email = _sanitize_email_for_key(email)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "device_info"},
        {"$set": {f"data.{sanitized_email}": device_info}},
        upsert=True
    )
    return True

async def get_device_info_for_email(telegram_user_id: int, email: str) -> Optional[Dict[str, str]]:
    """Get device info for a specific email (async)"""
    await _ensure_user_collection_exists(telegram_user_id)
    sanitized_email = _sanitize_email_for_key(email)
    user_db = _get_user_collection(telegram_user_id)
    device_doc = await user_db.find_one({"type": "device_info"})
    if device_doc and "data" in device_doc and sanitized_email in device_doc["data"]:
        return device_doc["data"][sanitized_email]
    return None

async def get_or_create_device_info_for_email(telegram_user_id: int, email: str) -> Dict[str, str]:
    """Get existing device info for email or create new one (async)"""
    device_info = await get_device_info_for_email(telegram_user_id, email)
    if not device_info:
        user_db = _get_user_collection(telegram_user_id)
        if await user_db.find_one({"type": "device_info"}) is None:
            await user_db.insert_one({"type": "device_info", "data": {}})
        device_info = generate_device_info()
        await store_device_info_for_email(telegram_user_id, email, device_info)
    return device_info

async def store_device_info_for_token(telegram_user_id: int, token: str, device_info: Dict[str, str]) -> bool:
    """Store device info for a specific token (async)"""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "token_device_info"}, {"$set": {f"data.{token}": device_info}}, upsert=True)
    return True

async def get_device_info_for_token(telegram_user_id: int, token: str) -> Optional[Dict[str, str]]:
    """Get device info for a specific token (async)"""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    device_doc = await user_db.find_one({"type": "token_device_info"})
    if device_doc and "data" in device_doc and token in device_doc["data"]:
        return device_doc["data"][token]
    return None

async def get_or_create_device_info_for_token(telegram_user_id: int, token: str) -> Dict[str, str]:
    """Get existing device info for token or create new one (async)"""
    device_info = await get_device_info_for_token(telegram_user_id, token)
    if not device_info:
        device_info = generate_device_info()
        await store_device_info_for_token(telegram_user_id, token, device_info)
    return device_info

def get_headers_with_device_info(base_headers: Dict[str, str], device_info: Dict[str, str]) -> Dict[str, str]:
    headers = base_headers.copy()
    headers["X-Device-Info"] = device_info["device_info_header"]
    return headers

def get_api_payload_with_device_info(base_payload: Dict, device_info: Dict[str, str]) -> Dict:
    payload = base_payload.copy()
    payload.update({
        "os": device_info["os"], "platform": device_info["platform"],
        "device": device_info["device_string"], "appVersion": device_info["app_version"],
        "deviceUniqueId": device_info["device_unique_id"], "pushToken": device_info["push_token"],
        "deviceLanguage": device_info["device_language"], "deviceRegion": device_info["device_region"],
        "simRegion": device_info["sim_region"], "deviceGmtOffset": device_info["device_gmt_offset"],
        "deviceRooted": device_info["device_rooted"], "deviceEmulator": device_info["device_emulator"]
    })
    return payload
