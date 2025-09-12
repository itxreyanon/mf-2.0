import random
import string
from typing import Dict, Optional
from db import _get_user_collection, _ensure_user_collection_exists

# --- Helper Functions (No DB interaction, remain synchronous) ---

def _sanitize_email_for_key(email: str) -> str:
    """Replaces characters that are invalid in MongoDB field names."""
    return email.replace('.', '_')

def generate_device_unique_id() -> str:
    """Generate a unique device ID (16 hex characters)."""
    return ''.join(random.choices('0123456789abcdef', k=16))

def generate_push_token() -> str:
    """Generate a realistic push token."""
    chars = string.ascii_letters + string.digits + '_-'
    part1 = ''.join(random.choices(chars, k=11))
    part2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=70))
    return f"{part1}:{part2}"

DEVICE_MODELS = ["iPhone16,2", "iPhone16,1", "iPhone15,5", "iPhone15,4", "iPhone15,3", "iPhone15,2", "iPhone14,8", "iPhone14,7", "iPhone14,6", "iPhone14,5", "iPhone14,4", "iPhone14,3", "iPhone14,2", "iPhone13,4", "iPhone13,3", "iPhone13,2", "iPhone13,1"]
DEVICE_NAMES = ["iPhone 15 Pro Max", "iPhone 15 Pro", "iPhone 15 Plus", "iPhone 15", "iPhone 14 Pro Max", "iPhone 14 Pro", "iPhone 14 Plus", "iPhone 14", "iPhone 13 Pro Max", "iPhone 13 Pro", "iPhone 13 mini", "iPhone 13"]
IOS_VERSIONS = ["iOS 17.6.1", "iOS 17.5.1", "iOS 17.4.1", "iOS 17.3.1", "iOS 17.2.1", "iOS 17.1.2", "iOS 17.0.3", "iOS 16.7.8"]
APP_VERSIONS = ["6.6.2", "6.6.1", "6.6.0", "6.5.9", "6.5.8"]

def generate_device_info() -> Dict[str, str]:
    """Generate complete device information."""
    model = random.choice(DEVICE_MODELS)
    device_name = random.choice(DEVICE_NAMES)
    ios_version = random.choice(IOS_VERSIONS)
    app_version = random.choice(APP_VERSIONS)
    return {
        "device_model": model, "device_name": device_name, "ios_version": ios_version,
        "app_version": app_version, "device_unique_id": generate_device_unique_id(),
        "push_token": generate_push_token(), "device_info_header": f"{model}-{ios_version}-{app_version}",
        "device_string": f"BRAND: Apple, MODEL: {model}, DEVICE: {model}, PRODUCT: {model}",
        "os": ios_version, "platform": "ios", "device_language": "en", "device_region": "US",
        "sim_region": "US", "device_gmt_offset": "-0500", "device_rooted": 0, "device_emulator": 0
    }

def get_headers_with_device_info(base_headers: Dict[str, str], device_info: Dict[str, str]) -> Dict[str, str]:
    """Injects device info into API request headers."""
    headers = base_headers.copy()
    headers["X-Device-Info"] = device_info["device_info_header"]
    return headers

def get_api_payload_with_device_info(base_payload: Dict, device_info: Dict[str, str]) -> Dict:
    """Injects device info into an API request payload."""
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


# --- Async DB Functions for Device Info ---

async def store_device_info_for_email(telegram_user_id: int, email: str, device_info: Dict[str, str]):
    """Store device info for a specific email asynchronously."""
    await _ensure_user_collection_exists(telegram_user_id)
    sanitized_email = _sanitize_email_for_key(email)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "device_info"},
        {"$set": {f"data.{sanitized_email}": device_info}},
        upsert=True
    )

async def get_device_info_for_email(telegram_user_id: int, email: str) -> Optional[Dict[str, str]]:
    """Get device info for a specific email asynchronously."""
    await _ensure_user_collection_exists(telegram_user_id)
    sanitized_email = _sanitize_email_for_key(email)
    user_db = _get_user_collection(telegram_user_id)
    device_doc = await user_db.find_one({"type": "device_info"})
    if device_doc and "data" in device_doc and sanitized_email in device_doc["data"]:
        return device_doc["data"][sanitized_email]
    return None

async def get_or_create_device_info_for_email(telegram_user_id: int, email: str) -> Dict[str, str]:
    """Get existing device info for email or create a new one asynchronously."""
    device_info = await get_device_info_for_email(telegram_user_id, email)
    if not device_info:
        user_db = _get_user_collection(telegram_user_id)
        if await user_db.find_one({"type": "device_info"}) is None:
            await user_db.insert_one({"type": "device_info", "data": {}})
        device_info = generate_device_info()
        await store_device_info_for_email(telegram_user_id, email, device_info)
    return device_info

async def store_device_info_for_token(telegram_user_id: int, token: str, device_info: Dict[str, str]):
    """Store device info for a specific token asynchronously."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "token_device_info"},
        {"$set": {f"data.{token}": device_info}},
        upsert=True
    )

async def get_device_info_for_token(telegram_user_id: int, token: str) -> Optional[Dict[str, str]]:
    """Get device info for a specific token asynchronously."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    device_doc = await user_db.find_one({"type": "token_device_info"})
    if device_doc and "data" in device_doc and token in device_doc["data"]:
        return device_doc["data"][token]
    return None

async def get_or_create_device_info_for_token(telegram_user_id: int, token: str) -> Dict[str, str]:
    """Get existing device info for a token or create a new one asynchronously."""
    device_info = await get_device_info_for_token(telegram_user_id, token)
    if not device_info:
        device_info = generate_device_info()
        await store_device_info_for_token(telegram_user_id, token, device_info)
    return device_info
