import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from cachetools import TTLCache
from functools import wraps

# --- Cache Setup ---
user_data_cache = TTLCache(maxsize=1024, ttl=15)

# --- Async Cache Decorator ---
def async_cached(cache):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = (func.__name__,) + args + tuple(sorted(kwargs.items()))
            try:
                return cache[key]
            except KeyError:
                value = await func(*args, **kwargs)
                cache[key] = value
                return value
        return wrapper
    return decorator

# --- Database Connection ---
client = AsyncIOMotorClient("mongodb+srv://irexanon:xUf7PCf9cvMHy8g6@rexdb.d9rwo.mongodb.net/?retryWrites=true&w=majority&appName=RexDB")
db = client.meeff_bot

def _get_user_collection(telegram_user_id):
    """Gets the specific database collection for a user."""
    return db[f"user_{telegram_user_id}"]

async def _ensure_user_collection_exists(telegram_user_id):
    """Initializes a user's database if it's their first time."""
    user_db = _get_user_collection(telegram_user_id)
    if await user_db.count_documents({}) == 0:
        await user_db.insert_many([
            {"type": "metadata", "created_at": datetime.datetime.utcnow(), "user_id": telegram_user_id},
            {"type": "tokens", "items": []},
            {"type": "settings", "current_token": None, "spam_filter": False},
            {"type": "sent_records", "data": {}},
            {"type": "filters", "data": {}},
            {"type": "info_cards", "data": {}}
        ])

# --- Write Operations (Clear Cache) ---

async def set_spam_filter(telegram_user_id, status: bool):
    """Sets the general spam filter, then clears the cache. (FIXED)"""
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    await _get_user_collection(telegram_user_id).update_one(
        {"type": "settings"},
        {"$set": {"spam_filter": status}},
        upsert=True
    )

async def set_token(telegram_user_id, token, meeff_user_id, email=None, filters=None):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    update_fields = {"items.$.name": meeff_user_id}
    if email: update_fields["items.$.email"] = email
    if filters: update_fields["items.$.filters"] = filters
    result = await user_db.update_one({"type": "tokens", "items.token": token}, {"$set": update_fields})
    if result.matched_count == 0:
        token_data = {"token": token, "name": meeff_user_id, "active": True}
        if email: token_data["email"] = email
        if filters: token_data["filters"] = filters
        await user_db.update_one({"type": "tokens"}, {"$push": {"items": token_data}}, upsert=True)

async def set_current_account(telegram_user_id, token):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    await _get_user_collection(telegram_user_id).update_one({"type": "settings"}, {"$set": {"current_token": token}}, upsert=True)

async def delete_token(telegram_user_id, token):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "tokens"}, {"$pull": {"items": {"token": token}}})
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    if settings and settings.get("current_token") == token:
        await set_current_account(telegram_user_id, None)
    await user_db.update_one({"type": "info_cards"}, {"$unset": {f"data.{token}": ""}})

async def toggle_token_status(telegram_user_id, token):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    token_obj = await user_db.find_one({"type": "tokens", "items.token": token}, {"items.$": 1})
    if token_obj and token_obj.get("items"):
        current_status = token_obj["items"][0].get("active", True)
        await user_db.update_one({"type": "tokens", "items.token": token}, {"$set": {"items.$.active": not current_status}})

async def set_individual_spam_filter(telegram_user_id, filter_type: str, status: bool):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    await _get_user_collection(telegram_user_id).update_one({"type": "settings"}, {"$set": {f"spam_filter_{filter_type}": status}}, upsert=True)

async def set_signup_config(telegram_user_id, config):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    await _get_user_collection(telegram_user_id).update_one({"type": "signup_config"}, {"$set": {"data": config}}, upsert=True)

async def set_info_card(telegram_user_id, token, info_text, email=None):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "info_cards"}, {"$set": {f"data.{token}": {"info": info_text, "email": email, "updated_at": datetime.datetime.utcnow()}}}, upsert=True)

async def set_user_filters(telegram_user_id, token, filters):
    user_data_cache.clear()
    await _ensure_user_collection_exists(telegram_user_id)
    await _get_user_collection(telegram_user_id).update_one({"type": "tokens", "items.token": token}, {"$set": {"items.$.filters": filters}})

async def add_sent_id(telegram_user_id, category, target_id):
    await _ensure_user_collection_exists(telegram_user_id)
    await _get_user_collection(telegram_user_id).update_one({"type": "sent_records"}, {"$addToSet": {f"data.{category}": target_id}}, upsert=True)

async def bulk_add_sent_ids(telegram_user_id, category, target_ids):
    if not target_ids: return
    await _ensure_user_collection_exists(telegram_user_id)
    await _get_user_collection(telegram_user_id).update_one({"type": "sent_records"}, {"$addToSet": {f"data.{category}": {"$each": list(target_ids)}}}, upsert=True)

# --- Cached Read Operations ---
@async_cached(cache=user_data_cache)
async def get_tokens(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    tokens_doc = await _get_user_collection(telegram_user_id).find_one({"type": "tokens"})
    return tokens_doc.get("items", []) if tokens_doc else []

get_all_tokens = get_tokens

@async_cached(cache=user_data_cache)
async def get_current_account(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    return settings.get("current_token") if settings else None

@async_cached(cache=user_data_cache)
async def get_all_spam_filters(telegram_user_id: int) -> dict:
    await _ensure_user_collection_exists(telegram_user_id)
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    return {
        "chatroom": settings.get("spam_filter_chatroom", False) if settings else False,
        "request": settings.get("spam_filter_request", False) if settings else False,
        "lounge": settings.get("spam_filter_lounge", False) if settings else False,
    }

@async_cached(cache=user_data_cache)
async def get_individual_spam_filter(telegram_user_id: int, filter_type: str) -> bool:
    filters = await get_all_spam_filters(telegram_user_id)
    return filters.get(filter_type, False)

@async_cached(cache=user_data_cache)
async def get_signup_config(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    config_doc = await _get_user_collection(telegram_user_id).find_one({"type": "signup_config"})
    return config_doc.get("data") if config_doc else None

@async_cached(cache=user_data_cache)
async def get_active_tokens(telegram_user_id):
    tokens = await get_tokens(telegram_user_id)
    return [t for t in tokens if t.get("active", True)]

@async_cached(cache=user_data_cache)
async def get_info_card(telegram_user_id, token):
    await _ensure_user_collection_exists(telegram_user_id)
    cards_doc = await _get_user_collection(telegram_user_id).find_one({"type": "info_cards"})
    if cards_doc and "data" in cards_doc and token in cards_doc["data"]:
        return cards_doc["data"][token].get("info")
    return None

@async_cached(cache=user_data_cache)
async def get_user_filters(telegram_user_id, token):
    tokens = await get_tokens(telegram_user_id)
    for t in tokens:
        if t.get("token") == token:
            return t.get("filters")
    return None

@async_cached(cache=user_data_cache)
async def get_spam_filter(telegram_user_id: int) -> bool:
    await _ensure_user_collection_exists(telegram_user_id)
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    return settings.get("spam_filter", False) if settings else False

# --- Uncached Read Operations ---
async def is_already_sent(telegram_user_id, category, target_id, bulk=False):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    if not bulk:
        return await user_db.count_documents({"type": "sent_records", f"data.{category}": target_id}) > 0
    else:
        records_doc = await user_db.find_one({"type": "sent_records"}, {f"data.{category}": 1})
        return set(records_doc.get("data", {}).get(category, [])) if records_doc else set()

async def get_already_sent_ids(telegram_user_id, category):
    await _ensure_user_collection_exists(telegram_user_id)
    records_doc = await _get_user_collection(telegram_user_id).find_one({"type": "sent_records"})
    return set(records_doc.get("data", {}).get(category, [])) if records_doc else set()

async def list_all_collections():
    collection_names = await db.list_collection_names()
    user_collections = []
    for name in filter(lambda n: n.startswith("user_") and n != "user_", collection_names):
        try:
            summary = await get_collection_summary(name)
            user_collections.append({"collection_name": name, "user_id": name[5:], "summary": summary})
        except Exception as e:
            print(f"Error processing collection {name}: {e}")
    return sorted(user_collections, key=lambda x: x.get("summary", {}).get("created_at") or datetime.datetime.min, reverse=True)

async def get_collection_summary(collection_name):
    collection = db[collection_name]
    query_types = ["tokens", "sent_records", "info_cards", "settings", "metadata"]
    all_docs = await collection.find({"type": {"$in": query_types}}).to_list(length=None)
    docs_by_type = {doc.get("type"): doc for doc in all_docs}
    tokens_doc = docs_by_type.get("tokens", {})
    sent_doc = docs_by_type.get("sent_records", {})
    info_doc = docs_by_type.get("info_cards", {})
    settings_doc = docs_by_type.get("settings", {})
    metadata_doc = docs_by_type.get("metadata", {})
    return {
        "tokens_count": len(tokens_doc.get("items", [])),
        "active_tokens": sum(1 for t in tokens_doc.get("items", []) if t.get("active", True)),
        "sent_records_total": sum(len(ids) for ids in sent_doc.get("data", {}).values() if isinstance(ids, list)),
        "info_cards_count": len(info_doc.get("data", {})),
        "created_at": metadata_doc.get("created_at"),
    }

# --- Legacy & Unchanged Functions (Now Async) ---
async def connect_to_collection(collection_name, target_user_id):
    user_data_cache.clear()
    if collection_name not in await db.list_collection_names(): return False, f"Collection '{collection_name}' not found"
    await _ensure_user_collection_exists(target_user_id)
    from_collection, to_collection = db[collection_name], _get_user_collection(target_user_id)
    all_docs = await from_collection.find({}).to_list(length=None)
    if not all_docs: return False, "Source collection is empty"
    await to_collection.delete_many({})
    for doc in all_docs:
        if doc.get("type") == "metadata":
            doc.update({"user_id": target_user_id, "connected_at": datetime.datetime.utcnow(), "original_collection": collection_name})
    await to_collection.insert_many(all_docs)
    return True, f"Successfully connected to '{collection_name}'"

async def rename_user_collection(user_id, new_collection_name):
    user_data_cache.clear()
    old_name = f"user_{user_id}"
    if old_name not in await db.list_collection_names(): return False, "Your collection not found"
    new_name = f"user_{new_collection_name}" if not new_collection_name.startswith("user_") else new_collection_name
    if new_name in await db.list_collection_names(): return False, "Target collection name already exists"
    old_collection = db[old_name]
    all_docs = await old_collection.find({}).to_list(length=None)
    if not all_docs: return False, "Your collection is empty"
    for doc in all_docs:
        if doc.get("type") == "metadata":
            doc.update({"renamed_at": datetime.datetime.utcnow(), "original_name": old_name})
    await db[new_name].insert_many(all_docs)
    await old_collection.drop()
    return True, f"Successfully renamed to '{new_name}'"

async def transfer_to_user(from_user_id, to_user_id):
    from_name = f"user_{from_user_id}"
    if from_name not in await db.list_collection_names(): return False, "Your collection not found"
    return await connect_to_collection(from_name, to_user_id)

async def has_interacted(telegram_user_id, action_type, user_token):
    return await db.interactions.find_one({"user_id": telegram_user_id, "action_type": action_type, "user_token": user_token}) is not None

async def log_interaction(telegram_user_id, action_type, user_token):
    await db.interactions.insert_one({"user_id": telegram_user_id, "action_type": action_type, "user_token": user_token, "timestamp": datetime.datetime.utcnow()})

