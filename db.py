# db.py
import datetime
from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB connection using the asynchronous Motor client
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
            {"type": "settings", "current_token": None},
            {"type": "sent_records", "data": {}},
            {"type": "filters", "data": {}},
            {"type": "info_cards", "data": {}}
        ])

async def list_all_collections():
    """Lists all user database collections."""
    collection_names = await db.list_collection_names()
    user_collections = []
    for name in filter(lambda n: n.startswith("user_") and n != "user_", collection_names):
        try:
            summary = await get_collection_summary(name)
            user_collections.append({"collection_name": name, "summary": summary})
        except Exception as e:
            print(f"Could not process collection {name}: {e}")
            continue
    return sorted(user_collections, key=lambda x: x.get("summary", {}).get("created_at") or datetime.datetime.min, reverse=True)

async def get_collection_summary(collection_name):
    """Gets a performance-optimized summary of a user's database."""
    collection = db[collection_name]
    query_types = ["tokens", "sent_records", "info_cards", "settings", "metadata"]
    all_docs = await collection.find({"type": {"$in": query_types}}).to_list(length=None)
    docs_by_type = {doc.get("type"): doc for doc in all_docs}
    tokens_doc = docs_by_type.get("tokens", {})
    return {
        "tokens_count": len(tokens_doc.get("items", [])),
        "active_tokens": sum(1 for t in tokens_doc.get("items", []) if t.get("active", True)),
        "created_at": docs_by_type.get("metadata", {}).get("created_at"),
    }

async def connect_to_collection(collection_name, target_user_id):
    """Connects a user's profile to an existing database collection."""
    if collection_name not in await db.list_collection_names():
        return False, f"Collection '{collection_name}' not found"
    from_collection = db[collection_name]
    all_docs = await from_collection.find({}).to_list(length=None)
    if not all_docs:
        return False, "Source collection is empty"
    to_collection = _get_user_collection(target_user_id)
    await to_collection.delete_many({})
    for doc in all_docs:
        if doc.get("type") == "metadata":
            doc.update({"user_id": target_user_id, "connected_at": datetime.datetime.utcnow(), "original_collection": collection_name})
    await to_collection.insert_many(all_docs)
    return True, f"Successfully connected to '{collection_name}'"

async def rename_user_collection(user_id, new_name):
    """Renames a user's database collection."""
    old_name = f"user_{user_id}"
    new_collection_name = f"user_{new_name}" if not new_name.startswith("user_") else new_name
    if old_name not in await db.list_collection_names():
        return False, "Your collection was not found"
    if new_collection_name in await db.list_collection_names():
        return False, "Target collection name already exists"
    old_collection = db[old_name]
    all_docs = await old_collection.find({}).to_list(length=None)
    if not all_docs:
        return False, "Your collection is empty"
    for doc in all_docs:
        if doc.get("type") == "metadata":
            doc.update({"renamed_at": datetime.datetime.utcnow(), "original_name": old_name})
    await db[new_collection_name].insert_many(all_docs)
    await old_collection.drop()
    return True, f"Successfully renamed to '{new_collection_name}'"

async def transfer_to_user(from_user_id, to_user_id):
    """Transfers a database from one Telegram user to another."""
    from_collection_name = f"user_{from_user_id}"
    if from_collection_name not in await db.list_collection_names():
        return False, "Your collection not found"
    return await connect_to_collection(from_collection_name, to_user_id)

async def get_current_collection_info(user_id):
    """Gets info about the user's current database."""
    collection_name = f"user_{user_id}"
    if collection_name in await db.list_collection_names():
        return {"exists": True, "collection_name": collection_name, "summary": await get_collection_summary(collection_name)}
    return {"exists": False, "collection_name": collection_name, "summary": None}

async def set_token(telegram_user_id, token, account_name, email=None, filters=None):
    """Adds a new Meeff account token or updates an existing one."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    
    update_fields = {"items.$.name": account_name}
    if email:
        update_fields["items.$.email"] = email
    if filters:
        update_fields["items.$.filters"] = filters
        
    result = await user_db.update_one(
        {"type": "tokens", "items.token": token},
        {"$set": update_fields}
    )
    if result.matched_count == 0:
        token_data = {"token": token, "name": account_name, "active": True}
        if email:
            token_data["email"] = email
        if filters:
            token_data["filters"] = filters
        await user_db.update_one(
            {"type": "tokens"},
            {"$push": {"items": token_data}},
            upsert=True
        )

async def get_tokens(telegram_user_id):
    """Gets all of a user's Meeff account tokens."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    tokens_doc = await user_db.find_one({"type": "tokens"})
    return tokens_doc.get("items", []) if tokens_doc else []

async def get_active_tokens(telegram_user_id):
    """Gets only the active Meeff account tokens for a user."""
    all_tokens = await get_tokens(telegram_user_id)
    return [t for t in all_tokens if t.get("active", True)]

async def set_current_account(telegram_user_id, token):
    """Sets the user's currently active Meeff account."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "settings"}, {"$set": {"current_token": token}}, upsert=True)

async def get_current_account(telegram_user_id):
    """Gets the user's currently active Meeff account token."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    settings = await user_db.find_one({"type": "settings"})
    return settings.get("current_token") if settings else None

async def delete_token(telegram_user_id, token):
    """Deletes a Meeff account token from a user's profile."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "tokens"}, {"$pull": {"items": {"token": token}}})
    if await get_current_account(telegram_user_id) == token:
        await set_current_account(telegram_user_id, None)
    await user_db.update_one({"type": "info_cards"}, {"$unset": {f"data.{token}": ""}})

async def toggle_token_status(telegram_user_id, token):
    """Toggles a Meeff account token between active and inactive."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    token_obj = await user_db.find_one({"type": "tokens", "items.token": token}, {"items.$": 1})
    if token_obj and token_obj.get("items"):
        current_status = token_obj["items"][0].get("active", True)
        await user_db.update_one({"type": "tokens", "items.token": token}, {"$set": {"items.$.active": not current_status}})

async def get_info_card(telegram_user_id, token):
    """Gets the stored profile info card for a Meeff account."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    cards_doc = await user_db.find_one({"type": "info_cards"})
    return cards_doc.get("data", {}).get(token, {}).get("info")

async def set_individual_spam_filter(telegram_user_id: int, filter_type: str, status: bool):
    """Sets the spam filter status for a specific feature (e.g., chatroom)."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "settings"}, {"$set": {f"spam_filter_{filter_type}": status}}, upsert=True)

async def get_individual_spam_filter(telegram_user_id: int, filter_type: str) -> bool:
    """Gets the spam filter status for a specific feature."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    settings = await user_db.find_one({"type": "settings"})
    return settings.get(f"spam_filter_{filter_type}", False) if settings else False

async def get_all_spam_filters(telegram_user_id: int) -> dict:
    """Gets all spam filter settings for a user."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    settings = await user_db.find_one({"type": "settings"})
    return {
        "chatroom": settings.get("spam_filter_chatroom", False) if settings else False,
        "request": settings.get("spam_filter_request", False) if settings else False,
        "lounge": settings.get("spam_filter_lounge", False) if settings else False,
    }

async def is_already_sent(telegram_user_id, category, target_id):
    """Checks if a message has already been sent to a target ID to prevent spam."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    return await user_db.count_documents({"type": "sent_records", f"data.{category}": target_id}) > 0

async def add_sent_id(telegram_user_id, category, target_id):
    """Records that a message has been sent to a target ID."""
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "sent_records"}, {"$addToSet": {f"data.{category}": target_id}}, upsert=True)

# Dummy functions for compatibility with other modules if they exist
async def set_user_filters(telegram_user_id, token, filters): pass
async def get_user_filters(telegram_user_id, token): pass
