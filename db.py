import asyncio
import datetime
from motor.motor_asyncio import AsyncIOMotorClient

# Connect using the async client. This single client will manage connection pools.
client = AsyncIOMotorClient("mongodb+srv://irexanon:xUf7PCf9cvMHy8g6@rexdb.d9rwo.mongodb.net/?retryWrites=true&w=majority&appName=RexDB")
db = client.meeff_bot

def _get_user_collection(telegram_user_id):
    """Helper to get the collection for a user."""
    return db[f"user_{telegram_user_id}"]

async def _ensure_user_collection_exists(telegram_user_id):
    """Make sure user collection exists with default documents. Now async."""
    user_db = _get_user_collection(telegram_user_id)
    if await user_db.count_documents({}) == 0:
        # Use asyncio.gather to run all initial inserts concurrently for speed
        await asyncio.gather(
            user_db.insert_one({"type": "metadata", "created_at": datetime.datetime.utcnow(), "user_id": telegram_user_id}),
            user_db.insert_one({"type": "tokens", "items": []}),
            user_db.insert_one({"type": "settings", "current_token": None, "spam_filter": False}),
            user_db.insert_one({"type": "sent_records", "data": {}}),
            user_db.insert_one({"type": "filters", "data": {}}),
            user_db.insert_one({"type": "info_cards", "data": {}})
        )
    return True

async def list_all_collections():
    """List all user collections with detailed data summary. Now async."""
    collection_names = await db.list_collection_names()
    user_collections = []
    
    collections_to_process = [name for name in collection_names if name.startswith("user_") and name != "user_"]
    
    # Run summary tasks in parallel for better performance
    summaries = await asyncio.gather(*(get_collection_summary(name) for name in collections_to_process))

    for name, summary in zip(collections_to_process, summaries):
        try:
            user_id = name[5:]  # Remove "user_" prefix
            user_collections.append({
                "collection_name": name, "user_id": user_id,
                "display_name": f"user_{user_id}", "summary": summary
            })
        except Exception as e:
            print(f"Error processing collection {name}: {e}")
            continue
    
    return sorted(user_collections, key=lambda x: x.get("summary", {}).get("created_at") or datetime.datetime.min, reverse=True)

async def get_collection_summary(collection_name):
    """Get a detailed summary of data in a collection. Now async."""
    try:
        collection = db[collection_name]
        query_types = ["tokens", "sent_records", "info_cards", "settings", "metadata"]
        # A motor cursor must be awaited to fetch the data
        all_docs = await collection.find({"type": {"$in": query_types}}).to_list(length=None)
        
        docs_by_type = {doc.get("type"): doc for doc in all_docs}
        tokens_doc = docs_by_type.get("tokens", {})
        sent_doc = docs_by_type.get("sent_records", {})
        info_doc = docs_by_type.get("info_cards", {})
        settings_doc = docs_by_type.get("settings", {})
        metadata_doc = docs_by_type.get("metadata", {})
        
        tokens_count = len(tokens_doc.get("items", [])) if tokens_doc else 0
        active_tokens = sum(1 for t in tokens_doc.get("items", []) if t.get("active", True)) if tokens_doc else 0
        
        sent_records = {"total": 0, "categories": {}}
        if sent_doc and "data" in sent_doc:
            for category, ids in sent_doc["data"].items():
                count = len(ids) if isinstance(ids, list) else 0
                sent_records["categories"][category] = count
                sent_records["total"] += count
        
        info_cards_count = len(info_doc.get("data", {})) if info_doc else 0
        current_token = settings_doc.get("current_token")
        spam_filter = settings_doc.get("spam_filter", False)
        created_at = metadata_doc.get("created_at")
        
        total_documents = await collection.count_documents({})
        
        return {
            "tokens_count": tokens_count, "active_tokens": active_tokens, "sent_records": sent_records,
            "info_cards_count": info_cards_count, "has_current_token": bool(current_token),
            "current_token_preview": current_token[:10] + "..." if current_token else None,
            "spam_filter_enabled": spam_filter, "created_at": created_at, "total_documents": total_documents
        }
    except Exception as e:
        return {"error": str(e)}

async def connect_to_collection(collection_name, target_user_id):
    try:
        if collection_name not in await db.list_collection_names():
            return False, f"Collection '{collection_name}' not found"
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
    except Exception as e: return False, f"Connection failed: {e}"

async def rename_user_collection(user_id, new_collection_name):
    try:
        old_collection_name = f"user_{user_id}"
        if old_collection_name not in await db.list_collection_names(): return False, "Your collection not found"
        if not new_collection_name.startswith("user_"): new_collection_name = f"user_{new_collection_name}"
        if new_collection_name in await db.list_collection_names(): return False, "Target name already exists"
        
        old_collection = db[old_collection_name]
        all_docs = await old_collection.find({}).to_list(length=None)
        if not all_docs: return False, "Your collection is empty"
        
        for doc in all_docs:
            if doc.get("type") == "metadata":
                doc.update({"renamed_at": datetime.datetime.utcnow(), "original_name": old_collection_name})
        
        new_collection = db[new_collection_name]
        await new_collection.insert_many(all_docs)
        await old_collection.drop()
        return True, f"Successfully renamed to '{new_collection_name}'"
    except Exception as e: return False, f"Rename failed: {e}"

async def transfer_to_user(from_user_id, to_user_id):
    try:
        from_collection_name = f"user_{from_user_id}"
        if from_collection_name not in await db.list_collection_names(): return False, "Your collection not found"
        await _ensure_user_collection_exists(to_user_id)
        from_collection, to_collection = db[from_collection_name], _get_user_collection(to_user_id)
        all_docs = await from_collection.find({}).to_list(length=None)
        if not all_docs: return False, "Your collection is empty"
        await to_collection.delete_many({})
        for doc in all_docs:
            if doc.get("type") == "metadata":
                doc.update({"user_id": to_user_id, "transferred_at": datetime.datetime.utcnow(), "transferred_from": from_user_id})
        await to_collection.insert_many(all_docs)
        return True, f"Successfully transferred data to user {to_user_id}"
    except Exception as e: return False, f"Transfer failed: {e}"

async def get_current_collection_info(user_id):
    collection_name = f"user_{user_id}"
    if collection_name in await db.list_collection_names():
        return {"collection_name": collection_name, "exists": True, "summary": await get_collection_summary(collection_name)}
    return {"collection_name": collection_name, "exists": False, "summary": None}

async def set_info_card(telegram_user_id, token, info_text, email=None):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "info_cards"},
        {"$set": {f"data.{token}": {"info": info_text, "email": email, "updated_at": datetime.datetime.utcnow()}}},
        upsert=True)
    return True

async def get_info_card(telegram_user_id, token):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    cards_doc = await user_db.find_one({"type": "info_cards"})
    return cards_doc.get("data", {}).get(token, {}).get("info") if cards_doc else None

async def set_token(telegram_user_id, token, meeff_user_id, email=None, filters=None):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    tokens_doc = await user_db.find_one({"type": "tokens"})
    tokens = tokens_doc.get("items", []) if tokens_doc else []
    
    token_exists = False
    for t in tokens:
        if t.get("token") == token:
            t.update({"name": meeff_user_id})
            if email: t["email"] = email
            if filters: t["filters"] = filters
            t.setdefault("active", True)
            token_exists = True
            break
    
    if not token_exists:
        token_data = {"token": token, "name": meeff_user_id, "active": True}
        if email: token_data["email"] = email
        if filters: token_data["filters"] = filters
        tokens.append(token_data)
        
    await user_db.update_one({"type": "tokens"}, {"$set": {"items": tokens}}, upsert=True)
    return True

async def toggle_token_status(telegram_user_id, token):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    result = await user_db.find_one({"type": "tokens"})
    if not result: return False
    
    tokens = result.get("items", [])
    for t in tokens:
        if t.get("token") == token:
            t["active"] = not t.get("active", True)
            await user_db.update_one({"type": "tokens"}, {"$set": {"items": tokens}})
            return True
    return False

async def set_account_active(telegram_user_id, token, active_status):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "tokens", "items.token": token},
        {"$set": {"items.$.active": active_status}}
    )
    return True

async def get_active_tokens(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    tokens_doc = await user_db.find_one({"type": "tokens"})
    if not tokens_doc: return []
    return [t for t in tokens_doc.get("items", []) if t.get("active", True)]

async def get_token_status(telegram_user_id, token):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    tokens_doc = await user_db.find_one({"type": "tokens"})
    if tokens_doc:
        for t in tokens_doc.get("items", []):
            if t.get("token") == token: return t.get("active", True)
    return None

async def get_tokens(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    tokens_doc = await user_db.find_one({"type": "tokens"})
    return tokens_doc.get("items", []) if tokens_doc else []

get_all_tokens = get_tokens  # Alias for compatibility

async def list_tokens():
    result = []
    collection_names = await db.list_collection_names()
    for name in [n for n in collection_names if n.startswith("user_")]:
        user_id = name.split("_", 1)[1]
        tokens_doc = await db[name].find_one({"type": "tokens"})
        if tokens_doc:
            for token in tokens_doc.get("items", []):
                result.append({"user_id": user_id, "token": token.get("token"), "name": token.get("name")})
    return result

async def set_current_account(telegram_user_id, token):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "settings"}, {"$set": {"current_token": token}}, upsert=True)
    return True

async def get_current_account(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    settings = await user_db.find_one({"type": "settings"})
    return settings.get("current_token") if settings else None

async def delete_token(telegram_user_id, token):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    
    # Atomically pull the token from the items array
    await user_db.update_one({"type": "tokens"}, {"$pull": {"items": {"token": token}}})
    
    # Unset current token if it was the one deleted
    await user_db.update_one({"type": "settings", "current_token": token}, {"$set": {"current_token": None}})
    
    # Also delete info card
    await user_db.update_one({"type": "info_cards"}, {"$unset": {f"data.{token}": ""}})
    return True

async def set_user_filters(telegram_user_id, token, filters):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "tokens", "items.token": token},
        {"$set": {"items.$.filters": filters}}
    )
    return True

async def get_user_filters(telegram_user_id, token):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    doc = await user_db.find_one({"type": "tokens", "items.token": token}, {"items.$": 1})
    if doc and doc.get("items"):
        return doc["items"][0].get("filters")
    return None

async def set_spam_filter(telegram_user_id, status: bool):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "settings"}, {"$set": {"spam_filter": status}}, upsert=True)
    return True

async def set_individual_spam_filter(telegram_user_id, filter_type: str, status: bool):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "settings"}, {"$set": {f"spam_filter_{filter_type}": status}}, upsert=True)
    return True

async def get_individual_spam_filter(telegram_user_id: int, filter_type: str) -> bool:
    await _ensure_user_collection_exists(telegram_user_id)
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    return settings.get(f"spam_filter_{filter_type}", False) if settings else False

async def get_all_spam_filters(telegram_user_id: int) -> dict:
    await _ensure_user_collection_exists(telegram_user_id)
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    if not settings: return {"chatroom": False, "request": False, "lounge": False}
    return {
        "chatroom": settings.get("spam_filter_chatroom", False),
        "request": settings.get("spam_filter_request", False),
        "lounge": settings.get("spam_filter_lounge", False)
    }

async def get_spam_filter(telegram_user_id: int) -> bool:
    await _ensure_user_collection_exists(telegram_user_id)
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    return settings.get("spam_filter", False) if settings else False

async def get_already_sent_ids(telegram_user_id, category):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    records_doc = await user_db.find_one({"type": "sent_records"}, {f"data.{category}": 1})
    if records_doc and "data" in records_doc and category in records_doc["data"]:
        return set(records_doc["data"][category])
    return set()

async def add_sent_id(telegram_user_id, category, target_id):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "sent_records"},
        {"$addToSet": {f"data.{category}": target_id}},
        upsert=True
    )
    return True

async def is_already_sent(telegram_user_id, category, target_id, bulk=False):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    records_doc = await user_db.find_one({"type": "sent_records"}, {f"data.{category}": 1})
    
    if not (records_doc and "data" in records_doc and category in records_doc["data"]):
        return False if not bulk else set()

    sent_ids = set(records_doc["data"][category])
    if not bulk:
        return target_id in sent_ids
    return sent_ids

async def bulk_add_sent_ids(telegram_user_id, category, target_ids):
    if not target_ids: return False
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "sent_records"},
        {"$addToSet": {f"data.{category}": {"$each": list(target_ids)}}},
        upsert=True
    )
    return True

async def has_valid_access(telegram_user_id):
    collection_name = f"user_{telegram_user_id}"
    if collection_name not in await db.list_collection_names(): return False
    return await db[collection_name].count_documents({"type": "metadata"}) > 0

async def add_used_email_variation(telegram_user_id, base_email, variation):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "email_variations"},
        {"$addToSet": {f"data.{base_email}": variation}},
        upsert=True)
    return True

async def get_used_email_variations(telegram_user_id, base_email):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    variations_doc = await user_db.find_one({"type": "email_variations"})
    if variations_doc and "data" in variations_doc and base_email in variations_doc["data"]:
        return variations_doc["data"][base_email]
    return []

async def set_auto_signup_enabled(telegram_user_id, enabled):

# Legacy functions for backward compatibility
def has_interacted(telegram_user_id, action_type, user_token):
    """Legacy function - checks a separate interactions collection"""
    interaction_record = db.interactions.find_one({
        "user_id": telegram_user_id,
        "action_type": action_type,
        "user_token": user_token
    })
    return interaction_record is not None

def log_interaction(telegram_user_id, action_type, user_token):
    """Legacy function - logs to a separate interactions collection"""
    interaction_data = {
        "user_id": telegram_user_id,
        "action_type": action_type,
        "user_token": user_token,
        "timestamp": datetime.datetime.utcnow()
    }
    db.interactions.insert_one(interaction_data)
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "settings"}, {"$set": {"auto_signup_enabled": enabled}}, upsert=True)
    return True

async def get_auto_signup_enabled(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    settings = await _get_user_collection(telegram_user_id).find_one({"type": "settings"})
    return settings.get("auto_signup_enabled", False) if settings else False

async def set_signup_config(telegram_user_id, config):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one({"type": "signup_config"}, {"$set": {"data": config}}, upsert=True)
    return True

async def get_signup_config(telegram_user_id):
    await _ensure_user_collection_exists(telegram_user_id)
    config_doc = await _get_user_collection(telegram_user_id).find_one({"type": "signup_config"})
    return config_doc.get("data") if config_doc else None

async def transfer_user_data(from_telegram_id, to_telegram_id):
    return await transfer_to_user(from_telegram_id, to_telegram_id)
