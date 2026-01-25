import json
import os
import time
import uuid
from typing import Dict, List

from app import db
from app.embedding import normalize

POSITIVE_ACTIONS = {"click", "save", "thumbs_up"}
NEGATIVE_ACTIONS = {"thumbs_down", "dismiss"}
ALL_ACTIONS = POSITIVE_ACTIONS | NEGATIVE_ACTIONS


def _parse_vector(raw: str) -> List[float]:
    return json.loads(raw)


def _decay_user_vector(user_vec: List[float], role_vec: List[float], last_updated: float) -> List[float]:
    decay_days = float(os.getenv("USER_DECAY_DAYS", "14"))
    if time.time() - last_updated <= decay_days * 86400:
        return user_vec
    decay_blend = float(os.getenv("USER_DECAY_BLEND", "0.05"))
    blended = [
        (1.0 - decay_blend) * u + decay_blend * r
        for u, r in zip(user_vec, role_vec)
    ]
    return normalize(blended)


def apply_feedback(user_id: str, project_id: str, thread_ts: str, action: str) -> Dict:
    if action not in ALL_ACTIONS:
        raise ValueError("invalid_action")
    user = db.fetch_user(user_id)
    if user is None:
        raise ValueError("user_not_found")
    role = db.fetch_role(user["role_id"]) if user["role_id"] else None
    if role is None:
        raise ValueError("role_not_found")
    embedding = db.fetch_embedding(thread_ts)
    if embedding is None:
        raise ValueError("embedding_not_found")

    role_vec = _parse_vector(role["role_vector_json"])
    user_vec_raw = user["user_vector_json"] or role["role_vector_json"]
    user_vec = _parse_vector(user_vec_raw)
    user_vec = normalize(user_vec)
    user_vec = _decay_user_vector(user_vec, role_vec, user["updated_at"] or time.time())
    item_vec = normalize(_parse_vector(embedding["vector_json"]))

    alpha = float(os.getenv("USER_EMBED_ALPHA", "0.90"))
    if action in POSITIVE_ACTIONS:
        updated = [alpha * u + (1.0 - alpha) * v for u, v in zip(user_vec, item_vec)]
        direction = "toward"
    else:
        updated = [alpha * u - (1.0 - alpha) * v for u, v in zip(user_vec, item_vec)]
        direction = "away"
    updated = normalize(updated)

    interaction_id = f"int-{uuid.uuid4().hex}"
    db.insert_interaction(interaction_id, user_id, project_id, thread_ts, action)
    db.update_user_vector(user_id, json.dumps(updated))

    return {
        "interaction_id": interaction_id,
        "user_id": user_id,
        "project_id": project_id,
        "thread_ts": thread_ts,
        "action": action,
        "direction": direction,
        "new_norm": sum(v * v for v in updated) ** 0.5,
    }
