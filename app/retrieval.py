import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional

import numpy as np

from app import db


def _load_project_channels(project_id: str) -> Optional[List[str]]:
    project = db.fetch_project(project_id)
    if project is None:
        raise ValueError("project_not_found")
    raw = project["channels_json"]
    if not raw:
        return None
    channels = json.loads(raw)
    return channels or None


def load_candidate_items(
    project_id: Optional[str] = None,
    channels: Optional[List[str]] = None,
    since_ts: Optional[float] = None,
    label_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    if project_id:
        channels = _load_project_channels(project_id)
    if since_ts is None:
        window_hours = float(os.getenv("RETRIEVAL_WINDOW_HOURS", "24"))
        since_ts = time.time() - window_hours * 3600
    label_filter = [label.upper() for label in (label_filter or [])]
    params: List[Any] = [since_ts]
    where_clauses = ["di.updated_at >= ?"]
    if channels:
        placeholders = ",".join(["?"] * len(channels))
        where_clauses.append(f"di.channel IN ({placeholders})")
        params.extend(channels)
    query = f"""
        SELECT di.thread_ts, di.channel, di.labels_json, di.entities_json, di.urgency,
               di.updated_at, di.title, di.summary, e.vector_json
        FROM digest_items di
        JOIN embeddings e ON e.thread_ts = di.thread_ts
        WHERE {' AND '.join(where_clauses)}
    """
    with db.db_cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    candidates = []
    for row in rows:
        labels = json.loads(row["labels_json"] or "[]")
        if label_filter and not any(label in labels for label in label_filter):
            continue
        vector = np.array(json.loads(row["vector_json"]), dtype=float)
        candidates.append(
            {
                "thread_ts": row["thread_ts"],
                "vector": vector,
                "urgency": row["urgency"] or 0.0,
                "labels": labels,
                "entities": json.loads(row["entities_json"] or "{}"),
                "updated_at": row["updated_at"],
                "title": row["title"],
                "summary": row["summary"],
            }
        )
    return candidates


def cosine_sim(q: np.ndarray, v: np.ndarray) -> float:
    return float(np.dot(q, v))


def retrieve_top_k(
    q: np.ndarray,
    candidates: Iterable[Dict[str, Any]],
    k: int,
    label_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    scored = []
    for candidate in candidates:
        score = cosine_sim(q, candidate["vector"])
        scored_candidate = dict(candidate)
        scored_candidate["sim_score"] = score
        scored_candidate["score"] = score
        scored.append(scored_candidate)
    scored.sort(
        key=lambda item: (
            -item["sim_score"],
            -item["urgency"],
            -item["updated_at"],
            item["thread_ts"],
        )
    )
    return scored[:k]
