import json
import uuid
from typing import Any, Dict, List

import numpy as np

from app import db
from app.profiles import get_query_vector
from app.retrieval import load_candidate_items, retrieve_top_k
from app.rerank import rerank_candidates

ROLE_SIGNAL_KEYWORDS = ["supply", "procure", "vendor", "lead time"]


def _why_shown(item: Dict[str, Any], role_description: str, phase_key: str | None) -> str:
    reasons = []
    if item["urgency"] >= 0.8:
        reasons.append("High urgency")
    entities = item.get("entities", {})
    if role_description:
        role_desc_lower = role_description.lower()
        if any(word in role_desc_lower for word in ROLE_SIGNAL_KEYWORDS):
            if entities.get("vendors") or entities.get("lead_times"):
                reasons.append("Role match: vendor/lead time")
    phases = [p.upper() for p in entities.get("phases", [])]
    if phase_key and phase_key.upper() in phases:
        reasons.append(f"Phase match: {phase_key.upper()}")
    if not reasons:
        reasons.append("Semantic similarity")
    return "; ".join(reasons)


def build_digest(user_id: str, project_id: str, n: int = 10) -> Dict[str, Any]:
    q_result = get_query_vector(user_id, project_id)
    q_vector = np.array(q_result["q_vector"], dtype=float)
    candidates = load_candidate_items(project_id=project_id)
    top_k = retrieve_top_k(q_vector, candidates, k=50)
    ranked = rerank_candidates(top_k, user_id, n=n)

    role = db.fetch_role(q_result["role_id"]) if q_result.get("role_id") else None
    role_description = role["description"] if role else ""
    phase_key = q_result.get("phase_key")

    items = []
    for item in ranked:
        items.append(
            {
                "thread_ts": item["thread_ts"],
                "title": item.get("title"),
                "summary": item.get("summary"),
                "labels": item.get("labels"),
                "entities": item.get("entities"),
                "urgency": item.get("urgency"),
                "why_shown": _why_shown(item, role_description, phase_key),
                "score_breakdown": {
                    "final_score": item["final_score"],
                    "sim": item["sim_score"],
                    "urgency": item["urgency"],
                    "ownership": item["ownership"],
                    "recency": item["recency"],
                    "diversity_penalty": item["diversity_penalty"],
                },
            }
        )

    digest_id = f"dig-{uuid.uuid4().hex}"
    db.insert_digest(digest_id, user_id, project_id, json.dumps(items))
    return {"digest_id": digest_id, "items": items}
