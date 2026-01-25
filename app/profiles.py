import json
import math
import os
from typing import Dict, List, Optional, Tuple

from app import db
from app.embedding import compute_embedding, normalize


def _normalized_vector(text: str) -> List[float]:
    vector = compute_embedding(text)
    return normalize(vector)


def create_role(role_id: str, name: str, description: str) -> List[float]:
    vector = _normalized_vector(description)
    db.upsert_role(role_id, name, description, json.dumps(vector))
    return vector


def create_phase(phase_key: str, description: str) -> List[float]:
    vector = _normalized_vector(description)
    db.upsert_phase(phase_key, description, json.dumps(vector))
    return vector


def create_project(project_id: str, name: str, current_phase: str) -> None:
    phase = db.fetch_phase(current_phase)
    if phase is None:
        raise ValueError("phase_not_found")
    db.upsert_project(project_id, name, current_phase)


def update_project_phase(project_id: str, phase_key: str) -> None:
    phase = db.fetch_phase(phase_key)
    if phase is None:
        raise ValueError("phase_not_found")
    db.update_project_phase(project_id, phase_key)


def create_user(user_id: str, name: str, role_id: Optional[str]) -> Tuple[Optional[List[float]], Optional[str]]:
    role_vector_json = None
    if role_id:
        role = db.fetch_role(role_id)
        if role is None:
            raise ValueError("role_not_found")
        role_vector_json = role["role_vector_json"]
    db.upsert_user(user_id, name, None, role_id, role_vector_json)
    if role_vector_json:
        return json.loads(role_vector_json), role_id
    return None, role_id


def update_user_role(user_id: str, role_id: str) -> List[float]:
    role = db.fetch_role(role_id)
    if role is None:
        raise ValueError("role_not_found")
    role_vector_json = role["role_vector_json"]
    db.update_user_role(user_id, role_id, role_vector_json)
    return json.loads(role_vector_json)


def add_user_to_project(user_id: str, project_id: str) -> None:
    user = db.fetch_user(user_id)
    if user is None:
        raise ValueError("user_not_found")
    project = db.fetch_project(project_id)
    if project is None:
        raise ValueError("project_not_found")
    db.add_user_project(user_id, project_id)


def get_user_profile(user_id: str) -> Dict:
    user = db.fetch_user(user_id)
    if user is None:
        raise ValueError("user_not_found")
    projects = db.fetch_user_projects(user_id)
    project_ids = [row["project_id"] for row in projects]
    vector = json.loads(user["user_vector_json"]) if user["user_vector_json"] else []
    return {
        "user_id": user["user_id"],
        "role_id": user["role_id"],
        "user_vector_dim": len(vector),
        "projects": project_ids,
    }


def get_project_profile(project_id: str) -> Dict:
    project = db.fetch_project(project_id)
    if project is None:
        raise ValueError("project_not_found")
    phase = db.fetch_phase(project["current_phase"]) if project["current_phase"] else None
    vector = json.loads(phase["phase_vector_json"]) if phase and phase["phase_vector_json"] else []
    return {
        "project_id": project["project_id"],
        "current_phase": project["current_phase"],
        "phase_vector_dim": len(vector),
        "phase_vector": vector,
    }


def _parse_vector(raw: Optional[str]) -> Optional[List[float]]:
    if not raw:
        return None
    return json.loads(raw)


def _top_indices(vector: List[float], top_k: int = 5) -> List[int]:
    indexed = list(enumerate(vector))
    indexed.sort(key=lambda pair: abs(pair[1]), reverse=True)
    return [idx for idx, _ in indexed[:top_k]]


def weighted_query_vector(
    role_vec: List[float],
    user_vec: Optional[List[float]],
    phase_vec: Optional[List[float]],
    w_role: float,
    w_user: float,
    w_phase: float,
) -> Dict:
    effective_user = user_vec or role_vec
    weights = {"role": w_role, "user": w_user, "phase": w_phase}
    if phase_vec is None:
        total = w_role + w_user
        weights["role"] = w_role / total
        weights["user"] = w_user / total
        weights["phase"] = 0.0
    contrib_role = [weights["role"] * v for v in role_vec]
    contrib_user = [weights["user"] * v for v in effective_user]
    contrib_phase = [weights["phase"] * v for v in phase_vec] if phase_vec else [0.0] * len(role_vec)
    combined = [a + b + c for a, b, c in zip(contrib_role, contrib_user, contrib_phase)]
    q_vector = normalize(combined)
    component_norms = {
        "role": math.sqrt(sum(v * v for v in contrib_role)),
        "user": math.sqrt(sum(v * v for v in contrib_user)),
        "phase": math.sqrt(sum(v * v for v in contrib_phase)),
    }
    component_top_indices = {
        "role": _top_indices(contrib_role),
        "user": _top_indices(contrib_user),
        "phase": _top_indices(contrib_phase),
    }
    return {
        "q_vector": q_vector,
        "weights": weights,
        "component_norms": component_norms,
        "component_top_indices": component_top_indices,
    }


def get_query_vector(user_id: str, project_id: str) -> Dict:
    user = db.fetch_user(user_id)
    if user is None:
        raise ValueError("user_not_found")
    project = db.fetch_project(project_id)
    if project is None:
        raise ValueError("project_not_found")
    role_id = user["role_id"]
    role = db.fetch_role(role_id) if role_id else None
    if role is None:
        raise ValueError("role_not_found")
    role_vec = _parse_vector(role["role_vector_json"])
    if role_vec is None:
        raise ValueError("role_vector_missing")
    user_vec = _parse_vector(user["user_vector_json"])
    phase_key = project["current_phase"]
    phase = db.fetch_phase(phase_key) if phase_key else None
    phase_vec = _parse_vector(phase["phase_vector_json"]) if phase else None
    w_role = float(os.getenv("QUERY_WEIGHT_ROLE", "0.45"))
    w_user = float(os.getenv("QUERY_WEIGHT_USER", "0.35"))
    w_phase = float(os.getenv("QUERY_WEIGHT_PHASE", "0.20"))
    result = weighted_query_vector(role_vec, user_vec, phase_vec, w_role, w_user, w_phase)
    result.update({"role_id": role_id, "phase_key": phase_key})
    return result
