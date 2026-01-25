import json
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
