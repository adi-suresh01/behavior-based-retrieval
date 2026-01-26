from fastapi import APIRouter, HTTPException
import numpy as np
from pydantic import BaseModel

from app import db
from app.profiles import (
    add_user_to_project,
    create_phase,
    create_project,
    create_role,
    create_user,
    get_query_vector,
    get_project_profile,
    get_user_profile,
    update_project_phase,
    update_user_role,
)
from app.retrieval import load_candidate_items, retrieve_top_k, cosine_sim
from app.rerank import rerank_candidates
from app.digest import build_digest
from app.feedback import apply_feedback, ALL_ACTIONS

router = APIRouter()


class RoleCreate(BaseModel):
    role_id: str
    name: str
    description: str


class PhaseCreate(BaseModel):
    phase_key: str
    description: str


class ProjectCreate(BaseModel):
    project_id: str
    name: str
    current_phase: str


class ProjectPhaseUpdate(BaseModel):
    phase_key: str


class UserCreate(BaseModel):
    user_id: str
    name: str
    role_id: str | None = None


class UserRoleUpdate(BaseModel):
    role_id: str


@router.post("/roles")
async def create_role_endpoint(payload: RoleCreate):
    vector = create_role(payload.role_id, payload.name, payload.description)
    return {"role_id": payload.role_id, "vector_dim": len(vector)}


@router.post("/phases")
async def create_phase_endpoint(payload: PhaseCreate):
    vector = create_phase(payload.phase_key, payload.description)
    return {"phase_key": payload.phase_key, "vector_dim": len(vector)}


@router.post("/projects")
async def create_project_endpoint(payload: ProjectCreate):
    try:
        create_project(payload.project_id, payload.name, payload.current_phase)
    except ValueError:
        raise HTTPException(status_code=400, detail="Unknown phase_key")
    return {"project_id": payload.project_id}


@router.patch("/projects/{project_id}/phase")
async def update_project_phase_endpoint(project_id: str, payload: ProjectPhaseUpdate):
    try:
        update_project_phase(project_id, payload.phase_key)
    except ValueError:
        raise HTTPException(status_code=400, detail="Unknown phase_key")
    return {"project_id": project_id, "current_phase": payload.phase_key}


@router.post("/users")
async def create_user_endpoint(payload: UserCreate):
    try:
        vector, role_id = create_user(payload.user_id, payload.name, payload.role_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Unknown role_id")
    return {"user_id": payload.user_id, "role_id": role_id, "vector_dim": len(vector or [])}


@router.patch("/users/{user_id}/role")
async def update_user_role_endpoint(user_id: str, payload: UserRoleUpdate):
    try:
        vector = update_user_role(user_id, payload.role_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Unknown role_id")
    return {"user_id": user_id, "role_id": payload.role_id, "vector_dim": len(vector)}


@router.post("/users/{user_id}/projects/{project_id}")
async def join_user_project_endpoint(user_id: str, project_id: str):
    try:
        add_user_to_project(user_id, project_id)
    except ValueError as exc:
        if str(exc) == "user_not_found":
            raise HTTPException(status_code=404, detail="Unknown user")
        raise HTTPException(status_code=404, detail="Unknown project")
    return {"user_id": user_id, "project_id": project_id}


class ChannelMapping(BaseModel):
    channel_id: str


@router.post("/projects/{project_id}/channels")
async def add_project_channel_endpoint(project_id: str, payload: ChannelMapping):
    project = db.fetch_project(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Unknown project")
    db.add_project_channel(project_id, payload.channel_id)
    return {"project_id": project_id, "channel_id": payload.channel_id}


@router.post("/users/{user_id}/channels")
async def add_user_channel_endpoint(user_id: str, payload: ChannelMapping):
    user = db.fetch_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Unknown user")
    db.add_user_channel(user_id, payload.channel_id)
    return {"user_id": user_id, "channel_id": payload.channel_id}


@router.get("/projects/{project_id}/channels")
async def list_project_channels_endpoint(project_id: str):
    project = db.fetch_project(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Unknown project")
    channels = [row["channel_id"] for row in db.fetch_project_channels(project_id)]
    return {"project_id": project_id, "channels": channels}


@router.get("/profiles/users/{user_id}")
async def user_profile_endpoint(user_id: str):
    try:
        return get_user_profile(user_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Unknown user")


@router.get("/profiles/projects/{project_id}")
async def project_profile_endpoint(project_id: str):
    try:
        return get_project_profile(project_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Unknown project")


@router.get("/debug/query_vector")
async def query_vector_debug(user_id: str, project_id: str):
    try:
        result = get_query_vector(user_id, project_id)
    except ValueError as exc:
        if str(exc) == "user_not_found":
            raise HTTPException(status_code=404, detail="Unknown user")
        if str(exc) == "project_not_found":
            raise HTTPException(status_code=404, detail="Unknown project")
        if str(exc) == "access_denied":
            raise HTTPException(status_code=403, detail="User lacks channel access")
        raise HTTPException(status_code=400, detail="Missing role or phase data")
    q_vector = result["q_vector"]
    return {
        "user_id": user_id,
        "project_id": project_id,
        "weights": result["weights"],
        "role_id": result["role_id"],
        "phase_key": result["phase_key"],
        "q_dim": len(q_vector),
        "q_vector": q_vector[:20],
        "component_norms": result["component_norms"],
        "component_top_indices": result["component_top_indices"],
    }


@router.get("/debug/retrieve")
async def debug_retrieve(user_id: str, project_id: str, k: int = 50, labels: str | None = None):
    label_filter = [label.strip().upper() for label in labels.split(",")] if labels else []
    try:
        q_result = get_query_vector(user_id, project_id)
    except ValueError as exc:
        if str(exc) == "user_not_found":
            raise HTTPException(status_code=404, detail="Unknown user")
        if str(exc) == "project_not_found":
            raise HTTPException(status_code=404, detail="Unknown project")
        raise HTTPException(status_code=400, detail="Missing role or phase data")
    candidates = load_candidate_items(project_id=project_id, label_filter=label_filter)
    q_vector = np.array(q_result["q_vector"], dtype=float)
    raw_results = retrieve_top_k(q_vector, candidates, k, label_filter=label_filter)
    results = [
        {
            "thread_ts": item["thread_ts"],
            "score": item["score"],
            "urgency": item["urgency"],
            "title": item["title"],
            "labels": item["labels"],
            "updated_at": item["updated_at"],
        }
        for item in raw_results
    ]
    return {
        "user_id": user_id,
        "project_id": project_id,
        "k": k,
        "results": results,
    }


@router.get("/debug/rerank")
async def debug_rerank(user_id: str, project_id: str, n: int = 10, labels: str | None = None):
    label_filter = [label.strip().upper() for label in labels.split(",")] if labels else []
    try:
        q_result = get_query_vector(user_id, project_id)
    except ValueError as exc:
        if str(exc) == "user_not_found":
            raise HTTPException(status_code=404, detail="Unknown user")
        if str(exc) == "project_not_found":
            raise HTTPException(status_code=404, detail="Unknown project")
        raise HTTPException(status_code=400, detail="Missing role or phase data")
    candidates = load_candidate_items(project_id=project_id, label_filter=label_filter)
    q_vector = np.array(q_result["q_vector"], dtype=float)
    scored_candidates = []
    for candidate in candidates:
        sim = cosine_sim(q_vector, candidate["vector"])
        scored_candidates.append({**candidate, "sim_score": sim})
    reranked = rerank_candidates(scored_candidates, user_id, n=n)
    results = []
    for item in reranked:
        results.append(
            {
                "thread_ts": item["thread_ts"],
                "final_score": item["final_score"],
                "score_breakdown": {
                    "sim": item["sim_score"],
                    "urgency": item["urgency"],
                    "ownership": item["ownership"],
                    "recency": item["recency"],
                    "diversity_penalty": item["diversity_penalty"],
                    "base_score": item["base_score"],
                },
                "force_included": item["force_included"],
                "title": item["title"],
                "labels": item["labels"],
                "updated_at": item["updated_at"],
            }
        )
    return {
        "user_id": user_id,
        "project_id": project_id,
        "n": n,
        "results": results,
    }


@router.get("/digest")
async def digest_endpoint(user_id: str, project_id: str, n: int = 10):
    user = db.fetch_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Unknown user")
    project = db.fetch_project(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Unknown project")
    project_channels = [row["channel_id"] for row in db.fetch_project_channels(project_id)]
    if not project_channels:
        raise HTTPException(status_code=403, detail="User lacks channel access")
    user_channels = {row["channel_id"] for row in db.fetch_user_channels(user_id)}
    if not user_channels.issuperset(project_channels):
        raise HTTPException(status_code=403, detail="User lacks channel access")
    try:
        result = build_digest(user_id, project_id, n=n)
    except ValueError as exc:
        if str(exc) == "user_not_found":
            raise HTTPException(status_code=404, detail="Unknown user")
        if str(exc) == "project_not_found":
            raise HTTPException(status_code=404, detail="Unknown project")
        if str(exc) == "access_denied":
            raise HTTPException(status_code=403, detail="User lacks channel access")
        raise HTTPException(status_code=400, detail="Missing role or phase data")
    return result


class FeedbackCreate(BaseModel):
    user_id: str
    project_id: str
    thread_ts: str
    action: str


@router.post("/feedback")
async def feedback_endpoint(payload: FeedbackCreate):
    try:
        result = apply_feedback(payload.user_id, payload.project_id, payload.thread_ts, payload.action)
    except ValueError as exc:
        if str(exc) == "invalid_action":
            raise HTTPException(status_code=400, detail=f"Action must be one of: {sorted(ALL_ACTIONS)}")
        if str(exc) == "user_not_found":
            raise HTTPException(status_code=404, detail="Unknown user")
        if str(exc) == "embedding_not_found":
            raise HTTPException(status_code=404, detail="Unknown thread embedding")
        if str(exc) == "role_not_found":
            raise HTTPException(status_code=400, detail="Missing role for user")
        raise HTTPException(status_code=400, detail="Invalid feedback payload")
    return {
        "interaction_id": result["interaction_id"],
        "user_id": payload.user_id,
        "project_id": payload.project_id,
        "thread_ts": payload.thread_ts,
        "action": payload.action,
        "update_summary": f"Updated user vector {result['direction']} item embedding.",
        "user_vector_norm": result["new_norm"],
    }
