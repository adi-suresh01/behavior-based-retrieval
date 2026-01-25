from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.profiles import (
    add_user_to_project,
    create_phase,
    create_project,
    create_role,
    create_user,
    get_project_profile,
    get_user_profile,
    update_project_phase,
    update_user_role,
)

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
