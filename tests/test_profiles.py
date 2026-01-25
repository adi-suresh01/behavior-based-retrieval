import json
import math

import pytest

from app import db
from app.profiles import create_phase, create_project, create_role, create_user, update_project_phase


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_profiles.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def is_normalized(vec):
    norm = math.sqrt(sum(v * v for v in vec))
    return abs(norm - 1.0) < 1e-6


def test_role_vector_normalized():
    vector = create_role("role-1", "PM", "Owns delivery timelines and decisions")
    row = db.fetch_role("role-1")
    stored = json.loads(row["role_vector_json"])
    assert len(vector) == 64
    assert stored == vector
    assert is_normalized(stored)


def test_phase_vector_normalized():
    vector = create_phase("EVT", "Engineering validation testing phase")
    row = db.fetch_phase("EVT")
    stored = json.loads(row["phase_vector_json"])
    assert len(vector) == 64
    assert stored == vector
    assert is_normalized(stored)


def test_user_vector_from_role():
    role_vec = create_role("role-2", "Design", "Owns mechanical design reviews")
    user_vec, role_id = create_user("user-1", "Ari", "role-2")
    assert role_id == "role-2"
    assert user_vec == role_vec


def test_project_phase_update():
    create_phase("DVT", "Design validation testing phase")
    create_phase("PVT", "Production validation testing phase")
    create_project("proj-1", "Alpha", "DVT")
    update_project_phase("proj-1", "PVT")
    project = db.fetch_project("proj-1")
    assert project["current_phase"] == "PVT"
