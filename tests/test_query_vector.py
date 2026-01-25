import json
import math

import pytest

from app import db
from app.profiles import (
    create_phase,
    create_project,
    create_role,
    create_user,
    get_query_vector,
    update_project_phase,
)


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_query.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def vector_norm(vec):
    return math.sqrt(sum(v * v for v in vec))


def test_query_vector_normalized():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")

    result = get_query_vector("user-1", "proj-1")
    norm = vector_norm(result["q_vector"])
    assert abs(norm - 1.0) < 1e-6


def test_query_vector_changes_with_phase():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_phase("DVT", "Design validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")

    result_evt = get_query_vector("user-1", "proj-1")["q_vector"]
    update_project_phase("proj-1", "DVT")
    result_dvt = get_query_vector("user-1", "proj-1")["q_vector"]
    assert result_evt != result_dvt


def test_query_vector_fallback_role_only():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_project("proj-1", "Alpha", "")
    create_user("user-1", "Ari", "role-1")

    with db.db_cursor() as cur:
        cur.execute(
            "UPDATE users SET user_vector_json = NULL WHERE user_id = ?",
            ("user-1",),
        )

    role_vec = json.loads(db.fetch_role("role-1")["role_vector_json"])
    result = get_query_vector("user-1", "proj-1")
    assert result["q_vector"] == role_vec
