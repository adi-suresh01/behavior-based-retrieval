import json
import math
import time
import uuid

import pytest

from app import db
from app.feedback import apply_feedback
from app.models import SlackEventPayload
from app.profiles import create_phase, create_project, create_role, create_user, get_query_vector
from app.workers import process_event


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_feedback.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def make_payload(text: str, thread_ts: str, ts: str, channel: str = "C001", user: str = "U001"):
    return SlackEventPayload(
        event_id=f"evt-{uuid.uuid4().hex}",
        event_time=int(time.time()),
        event_ts=ts,
        team_id="T001",
        type="event_callback",
        event={
            "type": "message",
            "channel": channel,
            "user": user,
            "text": text,
            "ts": ts,
            "thread_ts": thread_ts,
        },
    )


def dot(a, b):
    return sum(x * y for x, y in zip(a, b))


def get_user_vector(user_id: str):
    user = db.fetch_user(user_id)
    return json.loads(user["user_vector_json"])


def get_item_vector(thread_ts: str):
    emb = db.fetch_embedding(thread_ts)
    return json.loads(emb["vector_json"])


@pytest.mark.asyncio
async def test_positive_feedback_moves_closer():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    await process_event(make_payload("Decision needed", thread_ts=base_ts, ts=base_ts))

    u_old = get_user_vector("user-1")
    v = get_item_vector(base_ts)
    before = dot(u_old, v)

    apply_feedback("user-1", "proj-1", base_ts, "click")
    u_new = get_user_vector("user-1")
    after = dot(u_new, v)
    assert after > before


@pytest.mark.asyncio
async def test_negative_feedback_moves_away():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    await process_event(make_payload("Decision needed", thread_ts=base_ts, ts=base_ts))

    u_old = get_user_vector("user-1")
    v = get_item_vector(base_ts)
    before = dot(u_old, v)

    apply_feedback("user-1", "proj-1", base_ts, "dismiss")
    u_new = get_user_vector("user-1")
    after = dot(u_new, v)
    assert after < before


@pytest.mark.asyncio
async def test_query_vector_changes_after_feedback():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    await process_event(make_payload("Decision needed", thread_ts=base_ts, ts=base_ts))

    q_before = get_query_vector("user-1", "proj-1")["q_vector"]
    apply_feedback("user-1", "proj-1", base_ts, "click")
    q_after = get_query_vector("user-1", "proj-1")["q_vector"]

    assert q_before != q_after
