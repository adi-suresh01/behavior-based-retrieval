import time
import uuid

import pytest
from fastapi.testclient import TestClient

from app import db
from app.main import app
from app.models import SlackEventPayload
from app.profiles import create_phase, create_project, create_role, create_user
from app.retrieval import load_candidate_items
from app.workers import process_event


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_access.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def make_payload(text: str, channel: str, ts: str, thread_ts: str, user: str = "U001"):
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


@pytest.mark.asyncio
async def test_user_without_channel_access_forbidden():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    await process_event(make_payload("Decision needed", "C001", base_ts, base_ts))

    client = TestClient(app)
    response = client.get("/digest", params={"user_id": "user-1", "project_id": "proj-1", "n": 1})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_channel_filtering():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    await process_event(make_payload("Decision needed", "C001", base_ts, base_ts))
    other_ts = str(float(base_ts) + 1.0)
    await process_event(make_payload("Other channel", "C999", other_ts, other_ts))

    candidates = load_candidate_items(project_id="proj-1")
    assert all(c["thread_ts"] == base_ts for c in candidates)
