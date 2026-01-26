import time
import uuid

import pytest
from fastapi.testclient import TestClient

from app import db
from app.main import app
from app.models import SlackEventPayload
from app.profiles import create_phase, create_project, create_role, create_user
from app.workers import process_event


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_digest.db"
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


@pytest.mark.asyncio
async def test_digest_endpoint_and_storage():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")
    db.add_user_channel("user-1", "C001")

    base_ts = str(time.time())
    await process_event(
        make_payload(
            "Decision needed by Friday; EVT is blocked and urgent.",
            thread_ts=base_ts,
            ts=base_ts,
        )
    )

    client = TestClient(app)
    response = client.get("/digest", params={"user_id": "user-1", "project_id": "proj-1", "n": 1})
    assert response.status_code == 200
    payload = response.json()
    assert payload["digest_id"].startswith("dig-")
    assert len(payload["items"]) == 1
    assert "why_shown" in payload["items"][0]

    row = db.fetch_digest(payload["digest_id"])
    assert row is not None
