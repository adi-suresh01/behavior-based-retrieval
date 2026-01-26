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
    db_path = tmp_path / "test_schedules.db"
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
async def test_run_now_creates_delivery(monkeypatch):
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")
    db.add_user_channel("user-1", "C001")

    base_ts = str(time.time())
    await process_event(make_payload("Decision needed", "C001", base_ts, base_ts))

    def fake_slack_api_call(team_id, method, params=None):
        if method == "conversations.open":
            return {"channel": {"id": "D123"}}
        return {"ok": True, "ts": "123.456"}

    async def fake_call(team_id, method, params=None):
        return fake_slack_api_call(team_id, method, params)

    monkeypatch.setattr("app.delivery.slack_api_call", fake_call)

    client = TestClient(app)
    resp = client.post(
        "/schedules",
        json={
            "team_id": "T001",
            "project_id": "proj-1",
            "user_id": "user-1",
            "time_of_day": "09:00",
            "timezone": "UTC",
        },
    )
    schedule_id = resp.json()["schedule_id"]

    run = client.post(f"/schedules/{schedule_id}/run_now")
    assert run.status_code == 200
    assert run.json()["status"] == "delivered"
    deliveries = db.fetch_delivery_by_digest(run.json()["digest_id"])
    assert deliveries is not None


@pytest.mark.asyncio
async def test_run_now_idempotent(monkeypatch):
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")
    db.add_user_channel("user-1", "C001")

    base_ts = str(time.time())
    await process_event(make_payload("Decision needed", "C001", base_ts, base_ts))

    async def fake_call(team_id, method, params=None):
        if method == "conversations.open":
            return {"channel": {"id": "D123"}}
        return {"ok": True, "ts": "123.456"}

    monkeypatch.setattr("app.delivery.slack_api_call", fake_call)

    client = TestClient(app)
    resp = client.post(
        "/schedules",
        json={
            "team_id": "T001",
            "project_id": "proj-1",
            "user_id": "user-1",
            "time_of_day": "09:00",
            "timezone": "UTC",
        },
    )
    schedule_id = resp.json()["schedule_id"]

    first = client.post(f"/schedules/{schedule_id}/run_now")
    second = client.post(f"/schedules/{schedule_id}/run_now")
    assert first.json()["status"] == "delivered"
    assert second.json()["status"] in {"already_delivered", "duplicate"}
