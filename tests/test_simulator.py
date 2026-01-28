import json
import time

import pytest
from fastapi.testclient import TestClient

from app import db
from app.main import app
from app.models import SlackEventPayload
from app.profiles import create_phase, create_project, create_role, create_user
from app.sim.dataset import SimClock, get_scenario_events
from app.workers import process_event
from app.digest import build_digest


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_sim.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def ingest_events(events):
    for event in events:
        payload = SlackEventPayload.model_validate(event)
        yield payload


@pytest.mark.asyncio
async def test_sim_events_endpoint_accepts_payload():
    client = TestClient(app)
    events = get_scenario_events("carbon_fiber_demo", SimClock(), "test")
    response = client.post("/sim/events", json=events[0])
    assert response.status_code == 200
    assert response.json()["status"] in {"queued", "duplicate"}


@pytest.mark.asyncio
async def test_streaming_builds_threads_and_items():
    events = get_scenario_events("carbon_fiber_demo", SimClock(), "test")
    for payload in ingest_events(events):
        await process_event(payload)
    threads = db.fetch_threads(10)
    items = db.fetch_items(10)
    assert len(threads) >= 4
    assert len(items) >= 4


@pytest.mark.asyncio
async def test_digest_differs_by_role_and_phase():
    create_role("role-me", "ME", "materials structures weight manufacturability")
    create_role("role-supply", "Supply", "vendors lead times MOQ sourcing risk")
    create_role("role-pm", "PM", "timeline decisions owners milestones")
    create_phase("EVT", "early prototype build, unblock near-term decisions")
    create_phase("DVT", "validation testing focus, reliability risks")
    create_project("proj-1", "DroneV2", "EVT")
    db.add_project_channel("proj-1", "C_DRONE_STRUCT")
    db.add_project_channel("proj-1", "C_DRONE_SUPPLY")

    create_user("U_MAYA", "Maya", "role-me")
    create_user("U_SAM", "Sam", "role-supply")
    create_user("U_PRIYA", "Priya", "role-pm")
    for user_id in ["U_MAYA", "U_SAM", "U_PRIYA"]:
        db.add_user_channel(user_id, "C_DRONE_STRUCT")
        db.add_user_channel(user_id, "C_DRONE_SUPPLY")

    events = get_scenario_events("carbon_fiber_demo", SimClock(), "test")
    for payload in ingest_events(events):
        await process_event(payload)

    digest_me = build_digest("U_MAYA", "proj-1", n=5)["items"]
    digest_supply = build_digest("U_SAM", "proj-1", n=5)["items"]
    digest_pm = build_digest("U_PRIYA", "proj-1", n=5)["items"]

    top_me = [item["title"] for item in digest_me[:3]]
    top_supply = [item["title"] for item in digest_supply[:3]]
    top_pm = [item["title"] for item in digest_pm[:3]]
    assert top_me != top_supply or top_me != top_pm

    db.update_project_phase("proj-1", "DVT")
    digest_me_dvt = build_digest("U_MAYA", "proj-1", n=5)["items"]
    assert [item["title"] for item in digest_me_dvt] != [item["title"] for item in digest_me]


@pytest.mark.asyncio
async def test_feedback_changes_user_vector():
    create_role("role-supply", "Supply", "vendors lead times MOQ sourcing risk")
    create_phase("EVT", "early prototype build, unblock near-term decisions")
    create_project("proj-1", "DroneV2", "EVT")
    db.add_project_channel("proj-1", "C_DRONE_SUPPLY")
    create_user("U_SAM", "Sam", "role-supply")
    db.add_user_channel("U_SAM", "C_DRONE_SUPPLY")

    events = get_scenario_events("carbon_fiber_demo", SimClock(), "test")
    for payload in ingest_events(events):
        await process_event(payload)

    items = db.fetch_items(10)
    supply_item = next(item for item in items if "Vendor" in (item["summary"] or ""))
    thread_ts = supply_item["thread_ts"]
    emb = db.fetch_embedding(thread_ts)
    v = json.loads(emb["vector_json"])
    user_before = json.loads(db.fetch_user("U_SAM")["user_vector_json"])
    dot_before = sum(a * b for a, b in zip(user_before, v))

    from app.feedback import apply_feedback

    apply_feedback("U_SAM", "proj-1", thread_ts, "thumbs_up")
    user_after = json.loads(db.fetch_user("U_SAM")["user_vector_json"])
    dot_after = sum(a * b for a, b in zip(user_after, v))
    assert dot_after > dot_before
