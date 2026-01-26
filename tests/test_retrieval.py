import time
import uuid

import pytest
import numpy as np

from app import db
from app.models import SlackEventPayload
from app.profiles import create_phase, create_project, create_role, create_user, get_query_vector
from app.retrieval import load_candidate_items, retrieve_top_k
from app.workers import process_event


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_retrieval.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def seed_carbon_thread():
    base_ts = str(time.time())
    messages = [
        (
            "Decision needed: switch from aluminum to carbon fiber for EVT build.",
            "U001",
        ),
        (
            "By Friday we need to lock the material. DVT starts soon.",
            "U002",
        ),
    ]
    for idx, (text, user) in enumerate(messages):
        ts = str(float(base_ts) + idx * 0.001)
        payload = SlackEventPayload(
            event_id=f"evt-{uuid.uuid4().hex}",
            event_time=int(time.time()),
            event_ts=ts,
            team_id="T001",
            type="event_callback",
            event={
                "type": "message",
                "channel": "C001",
                "user": user,
                "text": text,
                "ts": ts,
                "thread_ts": base_ts,
            },
        )
        yield payload


@pytest.mark.asyncio
async def test_retrieval_returns_thread():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    for payload in seed_carbon_thread():
        await process_event(payload)

    q_result = get_query_vector("user-1", "proj-1")
    candidates = load_candidate_items(project_id="proj-1")
    results = retrieve_top_k(np.array(q_result["q_vector"]), candidates, k=10)
    assert any(r["thread_ts"] == candidates[0]["thread_ts"] for r in results)


@pytest.mark.asyncio
async def test_retrieval_tiebreaks_deterministically():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    messages = [
        ("Decision needed", "U001"),
        ("Decision needed", "U002"),
    ]
    thread_ids = []
    for idx, (text, user) in enumerate(messages):
        ts = str(float(base_ts) + idx * 0.01)
        payload = SlackEventPayload(
            event_id=f"evt-{uuid.uuid4().hex}",
            event_time=int(time.time()),
            event_ts=ts,
            team_id="T001",
            type="event_callback",
            event={
                "type": "message",
                "channel": "C001",
                "user": user,
                "text": text,
                "ts": ts,
                "thread_ts": ts,
            },
        )
        thread_ids.append(ts)
        await process_event(payload)

    q_result = get_query_vector("user-1", "proj-1")
    candidates = load_candidate_items(project_id="proj-1")
    results = retrieve_top_k(np.array(q_result["q_vector"]), candidates, k=2)
    assert results[0]["thread_ts"] != results[1]["thread_ts"]
    assert results == sorted(
        results,
        key=lambda item: (-item["score"], -item["urgency"], -item["updated_at"], item["thread_ts"]),
    )
