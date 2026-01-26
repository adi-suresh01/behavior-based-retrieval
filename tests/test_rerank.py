import time
import uuid

import pytest
import numpy as np

from app import db
from app.models import SlackEventPayload
from app.profiles import create_phase, create_project, create_role, create_user, get_query_vector
from app.rerank import rerank_candidates
from app.retrieval import load_candidate_items, cosine_sim
from app.workers import process_event


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_rerank.db"
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
async def test_rerank_must_include_blocker():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    urgent_payload = make_payload(
        "Decision needed by Friday; EVT is blocked and urgent.",
        thread_ts=base_ts,
        ts=base_ts,
    )
    await process_event(urgent_payload)

    other_ts = str(float(base_ts) + 1.0)
    other_payload = make_payload(
        "FYI: status update.",
        thread_ts=other_ts,
        ts=other_ts,
    )
    await process_event(other_payload)

    q_result = get_query_vector("user-1", "proj-1")
    candidates = load_candidate_items(project_id="proj-1")
    scored = []
    q_vec = np.array(q_result["q_vector"], dtype=float)
    for c in candidates:
        scored.append({**c, "sim_score": cosine_sim(q_vec, c["vector"])})
    results = rerank_candidates(scored, "user-1", n=1)
    assert results[0]["thread_ts"] == base_ts
    assert results[0]["force_included"] is True


@pytest.mark.asyncio
async def test_rerank_deterministic_order():
    create_role("role-1", "PM", "Owns delivery timelines and decisions")
    create_phase("EVT", "Engineering validation testing phase")
    create_project("proj-1", "Alpha", "EVT")
    create_user("user-1", "Ari", "role-1")
    db.add_project_channel("proj-1", "C001")

    base_ts = str(time.time())
    first_ts = str(float(base_ts) + 0.01)
    second_ts = str(float(base_ts) + 0.02)
    await process_event(make_payload("Decision needed", thread_ts=first_ts, ts=first_ts))
    await process_event(make_payload("Decision needed", thread_ts=second_ts, ts=second_ts))

    q_result = get_query_vector("user-1", "proj-1")
    candidates = load_candidate_items(project_id="proj-1")
    q_vec = np.array(q_result["q_vector"], dtype=float)
    scored = []
    for c in candidates:
        scored.append({**c, "sim_score": cosine_sim(q_vec, c["vector"])})

    results = rerank_candidates(scored, "user-1", n=2)
    assert results == sorted(
        results,
        key=lambda item: (
            -item["final_score"],
            -item["base_score"],
            -item["urgency"],
            -item["updated_at"],
            item["thread_ts"],
        ),
    )
