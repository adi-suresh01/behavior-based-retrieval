import json
import os
import time
import uuid

import pytest

from app import db
from app.enrichment import build_title
from app.models import SlackEventPayload
from app.queueing import QUEUES
from app.workers import process_event


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def make_payload(text: str, channel: str = "C001", user: str = "U001", ts: str = None, thread_ts: str = None):
    if ts is None:
        ts = str(time.time())
    if thread_ts is None:
        thread_ts = ts
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
async def test_dedupe_behavior():
    payload = make_payload("hello")
    inserted_first = db.insert_dedupe(payload.event_id)
    db.insert_raw_event(payload.event_id, payload.model_dump())
    QUEUES.standard.put_nowait(payload)

    inserted_second = db.insert_dedupe(payload.event_id)

    assert inserted_first is True
    assert inserted_second is False


@pytest.mark.asyncio
async def test_thread_aggregation():
    root = make_payload("Root message")
    reply = make_payload("Reply", ts=str(float(root.event.ts) + 0.001), thread_ts=root.event.ts, user="U002")

    await process_event(root)
    await process_event(reply)

    thread = db.get_thread(root.event.ts)
    assert thread is not None
    assert thread["reply_count"] == 1
    participants = json.loads(thread["participants_json"])
    assert "U001" in participants
    assert "U002" in participants


@pytest.mark.asyncio
async def test_enrichment_carbon_fiber():
    base_ts = str(time.time())
    messages = [
        (
            "We need a decision needed on switching from aluminum to carbon fiber for the chassis. EVT build is blocked.",
            "U001",
            [{"name": "rotating_light", "count": 1}],
        ),
        (
            "Vendor A can deliver carbon fiber in 8 weeks, but Vendor B says aluminum is still safer.",
            "U002",
            None,
        ),
        (
            "By Friday we need to lock the material. DVT starts soon.",
            "U003",
            None,
        ),
    ]
    for idx, (text, user, reactions) in enumerate(messages):
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
                "reactions": reactions,
            },
        )
        await process_event(payload)

    row = db.fetch_items(1)[0]
    labels = json.loads(row["labels_json"])
    entities = json.loads(row["entities_json"])
    assert "DECISION" in labels
    assert "RISK" in labels
    assert "carbon fiber" in entities.get("materials", [])
    assert "aluminum" in entities.get("materials", [])
    assert row["title"] == build_title(entities)
    assert row["urgency"] > 0.7


@pytest.mark.asyncio
async def test_embedding_created():
    payload = make_payload("Embedding test message")
    await process_event(payload)
    row = db.fetch_embedding(payload.event.ts)
    assert row is not None
    assert row["dim"] == 64
    vector = json.loads(row["vector_json"])
    assert len(vector) == 64
