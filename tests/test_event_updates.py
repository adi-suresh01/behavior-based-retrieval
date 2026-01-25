import time
import uuid

import pytest

from app import db
from app.models import SlackEventPayload
from app.workers import process_event


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_updates.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def make_message_payload(text: str, channel: str, ts: str, thread_ts: str, user: str = "U001"):
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


def make_message_changed_payload(channel: str, ts: str, thread_ts: str, text: str):
    return SlackEventPayload(
        event_id=f"evt-{uuid.uuid4().hex}",
        event_time=int(time.time()),
        event_ts=ts,
        team_id="T001",
        type="event_callback",
        event={
            "type": "message",
            "subtype": "message_changed",
            "channel": channel,
            "message": {"ts": ts, "text": text, "thread_ts": thread_ts, "channel": channel},
        },
    )


def make_message_deleted_payload(channel: str, ts: str, thread_ts: str):
    return SlackEventPayload(
        event_id=f"evt-{uuid.uuid4().hex}",
        event_time=int(time.time()),
        event_ts=ts,
        team_id="T001",
        type="event_callback",
        event={
            "type": "message",
            "subtype": "message_deleted",
            "channel": channel,
            "previous_message": {"ts": ts, "thread_ts": thread_ts, "channel": channel},
        },
    )


def make_reaction_payload(channel: str, ts: str, reaction: str, added: bool = True):
    return SlackEventPayload(
        event_id=f"evt-{uuid.uuid4().hex}",
        event_time=int(time.time()),
        event_ts=ts,
        team_id="T001",
        type="event_callback",
        event={
            "type": "reaction_added" if added else "reaction_removed",
            "item": {"channel": channel, "ts": ts},
            "reaction": reaction,
        },
    )


@pytest.mark.asyncio
async def test_edit_updates_summary_and_embedding():
    channel = "C001"
    base_ts = str(time.time())
    await process_event(make_message_payload("Original text", channel, base_ts, base_ts))

    item_before = db.fetch_items(1)[0]
    emb_before = db.fetch_embedding(base_ts)
    summary_before = item_before["summary"]
    updated_at_before = emb_before["updated_at"]

    time.sleep(0.01)
    await process_event(make_message_changed_payload(channel, base_ts, base_ts, "Edited text"))

    item_after = db.fetch_items(1)[0]
    emb_after = db.fetch_embedding(base_ts)
    assert "Edited text" in item_after["summary"]
    assert item_after["summary"] != summary_before
    assert emb_after["updated_at"] > updated_at_before


@pytest.mark.asyncio
async def test_reaction_increases_urgency():
    channel = "C001"
    base_ts = str(time.time())
    await process_event(make_message_payload("FYI: update", channel, base_ts, base_ts))

    urgency_before = db.fetch_items(1)[0]["urgency"]
    await process_event(make_reaction_payload(channel, base_ts, "rotating_light", added=True))
    urgency_after = db.fetch_items(1)[0]["urgency"]
    assert urgency_after > urgency_before


@pytest.mark.asyncio
async def test_delete_removes_from_summary():
    channel = "C001"
    base_ts = str(time.time())
    reply_ts = str(float(base_ts) + 0.001)
    await process_event(make_message_payload("Root", channel, base_ts, base_ts))
    await process_event(make_message_payload("Reply to remove", channel, reply_ts, base_ts))

    summary_before = db.fetch_items(1)[0]["summary"]
    assert "Reply to remove" in summary_before

    await process_event(make_message_deleted_payload(channel, reply_ts, base_ts))
    summary_after = db.fetch_items(1)[0]["summary"]
    assert "Reply to remove" not in summary_after
