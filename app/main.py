import asyncio
import json
import time
import uuid
from typing import Any, Dict, List

from fastapi import FastAPI, Request

from app import db
from app.ingest import handle_slack_event, ingest_payload
from app.models import (
    DigestItemView,
    EmbeddingView,
    HealthResponse,
    IngestResult,
    QueueStatus,
    SlackEventPayload,
    ThreadView,
)
from app.queueing import QUEUES, enqueue_backfill, queue_sizes
from app.routes_profiles import router as profiles_router
from app.workers import start_all_workers

app = FastAPI()
app.include_router(profiles_router)


@app.on_event("startup")
async def startup() -> None:
    db.init_db()
    loop = app.state.loop = asyncio.get_event_loop()
    start_all_workers(loop, QUEUES)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.post("/slack/events", response_model=IngestResult)
async def slack_events(request: Request, payload: SlackEventPayload) -> IngestResult:
    raw_body = await request.body()
    request.scope["raw_body"] = raw_body
    inserted, event_id = handle_slack_event(request, payload)
    return IngestResult(status="queued" if inserted else "duplicate", event_id=event_id)


@app.post("/seed_mock")
async def seed_mock() -> Dict[str, Any]:
    base_ts = str(time.time())
    thread_ts = base_ts
    events: List[SlackEventPayload] = []
    messages = [
        {
            "text": "We need a decision needed on switching from aluminum to carbon fiber for the chassis. EVT build is blocked.",
            "user": "U001",
            "reactions": [{"name": "rotating_light", "count": 1}],
        },
        {
            "text": "Vendor A can deliver carbon fiber in 8 weeks, but Vendor B says aluminum is still safer.",
            "user": "U002",
        },
        {
            "text": "By Friday we need to lock the material. DVT starts soon.",
            "user": "U003",
        },
        {
            "text": "Risk: carbon fiber tooling lead time is 8 weeks, but performance gains are big.",
            "user": "U004",
        },
    ]
    for idx, msg in enumerate(messages):
        ts = str(float(base_ts) + idx * 0.001)
        event_payload = SlackEventPayload(
            event_id=f"mock-{uuid.uuid4().hex}",
            event_time=int(time.time()),
            event_ts=ts,
            team_id="T001",
            type="event_callback",
            event={
                "type": "message",
                "channel": "C001",
                "user": msg["user"],
                "text": msg["text"],
                "ts": ts,
                "thread_ts": thread_ts,
                "reactions": msg.get("reactions"),
            },
        )
        events.append(event_payload)

    results = []
    for payload in events:
        inserted, event_id = ingest_payload(payload)
        results.append({"event_id": event_id, "status": "queued" if inserted else "duplicate"})
    return {"status": "seeded", "results": results}


@app.post("/backfill", response_model=IngestResult)
async def backfill(payload: SlackEventPayload) -> IngestResult:
    if not db.insert_dedupe(payload.event_id):
        return IngestResult(status="duplicate", event_id=payload.event_id)
    db.insert_raw_event(payload.event_id, payload.model_dump())
    enqueue_backfill(payload)
    return IngestResult(status="queued", event_id=payload.event_id)


@app.get("/queues/status", response_model=List[QueueStatus])
async def queues_status() -> List[QueueStatus]:
    sizes = queue_sizes()
    metrics = {row["queue_name"]: row for row in db.fetch_metrics()}
    statuses = []
    for name in ["hot", "standard", "backfill"]:
        row = metrics.get(name)
        statuses.append(
            QueueStatus(
                name=name,
                size=sizes.get(name, 0),
                processed_count=row["processed_count"] if row else 0,
                last_processed_at=row["last_processed_at"] if row else None,
            )
        )
    return statuses


@app.get("/raw_events")
async def raw_events(limit: int = 50) -> List[Dict[str, Any]]:
    rows = db.fetch_raw_events(limit)
    return [
        {
            "event_id": row["event_id"],
            "received_at": row["received_at"],
            "payload": json.loads(row["payload_json"]),
        }
        for row in rows
    ]


@app.get("/threads", response_model=List[ThreadView])
async def threads(limit: int = 50) -> List[ThreadView]:
    rows = db.fetch_threads(limit)
    result = []
    for row in rows:
        participants = json.loads(row["participants_json"] or "[]")
        result.append(
            ThreadView(
                thread_ts=row["thread_ts"],
                channel=row["channel"],
                root_ts=row["root_ts"],
                created_at=row["created_at"],
                last_activity=row["last_activity"],
                reply_count=row["reply_count"],
                reaction_count=row["reaction_count"],
                participants=participants,
            )
        )
    return result


@app.get("/items", response_model=List[DigestItemView])
async def items(limit: int = 50) -> List[DigestItemView]:
    rows = db.fetch_items(limit)
    result = []
    for row in rows:
        result.append(
            DigestItemView(
                thread_ts=row["thread_ts"],
                channel=row["channel"],
                title=row["title"],
                labels=json.loads(row["labels_json"] or "[]"),
                entities=json.loads(row["entities_json"] or "{}"),
                urgency=row["urgency"],
                summary=row["summary"],
                updated_at=row["updated_at"],
            )
        )
    return result


@app.get("/embeddings/{thread_ts}", response_model=EmbeddingView)
async def embeddings(thread_ts: str) -> EmbeddingView:
    row = db.fetch_embedding(thread_ts)
    if row is None:
        return EmbeddingView(thread_ts=thread_ts, dim=0, vector=[], updated_at=0.0)
    return EmbeddingView(
        thread_ts=row["thread_ts"],
        dim=row["dim"],
        vector=json.loads(row["vector_json"]),
        updated_at=row["updated_at"],
    )
