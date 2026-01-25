# EverCurrent Daily Digest Base Infrastructure

This is a production-shaped backend skeleton for a Slack-based Daily Digest system. It stops after embedding storage (no retrieval, re-ranking, or delivery yet).

## Run

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Optional environment variables:

- `DATABASE_PATH=./app.db`
- `SLACK_VERIFY_SIGNATURE=false` (disable signature verification locally)
- `SLACK_SIGNING_SECRET=...` (required when verification is enabled)

## Pipeline Stages

1. Slack Events API ingestion
2. Optional signature verification
3. Dedupe/idempotency
4. Raw event persistence
5. Job routing into three queues (hot/standard/backfill)
6. Worker builds thread objects
7. Deterministic enrichment into digest items
8. Deterministic embedding computation and storage

## Dedupe Key

Slack `event_id` is persisted to `dedupe_events`. If the insert conflicts, the event is treated as a duplicate and is not re-queued. Message inserts are also idempotent via `(channel, ts)` uniqueness.

## Example Usage

Seed a realistic carbon fiber vs aluminum thread:

```bash
curl -X POST http://localhost:8000/seed_mock
```

Inspect stages:

```bash
curl http://localhost:8000/queues/status
curl http://localhost:8000/raw_events
curl http://localhost:8000/threads
curl http://localhost:8000/items
curl http://localhost:8000/embeddings/<thread_ts>
```

Profile layer examples:

```bash
curl -X POST http://localhost:8000/roles \
  -H "Content-Type: application/json" \
  -d '{"role_id":"role-pm","name":"PM","description":"Owns delivery timelines and decisions"}'

curl -X POST http://localhost:8000/phases \
  -H "Content-Type: application/json" \
  -d '{"phase_key":"EVT","description":"Engineering validation testing phase"}'

curl -X POST http://localhost:8000/projects \
  -H "Content-Type: application/json" \
  -d '{"project_id":"proj-1","name":"Alpha","current_phase":"EVT"}'

curl -X PATCH http://localhost:8000/projects/proj-1/phase \
  -H "Content-Type: application/json" \
  -d '{"phase_key":"EVT"}'

curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-1","name":"Ari","role_id":"role-pm"}'

curl -X PATCH http://localhost:8000/users/user-1/role \
  -H "Content-Type: application/json" \
  -d '{"role_id":"role-pm"}'

curl -X POST http://localhost:8000/users/user-1/projects/proj-1

curl http://localhost:8000/profiles/users/user-1
curl http://localhost:8000/profiles/projects/proj-1
curl "http://localhost:8000/debug/query_vector?user_id=user-1&project_id=proj-1"
```

Send a Slack-style event (signature verification disabled):

```bash
curl -X POST http://localhost:8000/slack/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt-123",
    "event_time": 1700000000,
    "event_ts": "1700000000.0001",
    "team_id": "T001",
    "type": "event_callback",
    "event": {
      "type": "message",
      "channel": "C001",
      "user": "U123",
      "text": "Decision needed by Friday on EVT material change.",
      "ts": "1700000000.0001",
      "thread_ts": "1700000000.0001",
      "reactions": [{"name": "rotating_light", "count": 1}]
    }
  }'
```
