# Demo: Mock Slack Streaming Simulator

## Start the server

```bash
uvicorn app.main:app --reload
```

## Start the simulator

```bash
curl -X POST http://localhost:8000/simulate/start \
  -H "Content-Type: application/json" \
  -d '{"scenario_id":"carbon_fiber_demo","speed_multiplier":5}'

curl http://localhost:8000/simulate/status
```

## Run the demo runbook

```bash
python -m app.sim.demo_runbook
```

## Expected output (sample)

```
=== Digest: EVT (U_MAYA) ===
1. Material change proposal: aluminum -> carbon fiber  | why: High urgency; Phase match: EVT
2. Thread update                                   | why: Semantic similarity
...

=== Phase Change: EVT -> DVT ===
U_MAYA
- Material change proposal: aluminum -> carbon fiber (rank 1 -> 2)
- RF test risk near antenna mount (rank 4 -> 1)

=== Feedback Learning ===
U_SAM dot(v_pos) before: 0.412 after: 0.538 (+0.126)
U_SAM dot(v_neg) before: 0.301 after: 0.220 (-0.081)
```

Notes:
- The simulator emits deterministic Slack-like events into the same ingestion pipeline used by real Slack events.
- The runbook shows digest differences by role, phase change, and feedback-driven learning.
