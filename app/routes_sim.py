from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from app.ingest import ingest_payload
from app.models import IngestResult, SlackEventPayload
from app.queueing import queue_sizes
from app.sim.streamer import STATE, reset_state, start_streaming, stop_streaming

router = APIRouter()


@router.post("/sim/events")
async def sim_events(payload: Dict[str, Any]) -> IngestResult:
    try:
        event_payload = SlackEventPayload.model_validate(payload)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid event payload")
    inserted, event_id = ingest_payload(event_payload)
    return IngestResult(status="queued" if inserted else "duplicate", event_id=event_id)


@router.post("/simulate/start")
async def simulate_start(payload: Dict[str, Any]):
    scenario_id = payload.get("scenario_id")
    if not scenario_id:
        raise HTTPException(status_code=400, detail="scenario_id required")
    speed_multiplier = float(payload.get("speed_multiplier", 1.0))
    max_events = payload.get("max_events")
    loop = bool(payload.get("loop", False))
    run_id = payload.get("run_id")
    start_streaming(
        scenario_id,
        speed_multiplier=speed_multiplier,
        max_events=max_events,
        loop=loop,
        run_id=run_id,
    )
    return {"status": "started", "scenario_id": scenario_id}


@router.post("/simulate/stop")
async def simulate_stop():
    stop_streaming()
    return {"status": "stopped"}


@router.get("/simulate/status")
async def simulate_status():
    return {
        "running": STATE.running,
        "scenario_id": STATE.scenario_id,
        "emitted_count": STATE.emitted_count,
        "last_event_id": STATE.last_event_id,
        "queue_sizes": queue_sizes(),
    }


@router.post("/simulate/reset")
async def simulate_reset():
    reset_state()
    return {"status": "reset"}
