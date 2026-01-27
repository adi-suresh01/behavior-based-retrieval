import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional

from app.ingest import ingest_payload
from app.models import SlackEventPayload
from app.sim.dataset import SimClock, get_scenario_events


@dataclass
class SimState:
    running: bool = False
    scenario_id: Optional[str] = None
    emitted_count: int = 0
    last_event_id: Optional[str] = None
    speed_multiplier: float = 1.0
    max_events: Optional[int] = None
    loop: bool = False
    task: Optional[asyncio.Task] = None
    clock: SimClock = field(default_factory=SimClock)


STATE = SimState()


def reset_state() -> None:
    STATE.running = False
    STATE.scenario_id = None
    STATE.emitted_count = 0
    STATE.last_event_id = None
    STATE.speed_multiplier = 1.0
    STATE.max_events = None
    STATE.loop = False
    STATE.clock = SimClock()


async def _emit_events(scenario_id: str) -> None:
    try:
        while STATE.running:
            events = get_scenario_events(scenario_id, STATE.clock)
            for event in events:
                if not STATE.running:
                    break
                payload = SlackEventPayload.model_validate(event)
                ingest_payload(payload)
                STATE.emitted_count += 1
                STATE.last_event_id = event.get("event_id")
                if STATE.max_events and STATE.emitted_count >= STATE.max_events:
                    STATE.running = False
                    break
                delay = 1.0 / max(STATE.speed_multiplier, 0.01)
                await asyncio.sleep(delay)
            if not STATE.loop:
                STATE.running = False
    finally:
        STATE.task = None


def start_streaming(
    scenario_id: str,
    speed_multiplier: float = 1.0,
    max_events: Optional[int] = None,
    loop: bool = False,
) -> None:
    if STATE.running:
        return
    STATE.running = True
    STATE.scenario_id = scenario_id
    STATE.speed_multiplier = speed_multiplier
    STATE.max_events = max_events
    STATE.loop = loop
    STATE.task = asyncio.create_task(_emit_events(scenario_id))


def stop_streaming() -> None:
    STATE.running = False
    if STATE.task:
        STATE.task.cancel()
        STATE.task = None
