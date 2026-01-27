import time
from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass
class SimClock:
    start_epoch: float = 1700000000.0
    step_seconds: float = 1.0

    def __post_init__(self) -> None:
        self._current = self.start_epoch

    def tick(self) -> float:
        value = self._current
        self._current += self.step_seconds
        return value


def _event_id(prefix: str, idx: int) -> str:
    return f"Ev{prefix}{idx:04d}"


def carbon_fiber_demo(clock: SimClock) -> List[Dict]:
    events: List[Dict] = []
    idx = 0

    def emit_message(channel: str, user: str, text: str, thread_ts: float) -> None:
        nonlocal idx
        ts = clock.tick()
        events.append(
            {
                "event_id": _event_id("M", idx),
                "event_time": int(ts),
                "team_id": "T_DEMO",
                "type": "event_callback",
                "event": {
                    "type": "message",
                    "channel": channel,
                    "user": user,
                    "text": text,
                    "ts": f"{ts:.3f}",
                    "thread_ts": f"{thread_ts:.3f}",
                },
            }
        )
        idx += 1

    def emit_reaction(channel: str, reaction: str, item_ts: float) -> None:
        nonlocal idx
        ts = clock.tick()
        events.append(
            {
                "event_id": _event_id("R", idx),
                "event_time": int(ts),
                "team_id": "T_DEMO",
                "type": "event_callback",
                "event": {
                    "type": "reaction_added",
                    "item": {"channel": channel, "ts": f"{item_ts:.3f}"},
                    "reaction": reaction,
                    "event_ts": f"{ts:.3f}",
                },
            }
        )
        idx += 1

    def emit_edit(channel: str, ts: float, thread_ts: float, text: str) -> None:
        nonlocal idx
        now = clock.tick()
        events.append(
            {
                "event_id": _event_id("E", idx),
                "event_time": int(now),
                "team_id": "T_DEMO",
                "type": "event_callback",
                "event": {
                    "type": "message",
                    "subtype": "message_changed",
                    "channel": channel,
                    "message": {
                        "ts": f"{ts:.3f}",
                        "text": text,
                        "thread_ts": f"{thread_ts:.3f}",
                        "channel": channel,
                    },
                },
            }
        )
        idx += 1

    # Thread 1: Material change proposal
    thread1_ts = clock.tick()
    emit_message(
        "C_DRONE_STRUCT",
        "U_MAYA",
        "Aluminum bracket reacts with solvent X. Proposing carbon fiber for Rev C. Decision needed by Friday or EVT build slips.",
        thread1_ts,
    )
    emit_message(
        "C_DRONE_STRUCT",
        "U_MAYA",
        "ME note: carbon fiber saves 120g but tooling cost is higher.",
        thread1_ts,
    )
    emit_message(
        "C_DRONE_STRUCT",
        "U_PRIYA",
        "PM: if we miss Friday, EVT build schedule slips by 2 weeks.",
        thread1_ts,
    )
    emit_reaction("C_DRONE_STRUCT", "rotating_light", thread1_ts)

    # Thread 2: Supply chain lead time
    thread2_ts = clock.tick()
    emit_message(
        "C_DRONE_SUPPLY",
        "U_SAM",
        "Supply chain: Vendor A lead time 8 weeks, MOQ 500. Vendor B can do 6 weeks but higher cost.",
        thread2_ts,
    )
    emit_message(
        "C_DRONE_SUPPLY",
        "U_SAM",
        "Sourcing risk: carbon fiber fabric constrained. Alternative vendor C available.",
        thread2_ts,
    )

    # Thread 3: RF test risk
    thread3_ts = clock.tick()
    emit_message(
        "C_DRONE_STRUCT",
        "U_MAYA",
        "RF test risk: carbon fiber near antenna mount could worsen RF; need test before DVT.",
        thread3_ts,
    )

    # Thread 4: Build schedule / action items
    thread4_ts = clock.tick()
    emit_message(
        "C_DRONE_STRUCT",
        "U_PRIYA",
        "Build schedule: decision review tomorrow 2pm; owners <@U_MAYA> and <@U_SAM>; action list pending.",
        thread4_ts,
    )
    emit_message(
        "C_DRONE_STRUCT",
        "U_PRIYA",
        "Action items: update BOM, confirm vendor quotes, lock EVT build plan.",
        thread4_ts,
    )

    # Edit the supply chain root message to reflect updated MOQ
    emit_edit(
        "C_DRONE_SUPPLY",
        thread2_ts,
        thread2_ts,
        "Supply chain: Vendor A lead time 8 weeks, MOQ 600. Vendor B can do 6 weeks but higher cost.",
    )

    return events


def get_scenario_events(scenario_id: str, clock: SimClock) -> List[Dict]:
    if scenario_id == "carbon_fiber_demo":
        return carbon_fiber_demo(clock)
    raise ValueError("unknown_scenario")
