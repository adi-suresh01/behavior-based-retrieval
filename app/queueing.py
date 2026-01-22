import asyncio
from typing import Dict

from app.models import SlackEventPayload

HOT_SIGNALS = ["decision needed", "by friday", "blocker", "urgent", "evt"]


class QueueManager:
    def __init__(self) -> None:
        self.hot: asyncio.Queue = asyncio.Queue()
        self.standard: asyncio.Queue = asyncio.Queue()
        self.backfill: asyncio.Queue = asyncio.Queue()


QUEUES = QueueManager()


def _has_rotating_light(reactions) -> bool:
    if not reactions:
        return False
    return any(r.name == "rotating_light" for r in reactions)


def route_job(payload: SlackEventPayload) -> str:
    text = (payload.event.text or "").lower()
    reactions = payload.event.reactions or []
    if any(signal in text for signal in HOT_SIGNALS) or _has_rotating_light(reactions):
        QUEUES.hot.put_nowait(payload)
        return "hot"
    QUEUES.standard.put_nowait(payload)
    return "standard"


def enqueue_backfill(payload: SlackEventPayload) -> None:
    QUEUES.backfill.put_nowait(payload)


def queue_sizes() -> Dict[str, int]:
    return {
        "hot": QUEUES.hot.qsize(),
        "standard": QUEUES.standard.qsize(),
        "backfill": QUEUES.backfill.qsize(),
    }
