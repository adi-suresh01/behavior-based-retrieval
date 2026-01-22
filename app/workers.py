import asyncio
from typing import Callable

from app import db
from app.embedding import embed_and_store
from app.enrichment import enrich_thread
from app.models import SlackEventPayload
from app.threading import store_message, update_thread_stats, get_thread_text


async def process_event(payload: SlackEventPayload) -> None:
    inserted, thread_ts = store_message(payload.event)
    if not inserted:
        return
    update_thread_stats(thread_ts, payload.event.channel)
    title, labels, entities, urgency, summary = enrich_thread(thread_ts)
    db.upsert_digest_item(
        thread_ts=thread_ts,
        channel=payload.event.channel,
        title=title,
        labels=labels,
        entities=entities,
        urgency=urgency,
        summary=summary,
    )
    thread_text, _ = get_thread_text(thread_ts)
    embed_and_store(thread_ts, thread_text, db.upsert_embedding)


async def worker_loop(queue: asyncio.Queue, queue_name: str) -> None:
    while True:
        payload = await queue.get()
        try:
            await process_event(payload)
            db.increment_metric(queue_name)
        finally:
            queue.task_done()


def start_workers(
    loop: asyncio.AbstractEventLoop,
    queue: asyncio.Queue,
    queue_name: str,
) -> asyncio.Task:
    return loop.create_task(worker_loop(queue, queue_name))


def start_all_workers(loop: asyncio.AbstractEventLoop, queue_manager) -> None:
    start_workers(loop, queue_manager.hot, "hot")
    start_workers(loop, queue_manager.standard, "standard")
    start_workers(loop, queue_manager.backfill, "backfill")
