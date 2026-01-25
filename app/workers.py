import asyncio
from typing import Callable

from app import db
from app.embedding import embed_and_store
from app.enrichment import enrich_thread
from app.models import SlackEventPayload
from app.threading import store_message, update_thread_stats, get_thread_text


async def process_event(payload: SlackEventPayload) -> None:
    event = payload.event
    channel = event.channel
    if event.type == "message" and event.subtype in {"message_changed", "message_deleted"}:
        channel = channel or (event.message or {}).get("channel") or (event.previous_message or {}).get("channel")
        if event.subtype == "message_changed":
            message = event.message or {}
            ts = message.get("ts")
            text = message.get("text")
            thread_ts = message.get("thread_ts") or ts
            if channel and ts:
                db.update_message_text(channel, ts, text)
            else:
                return
        else:
            message = event.previous_message or event.message or {}
            ts = message.get("ts") or getattr(event, "deleted_ts", None)
            thread_ts = message.get("thread_ts") or ts
            if channel and ts:
                db.mark_message_deleted(channel, ts)
            else:
                return
    elif event.type in {"reaction_added", "reaction_removed"}:
        item = event.item or {}
        channel = item.get("channel") or channel
        ts = item.get("ts") or event.ts
        if not channel or not ts or not event.reaction:
            return
        delta = 1 if event.type == "reaction_added" else -1
        db.update_message_reactions(channel, ts, event.reaction, delta)
        message = db.fetch_message(channel, ts)
        if message is None:
            return
        thread_ts = message["thread_ts"]
    else:
        if event.channel is None or event.ts is None:
            return
        inserted, thread_ts = store_message(event)
        if not inserted:
            return
    if not channel:
        return
    update_thread_stats(thread_ts, channel)
    title, labels, entities, urgency, summary = enrich_thread(thread_ts)
    db.upsert_digest_item(
        thread_ts=thread_ts,
        channel=channel,
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
