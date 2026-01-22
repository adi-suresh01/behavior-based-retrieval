import json
import time
from typing import Dict, Iterable, List, Tuple

from app import db
from app.models import SlackInnerEvent


def _reaction_count(reactions_json: str) -> int:
    if not reactions_json:
        return 0
    try:
        reactions = json.loads(reactions_json)
    except json.JSONDecodeError:
        return 0
    count = 0
    for reaction in reactions:
        count += int(reaction.get("count", 1))
    return count


def _participants_from_messages(messages: Iterable[dict]) -> List[str]:
    participants = set()
    for msg in messages:
        if msg.get("user"):
            participants.add(msg["user"])
    return sorted(participants)


def store_message(event: SlackInnerEvent) -> Tuple[bool, str]:
    thread_ts = event.thread_ts or event.ts
    reactions_json = None
    if event.reactions:
        reactions_json = json.dumps([r.model_dump() for r in event.reactions])
    inserted = db.insert_message(
        channel=event.channel,
        ts=event.ts,
        thread_ts=thread_ts,
        user=event.user,
        text=event.text,
        reactions_json=reactions_json,
    )
    return inserted, thread_ts


def update_thread_stats(thread_ts: str, channel: str) -> None:
    messages = db.get_messages_for_thread(thread_ts)
    if not messages:
        return
    root_ts = thread_ts
    try:
        created_at = float(thread_ts)
    except ValueError:
        created_at = time.time()
    last_activity = 0.0
    reply_count = 0
    reaction_count = 0
    participants = []
    message_dicts: List[Dict] = []
    for msg in messages:
        msg_dict = dict(msg)
        message_dicts.append(msg_dict)
        ts_val = float(msg_dict.get("ts") or 0)
        last_activity = max(last_activity, ts_val)
        if msg_dict.get("ts") != thread_ts:
            reply_count += 1
        reaction_count += _reaction_count(msg_dict.get("reactions_json"))
    participants = _participants_from_messages(message_dicts)
    db.upsert_thread(
        thread_ts=thread_ts,
        channel=channel,
        root_ts=root_ts,
        created_at=created_at,
        last_activity=last_activity,
        reply_count=reply_count,
        reaction_count=reaction_count,
        participants=participants,
    )


def get_thread_text(thread_ts: str) -> Tuple[str, List[Dict]]:
    messages = db.get_messages_for_thread(thread_ts)
    message_dicts = [dict(msg) for msg in messages]
    text_parts = []
    for msg in message_dicts:
        if msg.get("text"):
            text_parts.append(msg["text"])
    return "\n".join(text_parts), message_dicts
