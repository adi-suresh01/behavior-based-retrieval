import json
import uuid
from typing import Any, Dict, List

from app import db
from app.slack import slack_api_call


def _format_message(items: List[Dict[str, Any]]) -> str:
    lines = ["Daily Digest"]
    for item in items:
        why = item.get("why_shown", "")
        title = item.get("title") or "Untitled"
        lines.append(f"• {title} — {why}")
    return "\n".join(lines)


def _format_blocks(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Daily Digest*"},
        }
    ]
    for item in items:
        title = item.get("title") or "Untitled"
        why = item.get("why_shown", "")
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"• *{title}*\n_{why}_"},
            }
        )
    return blocks


async def deliver_digest(digest_id: str, team_id: str, user_id: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    existing = db.fetch_delivery_by_digest(digest_id)
    if existing is not None:
        return {"status": "duplicate", "delivery_id": existing["delivery_id"]}

    message = _format_message(items)
    blocks = _format_blocks(items)
    delivery_id = f"del-{uuid.uuid4().hex}"
    try:
        # Open or fetch DM channel
        open_resp = await slack_api_call(team_id, "conversations.open", {"users": user_id})
        channel_id = open_resp.get("channel", {}).get("id")
        resp = await slack_api_call(
            team_id,
            "chat.postMessage",
            {"channel": channel_id, "text": message, "blocks": json.dumps(blocks)},
        )
        slack_ts = resp.get("ts")
        db.insert_delivery(delivery_id, digest_id, team_id, user_id, "delivered", slack_ts, None)
        return {"status": "delivered", "delivery_id": delivery_id, "slack_ts": slack_ts}
    except Exception as exc:
        db.insert_delivery(delivery_id, digest_id, team_id, user_id, "failed", None, str(exc))
        return {"status": "failed", "delivery_id": delivery_id}
