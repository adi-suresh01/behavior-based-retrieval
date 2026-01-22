import hashlib
import hmac
import os
import time
from typing import Tuple

from fastapi import HTTPException, Request

from app import db
from app.models import SlackEventPayload
from app.queueing import route_job


def verify_slack_signature(
    body: bytes,
    timestamp: str,
    signature: str,
    secret: str,
) -> bool:
    if not timestamp or not signature:
        return False
    try:
        ts_int = int(timestamp)
    except ValueError:
        return False
    if abs(time.time() - ts_int) > 60 * 5:
        return False
    base = f"v0:{timestamp}:{body.decode('utf-8')}".encode("utf-8")
    expected = "v0=" + hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def signature_verification_enabled() -> bool:
    value = os.getenv("SLACK_VERIFY_SIGNATURE", "true").lower()
    return value not in {"0", "false", "no"}


def ingest_payload(payload: SlackEventPayload) -> Tuple[bool, str]:
    if not db.insert_dedupe(payload.event_id):
        return False, payload.event_id
    db.insert_raw_event(payload.event_id, payload.model_dump())
    route_job(payload)
    return True, payload.event_id


def handle_slack_event(request: Request, payload: SlackEventPayload) -> Tuple[bool, str]:
    if signature_verification_enabled():
        raw_body = request.scope.get("raw_body")
        if raw_body is None:
            raise HTTPException(status_code=400, detail="Missing raw body")
        signature = request.headers.get("X-Slack-Signature", "")
        timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
        secret = os.getenv("SLACK_SIGNING_SECRET", "")
        if not secret:
            raise HTTPException(status_code=500, detail="Signing secret not configured")
        if not verify_slack_signature(raw_body, timestamp, signature, secret):
            raise HTTPException(status_code=401, detail="Invalid signature")
    return ingest_payload(payload)
