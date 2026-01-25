import os
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from app.ingest import ingest_payload, signature_verification_enabled, verify_slack_signature
from app.models import IngestResult, SlackEventPayload
from app.slack import build_install_url, exchange_code_for_token, store_workspace_token

router = APIRouter(prefix="/slack")


@router.get("/install")
async def slack_install(request: Request):
    redirect_uri = os.getenv("SLACK_REDIRECT_URI", str(request.url_for("slack_oauth_redirect")))
    return RedirectResponse(build_install_url(redirect_uri))


@router.get("/oauth_redirect", name="slack_oauth_redirect")
async def slack_oauth_redirect(code: str):
    redirect_uri = os.getenv("SLACK_REDIRECT_URI")
    if not redirect_uri:
        raise HTTPException(status_code=500, detail="SLACK_REDIRECT_URI not configured")
    try:
        oauth_payload = await exchange_code_for_token(code, redirect_uri)
        store_workspace_token(oauth_payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "installed"}


@router.post("/events")
async def slack_events(request: Request):
    raw_body = await request.body()
    request.scope["raw_body"] = raw_body
    if signature_verification_enabled():
        signature = request.headers.get("X-Slack-Signature", "")
        timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
        secret = os.getenv("SLACK_SIGNING_SECRET", "")
        if not secret:
            raise HTTPException(status_code=500, detail="Signing secret not configured")
        if not verify_slack_signature(raw_body, timestamp, signature, secret):
            raise HTTPException(status_code=401, detail="Invalid signature")
    payload: Dict[str, Any] = await request.json()
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge", "")}
    try:
        event_payload = SlackEventPayload.model_validate(payload)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid event payload")
    inserted, event_id = ingest_payload(event_payload)
    return IngestResult(status="queued" if inserted else "duplicate", event_id=event_id)
