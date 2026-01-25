import hmac
import hashlib
import json
import time

import pytest
import httpx
from fastapi.testclient import TestClient

from app import db
from app.ingest import verify_slack_signature
from app.main import app
from app.slack import exchange_code_for_token, store_workspace_token


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    db.reset_db()
    db_path = tmp_path / "test_slack.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    db.init_db()
    yield
    db.reset_db()


def _sign(secret: str, timestamp: str, body: bytes) -> str:
    base = f"v0:{timestamp}:{body.decode('utf-8')}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    return f"v0={digest}"


def test_signature_verification():
    secret = "secret"
    body = b"{\"type\":\"event_callback\"}"
    timestamp = str(int(time.time()))
    signature = _sign(secret, timestamp, body)
    assert verify_slack_signature(body, timestamp, signature, secret) is True
    assert verify_slack_signature(body, timestamp, "v0=bad", secret) is False


@pytest.mark.asyncio
async def test_oauth_token_storage(monkeypatch):
    monkeypatch.setenv("SLACK_CLIENT_ID", "client")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "secret")

    payload = {
        "ok": True,
        "access_token": "xoxb-test",
        "bot_user_id": "B123",
        "team": {"id": "T123"},
        "scope": "chat:write,channels:read",
    }

    def handler(request):
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await exchange_code_for_token("code", "http://redirect", http_client=client)
    store_workspace_token(response)
    row = db.fetch_slack_workspace("T123")
    assert row is not None
    assert row["access_token"] == "xoxb-test"


def test_url_verification_challenge(monkeypatch):
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "secret")
    payload = {"type": "url_verification", "challenge": "abc"}
    body = json.dumps(payload).encode("utf-8")
    timestamp = str(int(time.time()))
    signature = _sign("secret", timestamp, body)
    client = TestClient(app)
    response = client.post(
        "/slack/events",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Slack-Request-Timestamp": timestamp,
            "X-Slack-Signature": signature,
        },
    )
    assert response.status_code == 200
    assert response.json() == {"challenge": "abc"}
