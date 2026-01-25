import json
import os
from typing import Dict, Optional

import httpx

from app import db

SLACK_OAUTH_URL = "https://slack.com/oauth/v2/authorize"
SLACK_TOKEN_URL = "https://slack.com/api/oauth.v2.access"


def build_install_url(redirect_uri: str) -> str:
    client_id = os.getenv("SLACK_CLIENT_ID", "")
    scopes = os.getenv("SLACK_OAUTH_SCOPES", "commands,chat:write,channels:read")
    return (
        f"{SLACK_OAUTH_URL}?client_id={client_id}"
        f"&scope={scopes}&redirect_uri={redirect_uri}"
    )


async def exchange_code_for_token(code: str, redirect_uri: str, http_client: Optional[httpx.AsyncClient] = None) -> Dict:
    client_id = os.getenv("SLACK_CLIENT_ID", "")
    client_secret = os.getenv("SLACK_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise ValueError("missing_client_config")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
    }
    close_client = False
    if http_client is None:
        http_client = httpx.AsyncClient(timeout=10)
        close_client = True
    try:
        response = await http_client.post(SLACK_TOKEN_URL, data=payload)
        response.raise_for_status()
        data = response.json()
    finally:
        if close_client:
            await http_client.aclose()
    if not data.get("ok"):
        raise ValueError("oauth_failed")
    return data


def store_workspace_token(oauth_payload: Dict) -> None:
    team_id = oauth_payload.get("team", {}).get("id")
    access_token = oauth_payload.get("access_token")
    bot_user_id = oauth_payload.get("bot_user_id")
    scopes = oauth_payload.get("scope", "")
    if not team_id or not access_token:
        raise ValueError("invalid_oauth_payload")
    scopes_json = json.dumps(scopes.split(",") if scopes else [])
    db.upsert_slack_workspace(team_id, access_token, bot_user_id or "", scopes_json)


async def slack_api_call(team_id: str, method: str, params: Optional[Dict] = None) -> Dict:
    workspace = db.fetch_slack_workspace(team_id)
    if workspace is None:
        raise ValueError("workspace_not_found")
    token = workspace["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(f"https://slack.com/api/{method}", headers=headers, data=params or {})
        response.raise_for_status()
        return response.json()
