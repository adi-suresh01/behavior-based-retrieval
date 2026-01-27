import json
from typing import Dict, Optional

import httpx


class SimClient:
    def __init__(self, base_url: str = "http://localhost:8000") -> None:
        self.base_url = base_url.rstrip("/")

    async def send_event(self, event: Dict) -> Dict:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(f"{self.base_url}/sim/events", json=event)
            response.raise_for_status()
            return response.json()

    async def post(self, path: str, payload: Dict) -> Dict:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(f"{self.base_url}{path}", json=payload)
            response.raise_for_status()
            return response.json()

    async def get(self, path: str, params: Optional[Dict] = None) -> Dict:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(f"{self.base_url}{path}", params=params)
            response.raise_for_status()
            return response.json()
