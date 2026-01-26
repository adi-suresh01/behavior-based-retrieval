import asyncio
import json
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app import db
from app.digest import build_digest
from app.delivery import deliver_digest

CHECK_INTERVAL_SECONDS = 60


def _is_due(schedule_row, now_utc: float) -> bool:
    cron = json.loads(schedule_row["cron_json"])
    time_of_day = cron.get("time_of_day", "09:00")
    tz_name = cron.get("timezone", "UTC")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    dt = datetime.fromtimestamp(now_utc, tz=timezone.utc).astimezone(tz)
    now = dt.strftime("%H:%M")
    if now != time_of_day:
        return False
    last_delivery = db.fetch_latest_delivery_for_schedule(schedule_row["team_id"], schedule_row["project_id"], schedule_row["user_id"], now_utc, tz_name)
    return last_delivery is None


async def scheduler_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        schedules = db.fetch_schedules()
        now_utc = time.time()
        for schedule in schedules:
            if not schedule["is_enabled"]:
                continue
            if _is_due(schedule, now_utc):
                digest = build_digest(schedule["user_id"], schedule["project_id"], n=10)
                await deliver_digest(digest["digest_id"], schedule["team_id"], schedule["user_id"], digest["items"])
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=CHECK_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            continue
