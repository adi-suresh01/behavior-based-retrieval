import asyncio
import json
import os
from typing import Dict, List

from app import db
from app.sim.client import SimClient
from app.sim.dataset import SimClock, get_scenario_events


def _dot(a: List[float], b: List[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _format_digest(items: List[Dict]) -> str:
    lines = []
    for idx, item in enumerate(items, start=1):
        why = item.get("why_shown", "")
        score = item.get("score_breakdown", {}).get("final_score", 0.0)
        lines.append(f"{idx}. {item.get('title')} | why: {why} | score: {score:.3f}")
    return "\n".join(lines)


def _diff(before: List[Dict], after: List[Dict]) -> List[str]:
    before_rank = {item["title"]: idx for idx, item in enumerate(before, start=1)}
    after_rank = {item["title"]: idx for idx, item in enumerate(after, start=1)}
    titles = [item["title"] for item in after]
    diffs = []
    for title in titles:
        if title in before_rank:
            diffs.append(f"- {title} (rank {before_rank[title]} -> {after_rank[title]})")
    return diffs


def _find_thread(items: List[Dict], contains: str) -> str:
    for item in items:
        summary = item.get("summary") or ""
        if contains.lower() in summary.lower():
            return item["thread_ts"]
    raise RuntimeError("thread not found")


async def main() -> None:
    base_url = os.getenv("SIM_BASE_URL", "http://localhost:8000")
    client = SimClient(base_url)

    # Setup roles, phases, project, users, channels
    await client.post(
        "/roles",
        {"role_id": "role-me", "name": "ME", "description": "materials structures weight manufacturability"},
    )
    await client.post(
        "/roles",
        {"role_id": "role-supply", "name": "Supply", "description": "vendors lead times MOQ sourcing risk"},
    )
    await client.post(
        "/roles",
        {"role_id": "role-pm", "name": "PM", "description": "timeline decisions owners milestones"},
    )

    await client.post(
        "/phases",
        {"phase_key": "EVT", "description": "early prototype build, unblock near-term decisions"},
    )
    await client.post(
        "/phases",
        {"phase_key": "DVT", "description": "validation testing focus, reliability risks"},
    )

    await client.post(
        "/projects",
        {"project_id": "proj-drone", "name": "DroneV2", "current_phase": "EVT"},
    )
    await client.post("/projects/proj-drone/channels", {"channel_id": "C_DRONE_STRUCT"})
    await client.post("/projects/proj-drone/channels", {"channel_id": "C_DRONE_SUPPLY"})

    await client.post(
        "/users",
        {"user_id": "U_MAYA", "name": "Maya", "role_id": "role-me"},
    )
    await client.post(
        "/users",
        {"user_id": "U_SAM", "name": "Sam", "role_id": "role-supply"},
    )
    await client.post(
        "/users",
        {"user_id": "U_PRIYA", "name": "Priya", "role_id": "role-pm"},
    )

    for user_id in ["U_MAYA", "U_SAM", "U_PRIYA"]:
        await client.post(f"/users/{user_id}/channels", {"channel_id": "C_DRONE_STRUCT"})
        await client.post(f"/users/{user_id}/channels", {"channel_id": "C_DRONE_SUPPLY"})

    # Start simulator
    await client.post(
        "/simulate/start",
        {"scenario_id": "carbon_fiber_demo", "speed_multiplier": 5},
    )

    target_events = len(get_scenario_events("carbon_fiber_demo", SimClock()))
    while True:
        status = await client.get("/simulate/status")
        if status["emitted_count"] >= target_events and sum(status["queue_sizes"].values()) == 0:
            break
        await asyncio.sleep(0.5)

    await client.post("/simulate/stop", {})

    # Digests per user
    digest_me = await client.get("/digest", {"user_id": "U_MAYA", "project_id": "proj-drone", "n": 5})
    digest_supply = await client.get("/digest", {"user_id": "U_SAM", "project_id": "proj-drone", "n": 5})
    digest_pm = await client.get("/digest", {"user_id": "U_PRIYA", "project_id": "proj-drone", "n": 5})

    print("=== Digest: EVT (U_MAYA) ===")
    print(_format_digest(digest_me["items"]))
    print("\n=== Digest: EVT (U_SAM) ===")
    print(_format_digest(digest_supply["items"]))
    print("\n=== Digest: EVT (U_PRIYA) ===")
    print(_format_digest(digest_pm["items"]))

    # Phase change
    await client.post("/projects/proj-drone/phase", {"phase_key": "DVT"})
    digest_me_dvt = await client.get("/digest", {"user_id": "U_MAYA", "project_id": "proj-drone", "n": 5})
    digest_supply_dvt = await client.get("/digest", {"user_id": "U_SAM", "project_id": "proj-drone", "n": 5})
    print("\n=== Phase Change: EVT -> DVT ===")
    print("U_MAYA")
    print("\n".join(_diff(digest_me["items"], digest_me_dvt["items"])))
    print("U_SAM")
    print("\n".join(_diff(digest_supply["items"], digest_supply_dvt["items"])))

    # Feedback learning for U_SAM
    items = await client.get("/items", {"limit": 20})
    supply_thread = _find_thread(items, "Vendor A lead time")
    rf_thread = _find_thread(items, "RF test risk")

    db.init_db()
    user_before = db.fetch_user("U_SAM")
    v_pos = db.fetch_embedding(supply_thread)
    v_neg = db.fetch_embedding(rf_thread)

    u_before = json.loads(user_before["user_vector_json"])
    dot_pos_before = _dot(u_before, json.loads(v_pos["vector_json"]))
    dot_neg_before = _dot(u_before, json.loads(v_neg["vector_json"]))

    await client.post(
        "/feedback",
        {"user_id": "U_SAM", "project_id": "proj-drone", "thread_ts": supply_thread, "action": "thumbs_up"},
    )
    await client.post(
        "/feedback",
        {"user_id": "U_SAM", "project_id": "proj-drone", "thread_ts": rf_thread, "action": "dismiss"},
    )

    user_after = db.fetch_user("U_SAM")
    u_after = json.loads(user_after["user_vector_json"])
    dot_pos_after = _dot(u_after, json.loads(v_pos["vector_json"]))
    dot_neg_after = _dot(u_after, json.loads(v_neg["vector_json"]))

    print("\n=== Feedback Learning ===")
    print(f"U_SAM dot(v_pos) before: {dot_pos_before:.3f} after: {dot_pos_after:.3f} ({dot_pos_after - dot_pos_before:+.3f})")
    print(f"U_SAM dot(v_neg) before: {dot_neg_before:.3f} after: {dot_neg_after:.3f} ({dot_neg_after - dot_neg_before:+.3f})")


if __name__ == "__main__":
    asyncio.run(main())
