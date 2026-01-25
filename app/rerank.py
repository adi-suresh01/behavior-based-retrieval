import os
import time
from typing import Any, Dict, Iterable, List

import numpy as np

from app import db
from app.retrieval import cosine_sim


def _recency_score(updated_at: float, now: float, window_seconds: float) -> float:
    if window_seconds <= 0:
        return 0.0
    age = now - updated_at
    if age <= 0:
        return 1.0
    if age >= window_seconds:
        return 0.0
    return 1.0 - (age / window_seconds)


def _ownership_score(thread_ts: str, user_id: str) -> float:
    messages = db.get_messages_for_thread(thread_ts)
    mention = f"<@{user_id}>"
    for msg in messages:
        if msg["user"] == user_id:
            return 1.0
        text = msg["text"] or ""
        if mention in text:
            return 1.0
    return 0.0


def _base_score(sim: float, urgency: float, ownership: float, recency: float) -> float:
    return 0.55 * sim + 0.20 * urgency + 0.15 * ownership + 0.10 * recency


def rerank_candidates(
    candidates: Iterable[Dict[str, Any]],
    user_id: str,
    n: int = 10,
    lambda_diversity: float = 0.2,
) -> List[Dict[str, Any]]:
    window_hours = float(os.getenv("RETRIEVAL_WINDOW_HOURS", "24"))
    window_seconds = window_hours * 3600
    now = time.time()

    enriched = []
    for candidate in candidates:
        recency = _recency_score(candidate["updated_at"], now, window_seconds)
        ownership = _ownership_score(candidate["thread_ts"], user_id)
        base_score = _base_score(candidate["sim_score"], candidate["urgency"], ownership, recency)
        enriched.append(
            {
                **candidate,
                "recency": recency,
                "ownership": ownership,
                "base_score": base_score,
                "force_included": False,
                "diversity_penalty": 0.0,
                "final_score": base_score,
            }
        )

    must_include = [
        c
        for c in enriched
        if ("BLOCKER" in c["labels"] or "DECISION" in c["labels"]) and c["urgency"] >= 0.8
    ]

    selected: List[Dict[str, Any]] = []
    if must_include:
        must_include.sort(
            key=lambda item: (
                -item["base_score"],
                -item["urgency"],
                -item["updated_at"],
                item["thread_ts"],
            )
        )
        forced = must_include[0]
        forced["force_included"] = True
        selected.append(forced)

    remaining = [c for c in enriched if c["thread_ts"] not in {s["thread_ts"] for s in selected}]

    while remaining and len(selected) < n:
        for candidate in remaining:
            if not selected:
                max_sim = 0.0
            else:
                sims = [cosine_sim(candidate["vector"], sel["vector"]) for sel in selected]
                max_sim = max(sims)
            penalty = lambda_diversity * max_sim
            candidate["diversity_penalty"] = penalty
            candidate["final_score"] = candidate["base_score"] - penalty
        remaining.sort(
            key=lambda item: (
                -item["final_score"],
                -item["base_score"],
                -item["urgency"],
                -item["updated_at"],
                item["thread_ts"],
            )
        )
        selected.append(remaining.pop(0))

    return selected
