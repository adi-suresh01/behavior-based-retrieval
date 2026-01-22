import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Optional

_DB_LOCK = threading.Lock()
_DB_CONN: Optional[sqlite3.Connection] = None


def get_db_path() -> str:
    return os.getenv("DATABASE_PATH", "./app.db")


def get_db() -> sqlite3.Connection:
    global _DB_CONN
    if _DB_CONN is None:
        _DB_CONN = sqlite3.connect(get_db_path(), check_same_thread=False)
        _DB_CONN.row_factory = sqlite3.Row
    return _DB_CONN


def reset_db() -> None:
    global _DB_CONN
    if _DB_CONN is not None:
        _DB_CONN.close()
        _DB_CONN = None


@contextmanager
def db_cursor():
    conn = get_db()
    with _DB_LOCK:
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        finally:
            cur.close()


def init_db() -> None:
    schema_statements = [
        """
        CREATE TABLE IF NOT EXISTS dedupe_events (
            event_id TEXT PRIMARY KEY,
            received_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_events (
            event_id TEXT PRIMARY KEY,
            received_at REAL,
            payload_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS messages (
            channel TEXT NOT NULL,
            ts TEXT NOT NULL,
            thread_ts TEXT NOT NULL,
            user TEXT,
            text TEXT,
            reactions_json TEXT,
            created_at REAL,
            PRIMARY KEY (channel, ts)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS threads (
            thread_ts TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            root_ts TEXT NOT NULL,
            created_at REAL,
            last_activity REAL,
            reply_count INTEGER,
            reaction_count INTEGER,
            participants_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS digest_items (
            thread_ts TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            title TEXT,
            labels_json TEXT,
            entities_json TEXT,
            urgency REAL,
            summary TEXT,
            updated_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS embeddings (
            thread_ts TEXT PRIMARY KEY,
            dim INTEGER,
            vector_json TEXT,
            updated_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS job_metrics (
            queue_name TEXT PRIMARY KEY,
            processed_count INTEGER,
            last_processed_at REAL
        )
        """,
    ]
    with db_cursor() as cur:
        for stmt in schema_statements:
            cur.execute(stmt)


def insert_raw_event(event_id: str, payload: Dict[str, Any]) -> None:
    with db_cursor() as cur:
        cur.execute(
            "INSERT OR REPLACE INTO raw_events(event_id, received_at, payload_json) VALUES (?, ?, ?)",
            (event_id, time.time(), json.dumps(payload)),
        )


def insert_dedupe(event_id: str) -> bool:
    with db_cursor() as cur:
        cur.execute(
            "INSERT OR IGNORE INTO dedupe_events(event_id, received_at) VALUES (?, ?)",
            (event_id, time.time()),
        )
        return cur.rowcount == 1


def insert_message(
    channel: str,
    ts: str,
    thread_ts: str,
    user: Optional[str],
    text: Optional[str],
    reactions_json: Optional[str],
) -> bool:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT OR IGNORE INTO messages
            (channel, ts, thread_ts, user, text, reactions_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (channel, ts, thread_ts, user, text, reactions_json, time.time()),
        )
        return cur.rowcount == 1


def get_messages_for_thread(thread_ts: str) -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM messages WHERE thread_ts = ? ORDER BY CAST(ts AS REAL) ASC",
            (thread_ts,),
        )
        rows = cur.fetchall()
    return rows


def get_thread(thread_ts: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM threads WHERE thread_ts = ?", (thread_ts,))
        return cur.fetchone()


def upsert_thread(
    thread_ts: str,
    channel: str,
    root_ts: str,
    created_at: float,
    last_activity: float,
    reply_count: int,
    reaction_count: int,
    participants: Iterable[str],
) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO threads
            (thread_ts, channel, root_ts, created_at, last_activity, reply_count, reaction_count, participants_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_ts) DO UPDATE SET
                last_activity=excluded.last_activity,
                reply_count=excluded.reply_count,
                reaction_count=excluded.reaction_count,
                participants_json=excluded.participants_json
            """,
            (
                thread_ts,
                channel,
                root_ts,
                created_at,
                last_activity,
                reply_count,
                reaction_count,
                json.dumps(sorted(set(participants))),
            ),
        )


def upsert_digest_item(
    thread_ts: str,
    channel: str,
    title: str,
    labels: Iterable[str],
    entities: Dict[str, Any],
    urgency: float,
    summary: str,
) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO digest_items
            (thread_ts, channel, title, labels_json, entities_json, urgency, summary, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_ts) DO UPDATE SET
                title=excluded.title,
                labels_json=excluded.labels_json,
                entities_json=excluded.entities_json,
                urgency=excluded.urgency,
                summary=excluded.summary,
                updated_at=excluded.updated_at
            """,
            (
                thread_ts,
                channel,
                title,
                json.dumps(sorted(set(labels))),
                json.dumps(entities),
                urgency,
                summary,
                time.time(),
            ),
        )


def upsert_embedding(thread_ts: str, dim: int, vector: Iterable[float]) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO embeddings(thread_ts, dim, vector_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(thread_ts) DO UPDATE SET
                dim=excluded.dim,
                vector_json=excluded.vector_json,
                updated_at=excluded.updated_at
            """,
            (thread_ts, dim, json.dumps(list(vector)), time.time()),
        )


def increment_metric(queue_name: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_metrics(queue_name, processed_count, last_processed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(queue_name) DO UPDATE SET
                processed_count=processed_count + 1,
                last_processed_at=excluded.last_processed_at
            """,
            (queue_name, 1, time.time()),
        )


def fetch_raw_events(limit: int) -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM raw_events ORDER BY received_at DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()


def fetch_threads(limit: int) -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM threads ORDER BY last_activity DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()


def fetch_items(limit: int) -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM digest_items ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()


def fetch_embedding(thread_ts: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM embeddings WHERE thread_ts = ?", (thread_ts,))
        return cur.fetchone()


def fetch_metrics() -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM job_metrics")
        return cur.fetchall()
