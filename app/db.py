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
            is_deleted INTEGER DEFAULT 0,
            edited_at REAL,
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
        """
        CREATE TABLE IF NOT EXISTS roles (
            role_id TEXT PRIMARY KEY,
            name TEXT,
            description TEXT,
            role_vector_json TEXT,
            updated_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS phases (
            phase_key TEXT PRIMARY KEY,
            description TEXT,
            phase_vector_json TEXT,
            updated_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS projects (
            project_id TEXT PRIMARY KEY,
            name TEXT,
            current_phase TEXT,
            channels_json TEXT,
            created_at REAL,
            updated_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            role_id TEXT,
            user_vector_json TEXT,
            created_at REAL,
            updated_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_project (
            user_id TEXT,
            project_id TEXT,
            PRIMARY KEY (user_id, project_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS digests (
            digest_id TEXT PRIMARY KEY,
            user_id TEXT,
            project_id TEXT,
            created_at REAL,
            items_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS interactions (
            interaction_id TEXT PRIMARY KEY,
            user_id TEXT,
            project_id TEXT,
            thread_ts TEXT,
            action TEXT,
            created_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS slack_workspaces (
            team_id TEXT PRIMARY KEY,
            access_token TEXT,
            bot_user_id TEXT,
            installed_at REAL,
            scopes_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS project_channels (
            project_id TEXT,
            channel_id TEXT,
            PRIMARY KEY (project_id, channel_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_channels (
            user_id TEXT,
            channel_id TEXT,
            PRIMARY KEY (user_id, channel_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS digest_schedules (
            schedule_id TEXT PRIMARY KEY,
            team_id TEXT,
            project_id TEXT,
            user_id TEXT,
            cron_json TEXT,
            is_enabled INTEGER,
            created_at REAL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS digest_deliveries (
            delivery_id TEXT PRIMARY KEY,
            digest_id TEXT,
            team_id TEXT,
            user_id TEXT,
            delivered_at REAL,
            status TEXT,
            slack_ts TEXT,
            error TEXT
        )
        """,
    ]
    with db_cursor() as cur:
        for stmt in schema_statements:
            cur.execute(stmt)
        _ensure_messages_columns(cur)


def _ensure_messages_columns(cur: sqlite3.Cursor) -> None:
    cur.execute("PRAGMA table_info(messages)")
    existing = {row[1] for row in cur.fetchall()}
    if "is_deleted" not in existing:
        cur.execute("ALTER TABLE messages ADD COLUMN is_deleted INTEGER DEFAULT 0")
    if "edited_at" not in existing:
        cur.execute("ALTER TABLE messages ADD COLUMN edited_at REAL")


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
            (channel, ts, thread_ts, user, text, reactions_json, is_deleted, edited_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (channel, ts, thread_ts, user, text, reactions_json, 0, None, time.time()),
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


def fetch_message(channel: str, ts: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM messages WHERE channel = ? AND ts = ?", (channel, ts))
        return cur.fetchone()


def update_message_text(channel: str, ts: str, text: Optional[str]) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE messages SET text = ?, edited_at = ?, is_deleted = 0 WHERE channel = ? AND ts = ?
            """,
            (text, time.time(), channel, ts),
        )


def mark_message_deleted(channel: str, ts: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE messages SET is_deleted = 1, edited_at = ? WHERE channel = ? AND ts = ?
            """,
            (time.time(), channel, ts),
        )


def update_message_reactions(channel: str, ts: str, reaction: str, delta: int) -> None:
    row = fetch_message(channel, ts)
    if row is None:
        return
    reactions_json = row["reactions_json"] or "[]"
    try:
        reactions = json.loads(reactions_json)
    except json.JSONDecodeError:
        reactions = []
    updated = False
    for entry in reactions:
        if entry.get("name") == reaction:
            entry["count"] = max(0, int(entry.get("count", 0)) + delta)
            updated = True
            break
    if not updated and delta > 0:
        reactions.append({"name": reaction, "count": 1})
    reactions = [r for r in reactions if int(r.get("count", 0)) > 0]
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE messages SET reactions_json = ? WHERE channel = ? AND ts = ?
            """,
            (json.dumps(reactions), channel, ts),
        )


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


def upsert_role(role_id: str, name: str, description: str, role_vector: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO roles(role_id, name, description, role_vector_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(role_id) DO UPDATE SET
                name=excluded.name,
                description=excluded.description,
                role_vector_json=excluded.role_vector_json,
                updated_at=excluded.updated_at
            """,
            (role_id, name, description, role_vector, time.time()),
        )


def fetch_role(role_id: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM roles WHERE role_id = ?", (role_id,))
        return cur.fetchone()


def upsert_phase(phase_key: str, description: str, phase_vector: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO phases(phase_key, description, phase_vector_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(phase_key) DO UPDATE SET
                description=excluded.description,
                phase_vector_json=excluded.phase_vector_json,
                updated_at=excluded.updated_at
            """,
            (phase_key, description, phase_vector, time.time()),
        )


def fetch_phase(phase_key: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM phases WHERE phase_key = ?", (phase_key,))
        return cur.fetchone()


def upsert_project(project_id: str, name: str, current_phase: str, channels_json: str) -> None:
    now = time.time()
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO projects(project_id, name, current_phase, channels_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                name=excluded.name,
                current_phase=excluded.current_phase,
                channels_json=excluded.channels_json,
                updated_at=excluded.updated_at
            """,
            (project_id, name, current_phase, channels_json, now, now),
        )


def update_project_phase(project_id: str, phase_key: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE projects SET current_phase = ?, updated_at = ? WHERE project_id = ?
            """,
            (phase_key, time.time(), project_id),
        )


def fetch_project(project_id: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM projects WHERE project_id = ?", (project_id,))
        return cur.fetchone()


def upsert_user(user_id: str, name: str, email: Optional[str], role_id: Optional[str], user_vector: Optional[str]) -> None:
    now = time.time()
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO users(user_id, name, email, role_id, user_vector_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                name=excluded.name,
                email=excluded.email,
                role_id=excluded.role_id,
                user_vector_json=excluded.user_vector_json,
                updated_at=excluded.updated_at
            """,
            (user_id, name, email, role_id, user_vector, now, now),
        )


def update_user_role(user_id: str, role_id: str, user_vector: Optional[str]) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE users SET role_id = ?, user_vector_json = ?, updated_at = ? WHERE user_id = ?
            """,
            (role_id, user_vector, time.time(), user_id),
        )


def fetch_user(user_id: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()


def add_user_project(user_id: str, project_id: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT OR IGNORE INTO user_project(user_id, project_id) VALUES (?, ?)
            """,
            (user_id, project_id),
        )


def fetch_user_projects(user_id: str) -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT p.* FROM projects p
            JOIN user_project up ON up.project_id = p.project_id
            WHERE up.user_id = ?
            """,
            (user_id,),
        )
        return cur.fetchall()


def insert_digest(digest_id: str, user_id: str, project_id: str, items_json: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO digests(digest_id, user_id, project_id, created_at, items_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (digest_id, user_id, project_id, time.time(), items_json),
        )


def fetch_digest(digest_id: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM digests WHERE digest_id = ?", (digest_id,))
        return cur.fetchone()


def insert_interaction(
    interaction_id: str,
    user_id: str,
    project_id: str,
    thread_ts: str,
    action: str,
) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO interactions(interaction_id, user_id, project_id, thread_ts, action, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (interaction_id, user_id, project_id, thread_ts, action, time.time()),
        )


def update_user_vector(user_id: str, user_vector_json: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE users SET user_vector_json = ?, updated_at = ? WHERE user_id = ?
            """,
            (user_vector_json, time.time(), user_id),
        )


def upsert_slack_workspace(
    team_id: str,
    access_token: str,
    bot_user_id: str,
    scopes_json: str,
) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO slack_workspaces(team_id, access_token, bot_user_id, installed_at, scopes_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                access_token=excluded.access_token,
                bot_user_id=excluded.bot_user_id,
                installed_at=excluded.installed_at,
                scopes_json=excluded.scopes_json
            """,
            (team_id, access_token, bot_user_id, time.time(), scopes_json),
        )


def fetch_slack_workspace(team_id: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM slack_workspaces WHERE team_id = ?", (team_id,))
        return cur.fetchone()


def add_project_channel(project_id: str, channel_id: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT OR IGNORE INTO project_channels(project_id, channel_id) VALUES (?, ?)
            """,
            (project_id, channel_id),
        )


def add_user_channel(user_id: str, channel_id: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT OR IGNORE INTO user_channels(user_id, channel_id) VALUES (?, ?)
            """,
            (user_id, channel_id),
        )


def fetch_project_channels(project_id: str) -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT channel_id FROM project_channels WHERE project_id = ?
            """,
            (project_id,),
        )
        return cur.fetchall()


def fetch_user_channels(user_id: str) -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT channel_id FROM user_channels WHERE user_id = ?
            """,
            (user_id,),
        )
        return cur.fetchall()


def insert_schedule(
    schedule_id: str,
    team_id: str,
    project_id: str,
    user_id: str,
    cron_json: str,
    is_enabled: int,
) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO digest_schedules(schedule_id, team_id, project_id, user_id, cron_json, is_enabled, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (schedule_id, team_id, project_id, user_id, cron_json, is_enabled, time.time()),
        )


def fetch_schedules() -> Iterable[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM digest_schedules")
        return cur.fetchall()


def fetch_delivery_by_digest(digest_id: str) -> Optional[sqlite3.Row]:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM digest_deliveries WHERE digest_id = ?", (digest_id,))
        return cur.fetchone()


def insert_delivery(
    delivery_id: str,
    digest_id: str,
    team_id: str,
    user_id: str,
    status: str,
    slack_ts: Optional[str],
    error: Optional[str],
) -> None:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO digest_deliveries(delivery_id, digest_id, team_id, user_id, delivered_at, status, slack_ts, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (delivery_id, digest_id, team_id, user_id, time.time(), status, slack_ts, error),
        )


def fetch_latest_delivery_for_schedule(team_id: str, project_id: str, user_id: str, now_utc: float, tz_name: str) -> Optional[sqlite3.Row]:
    # Find latest delivery for user/project/team by digest join on digests table
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT dd.* FROM digest_deliveries dd
            JOIN digests d ON d.digest_id = dd.digest_id
            WHERE dd.team_id = ? AND dd.user_id = ? AND d.project_id = ?
            ORDER BY dd.delivered_at DESC
            LIMIT 1
            """,
            (team_id, user_id, project_id),
        )
        return cur.fetchone()
