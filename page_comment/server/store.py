"""会话持久化: SQLite 存储层"""
import os
import sqlite3
import uuid
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), "page_comment.db")

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    page_key TEXT NOT NULL,
    page_url TEXT,
    cli_session_id TEXT,
    title TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    parent_id TEXT REFERENCES messages(id),
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    selected_text TEXT,
    chart_info TEXT,
    edits_json TEXT,
    edit_success INTEGER,
    cli_thread_id TEXT,
    created_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_page_key ON sessions(page_key);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);
"""


def _now():
    return datetime.now(timezone.utc).isoformat()


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    with _connect() as conn:
        conn.executescript(_CREATE_SQL)


def get_active_session(page_key: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM sessions WHERE page_key=? AND is_active=1 ORDER BY created_at DESC LIMIT 1",
            (page_key,),
        ).fetchone()
        return dict(row) if row else None


def get_session(session_id: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM sessions WHERE id=?",
            (session_id,),
        ).fetchone()
        return dict(row) if row else None


def create_session(page_key: str, page_url: str = None, cli_session_id: str = None) -> dict:
    now = _now()
    session = {
        "id": str(uuid.uuid4()),
        "page_key": page_key,
        "page_url": page_url,
        "cli_session_id": cli_session_id,
        "title": None,
        "is_active": 1,
        "created_at": now,
        "updated_at": now,
    }
    with _connect() as conn:
        conn.execute(
            "INSERT INTO sessions (id, page_key, page_url, cli_session_id, title, is_active, created_at, updated_at) "
            "VALUES (:id, :page_key, :page_url, :cli_session_id, :title, :is_active, :created_at, :updated_at)",
            session,
        )
    return session


def deactivate_sessions(page_key: str):
    with _connect() as conn:
        conn.execute(
            "UPDATE sessions SET is_active=0, updated_at=? WHERE page_key=? AND is_active=1",
            (_now(), page_key),
        )


def update_session(session_id: str, **kwargs):
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [session_id]
    with _connect() as conn:
        conn.execute(f"UPDATE sessions SET {sets} WHERE id=?", vals)


def add_message(
    session_id: str,
    role: str,
    content: str,
    selected_text: str = None,
    chart_info: str = None,
    parent_id: str = None,
    edits_json: str = None,
    edit_success: int = None,
    cli_thread_id: str = None,
) -> dict:
    msg = {
        "id": str(uuid.uuid4()),
        "session_id": session_id,
        "parent_id": parent_id,
        "role": role,
        "content": content,
        "selected_text": selected_text,
        "chart_info": chart_info,
        "edits_json": edits_json,
        "edit_success": edit_success,
        "cli_thread_id": cli_thread_id,
        "created_at": _now(),
    }
    with _connect() as conn:
        conn.execute(
            "INSERT INTO messages (id, session_id, parent_id, role, content, selected_text, "
            "chart_info, edits_json, edit_success, cli_thread_id, created_at) "
            "VALUES (:id, :session_id, :parent_id, :role, :content, :selected_text, "
            ":chart_info, :edits_json, :edit_success, :cli_thread_id, :created_at)",
            msg,
        )
    return msg


def get_session_messages(session_id: str) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM messages WHERE session_id=? ORDER BY created_at",
            (session_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_sessions_for_page(page_key: str) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM sessions WHERE page_key=? ORDER BY created_at DESC",
            (page_key,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_history(page_key: str) -> list[dict]:
    """获取某个页面的所有会话及其消息"""
    sessions = get_sessions_for_page(page_key)
    for s in sessions:
        s["messages"] = get_session_messages(s["id"])
    return sessions


def get_thread_id(message_id: str) -> str | None:
    """获取某条消息所在回复串的 cli_thread_id"""
    with _connect() as conn:
        row = conn.execute("SELECT cli_thread_id FROM messages WHERE id=?", (message_id,)).fetchone()
        return row["cli_thread_id"] if row else None


# 启动时初始化
init_db()
