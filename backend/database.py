"""
PostgreSQL operations via pg8000 (pure-Python driver, no compilation needed).
Uses JSONB for flexible schema-less row storage.
"""
import json
import time
import pg8000.native
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS


def get_conn():
    """Open a new PostgreSQL connection."""
    return pg8000.native.Connection(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def init_db():
    """Create tables if they don't exist."""
    conn = get_conn()
    try:
        conn.run("""
            CREATE TABLE IF NOT EXISTS records (
                table_name  TEXT    NOT NULL,
                row_key     TEXT    NOT NULL,
                data        JSONB   NOT NULL,
                synced_at   BIGINT,
                dirty       BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (table_name, row_key)
            )
        """)
        conn.run("""
            CREATE INDEX IF NOT EXISTS idx_records_table
                ON records (table_name)
        """)
        conn.run("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id          SERIAL  PRIMARY KEY,
                synced_at   BIGINT  NOT NULL,
                table_name  TEXT,
                rows_pulled INTEGER NOT NULL DEFAULT 0,
                error       TEXT
            )
        """)
        conn.run("COMMIT")
        print("[DB] Tables ready.")
    finally:
        conn.close()


# ── Write helpers ──────────────────────────────────────────────────────────────

def upsert_record(table_name: str, row_key: str, data: dict):
    """
    Insert or update a record from AppSheet sync.
    Does NOT overwrite data if the local record is dirty (pending write-back).
    """
    conn = get_conn()
    try:
        conn.run("""
            INSERT INTO records (table_name, row_key, data, synced_at, dirty)
            VALUES (:tn, :rk, CAST(:data AS JSONB), :ts, FALSE)
            ON CONFLICT (table_name, row_key) DO UPDATE
              SET data      = CASE WHEN records.dirty THEN records.data
                                   ELSE CAST(EXCLUDED.data AS JSONB) END,
                  synced_at = EXCLUDED.synced_at
        """, tn=table_name, rk=row_key, data=json.dumps(data, ensure_ascii=False),
             ts=int(time.time() * 1000))
        conn.run("COMMIT")
    finally:
        conn.close()


def mark_dirty(table_name: str, row_key: str, data: dict):
    """Save a locally-edited record (dirty = True, needs write-back to AppSheet)."""
    conn = get_conn()
    try:
        conn.run("""
            INSERT INTO records (table_name, row_key, data, synced_at, dirty)
            VALUES (:tn, :rk, CAST(:data AS JSONB), :ts, TRUE)
            ON CONFLICT (table_name, row_key) DO UPDATE
              SET data  = CAST(EXCLUDED.data AS JSONB),
                  dirty = TRUE
        """, tn=table_name, rk=row_key, data=json.dumps(data, ensure_ascii=False),
             ts=int(time.time() * 1000))
        conn.run("COMMIT")
    finally:
        conn.close()


def mark_clean(table_name: str, row_key: str):
    """Clear dirty flag after successful AppSheet write-back."""
    conn = get_conn()
    try:
        conn.run(
            "UPDATE records SET dirty = FALSE WHERE table_name = :tn AND row_key = :rk",
            tn=table_name, rk=row_key
        )
        conn.run("COMMIT")
    finally:
        conn.close()


def delete_record(table_name: str, row_key: str):
    conn = get_conn()
    try:
        conn.run(
            "DELETE FROM records WHERE table_name = :tn AND row_key = :rk",
            tn=table_name, rk=row_key
        )
        conn.run("COMMIT")
    finally:
        conn.close()


# ── Read helpers ───────────────────────────────────────────────────────────────

def get_records(table_name: str, search: str = "") -> list[dict]:
    conn = get_conn()
    try:
        if search:
            rows = conn.run(
                "SELECT data FROM records WHERE table_name = :tn "
                "AND data::text ILIKE :search ORDER BY ctid DESC",
                tn=table_name, search=f"%{search}%"
            )
        else:
            rows = conn.run(
                "SELECT data FROM records WHERE table_name = :tn ORDER BY ctid DESC",
                tn=table_name
            )
        return [r[0] for r in rows]
    finally:
        conn.close()


def get_record(table_name: str, row_key: str) -> dict | None:
    conn = get_conn()
    try:
        rows = conn.run(
            "SELECT data FROM records WHERE table_name = :tn AND row_key = :rk",
            tn=table_name, rk=row_key
        )
        return rows[0][0] if rows else None
    finally:
        conn.close()


def get_table_counts() -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.run(
            "SELECT table_name, COUNT(*) FROM records GROUP BY table_name"
        )
        return [{"table_name": r[0], "count": r[1]} for r in rows]
    finally:
        conn.close()


def get_sync_log(limit: int = 30) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.run(
            "SELECT id, synced_at, table_name, rows_pulled, error "
            "FROM sync_log ORDER BY synced_at DESC LIMIT :lim",
            lim=limit
        )
        return [
            {"id": r[0], "synced_at": r[1], "table_name": r[2],
             "rows_pulled": r[3], "error": r[4]}
            for r in rows
        ]
    finally:
        conn.close()


def log_sync(table_name: str, rows_pulled: int, error: str = None):
    conn = get_conn()
    try:
        conn.run(
            "INSERT INTO sync_log (synced_at, table_name, rows_pulled, error) "
            "VALUES (:ts, :tn, :rp, :err)",
            ts=int(time.time() * 1000), tn=table_name, rp=rows_pulled, err=error
        )
        conn.run("COMMIT")
    finally:
        conn.close()
