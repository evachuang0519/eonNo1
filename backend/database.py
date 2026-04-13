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
        conn.run("COMMIT")
        print("[DB] Tables ready.")
    finally:
        conn.close()


# ── Write helpers ──────────────────────────────────────────────────────────────

def save_record(table_name: str, row_key: str, data: dict):
    """Insert or update a record in the local database (no sync/dirty flag)."""
    conn = get_conn()
    try:
        conn.run("""
            INSERT INTO records (table_name, row_key, data, synced_at, dirty)
            VALUES (:tn, :rk, CAST(:data AS JSONB), :ts, FALSE)
            ON CONFLICT (table_name, row_key) DO UPDATE
              SET data=CAST(EXCLUDED.data AS JSONB), synced_at=EXCLUDED.synced_at, dirty=FALSE
        """, tn=table_name, rk=row_key, data=json.dumps(data, ensure_ascii=False),
             ts=int(time.time() * 1000))
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

def get_records(table_name: str, search: str = "", limit: int = 0) -> list[dict]:
    conn = get_conn()
    try:
        lim_sql = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
        if search:
            rows = conn.run(
                "SELECT row_key, data FROM records WHERE table_name = :tn "
                f"AND data::text ILIKE :search ORDER BY ctid DESC{lim_sql}",
                tn=table_name, search=f"%{search}%"
            )
        else:
            rows = conn.run(
                f"SELECT row_key, data FROM records WHERE table_name = :tn ORDER BY ctid DESC{lim_sql}",
                tn=table_name
            )
        result = []
        for r in rows:
            rec = dict(r[1])
            rec['_key'] = r[0]
            result.append(rec)
        return result
    finally:
        conn.close()


def get_record(table_name: str, row_key: str) -> dict | None:
    conn = get_conn()
    try:
        rows = conn.run(
            "SELECT row_key, data FROM records WHERE table_name = :tn AND row_key = :rk",
            tn=table_name, rk=row_key
        )
        if rows:
            rec = dict(rows[0][1])
            rec['_key'] = rows[0][0]
            return rec
        return None
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
