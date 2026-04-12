"""
Sync engine: pulls all AppSheet tables into PostgreSQL.
Respects dirty flag — local edits are not overwritten by remote sync.
"""
import time
import logging
from config import TABLE_CONFIG
from appsheet_client import find_all
from database import upsert_record, log_sync

log = logging.getLogger(__name__)

_syncing = False
_last_sync_time: int | None = None
_last_sync_error: list | None = None
_last_results: dict = {}

# Per-table last sync info: { table_name: { count, timestamp, error } }
_table_status: dict = {}


def sync_table(table_name: str, key_field: str) -> int:
    """Pull one table from AppSheet into PostgreSQL. Returns row count."""
    rows = find_all(table_name)
    for row in rows:
        key = row.get(key_field) or row.get("_RowNumber")
        if key is None:
            continue
        upsert_record(table_name, str(key), row)
    log_sync(table_name, len(rows))
    return len(rows)


def sync_single(table_name: str, progress_cb=None) -> dict:
    """
    Sync a single table. Returns { table, count } or { table, error }.
    progress_cb is called with SSE-style event dicts.
    """
    global _table_status

    cfg = TABLE_CONFIG.get(table_name)
    if not cfg:
        return {"table": table_name, "error": f"找不到資料表：{table_name}"}

    if progress_cb:
        progress_cb({
            "type": "sync_progress",
            "table": table_name,
            "done": 0, "total": 1, "percent": 0,
        })

    try:
        count = sync_table(table_name, cfg["key"])
        ts = int(time.time() * 1000)
        _table_status[table_name] = {"count": count, "timestamp": ts, "error": None}
        _last_results[table_name] = count

        if progress_cb:
            progress_cb({
                "type": "sync_progress",
                "table": table_name,
                "done": 1, "total": 1, "percent": 100,
                "count": count,
            })
        return {"table": table_name, "count": count, "timestamp": ts}

    except Exception as e:
        msg = str(e)
        log.error(f"[Sync] {table_name} error: {msg}")
        log_sync(table_name, 0, msg)
        ts = int(time.time() * 1000)
        _table_status[table_name] = {"count": 0, "timestamp": ts, "error": msg}

        if progress_cb:
            progress_cb({
                "type": "sync_progress",
                "table": table_name,
                "done": 1, "total": 1, "percent": 100,
                "error": msg,
            })
        return {"table": table_name, "error": msg, "timestamp": ts}


def sync_all(progress_cb=None) -> dict:
    """
    Sync every table in TABLE_CONFIG.
    progress_cb receives { type, table, done, total, percent, count? } events.
    """
    global _syncing, _last_sync_time, _last_sync_error, _last_results

    if _syncing:
        return {"skipped": True, "reason": "already syncing"}

    _syncing = True
    _last_sync_error = None
    results = {}
    errors = []

    tables = list(TABLE_CONFIG.items())
    total = len(tables)

    # Announce start
    if progress_cb:
        progress_cb({
            "type": "sync_start",
            "total": total,
            "tables": [t for t, _ in tables],
        })

    for i, (table_name, cfg) in enumerate(tables):
        # Before — announce which table is starting
        if progress_cb:
            progress_cb({
                "type": "sync_progress",
                "table": table_name,
                "done": i, "total": total,
                "percent": int(i / total * 100),
            })

        try:
            count = sync_table(table_name, cfg["key"])
            results[table_name] = count
            ts = int(time.time() * 1000)
            _table_status[table_name] = {"count": count, "timestamp": ts, "error": None}
            log.info(f"[Sync] {table_name}: {count} rows")

            # After — report count
            if progress_cb:
                progress_cb({
                    "type": "sync_progress",
                    "table": table_name,
                    "done": i + 1, "total": total,
                    "percent": int((i + 1) / total * 100),
                    "count": count,
                })
            time.sleep(0.3)          # throttle

        except Exception as e:
            msg = str(e)
            log.error(f"[Sync] {table_name} error: {msg}")
            log_sync(table_name, 0, msg)
            errors.append({"table": table_name, "error": msg})
            _table_status[table_name] = {
                "count": 0, "timestamp": int(time.time() * 1000), "error": msg
            }
            if progress_cb:
                progress_cb({
                    "type": "sync_progress",
                    "table": table_name,
                    "done": i + 1, "total": total,
                    "percent": int((i + 1) / total * 100),
                    "error": msg,
                })

    _syncing = False
    _last_sync_time = int(time.time() * 1000)
    _last_results = results
    if errors:
        _last_sync_error = errors

    return {"results": results, "errors": errors, "timestamp": _last_sync_time}


def get_status() -> dict:
    return {
        "syncing":       _syncing,
        "lastSyncTime":  _last_sync_time,
        "lastSyncError": _last_sync_error,
        "results":       _last_results,
        "tableStatus":   _table_status,
    }
