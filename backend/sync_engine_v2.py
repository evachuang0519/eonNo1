"""
sync_engine_v2.py — 推送方向同步引擎（本機 PostgreSQL → AppSheet）

設計原則
  - 所有日常操作在本機正規化資料表進行
  - dirty=TRUE 代表該筆資料有變更，需推送至 AppSheet
  - appsheet_key 存在 → edit_row（已知 AppSheet 主鍵）
  - appsheet_key 為空 → add_row（本機新建，AppSheet 尚無此筆）
  - 推送成功 → dirty=FALSE；失敗 → 保持 dirty，記錄 error_msg

使用方式
  from sync_engine_v2 import push_all, push_table, get_push_status
"""

import logging
import time
from datetime import datetime, timezone, timedelta

from appsheet_assembler import ASSEMBLERS
from appsheet_client import add_row, edit_row
from database_v2 import get_conn_v2, get_dirty_counts

log = logging.getLogger(__name__)

TAIPEI_TZ = timezone(timedelta(hours=8))


# ── AppSheet 資料表的主鍵欄位 ───────────────────────────────────────────────────

TABLE_KEY_FIELDS: dict[str, str] = {
    "日照名單":  "據點名稱",
    "司機":      "姓名",
    "司機名單":  "司機帳號",
    "個案總表":  "姓名路程",
    "日照班表":  "訂單編號(Task ID)",
}

# 推送後要清除 dirty 的本機資料表（一個 AppSheet 表可能對應多個本機表）
TABLE_SOURCE_MAP: dict[str, list[str]] = {
    "日照名單":  ["centers"],
    "司機":      ["drivers"],
    "司機名單":  [],           # 與「司機」共用 drivers；避免重複清除
    "個案總表":  ["patients", "patient_routes"],
    "日照班表":  ["orders"],
}

# 推送順序（依賴關係：centers/drivers 先於 patients/orders）
PUSH_ORDER = ["日照名單", "司機", "司機名單", "個案總表", "日照班表"]


# ── 本機骯髒記錄查詢（appsheet_key 判斷 edit/add）────────────────────────────

def _get_appsheet_keys(source_table: str) -> set[str]:
    """回傳 source_table 中所有非空 appsheet_key（代表已知 AppSheet 記錄）"""
    conn = get_conn_v2()
    try:
        rows = conn.run(
            f"SELECT appsheet_key FROM {source_table} "
            f"WHERE appsheet_key IS NOT NULL AND deleted=FALSE"
        )
        return {r[0] for r in rows if r[0]}
    finally:
        conn.close()


def _mark_source_clean(source_tables: list[str]) -> int:
    """將指定 source_tables 中全部 dirty=TRUE 的記錄清為 FALSE，回傳清除筆數"""
    if not source_tables:
        return 0
    conn = get_conn_v2()
    total = 0
    try:
        for tbl in source_tables:
            rows = conn.run(
                f"UPDATE {tbl} SET dirty=FALSE "
                f"WHERE dirty=TRUE AND deleted=FALSE RETURNING id"
            )
            total += len(rows) if rows else 0
        return total
    finally:
        conn.close()


def _write_sync_log(appsheet_table: str, total: int, ok: int,
                    fail: int, errors: list[str]):
    """寫入 sync_log 記錄本次推送結果（配合 schema_v2 欄位）"""
    conn = get_conn_v2()
    try:
        epoch_ms = int(time.time() * 1000)
        err_str = "; ".join(errors[:5]) if errors else None
        conn.run("""
            INSERT INTO sync_log
                (synced_at, direction, table_name, rows_pushed, error)
            VALUES (:ts, 'push', :tn, :pushed, :err)
        """, ts=epoch_ms, tn=appsheet_table, pushed=ok, err=err_str)
    except Exception as e:
        log.warning(f"[SyncLog] 寫入失敗：{e}")
    finally:
        conn.close()


# ── 單一資料表推送 ──────────────────────────────────────────────────────────────

def push_table(appsheet_table: str,
               progress_cb=None) -> dict:
    """
    將指定 AppSheet 資料表的骯髒記錄推送過去。

    回傳 dict:
      {"table": str, "count": int}           # 成功
      {"table": str, "count": int,
       "errors": list, "failed": int}        # 部分失敗
      {"table": str, "error": str}           # 整體失敗
    """
    fn = ASSEMBLERS.get(appsheet_table)
    if fn is None:
        return {"table": appsheet_table, "error": f"無組裝器：{appsheet_table}"}

    key_field = TABLE_KEY_FIELDS.get(appsheet_table, "")
    source_tables = TABLE_SOURCE_MAP.get(appsheet_table, [])

    try:
        rows = fn(dirty_only=True)
    except Exception as e:
        err = f"組裝失敗：{e}"
        log.error(f"[Push:{appsheet_table}] {err}")
        return {"table": appsheet_table, "error": err}

    if not rows:
        return {"table": appsheet_table, "count": 0, "skipped": True}

    # 取得已知 appsheet_key 集合（決定 edit vs add）
    known_keys: set[str] = set()
    for src in source_tables:
        known_keys |= _get_appsheet_keys(src)

    total = len(rows)
    ok = 0
    failed = 0
    errors: list[str] = []

    for i, row in enumerate(rows):
        key_val = row.get(key_field, "") or ""
        try:
            if key_val and key_val in known_keys:
                edit_row(appsheet_table, row)
            else:
                add_row(appsheet_table, row)
            ok += 1
        except Exception as e:
            failed += 1
            err_msg = f"{key_val or '?'}: {e}"
            errors.append(err_msg)
            log.warning(f"[Push:{appsheet_table}] 失敗 {err_msg}")

        # 每推送一筆回報進度
        if progress_cb:
            progress_cb({
                "type":    "sync_push_progress",
                "table":   appsheet_table,
                "done":    i + 1,
                "total":   total,
                "percent": round((i + 1) / total * 100),
            })

    # 全部成功或部分成功 → 清除本機 dirty
    if ok > 0 and source_tables:
        _mark_source_clean(source_tables)

    _write_sync_log(appsheet_table, total, ok, failed, errors)

    result: dict = {"table": appsheet_table, "count": ok,
                    "timestamp": datetime.now(TAIPEI_TZ).isoformat()}
    if errors:
        result["failed"] = failed
        result["errors"] = errors[:10]
    return result


# ── 全量推送 ────────────────────────────────────────────────────────────────────

def push_all(progress_cb=None) -> dict:
    """
    推送所有 AppSheet 資料表的骯髒記錄。

    推播 SSE 事件：
      push_start         → 開始
      sync_push_progress → 單表推送進度（row level）
      sync_progress      → 跨表進度（table level）
      push_complete      → 全部完成
    """
    start_ts = time.time()
    results: dict[str, int] = {}
    errors:  dict[str, str] = {}

    if progress_cb:
        progress_cb({
            "type":   "push_start",
            "tables": PUSH_ORDER,
            "total":  len(PUSH_ORDER),
        })

    for idx, tbl in enumerate(PUSH_ORDER):
        if progress_cb:
            progress_cb({
                "type":    "sync_progress",
                "table":   tbl,
                "done":    idx,
                "total":   len(PUSH_ORDER),
                "percent": round(idx / len(PUSH_ORDER) * 100),
            })

        log.info(f"[PushAll] [{idx+1}/{len(PUSH_ORDER)}] {tbl}...")
        result = push_table(tbl, progress_cb=progress_cb)

        if result.get("error"):
            errors[tbl] = result["error"]
        elif not result.get("skipped"):
            results[tbl] = result.get("count", 0)

        if progress_cb:
            progress_cb({
                "type":    "sync_table_done",
                "table":   tbl,
                "count":   result.get("count", 0),
                "error":   result.get("error"),
                "skipped": result.get("skipped", False),
            })

    elapsed = round(time.time() - start_ts, 1)
    total_pushed = sum(results.values())

    summary = {
        "type":     "push_complete",
        "results":  results,
        "errors":   errors,
        "total":    total_pushed,
        "elapsed":  elapsed,
        "percent":  100,
        "done":     len(PUSH_ORDER),
        "total_tables": len(PUSH_ORDER),
    }

    if progress_cb:
        progress_cb(summary)

    log.info(f"[PushAll] 完成 — {total_pushed} 筆，耗時 {elapsed}s")
    return summary


# ── 狀態查詢 ────────────────────────────────────────────────────────────────────

_last_push: dict = {}


def get_push_status() -> dict:
    """回傳各資料表的 dirty 筆數與最近一次推送紀錄"""
    try:
        dirty = get_dirty_counts()
    except Exception as e:
        dirty = {"error": str(e)}

    conn = get_conn_v2()
    last_logs: list[dict] = []
    try:
        rows = conn.run("""
            SELECT table_name, direction, rows_pushed, rows_pulled,
                   error, synced_at
            FROM sync_log
            ORDER BY synced_at DESC
            LIMIT 30
        """)
        cols = ["table_name","direction","rows_pushed","rows_pulled",
                "error","synced_at"]
        last_logs = [dict(zip(cols, r)) for r in rows]
    except Exception:
        pass
    finally:
        conn.close()

    return {
        "dirtyCount": dirty,
        "logs":       last_logs,
    }
