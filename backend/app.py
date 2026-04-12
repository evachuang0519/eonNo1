"""
日照交通服務系統 — Python Flask Backend (v2)
  - REST API for browser JS frontend
  - Server-Sent Events (SSE) for real-time push
  - 資料全存本機正規化 PostgreSQL；手動推送至 AppSheet
  - 不再有 APScheduler 自動拉取
"""
import json
import queue
import time
import random
import string
import logging
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from flask import Flask, Response, request, jsonify, stream_with_context, render_template
from config import API_PORT, TABLE_CONFIG

# ── 舊版資料庫（JSONB records 表，供尚未遷移的功能使用）
from database import (
    init_db, get_records, get_record, delete_record,
    get_table_counts, get_sync_log, mark_dirty, mark_clean,
    upsert_record
)
# ── 新版正規化資料庫
from database_v2 import (
    get_conn_v2,
    get_centers, upsert_center,
    get_drivers, upsert_driver,
    get_patients, get_patient_full, upsert_patient,
    get_orders, upsert_order,
    get_users, get_dirty_counts,
)
# ── 推送同步引擎（本機 → AppSheet）
from sync_engine_v2 import push_all, push_table, get_push_status

# ── AppSheet 直接操作（保留供特殊路由使用）
from appsheet_client import add_row, edit_row, delete_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# ── SSE broadcast ──────────────────────────────────────────────────────────────

_sse_clients: list[queue.Queue] = []
_sse_lock = threading.Lock()


def broadcast(data: dict):
    payload = json.dumps(data, ensure_ascii=False)
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


# ── CORS ───────────────────────────────────────────────────────────────────────

@app.after_request
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@app.route("/api/", defaults={"path": ""}, methods=["OPTIONS"])
@app.route("/api/<path:path>", methods=["OPTIONS"])
def options(_path=""):
    return "", 204


# ── SSE endpoint ───────────────────────────────────────────────────────────────

@app.route("/events")
def events():
    q: queue.Queue = queue.Queue(maxsize=50)
    with _sse_lock:
        _sse_clients.append(q)

    def generate():
        yield f"data: {json.dumps({'type': 'connected', 'timestamp': int(time.time()*1000)})}\n\n"
        try:
            while True:
                try:
                    payload = q.get(timeout=25)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    yield ": ping\n\n"
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                if q in _sse_clients:
                    _sse_clients.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Sync API (v2 push) ─────────────────────────────────────────────────────────

@app.route("/api/sync/status")
def sync_status():
    """回傳各表 dirty 數量與最近推送紀錄"""
    status = get_push_status()
    return jsonify(status)


@app.route("/api/sync/trigger", methods=["POST"])
def sync_trigger():
    """觸發全量推送（所有 dirty 記錄 → AppSheet）"""
    def run():
        result = push_all(progress_cb=broadcast)
        broadcast({"type": "push_complete", **result})
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "message": "push started"})


@app.route("/api/sync/one/<path:table_name>", methods=["POST"])
def sync_one_table(table_name):
    """推送單一資料表的 dirty 記錄"""
    def run():
        result = push_table(table_name, progress_cb=broadcast)
        if result.get("error"):
            broadcast({"type": "sync_table_done", "table": table_name,
                       "error": result["error"]})
        else:
            broadcast({"type": "sync_table_done", "table": table_name,
                       "count": result.get("count", 0),
                       "skipped": result.get("skipped", False),
                       "timestamp": result.get("timestamp")})
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "table": table_name})


@app.route("/api/sync/dirty")
def sync_dirty():
    """回傳各正規化資料表的 dirty 筆數"""
    try:
        counts = get_dirty_counts()
        return jsonify(counts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Tables meta ────────────────────────────────────────────────────────────────

@app.route("/api/tables")
def tables_list():
    counts = {r["table_name"]: r["count"] for r in get_table_counts()}
    result = []
    for name, cfg in TABLE_CONFIG.items():
        result.append({
            "name":  name,
            "label": cfg["label"],
            "icon":  cfg["icon"],
            "color": cfg["color"],
            "desc":  cfg["desc"],
            "count": counts.get(name, 0),
        })
    return jsonify(result)


# ── 舊版 Records CRUD（JSONB records 表，保留向下相容）─────────────────────────

@app.route("/api/<table>", methods=["GET"])
def records_list(table):
    search = request.args.get("search", "")
    records = get_records(table, search)
    return jsonify(records)


@app.route("/api/<table>/<path:key>", methods=["GET"])
def record_get(table, key):
    rec = get_record(table, key)
    if rec is None:
        return jsonify({"error": "Not found"}), 404
    return jsonify(rec)


@app.route("/api/<table>", methods=["POST"])
def record_add(table):
    cfg = TABLE_CONFIG.get(table)
    if not cfg:
        return jsonify({"error": f"Unknown table: {table}"}), 400
    row = request.get_json(force=True)
    key_value = row.get(cfg["key"])
    if not key_value:
        return jsonify({"error": f"Key field [{cfg['key']}] is required"}), 400
    try:
        mark_dirty(table, str(key_value), row)
        add_row(table, row)
        mark_clean(table, str(key_value))
        broadcast({"type": "row_added", "table": table, "key": key_value})
        return jsonify({"ok": True, "key": key_value})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<table>/<path:key>", methods=["PUT"])
def record_edit(table, key):
    cfg = TABLE_CONFIG.get(table)
    if not cfg:
        return jsonify({"error": f"Unknown table: {table}"}), 400
    row = request.get_json(force=True)
    try:
        mark_dirty(table, key, row)
        edit_row(table, row)
        mark_clean(table, key)
        broadcast({"type": "row_updated", "table": table, "key": key})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<table>/<path:key>", methods=["DELETE"])
def record_delete(table, key):
    cfg = TABLE_CONFIG.get(table)
    if not cfg:
        return jsonify({"error": f"Unknown table: {table}"}), 400
    try:
        delete_row(table, cfg["key"], key)
        delete_record(table, key)
        broadcast({"type": "row_deleted", "table": table, "key": key})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 正規化 API v2 ──────────────────────────────────────────────────────────────

# ---------- centers ----------

@app.route("/api/v2/centers", methods=["GET"])
def v2_centers_list():
    return jsonify(get_centers())


@app.route("/api/v2/centers/<int:cid>", methods=["GET"])
def v2_center_get(cid):
    items = [c for c in get_centers(include_deleted=True) if c["id"] == cid]
    if not items:
        return jsonify({"error": "Not found"}), 404
    return jsonify(items[0])


@app.route("/api/v2/centers", methods=["POST"])
def v2_center_add():
    data = request.get_json(force=True)
    try:
        new_id = upsert_center(data)
        broadcast({"type": "row_added", "table": "centers", "id": new_id})
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/centers/<int:cid>", methods=["PUT"])
def v2_center_edit(cid):
    data = request.get_json(force=True)
    data["id"] = cid
    try:
        upsert_center(data)
        broadcast({"type": "row_updated", "table": "centers", "id": cid})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/centers/<int:cid>", methods=["DELETE"])
def v2_center_delete(cid):
    conn = get_conn_v2()
    try:
        conn.run("UPDATE centers SET deleted=TRUE, dirty=TRUE WHERE id=:id", id=cid)
        broadcast({"type": "row_deleted", "table": "centers", "id": cid})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------- drivers ----------

@app.route("/api/v2/drivers", methods=["GET"])
def v2_drivers_list():
    return jsonify(get_drivers())


@app.route("/api/v2/drivers", methods=["POST"])
def v2_driver_add():
    data = request.get_json(force=True)
    try:
        new_id = upsert_driver(data)
        broadcast({"type": "row_added", "table": "drivers", "id": new_id})
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/drivers/<int:did>", methods=["PUT"])
def v2_driver_edit(did):
    data = request.get_json(force=True)
    data["id"] = did
    try:
        upsert_driver(data)
        broadcast({"type": "row_updated", "table": "drivers", "id": did})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/drivers/<int:did>", methods=["DELETE"])
def v2_driver_delete(did):
    conn = get_conn_v2()
    try:
        conn.run("UPDATE drivers SET deleted=TRUE, dirty=TRUE WHERE id=:id", id=did)
        broadcast({"type": "row_deleted", "table": "drivers", "id": did})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------- patients ----------

@app.route("/api/v2/patients", methods=["GET"])
def v2_patients_list():
    center_id = request.args.get("center_id", type=int)
    return jsonify(get_patients(center_id=center_id))


@app.route("/api/v2/patients/<int:pid>", methods=["GET"])
def v2_patient_get(pid):
    rec = get_patient_full(pid)
    if not rec:
        return jsonify({"error": "Not found"}), 404
    return jsonify(rec)


@app.route("/api/v2/patients", methods=["POST"])
def v2_patient_add():
    data = request.get_json(force=True)
    try:
        new_id = upsert_patient(data)
        broadcast({"type": "row_added", "table": "patients", "id": new_id})
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/patients/<int:pid>", methods=["PUT"])
def v2_patient_edit(pid):
    data = request.get_json(force=True)
    data["id"] = pid
    try:
        upsert_patient(data)
        broadcast({"type": "row_updated", "table": "patients", "id": pid})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/patients/<int:pid>", methods=["DELETE"])
def v2_patient_delete(pid):
    conn = get_conn_v2()
    try:
        conn.run("UPDATE patients SET deleted=TRUE, dirty=TRUE WHERE id=:id", id=pid)
        broadcast({"type": "row_deleted", "table": "patients", "id": pid})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------- orders ----------

@app.route("/api/v2/orders", methods=["GET"])
def v2_orders_list():
    return jsonify(get_orders(
        date_from=request.args.get("from"),
        date_to=request.args.get("to"),
        driver_id=request.args.get("driver_id", type=int),
        center_id=request.args.get("center_id", type=int),
        status=request.args.get("status"),
    ))


@app.route("/api/v2/orders", methods=["POST"])
def v2_order_add():
    data = request.get_json(force=True)
    try:
        new_id = upsert_order(data)
        broadcast({"type": "row_added", "table": "orders", "id": new_id})
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/orders/<int:oid>", methods=["PUT"])
def v2_order_edit(oid):
    data = request.get_json(force=True)
    data["id"] = oid
    try:
        upsert_order(data)
        broadcast({"type": "row_updated", "table": "orders", "id": oid})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v2/orders/<int:oid>", methods=["DELETE"])
def v2_order_delete(oid):
    conn = get_conn_v2()
    try:
        conn.run("UPDATE orders SET deleted=TRUE, dirty=TRUE WHERE id=:id", id=oid)
        broadcast({"type": "row_deleted", "table": "orders", "id": oid})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------- users ----------

@app.route("/api/v2/users", methods=["GET"])
def v2_users_list():
    try:
        return jsonify(get_users())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Helpers ────────────────────────────────────────────────────────────────────

TAIPEI_TZ = timezone(timedelta(hours=8))
WEEKDAY_COLS = ["週一", "週二", "週三", "週四", "週五", "週六", "週日"]


def taipei_now() -> datetime:
    return datetime.now(tz=TAIPEI_TZ)


def parse_date(s: str) -> datetime | None:
    for fmt in ("%m/%d/%Y", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    return None


def format_date_mdy(dt: datetime) -> str:
    return dt.strftime("%m/%d/%Y")


def gen_task_id(dt: datetime) -> str:
    code = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"T{dt.strftime('%Y%m%d')}-{code}"


# ── Schedule views ──────────────────────────────────────────────────────────────

@app.route("/api/schedule/today")
def schedule_today():
    today_str = format_date_mdy(taipei_now())
    records = get_records("日照班表")
    result = [r for r in records if r.get("搭乘日期", "") == today_str]
    result.sort(key=lambda r: r.get("搭乘時間", "") or "")
    return jsonify(result)


@app.route("/api/schedule/range")
def schedule_range():
    from_str = request.args.get("from", "")
    to_str   = request.args.get("to", "")
    driver   = request.args.get("driver", "")
    center   = request.args.get("center", "")
    dt_from  = parse_date(from_str) if from_str else None
    dt_to    = parse_date(to_str)   if to_str   else None

    records = get_records("日照班表")
    result  = []
    for r in records:
        dt = parse_date(r.get("搭乘日期", ""))
        if dt is None:
            continue
        if dt_from and dt.date() < dt_from.date():
            continue
        if dt_to and dt.date() > dt_to.date():
            continue
        if driver and r.get("服務司機", "") != driver:
            continue
        if center and r.get("日照中心", "") != center:
            continue
        result.append(r)

    result.sort(key=lambda r: (r.get("搭乘日期", ""), r.get("搭乘時間", "") or ""))
    return jsonify(result)


@app.route("/api/schedule/drivers")
def schedule_drivers():
    drivers = get_records("司機")
    return jsonify([d.get("姓名", "") for d in drivers if d.get("姓名")])


@app.route("/api/schedule/centers")
def schedule_centers():
    centers = get_records("日照名單")
    return jsonify([c.get("據點名稱", "") for c in centers if c.get("據點名稱")])


# ── Statistics ──────────────────────────────────────────────────────────────────

@app.route("/api/stats/summary")
def stats_summary():
    from_str = request.args.get("from", "")
    to_str   = request.args.get("to", "")
    dt_from  = parse_date(from_str) if from_str else None
    dt_to    = parse_date(to_str)   if to_str   else None

    records   = get_records("日照班表")
    by_status  = defaultdict(int)
    by_driver  = defaultdict(int)
    by_center  = defaultdict(int)
    by_date    = defaultdict(int)

    for r in records:
        dt = parse_date(r.get("搭乘日期", ""))
        if dt_from and dt and dt.date() < dt_from.date():
            continue
        if dt_to   and dt and dt.date() > dt_to.date():
            continue
        by_status[r.get("訂單狀態", "未知")] += 1
        by_driver[r.get("服務司機", "") or "未分配"] += 1
        by_center[r.get("日照中心", "") or "未知"] += 1
        if dt:
            by_date[dt.strftime("%Y-%m-%d")] += 1

    return jsonify({
        "byStatus":  dict(sorted(by_status.items(), key=lambda x: -x[1])),
        "byDriver":  dict(sorted(by_driver.items(),  key=lambda x: -x[1])),
        "byCenter":  dict(sorted(by_center.items(),  key=lambda x: -x[1])),
        "byDate":    dict(sorted(by_date.items())),
        "total":     sum(by_status.values()),
    })


# ── Behavior actions ────────────────────────────────────────────────────────────

def _update_order_status(key: str, updates: dict) -> tuple[dict, str | None]:
    rec = get_record("日照班表", key)
    if not rec:
        return {}, f"找不到訂單：{key}"
    rec.update(updates)
    try:
        mark_dirty("日照班表", key, rec)
        edit_row("日照班表", rec)
        mark_clean("日照班表", key)
        broadcast({"type": "row_updated", "table": "日照班表", "key": key})
        return rec, None
    except Exception as e:
        return rec, str(e)


@app.route("/api/action/pickup", methods=["POST"])
def action_pickup():
    body = request.get_json(force=True)
    key  = body.get("key", "")
    lat  = body.get("lat", "")
    lng  = body.get("lng", "")
    now  = taipei_now().strftime("%H:%M:%S")
    updates = {"上車時間": now, "訂單狀態": "客上"}
    if lat and lng:
        updates["上車座標"] = f"{lat}, {lng}"
    rec, err = _update_order_status(key, updates)
    if err:
        return jsonify({"error": err}), 400 if "找不到" in err else 500
    return jsonify({"ok": True, "record": rec})


@app.route("/api/action/dropoff", methods=["POST"])
def action_dropoff():
    body = request.get_json(force=True)
    key  = body.get("key", "")
    lat  = body.get("lat", "")
    lng  = body.get("lng", "")
    now  = taipei_now().strftime("%H:%M:%S")
    updates = {"下車時間": now, "訂單狀態": "已完成"}
    if lat and lng:
        updates["下車座標"] = f"{lat}, {lng}"
    rec, err = _update_order_status(key, updates)
    if err:
        return jsonify({"error": err}), 400 if "找不到" in err else 500
    return jsonify({"ok": True, "record": rec})


@app.route("/api/action/leave", methods=["POST"])
def action_leave():
    body = request.get_json(force=True)
    key  = body.get("key", "")
    rec, err = _update_order_status(key, {"訂單狀態": "請假"})
    if err:
        return jsonify({"error": err}), 400 if "找不到" in err else 500
    return jsonify({"ok": True, "record": rec})


@app.route("/api/action/generate", methods=["POST"])
def action_generate():
    body     = request.get_json(force=True)
    date_str = body.get("date", "")
    dry_run  = body.get("dry_run", False)

    dt = parse_date(date_str)
    if not dt:
        return jsonify({"error": f"日期格式錯誤：{date_str}（請用 MM/DD/YYYY）"}), 400

    weekday_col = WEEKDAY_COLS[dt.weekday()]
    date_mdy    = format_date_mdy(dt)

    all_cases = get_records("個案總表")
    eligible  = [c for c in all_cases if c.get(weekday_col) == "Y"]

    existing = get_records("日照班表")
    existing_keys = {
        (r.get("乘客姓名", ""), r.get("路線", ""), r.get("搭乘日期", ""))
        for r in existing
    }

    generated, skipped, errors = [], [], []

    for case in eligible:
        name   = case.get("乘客姓名", "")
        route  = case.get("路線", "")
        lookup = (name, route, date_mdy)

        if lookup in existing_keys:
            skipped.append(f"{name}（{route}）")
            continue

        task_id = gen_task_id(dt)
        order = {
            "訂單編號(Task ID)": task_id,
            "訂單狀態":          "待接送",
            "日照中心":          case.get("日照中心", ""),
            "建單日期":          format_date_mdy(taipei_now()),
            "搭乘日期":          date_mdy,
            "搭乘時間":          case.get("表定搭乘時間", ""),
            "服務司機":          case.get("預設服務司機", ""),
            "車號":              case.get("車號", ""),
            "路線":              route,
            "乘客姓名":          name,
            "備註":              case.get("備註", ""),
            "上車地址":          case.get("上車地點", ""),
            "下車地點":          case.get("下車地點", ""),
        }

        if not dry_run:
            try:
                add_row("日照班表", order)
                upsert_record("日照班表", task_id, order)
                generated.append(task_id)
                existing_keys.add(lookup)
            except Exception as e:
                errors.append({"case": case.get("姓名路程"), "error": str(e)})
        else:
            generated.append({"preview": True, "case": case.get("姓名路程"), "order": order})

    if not dry_run and generated:
        broadcast({"type": "sync_complete", "source": "generate",
                   "results": {"日照班表": len(generated)}})

    return jsonify({
        "ok":        True,
        "date":      date_mdy,
        "weekday":   weekday_col,
        "generated": len(generated) if not dry_run else generated,
        "skipped":   skipped,
        "errors":    errors,
    })


# ── AppSheet webhook ───────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True) or {}
    log.info(f"[Webhook] {str(body)[:200]}")
    return jsonify({"ok": True, "message": "received"})


# ── Health ─────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"ok": True, "uptime": time.time()})


# ── Jinja2 helpers ─────────────────────────────────────────────────────────────

def _mdy_to_iso(mdy: str) -> str:
    dt = parse_date(mdy)
    return dt.strftime("%Y-%m-%d") if dt else ""

app.jinja_env.filters["mdy_to_iso"] = _mdy_to_iso


def _make_tables():
    counts = {r["table_name"]: r["count"] for r in get_table_counts()}
    return [{"name": name, **cfg, "count": counts.get(name, 0)}
            for name, cfg in TABLE_CONFIG.items()]


# ── Page routes ────────────────────────────────────────────────────────────────

@app.route("/")
def page_index():
    return render_template("index.html",
        current_page="index", tables=_make_tables())


@app.route("/today")
def page_today():
    today_str = format_date_mdy(taipei_now())
    orders = sorted(
        [r for r in get_records("日照班表") if r.get("搭乘日期") == today_str],
        key=lambda r: r.get("搭乘時間") or "")
    return render_template("today.html",
        current_page="today", tables=_make_tables(), orders=orders)


@app.route("/schedule")
def page_schedule():
    today_str = format_date_mdy(taipei_now())
    orders  = [r for r in get_records("日照班表") if r.get("搭乘日期") == today_str]
    drivers = sorted({r.get("姓名","") for r in get_records("司機") if r.get("姓名")})
    centers = sorted({r.get("據點名稱","") for r in get_records("日照名單") if r.get("據點名稱")})
    return render_template("schedule.html",
        current_page="schedule", tables=_make_tables(),
        initial=orders, drivers=drivers, centers=centers,
        today=today_str)


@app.route("/map")
def page_map():
    today_str = format_date_mdy(taipei_now())
    map_data = [r for r in get_records("日照班表") if r.get("搭乘日期") == today_str]
    drivers  = sorted({r.get("姓名","") for r in get_records("司機") if r.get("姓名")})
    return render_template("map.html",
        current_page="map", tables=_make_tables(),
        map_data=map_data, drivers=drivers, today=today_str)


@app.route("/stats")
def page_stats():
    now       = taipei_now()
    first_day = format_date_mdy(now.replace(day=1))
    today_str = format_date_mdy(now)
    records   = get_records("日照班表")
    by_status: dict = defaultdict(int)
    by_driver: dict = defaultdict(int)
    by_center: dict = defaultdict(int)
    by_date:   dict = defaultdict(int)
    dt_from   = now.replace(day=1).date()
    dt_to     = now.date()
    for r in records:
        dt = parse_date(r.get("搭乘日期", ""))
        if dt and not (dt_from <= dt.date() <= dt_to):
            continue
        by_status[r.get("訂單狀態") or "未知"] += 1
        by_driver[r.get("服務司機") or "未分配"] += 1
        by_center[r.get("日照中心") or "未知"] += 1
        if dt:
            by_date[dt.strftime("%Y-%m-%d")] += 1
    stats = {
        "byStatus": dict(sorted(by_status.items(), key=lambda x: -x[1])),
        "byDriver": dict(sorted(by_driver.items(), key=lambda x: -x[1])),
        "byCenter": dict(sorted(by_center.items(), key=lambda x: -x[1])),
        "byDate":   dict(sorted(by_date.items())),
        "total":    sum(by_status.values()),
    }
    return render_template("stats.html",
        current_page="stats", tables=_make_tables(),
        stats=stats, stat_from=first_day, stat_to=today_str)


@app.route("/cases")
def page_cases():
    cases   = get_records("個案總表")
    centers = sorted({r.get("據點名稱","") for r in get_records("日照名單") if r.get("據點名稱")})
    return render_template("cases.html",
        current_page="cases", tables=_make_tables(),
        cases=cases, centers=centers)


@app.route("/table/<table_name>")
def page_table(table_name):
    cfg = TABLE_CONFIG.get(table_name)
    if not cfg:
        return f"<h2>找不到資料表：{table_name}</h2>", 404
    records = get_records(table_name)
    return render_template("table.html",
        current_page="table", current_table=table_name,
        tables=_make_tables(), cfg=cfg, records=records,
        table_name=table_name)


@app.route("/synclog")
def page_synclog():
    status = get_push_status()
    return render_template("synclog.html",
        current_page="synclog", tables=_make_tables(), sync_status=status)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  ╔══════════════════════════════════════════╗")
    print("  ║  日照交通服務系統 — Python API Backend    ║")
    print(f"  ║  http://localhost:{API_PORT}                     ║")
    print("  ╚══════════════════════════════════════════╝\n")

    print("[DB] Initialising PostgreSQL...")
    init_db()
    print("[DB] Ready.\n")
    print("[Info] 同步模式：手動推送（本機 → AppSheet）\n")

    app.run(host="0.0.0.0", port=API_PORT, threaded=True, debug=False)
