"""
database_v2.py — 正規化資料表的 CRUD 操作
對應 schema_v2.sql 的 centers / drivers / patients / patient_routes / orders / users
"""
import time
import logging
import pg8000.native
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

log = logging.getLogger(__name__)


# ── JSON 序列化輔助 ─────────────────────────────────────────────────────────────

import datetime, decimal


def _ser(v):
    """將 Python date/time/Decimal 轉為 JSON-friendly 型態"""
    if isinstance(v, (datetime.date, datetime.time, datetime.datetime)):
        return str(v)
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def _to_dict(cols: list[str], row) -> dict:
    return {k: _ser(v) for k, v in zip(cols, row)}


# ── 連線 ───────────────────────────────────────────────────────────────────────

def get_conn_v2():
    return pg8000.native.Connection(
        host=PG_HOST, port=PG_PORT, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )


# ════════════════════════════════════════════════════════════════
#  通用工具
# ════════════════════════════════════════════════════════════════

def _rows_to_dicts(conn, sql: str, **params) -> list[dict]:
    """執行 SQL，回傳 list[dict]（欄位名來自 columns 屬性）"""
    conn.run(sql, **params)
    cols = [c["name"] for c in conn.columns]
    rows = conn.run(sql, **params)
    return [_to_dict(cols, row) for row in rows]


# ════════════════════════════════════════════════════════════════
#  fleets — 車行（運輸公司）
# ════════════════════════════════════════════════════════════════

def get_fleets(include_deleted: bool = False) -> list[dict]:
    conn = get_conn_v2()
    try:
        where = "" if include_deleted else "WHERE f.deleted=FALSE"
        rows = conn.run(f"""
            SELECT f.id, f.name, f.short_name, f.phone, f.address,
                   f.contact_person, f.email, f.tax_id, f.notes,
                   f.is_active, f.dirty,
                   f.created_at, f.updated_at,
                   COUNT(DISTINCT d.id) FILTER (WHERE d.deleted=FALSE) AS driver_count,
                   COUNT(DISTINCT v.id) FILTER (WHERE v.deleted=FALSE) AS vehicle_count
            FROM fleets f
            LEFT JOIN drivers  d ON d.fleet_id = f.id
            LEFT JOIN vehicles v ON v.fleet_id = f.id
            {where}
            GROUP BY f.id ORDER BY f.name
        """)
        cols = ["id","name","short_name","phone","address","contact_person","email",
                "tax_id","notes","is_active","dirty","created_at","updated_at",
                "driver_count","vehicle_count"]
        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()


def upsert_fleet(data: dict) -> int:
    conn = get_conn_v2()
    try:
        if data.get("id"):
            conn.run("""
                UPDATE fleets SET name=:n, short_name=:sn, phone=:ph,
                    address=:ad, contact_person=:cp, email=:em,
                    tax_id=:ti, notes=:nt, is_active=:ia, dirty=TRUE
                WHERE id=:id
            """, id=data["id"], n=data["name"], sn=data.get("short_name"),
                ph=data.get("phone"), ad=data.get("address"),
                cp=data.get("contact_person"), em=data.get("email"),
                ti=data.get("tax_id"), nt=data.get("notes"),
                ia=data.get("is_active", True))
            conn.run("COMMIT")
            return data["id"]
        else:
            rows = conn.run("""
                INSERT INTO fleets (name,short_name,phone,address,contact_person,
                                    email,tax_id,notes,is_active,dirty)
                VALUES (:n,:sn,:ph,:ad,:cp,:em,:ti,:nt,:ia,TRUE) RETURNING id
            """, n=data["name"], sn=data.get("short_name"), ph=data.get("phone"),
                ad=data.get("address"), cp=data.get("contact_person"),
                em=data.get("email"), ti=data.get("tax_id"),
                nt=data.get("notes"), ia=data.get("is_active", True))
            conn.run("COMMIT")
            return rows[0][0]
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  vehicles — 車輛
# ════════════════════════════════════════════════════════════════

def get_vehicles(fleet_id: int = None, include_deleted: bool = False) -> list[dict]:
    conn = get_conn_v2()
    try:
        conditions = [] if include_deleted else ["v.deleted=FALSE"]
        if fleet_id:
            conditions.append(f"v.fleet_id={fleet_id}")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = conn.run(f"""
            SELECT v.id, v.plate_no, v.brand, v.model, v.year, v.color,
                   v.seats, v.wheelchair, v.fleet_id,
                   f.name AS fleet_name, f.short_name AS fleet_short,
                   v.status, v.license_expiry, v.insurance_expiry,
                   v.photo_url, v.notes, v.appsheet_key,
                   v.dirty, v.created_at, v.updated_at,
                   d.id AS current_driver_id, d.name AS current_driver_name
            FROM vehicles v
            LEFT JOIN fleets  f ON f.id = v.fleet_id
            LEFT JOIN drivers d ON d.vehicle_id = v.id AND d.deleted=FALSE
            {where}
            ORDER BY f.name NULLS LAST, v.plate_no
        """)
        cols = ["id","plate_no","brand","model","year","color","seats","wheelchair",
                "fleet_id","fleet_name","fleet_short","status",
                "license_expiry","insurance_expiry","photo_url","notes","appsheet_key",
                "dirty","created_at","updated_at",
                "current_driver_id","current_driver_name"]
        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()


def upsert_vehicle(data: dict) -> int:
    conn = get_conn_v2()
    try:
        if data.get("id"):
            conn.run("""
                UPDATE vehicles SET plate_no=:pn, brand=:br, model=:mo, year=:yr,
                    color=:cl, seats=:se, wheelchair=:wc, fleet_id=:fi,
                    status=:st, license_expiry=:le, insurance_expiry=:ie,
                    notes=:nt, dirty=TRUE
                WHERE id=:id
            """, id=data["id"], pn=data["plate_no"], br=data.get("brand"),
                mo=data.get("model"), yr=data.get("year") or None,
                cl=data.get("color"), se=data.get("seats") or 7,
                wc=data.get("wheelchair", False),
                fi=data.get("fleet_id") or None,
                st=data.get("status","active"),
                le=data.get("license_expiry") or None,
                ie=data.get("insurance_expiry") or None,
                nt=data.get("notes"))
            conn.run("COMMIT")
            return data["id"]
        else:
            rows = conn.run("""
                INSERT INTO vehicles (plate_no,brand,model,year,color,seats,wheelchair,
                                      fleet_id,status,license_expiry,insurance_expiry,
                                      notes,dirty)
                VALUES (:pn,:br,:mo,:yr,:cl,:se,:wc,:fi,:st,:le,:ie,:nt,TRUE)
                RETURNING id
            """, pn=data["plate_no"], br=data.get("brand"), mo=data.get("model"),
                yr=data.get("year") or None, cl=data.get("color"),
                se=data.get("seats") or 7, wc=data.get("wheelchair", False),
                fi=data.get("fleet_id") or None, st=data.get("status","active"),
                le=data.get("license_expiry") or None,
                ie=data.get("insurance_expiry") or None,
                nt=data.get("notes"))
            conn.run("COMMIT")
            return rows[0][0]
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  centers — 日照中心
# ════════════════════════════════════════════════════════════════

def get_centers(include_deleted: bool = False) -> list[dict]:
    conn = get_conn_v2()
    try:
        where = "" if include_deleted else "WHERE deleted=FALSE"
        rows = conn.run(f"""
            SELECT id, name, phone, email, address, appsheet_key,
                   is_active, dirty, created_at, updated_at
            FROM centers {where} ORDER BY name
        """)
        cols = ["id","name","phone","email","address","appsheet_key",
                "is_active","dirty","created_at","updated_at"]
        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()


def upsert_center(data: dict) -> int:
    """新增或更新日照中心，回傳 id"""
    conn = get_conn_v2()
    try:
        if data.get("id"):
            conn.run("""
                UPDATE centers SET name=:n,phone=:ph,email=:em,address=:ad,dirty=TRUE
                WHERE id=:id
            """, id=data["id"], n=data["name"], ph=data.get("phone"),
                em=data.get("email"), ad=data.get("address"))
            return data["id"]
        else:
            rows = conn.run("""
                INSERT INTO centers (name,phone,email,address,dirty)
                VALUES (:n,:ph,:em,:ad,TRUE) RETURNING id
            """, n=data["name"], ph=data.get("phone"),
                em=data.get("email"), ad=data.get("address"))
            return rows[0][0]
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  drivers — 司機
# ════════════════════════════════════════════════════════════════

def get_drivers(fleet_id: int = None, include_deleted: bool = False) -> list[dict]:
    conn = get_conn_v2()
    try:
        conditions = [] if include_deleted else ["d.deleted=FALSE"]
        if fleet_id:
            conditions.append(f"d.fleet_id={fleet_id}")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = conn.run(f"""
            SELECT d.id, d.name, d.phone, d.email, d.vehicle_no,
                   d.fleet, d.fleet_id, f.name AS fleet_name,
                   d.vehicle_id, v.plate_no AS vehicle_plate,
                   d.photo_url, d.vehicle_photo_url, d.notes,
                   d.appsheet_key, d.is_active, d.dirty
            FROM drivers d
            LEFT JOIN fleets   f ON f.id = d.fleet_id
            LEFT JOIN vehicles v ON v.id = d.vehicle_id
            {where} ORDER BY d.name
        """)
        cols = ["id","name","phone","email","vehicle_no","fleet","fleet_id","fleet_name",
                "vehicle_id","vehicle_plate","photo_url","vehicle_photo_url","notes",
                "appsheet_key","is_active","dirty"]
        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()


def upsert_driver(data: dict) -> int:
    conn = get_conn_v2()
    try:
        if data.get("id"):
            conn.run("""
                UPDATE drivers
                SET name=:n, phone=:ph, email=:em, vehicle_no=:vn,
                    fleet=:fl, fleet_id=:fi, vehicle_id=:vi,
                    notes=:nt, is_active=:ia, dirty=TRUE
                WHERE id=:id
            """, id=data["id"], n=data["name"], ph=data.get("phone"),
                em=data.get("email"), vn=data.get("vehicle_no"),
                fl=data.get("fleet"), fi=data.get("fleet_id") or None,
                vi=data.get("vehicle_id") or None,
                nt=data.get("notes"), ia=data.get("is_active", True))
            conn.run("COMMIT")
            return data["id"]
        else:
            rows = conn.run("""
                INSERT INTO drivers (name,phone,email,vehicle_no,fleet,fleet_id,
                                     vehicle_id,notes,is_active,dirty)
                VALUES (:n,:ph,:em,:vn,:fl,:fi,:vi,:nt,:ia,TRUE) RETURNING id
            """, n=data["name"], ph=data.get("phone"), em=data.get("email"),
                vn=data.get("vehicle_no"), fl=data.get("fleet"),
                fi=data.get("fleet_id") or None, vi=data.get("vehicle_id") or None,
                nt=data.get("notes"), ia=data.get("is_active", True))
            conn.run("COMMIT")
            return rows[0][0]
    finally:
        conn.close()


def dispatch_order(order_id: int, driver_id: int = None, vehicle_id: int = None,
                   fleet_id: int = None, dispatched_by: str = "system") -> bool:
    """調度：將訂單指派給特定司機/車輛/車行"""
    conn = get_conn_v2()
    try:
        # 取車牌（若有 vehicle_id）
        vehicle_no = None
        if vehicle_id:
            rows = conn.run("SELECT plate_no FROM vehicles WHERE id=:id", id=vehicle_id)
            if rows:
                vehicle_no = rows[0][0]
        conn.run("""
            UPDATE orders SET
                driver_id     = COALESCE(:di, driver_id),
                vehicle_id    = COALESCE(:vi, vehicle_id),
                vehicle_no    = COALESCE(:vn, vehicle_no),
                fleet_id      = COALESCE(:fi, fleet_id),
                dispatched_at = NOW(),
                dispatched_by = :db,
                dirty         = TRUE
            WHERE id = :oid AND deleted = FALSE
        """, di=driver_id, vi=vehicle_id, vn=vehicle_no,
            fi=fleet_id, db=dispatched_by, oid=order_id)
        conn.run("COMMIT")
        return True
    except Exception as e:
        log.error(f"dispatch_order error: {e}")
        return False
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  patients — 個案（人）
# ════════════════════════════════════════════════════════════════

def get_patients(center_id: int = None, include_deleted: bool = False) -> list[dict]:
    conn = get_conn_v2()
    try:
        conditions = [] if include_deleted else ["p.deleted=FALSE"]
        if center_id:
            conditions.append(f"p.center_id={center_id}")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = conn.run(f"""
            SELECT p.id, p.name, p.phone, p.contact_phone,
                   p.emergency_contact, p.emergency_phone,
                   p.wheelchair, p.status, p.fleet, p.notes,
                   p.center_id, c.name AS center_name,
                   p.dirty, p.created_at, p.updated_at
            FROM patients p
            LEFT JOIN centers c ON c.id = p.center_id
            {where} ORDER BY p.name
        """)
        cols = ["id","name","phone","contact_phone","emergency_contact","emergency_phone",
                "wheelchair","status","fleet","notes","center_id","center_name",
                "dirty","created_at","updated_at"]
        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()


def get_patient_full(patient_id: int) -> dict | None:
    """個案 + 全部路程"""
    conn = get_conn_v2()
    try:
        rows = conn.run("""
            SELECT p.id, p.name, p.phone, p.contact_phone,
                   p.emergency_contact, p.emergency_phone,
                   p.wheelchair, p.status, p.fleet, p.notes,
                   p.center_id, c.name AS center_name
            FROM patients p
            LEFT JOIN centers c ON c.id = p.center_id
            WHERE p.id=:id
        """, id=patient_id)
        if not rows:
            return None
        cols = ["id","name","phone","contact_phone","emergency_contact","emergency_phone",
                "wheelchair","status","fleet","notes","center_id","center_name"]
        patient = _to_dict(cols, rows[0])

        # 取路程
        rrows = conn.run("""
            SELECT pr.id, pr.direction, pr.pickup_addr, pr.pickup_gps,
                   pr.dropoff_addr, pr.dropoff_gps, pr.scheduled_time,
                   pr.vehicle_no, pr.default_driver_id, d.name AS default_driver_name,
                   pr.mon, pr.tue, pr.wed, pr.thu, pr.fri, pr.sat, pr.sun,
                   pr.appsheet_key
            FROM patient_routes pr
            LEFT JOIN drivers d ON d.id = pr.default_driver_id
            WHERE pr.patient_id=:pid AND pr.deleted=FALSE
        """, pid=patient_id)
        rcols = ["id","direction","pickup_addr","pickup_gps","dropoff_addr","dropoff_gps",
                 "scheduled_time","vehicle_no","default_driver_id","default_driver_name",
                 "mon","tue","wed","thu","fri","sat","sun","appsheet_key"]
        patient["routes"] = [_to_dict(rcols, r) for r in rrows]
        return patient
    finally:
        conn.close()


def upsert_patient(data: dict) -> int:
    conn = get_conn_v2()
    try:
        if data.get("id"):
            conn.run("""
                UPDATE patients
                SET name=:n,phone=:ph,contact_phone=:cp,
                    emergency_contact=:ec,emergency_phone=:ep,
                    wheelchair=:wc,status=:st,fleet=:fl,notes=:nt,
                    center_id=:ci,dirty=TRUE
                WHERE id=:id
            """, id=data["id"], n=data["name"],
                ph=data.get("phone"), cp=data.get("contact_phone"),
                ec=data.get("emergency_contact"), ep=data.get("emergency_phone"),
                wc=data.get("wheelchair","無"), st=data.get("status"),
                fl=data.get("fleet"), nt=data.get("notes"),
                ci=data.get("center_id"))
            return data["id"]
        else:
            rows = conn.run("""
                INSERT INTO patients
                  (name,phone,contact_phone,emergency_contact,emergency_phone,
                   wheelchair,status,fleet,notes,center_id,dirty)
                VALUES (:n,:ph,:cp,:ec,:ep,:wc,:st,:fl,:nt,:ci,TRUE)
                RETURNING id
            """, n=data["name"], ph=data.get("phone"),
                cp=data.get("contact_phone"), ec=data.get("emergency_contact"),
                ep=data.get("emergency_phone"), wc=data.get("wheelchair","無"),
                st=data.get("status"), fl=data.get("fleet"),
                nt=data.get("notes"), ci=data.get("center_id"))
            return rows[0][0]
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  orders — 班表訂單
# ════════════════════════════════════════════════════════════════

def get_orders(date_from: str = None, date_to: str = None,
               driver_id: int = None, center_id: int = None,
               status: str = None) -> list[dict]:
    conn = get_conn_v2()
    try:
        conditions = ["o.deleted=FALSE"]
        params = {}
        if date_from:
            conditions.append("o.order_date >= :df")
            params["df"] = date_from
        if date_to:
            conditions.append("o.order_date <= :dt")
            params["dt"] = date_to
        if driver_id:
            conditions.append("o.driver_id = :did")
            params["did"] = driver_id
        if center_id:
            conditions.append("o.center_id = :cid")
            params["cid"] = center_id
        if status:
            conditions.append("o.status = :st")
            params["st"] = status

        where = "WHERE " + " AND ".join(conditions)
        rows = conn.run(f"""
            SELECT o.id, o.order_no, o.order_date, o.scheduled_time,
                   o.status, o.direction,
                   p.name  AS patient_name,
                   d.name  AS driver_name,
                   d.vehicle_no AS driver_vehicle,
                   c.name  AS center_name,
                   o.pickup_addr, o.pickup_gps, o.pickup_time,
                   o.dropoff_addr, o.dropoff_gps, o.dropoff_time,
                   o.vehicle_no, o.fleet,
                   o.distance_km, o.duration_min,
                   o.notes, o.signature, o.dirty
            FROM orders o
            LEFT JOIN patients p ON p.id = o.patient_id
            LEFT JOIN drivers  d ON d.id = o.driver_id
            LEFT JOIN centers  c ON c.id = o.center_id
            {where}
            ORDER BY o.order_date DESC, o.scheduled_time
        """, **params)

        cols = ["id","order_no","order_date","scheduled_time","status","direction",
                "patient_name","driver_name","driver_vehicle","center_name",
                "pickup_addr","pickup_gps","pickup_time",
                "dropoff_addr","dropoff_gps","dropoff_time",
                "vehicle_no","fleet","distance_km","duration_min",
                "notes","signature","dirty"]

        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()


def upsert_order(data: dict) -> int:
    conn = get_conn_v2()
    try:
        if data.get("id"):
            conn.run("""
                UPDATE orders
                SET status=:st, driver_id=:di, vehicle_no=:vn,
                    pickup_time=:pt, dropoff_time=:dt,
                    pickup_gps=:pg, dropoff_gps=:dg,
                    distance_km=:km, duration_min=:mn,
                    signature=:sg, notes=:nt, dirty=TRUE
                WHERE id=:id
            """, id=data["id"], st=data.get("status"),
                di=data.get("driver_id"), vn=data.get("vehicle_no"),
                pt=data.get("pickup_time"), dt=data.get("dropoff_time"),
                pg=data.get("pickup_gps"), dg=data.get("dropoff_gps"),
                km=data.get("distance_km"), mn=data.get("duration_min"),
                sg=data.get("signature"), nt=data.get("notes"))
            return data["id"]
        else:
            rows = conn.run("""
                INSERT INTO orders
                  (order_no,patient_id,route_id,center_id,driver_id,
                   order_date,scheduled_time,created_date,status,direction,
                   pickup_addr,pickup_gps,dropoff_addr,dropoff_gps,
                   vehicle_no,fleet,notes,dirty)
                VALUES
                  (:on,:pi,:ri,:ci,:di,
                   :od,:st,:cd,:ss,:dr,
                   :pa,:pg,:da,:dg,
                   :vn,:fl,:nt,TRUE)
                RETURNING id
            """, on=data.get("order_no"), pi=data.get("patient_id"),
                ri=data.get("route_id"), ci=data.get("center_id"),
                di=data.get("driver_id"),
                od=data["order_date"], st=data.get("scheduled_time"),
                cd=data.get("created_date"), ss=data.get("status","預約"),
                dr=data.get("direction"),
                pa=data.get("pickup_addr"), pg=data.get("pickup_gps"),
                da=data.get("dropoff_addr"), dg=data.get("dropoff_gps"),
                vn=data.get("vehicle_no"), fl=data.get("fleet"),
                nt=data.get("notes"))
            return rows[0][0]
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  sync_queue — 推送佇列
# ════════════════════════════════════════════════════════════════

def get_pending_queue(table_name: str = None) -> list[dict]:
    conn = get_conn_v2()
    try:
        where = "WHERE status='pending'"
        if table_name:
            where += f" AND source_table='{table_name}'"
        rows = conn.run(f"""
            SELECT id, source_table, record_id, appsheet_key,
                   operation, status, attempts, error_msg, created_at
            FROM sync_queue {where}
            ORDER BY created_at
        """)
        cols = ["id","source_table","record_id","appsheet_key",
                "operation","status","attempts","error_msg","created_at"]
        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()



def mark_synced(source_table: str, record_id: int):
    """推送成功後清除 dirty 旗標"""
    conn = get_conn_v2()
    try:
        conn.run(f"UPDATE {source_table} SET dirty=FALSE WHERE id=:id", id=record_id)
        conn.run("""
            UPDATE sync_queue SET status='done', synced_at=NOW()
            WHERE source_table=:t AND record_id=:id AND status='syncing'
        """, t=source_table, id=record_id)
    finally:
        conn.close()


def mark_sync_failed(source_table: str, record_id: int, error: str):
    conn = get_conn_v2()
    try:
        conn.run("""
            UPDATE sync_queue
            SET status='failed', error_msg=:e,
                attempts = attempts + 1
            WHERE source_table=:t AND record_id=:id AND status='syncing'
        """, t=source_table, id=record_id, e=error)
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  users — 帳號查詢
# ════════════════════════════════════════════════════════════════

def get_users() -> list[dict]:
    conn = get_conn_v2()
    try:
        rows = conn.run("""
            SELECT u.id, u.username, r.name AS role, r.label AS role_label,
                   u.driver_id, d.name AS driver_name,
                   u.center_id, c.name AS center_name,
                   u.is_active, u.last_login
            FROM users u
            JOIN roles r    ON r.id = u.role_id
            LEFT JOIN drivers d ON d.id = u.driver_id
            LEFT JOIN centers c ON c.id = u.center_id
            ORDER BY r.id, u.username
        """)
        cols = ["id","username","role","role_label","driver_id","driver_name",
                "center_id","center_name","is_active","last_login"]
        return [_to_dict(cols, r) for r in rows]
    finally:
        conn.close()
