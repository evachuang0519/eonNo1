"""
appsheet_assembler.py
將本機正規化資料表組裝成 AppSheet 格式，供推送同步使用。

每個 assemble_* 函式回傳 list[dict]，格式與 AppSheet API 相同。
"""

from database_v2 import get_conn_v2


def _conn():
    return get_conn_v2()


# ════════════════════════════════════════════════════════════════
#  日照名單（centers → AppSheet 日照名單）
# ════════════════════════════════════════════════════════════════

def assemble_centers(dirty_only: bool = True) -> list[dict]:
    """
    AppSheet 欄位：據點名稱, 據點帳號, 登入密碼, Email
    """
    conn = _conn()
    try:
        where = "WHERE c.deleted=FALSE" + (" AND c.dirty=TRUE" if dirty_only else "")
        rows = conn.run(f"""
            SELECT c.name, c.email,
                   u.username AS acct,
                   u.password_hash AS pwd_hash
            FROM centers c
            LEFT JOIN users u ON u.center_id = c.id
            {where}
        """)
        result = []
        for name, email, acct, pwd_hash in rows:
            result.append({
                "據點名稱":  name or "",
                "據點帳號":  acct or "",
                "登入密碼":  "",          # 明碼不回寫；同步時帶空值
                "Email":     email or "",
            })
        return result
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  司機（drivers → AppSheet 司機 + 司機名單）
# ════════════════════════════════════════════════════════════════

def assemble_drivers(dirty_only: bool = True) -> list[dict]:
    """AppSheet 司機欄位：姓名, Email, 照片, 聯絡電話, 車號, 車輛照片, 備註"""
    conn = _conn()
    try:
        where = "WHERE deleted=FALSE" + (" AND dirty=TRUE" if dirty_only else "")
        rows = conn.run(f"""
            SELECT name, email, phone, vehicle_no, photo_url, vehicle_photo_url, notes
            FROM drivers {where}
        """)
        return [{
            "姓名":   r[0] or "",
            "Email":  r[1] or "",
            "照片":   r[4] or "",
            "聯絡電話": r[2] or "",
            "車號":   r[3] or "",
            "車輛照片": r[5] or "",
            "備註":   r[6] or "",
        } for r in rows]
    finally:
        conn.close()


def assemble_driver_accounts(dirty_only: bool = True) -> list[dict]:
    """AppSheet 司機名單欄位：司機帳號, 登入密碼, 司機姓名, Email, 車號"""
    conn = _conn()
    try:
        where = "WHERE d.deleted=FALSE" + (" AND d.dirty=TRUE" if dirty_only else "")
        rows = conn.run(f"""
            SELECT u.username, d.name, d.email, d.vehicle_no
            FROM drivers d
            LEFT JOIN users u ON u.driver_id = d.id
            {where}
        """)
        return [{
            "司機帳號": r[0] or "",
            "登入密碼": "",          # 明碼不回寫
            "司機姓名": r[1] or "",
            "Email":   r[2] or "",
            "車號":    r[3] or "",
        } for r in rows]
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  個案總表（patients + patient_routes → AppSheet 個案總表）
# ════════════════════════════════════════════════════════════════

def assemble_cases(dirty_only: bool = True) -> list[dict]:
    """
    AppSheet 個案總表欄位（完整）：
    姓名路程, 日照中心, 連絡電話, 服務狀態, 乘客姓名, 輪椅, 備註,
    表定搭乘時間, 路線, 上車地點, 下車地點, 乘客電話,
    緊急聯絡人, 聯絡人電話, 預設服務司機, 車號, 所屬車隊,
    週一, 週二, 週三, 週四, 週五, 週六, 週日
    """
    conn = _conn()
    try:
        dirty_clause = " AND (p.dirty=TRUE OR pr.dirty=TRUE)" if dirty_only else ""
        rows = conn.run(f"""
            SELECT
              pr.appsheet_key,
              c.name         AS center,
              p.contact_phone,
              p.status,
              p.name         AS patient_name,
              p.wheelchair,
              p.notes,
              pr.scheduled_time,
              pr.direction,
              pr.pickup_addr,
              pr.dropoff_addr,
              p.phone        AS patient_phone,
              p.emergency_contact,
              p.emergency_phone,
              d.name         AS default_driver,
              pr.vehicle_no,
              p.fleet,
              pr.mon, pr.tue, pr.wed, pr.thu, pr.fri, pr.sat, pr.sun
            FROM patient_routes pr
            JOIN patients p  ON p.id  = pr.patient_id
            LEFT JOIN centers c ON c.id = p.center_id
            LEFT JOIN drivers d ON d.id = pr.default_driver_id
            WHERE p.deleted=FALSE AND pr.deleted=FALSE
            {dirty_clause}
        """)

        def yn(v):
            return "Y" if v else ""

        result = []
        for r in rows:
            result.append({
                "姓名路程":      r[0]  or f"{r[4]}{r[8]}",
                "日照中心":      r[1]  or "",
                "連絡電話":      r[2]  or "",
                "服務狀態":      r[3]  or "",
                "乘客姓名":      r[4]  or "",
                "輪椅":          r[5]  or "無",
                "備註":          r[6]  or "",
                "表定搭乘時間":  str(r[7]) if r[7] else "",
                "路線":          r[8]  or "",
                "上車地點":      r[9]  or "",
                "下車地點":      r[10] or "",
                "乘客電話":      r[11] or "",
                "緊急聯絡人":    r[12] or "",
                "聯絡人電話":    r[13] or "",
                "預設服務司機":  r[14] or "",
                "車號":          r[15] or "",
                "所屬車隊":      r[16] or "",
                "週一": yn(r[17]), "週二": yn(r[18]),
                "週三": yn(r[19]), "週四": yn(r[20]),
                "週五": yn(r[21]), "週六": yn(r[22]),
                "週日": yn(r[23]),
            })
        return result
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  日照班表（orders → AppSheet 日照班表）
# ════════════════════════════════════════════════════════════════

def _fmt_date_mdy(d) -> str:
    """date 物件或 YYYY-MM-DD 字串 → MM/DD/YYYY"""
    if d is None:
        return ""
    s = str(d)
    if len(s) == 10 and s[4] == "-":
        y, m, dd = s.split("-")
        return f"{m}/{dd}/{y}"
    return s


def assemble_orders(dirty_only: bool = True) -> list[dict]:
    """
    AppSheet 日照班表欄位（完整）
    """
    conn = _conn()
    try:
        dirty_clause = " AND o.dirty=TRUE" if dirty_only else ""
        rows = conn.run(f"""
            SELECT
              o.order_no,
              o.status,
              c.name        AS center,
              o.created_date,
              o.order_date,
              o.scheduled_time,
              d.vehicle_no  AS drv_vehicle,
              d.name        AS driver_name,
              o.vehicle_no,
              o.direction,
              p.name        AS patient_name,
              o.notes,
              o.pickup_addr,
              o.dropoff_addr,
              o.pickup_time,
              o.dropoff_time,
              o.pickup_gps,
              o.dropoff_gps,
              o.distance_km,
              o.duration_min,
              o.signature,
              o.center_phone,
              o.passenger_phone,
              o.fleet,
              o.passenger_photo
            FROM orders o
            LEFT JOIN patients p ON p.id = o.patient_id
            LEFT JOIN centers  c ON c.id = o.center_id
            LEFT JOIN drivers  d ON d.id = o.driver_id
            WHERE o.deleted=FALSE
            {dirty_clause}
        """)

        result = []
        for r in rows:
            result.append({
                "訂單編號(Task ID)": r[0]  or "",
                "訂單狀態":         r[1]  or "預約",
                "日照中心":         r[2]  or "",
                "建單日期":         _fmt_date_mdy(r[3]),
                "搭乘日期":         _fmt_date_mdy(r[4]),
                "搭乘時間":         str(r[5]) if r[5] else "",
                "服務車隊":         r[23] or "",
                "服務司機":         r[7]  or "",
                "車號":             r[8]  or (r[6] or ""),
                "路線":             r[9]  or "",
                "乘客姓名":         r[10] or "",
                "備註":             r[11] or "",
                "上車地址":         r[12] or "",
                "下車地點":         r[13] or "",
                "上車時間":         str(r[14]) if r[14] else "",
                "下車時間":         str(r[15]) if r[15] else "",
                "上車座標":         r[16] or "",
                "下車座標":         r[17] or "",
                "運輸距離(公里)":   str(r[18]) if r[18] else "",
                "運輸時間(分鐘)":   str(r[19]) if r[19] else "",
                "簽名":             r[20] or "",
                "日照中心電話":     r[21] or "",
                "乘客聯絡人電話":   r[22] or "",
                "乘客頭像":         r[24] or "",
                "空值":             "",
            })
        return result
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════
#  統一入口：依資料表名稱取得組裝結果
# ════════════════════════════════════════════════════════════════

ASSEMBLERS = {
    "日照名單":  assemble_centers,
    "司機":      assemble_drivers,
    "司機名單":  assemble_driver_accounts,
    "個案總表":  assemble_cases,
    "日照班表":  assemble_orders,
}


def assemble(table_name: str, dirty_only: bool = True) -> list[dict]:
    fn = ASSEMBLERS.get(table_name)
    if fn is None:
        raise ValueError(f"無對應的組裝器：{table_name}")
    return fn(dirty_only=dirty_only)


def assemble_all(dirty_only: bool = True) -> dict[str, list[dict]]:
    return {name: fn(dirty_only=dirty_only) for name, fn in ASSEMBLERS.items()}
