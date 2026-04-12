# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
migrate_v2.py — 資料遷移：JSONB records → 正規化資料表 (v2)
執行方式：python sql/migrate_v2.py
注意：
  1. 請先備份資料庫：pg_dump rizhaodb > rizhaodb_backup.sql
  2. 本機 Flask server 可以繼續運作（只讀 records 表）
  3. 遷移完成後新資料表為空，需確認後才切換 API 指向新表
"""

import sys
import os
import re
import hashlib
import time

# 讓 script 能找到 backend 模組
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pg8000.native
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

# ── 連線 ──────────────────────────────────────────────────────────────────────

def get_conn():
    return pg8000.native.Connection(
        host=PG_HOST, port=PG_PORT, database=PG_DB,
        user=PG_USER, password=PG_PASS,
        application_name="migrate_v2"
    )

# ── 讀取 JSONB records ─────────────────────────────────────────────────────────

def load_table(conn, table_name: str) -> list[dict]:
    rows = conn.run(
        "SELECT row_key, data FROM records WHERE table_name=:t",
        t=table_name
    )
    result = []
    for row_key, data in rows:
        if isinstance(data, str):
            import json
            data = json.loads(data)
        data['_row_key'] = row_key
        result.append(data)
    return result

def yn_bool(val) -> bool:
    """AppSheet Y/N → Python bool"""
    return str(val or '').upper() in ('Y', 'YES', 'TRUE', '1')

def safe_time(val) -> str | None:
    """各種時間格式 → HH:MM:SS"""
    if not val:
        return None
    s = str(val).strip()
    # HH:MM:SS or HH:MM
    if re.match(r'^\d{1,2}:\d{2}(:\d{2})?$', s):
        return s if s.count(':') == 2 else s + ':00'
    return None

def safe_date(val) -> str | None:
    """MM/DD/YYYY 或 YYYY/MM/DD → YYYY-MM-DD"""
    if not val:
        return None
    s = str(val).strip()
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    m2 = re.match(r'^(\d{4})/(\d{1,2})/(\d{1,2})$', s)
    if m2:
        return f"{m2.group(1)}-{m2.group(2).zfill(2)}-{m2.group(3).zfill(2)}"
    m3 = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
    if m3:
        return s
    return None

def safe_decimal(val) -> float | None:
    try:
        return float(val) if val not in (None, '', ' ') else None
    except (ValueError, TypeError):
        return None

def safe_int(val) -> int | None:
    """字串數字（含 "0.0" 浮點格式）→ int"""
    try:
        if val in (None, '', ' '):
            return None
        return int(float(val))
    except (ValueError, TypeError):
        return None

def hash_password(plain: str) -> str:
    """簡易 SHA-256 雜湊（正式環境請改用 bcrypt）"""
    return hashlib.sha256(plain.encode()).hexdigest()

# ═══════════════════════════════════════════════════════════════════════════════
#  Step 1：建立 Schema（執行 schema_v2.sql）
# ═══════════════════════════════════════════════════════════════════════════════

def step1_create_schema(conn):
    """用 psql 執行 schema_v2.sql（pg8000 不支援多語句）"""
    import subprocess
    print("\n[Step 1] 建立正規化資料表結構...")
    sql_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'schema_v2.sql'))
    env = os.environ.copy()
    env['PGPASSWORD'] = PG_PASS
    result = subprocess.run(
        ['psql', '-h', PG_HOST, '-p', str(PG_PORT),
         '-U', PG_USER, '-d', PG_DB, '-f', sql_path,
         '-v', 'ON_ERROR_STOP=0'],
        capture_output=True, text=True, encoding='utf-8', env=env
    )
    if result.returncode != 0 and 'ERROR' in (result.stderr or ''):
        # 只顯示真正的錯誤（非 already exists 類）
        errs = [l for l in result.stderr.splitlines()
                if 'ERROR' in l and 'already exists' not in l and 'duplicate' not in l.lower()]
        if errs:
            print(f"  [警告] schema 執行時有錯誤：")
            for e in errs[:5]:
                print(f"    {e}")
        else:
            print("  schema 已存在，略過重複建立")
    print("  [OK] schema 建立/確認完成")

# ═══════════════════════════════════════════════════════════════════════════════
#  Step 2：遷移 日照名單 → centers
# ═══════════════════════════════════════════════════════════════════════════════

def step2_centers(conn) -> dict:
    """回傳 {name: id}"""
    print("\n[Step 2] 遷移 日照名單 → centers...")
    rows = load_table(conn, '日照名單')
    center_map = {}
    for r in rows:
        name = (r.get('據點名稱') or '').strip()
        if not name:
            continue
        conn.run("""
            INSERT INTO centers (name, email, appsheet_key)
            VALUES (:n, :e, :k)
            ON CONFLICT (name) DO UPDATE SET email=EXCLUDED.email
            RETURNING id
        """, n=name, e=r.get('Email') or None, k=name)
        row = conn.run("SELECT id FROM centers WHERE name=:n", n=name)
        center_map[name] = row[0][0]
        print(f"  ✓ {name}  id={center_map[name]}")
    print(f"  共 {len(center_map)} 筆")
    return center_map

# ═══════════════════════════════════════════════════════════════════════════════
#  Step 3：遷移 司機 + 司機名單 → drivers
# ═══════════════════════════════════════════════════════════════════════════════

def step3_drivers(conn) -> dict:
    """回傳 {name: id}"""
    print("\n[Step 3] 遷移 司機 + 司機名單 → drivers...")
    driver_basic  = {r.get('姓名','').strip(): r for r in load_table(conn, '司機')}
    driver_acct   = {r.get('司機姓名','').strip(): r for r in load_table(conn, '司機名單')}
    all_names = set(driver_basic) | set(driver_acct)
    driver_map = {}
    for name in sorted(all_names):
        if not name:
            continue
        b = driver_basic.get(name, {})
        a = driver_acct.get(name, {})
        conn.run("""
            INSERT INTO drivers (name, phone, email, vehicle_no, photo_url, vehicle_photo_url, appsheet_key)
            VALUES (:n, :ph, :em, :vn, :pu, :vpu, :k)
            ON CONFLICT DO NOTHING
        """,
            n=name,
            ph=b.get('聯絡電話') or a.get('Email') or None,
            em=b.get('Email')    or a.get('Email')  or None,
            vn=b.get('車號')     or a.get('車號')    or None,
            pu=b.get('照片')     or None,
            vpu=b.get('車輛照片') or None,
            k=name
        )
        row = conn.run("SELECT id FROM drivers WHERE name=:n", n=name)
        driver_map[name] = row[0][0]
        print(f"  ✓ {name}  id={driver_map[name]}")
    print(f"  共 {len(driver_map)} 筆")
    return driver_map

# ═══════════════════════════════════════════════════════════════════════════════
#  Step 4：遷移 個案總表 → patients + patient_routes
# ═══════════════════════════════════════════════════════════════════════════════

def step4_patients(conn, center_map: dict, driver_map: dict) -> dict:
    """回傳 {姓名路程: route_id}"""
    print("\n[Step 4] 遷移 個案總表 → patients + patient_routes...")
    rows = load_table(conn, '個案總表')
    patient_map = {}   # {乘客姓名: patient_id}
    route_map   = {}   # {姓名路程: route_id}

    # 先按姓名分組，確保同一個人只建一筆 patient
    by_name = {}
    for r in rows:
        name = (r.get('乘客姓名') or '').strip()
        if name:
            by_name.setdefault(name, []).append(r)

    for name, person_rows in sorted(by_name.items()):
        # 取任意一筆的共用欄位
        rep = person_rows[0]
        center_name = (rep.get('日照中心') or '').strip()
        center_id   = center_map.get(center_name)

        # 服務狀態 mapping（正規化到 ENUM）
        status_raw = (rep.get('服務狀態') or '').strip()
        status_map = {
            '服務中': '服務中', '待確認': '待確認',
            '停止服務': '停止服務', '自行接送': '自行接送',
            '其他車隊': '其他車隊'
        }
        status = status_map.get(status_raw)

        conn.run("""
            INSERT INTO patients
              (name, phone, contact_phone, emergency_contact, emergency_phone,
               wheelchair, status, fleet, notes, center_id)
            VALUES (:n,:ph,:cp,:ec,:ep,:wc,:st,:fl,:nt,:ci)
            ON CONFLICT DO NOTHING
        """,
            n=name,
            ph=rep.get('乘客電話') or None,
            cp=rep.get('連絡電話') or None,
            ec=rep.get('緊急聯絡人') or None,
            ep=rep.get('聯絡人電話') or None,
            wc=rep.get('輪椅') or '無',
            st=status,
            fl=rep.get('所屬車隊') or None,
            nt=rep.get('備註') or None,
            ci=center_id
        )
        pid_row = conn.run("SELECT id FROM patients WHERE name=:n", n=name)
        patient_id = pid_row[0][0]
        patient_map[name] = patient_id

        # 為每個路程建 patient_routes
        for r in person_rows:
            direction_raw = (r.get('路線') or '').strip()
            if direction_raw not in ('去程', '回程'):
                print(f"    ⚠ {name} 路線未知：{direction_raw!r}，略過")
                continue
            appsheet_key = (r.get('姓名路程') or '').strip()
            default_driver = (r.get('預設服務司機') or '').strip()
            driver_id = driver_map.get(default_driver)

            conn.run("""
                INSERT INTO patient_routes
                  (patient_id, direction, pickup_addr, pickup_gps,
                   dropoff_addr, dropoff_gps, scheduled_time,
                   vehicle_no, default_driver_id,
                   mon, tue, wed, thu, fri, sat, sun, appsheet_key)
                VALUES
                  (:pi,:di,:pa,:pg,:da,:dg,:st,
                   :vn,:dd,
                   :mo,:tu,:we,:th,:fr,:sa,:su,:ak)
                ON CONFLICT (patient_id, direction) DO UPDATE
                  SET pickup_addr=EXCLUDED.pickup_addr,
                      pickup_gps=EXCLUDED.pickup_gps,
                      dropoff_addr=EXCLUDED.dropoff_addr,
                      dropoff_gps=EXCLUDED.dropoff_gps,
                      scheduled_time=EXCLUDED.scheduled_time,
                      vehicle_no=EXCLUDED.vehicle_no,
                      default_driver_id=EXCLUDED.default_driver_id,
                      mon=EXCLUDED.mon, tue=EXCLUDED.tue,
                      wed=EXCLUDED.wed, thu=EXCLUDED.thu,
                      fri=EXCLUDED.fri, sat=EXCLUDED.sat,
                      sun=EXCLUDED.sun
            """,
                pi=patient_id,
                di=direction_raw,
                pa=r.get('上車地點') or None,
                pg=None,  # 個案總表無座標，班表才有
                da=r.get('下車地點') or None,
                dg=None,
                st=safe_time(r.get('表定搭乘時間')),
                vn=r.get('車號') or None,
                dd=driver_id,
                mo=yn_bool(r.get('週一')), tu=yn_bool(r.get('週二')),
                we=yn_bool(r.get('週三')), th=yn_bool(r.get('週四')),
                fr=yn_bool(r.get('週五')), sa=yn_bool(r.get('週六')),
                su=yn_bool(r.get('週日')),
                ak=appsheet_key
            )
            rid = conn.run(
                "SELECT id FROM patient_routes WHERE patient_id=:pi AND direction=:di",
                pi=patient_id, di=direction_raw
            )
            if rid:
                route_map[appsheet_key] = rid[0][0]

    print(f"  患者 {len(patient_map)} 人，路程 {len(route_map)} 筆")
    return route_map, patient_map

# ═══════════════════════════════════════════════════════════════════════════════
#  Step 5：遷移 日照班表 → orders
# ═══════════════════════════════════════════════════════════════════════════════

def step5_orders(conn, center_map, driver_map, patient_map, route_map):
    print("\n[Step 5] 遷移 日照班表 → orders...")
    rows = load_table(conn, '日照班表')
    ok = 0
    skip = 0
    for r in rows:
        order_no     = (r.get('訂單編號(Task ID)') or '').strip()
        patient_name = (r.get('乘客姓名') or '').strip()
        driver_name  = (r.get('服務司機') or '').strip()
        center_name  = (r.get('日照中心') or '').strip()
        direction_raw= (r.get('路線') or '').strip()

        patient_id = patient_map.get(patient_name)
        driver_id  = driver_map.get(driver_name)
        center_id  = center_map.get(center_name)
        # 找對應路程
        if patient_name and direction_raw in ('去程','回程'):
            route_id = route_map.get(f"{patient_name}{direction_raw}")
        else:
            route_id = None

        # 訂單狀態 mapping
        status_raw = (r.get('訂單狀態') or '').strip()
        status_map = {'預約':'預約','客上':'客上','已完成':'已完成','請假':'請假','取消':'取消'}
        status = status_map.get(status_raw, '預約')
        direction = direction_raw if direction_raw in ('去程','回程') else None

        order_date = safe_date(r.get('搭乘日期'))
        if not order_date:
            skip += 1
            continue

        conn.run("""
            INSERT INTO orders
              (order_no, patient_id, route_id, center_id, driver_id,
               order_date, scheduled_time, created_date, status, direction,
               pickup_addr, pickup_gps, pickup_time,
               dropoff_addr, dropoff_gps, dropoff_time,
               vehicle_no, fleet,
               distance_km, duration_min,
               signature, passenger_photo,
               center_phone, passenger_phone, notes, appsheet_key)
            VALUES
              (:on,:pi,:ri,:ci,:di,
               :od,:st,:cd,:ss,:dr,
               :pa,:pg,:pt,
               :da,:dg,:dt,
               :vn,:fl,
               :km,:mn,
               :sg,:pp,:cp,:pp2,:nt,:ak)
            ON CONFLICT (order_no) DO NOTHING
        """,
            on=order_no or None,
            pi=patient_id, ri=route_id, ci=center_id, di=driver_id,
            od=order_date,
            st=safe_time(r.get('搭乘時間')),
            cd=safe_date(r.get('建單日期')),
            ss=status, dr=direction,
            pa=r.get('上車地址') or None,
            pg=r.get('上車座標') or None,
            pt=safe_time(r.get('上車時間')),
            da=r.get('下車地點') or None,
            dg=r.get('下車座標') or None,
            dt=safe_time(r.get('下車時間')),
            vn=r.get('車號') or None,
            fl=r.get('服務車隊') or None,
            km=safe_decimal(r.get('運輸距離(公里)')),
            mn=safe_int(r.get('運輸時間(分鐘)')),
            sg=r.get('簽名') or None,
            pp=r.get('乘客頭像') or None,
            cp=r.get('日照中心電話') or None,
            pp2=r.get('乘客聯絡人電話') or None,
            nt=r.get('備註') or None,
            ak=order_no or None
        )
        ok += 1
    print(f"  ✓ 成功 {ok} 筆，跳過（日期格式錯誤）{skip} 筆")

# ═══════════════════════════════════════════════════════════════════════════════
#  Step 6：遷移帳號 → users
# ═══════════════════════════════════════════════════════════════════════════════

def step6_users(conn, center_map, driver_map):
    print("\n[Step 6] 遷移帳號 → users...")

    # 取角色 id
    roles = {r[0]: r[1] for r in conn.run("SELECT name, id FROM roles")}
    r_admin  = roles.get('admin')
    r_center = roles.get('center')
    r_driver = roles.get('driver')

    # ── 日照中心帳號 ──────────────────────────────────────────────────────────
    center_accts = load_table(conn, '日照名單')
    for r in center_accts:
        center_name = (r.get('據點名稱') or '').strip()
        username    = (r.get('據點帳號') or center_name).strip()
        password    = (r.get('登入密碼') or '').strip()
        if not username or not center_name:
            continue
        center_id = center_map.get(center_name)
        conn.run("""
            INSERT INTO users (username, password_hash, role_id, center_id)
            VALUES (:u, :p, :r, :c)
            ON CONFLICT (username) DO NOTHING
        """, u=username, p=hash_password(password), r=r_center, c=center_id)
        print(f"  ✓ center 帳號：{username}（{center_name}）")

    # ── 司機帳號 ─────────────────────────────────────────────────────────────
    driver_accts = load_table(conn, '司機名單')
    for r in driver_accts:
        username  = (r.get('司機帳號') or '').strip()
        drv_name  = (r.get('司機姓名') or '').strip()
        password  = (r.get('登入密碼') or '').strip()
        if not username:
            continue
        driver_id = driver_map.get(drv_name or username)
        conn.run("""
            INSERT INTO users (username, password_hash, role_id, driver_id)
            VALUES (:u, :p, :r, :d)
            ON CONFLICT (username) DO NOTHING
        """, u=username, p=hash_password(password), r=r_driver, d=driver_id)
        print(f"  ✓ driver 帳號：{username}（{drv_name}）")

    # ── 建立預設 admin ────────────────────────────────────────────────────────
    conn.run("""
        INSERT INTO users (username, password_hash, role_id)
        VALUES ('admin', :p, :r)
        ON CONFLICT (username) DO NOTHING
    """, p=hash_password('admin1234'), r=r_admin)
    print("  ✓ admin 帳號建立（預設密碼 admin1234，請立即修改）")

# ═══════════════════════════════════════════════════════════════════════════════
#  主程式
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  日照交通服務系統 — 資料庫遷移 v2")
    print("  JSONB records → 正規化資料表")
    print("=" * 60)

    conn = get_conn()
    try:
        conn.run("BEGIN")

        step1_create_schema(conn)
        center_map              = step2_centers(conn)
        driver_map              = step3_drivers(conn)
        route_map, patient_map  = step4_patients(conn, center_map, driver_map)
        step5_orders(conn, center_map, driver_map, patient_map, route_map)
        step6_users(conn, center_map, driver_map)

        conn.run("COMMIT")
        print("\n" + "=" * 60)
        print("  ✅ 遷移完成！")
        print()
        print("  下一步：")
        print("  1. 檢查各資料表筆數是否正確")
        print("  2. 執行 python sql/verify_migration.py 驗證")
        print("  3. 確認無誤後切換 API 指向新資料表")
        print("=" * 60)

    except Exception as e:
        conn.run("ROLLBACK")
        print(f"\n❌ 遷移失敗，已 ROLLBACK：{e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
