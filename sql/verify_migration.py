# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
verify_migration.py — 驗證遷移結果
執行方式：python sql/verify_migration.py
"""
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
import pg8000.native

def get_conn():
    return pg8000.native.Connection(
        host=PG_HOST, port=PG_PORT, database=PG_DB,
        user=PG_USER, password=PG_PASS
    )

def main():
    conn = get_conn()
    print("\n=== 遷移驗證報告 ===\n")

    checks = [
        ("centers",         "日照中心",  "SELECT COUNT(*) FROM centers"),
        ("drivers",         "司機",      "SELECT COUNT(*) FROM drivers"),
        ("patients",        "個案（人）", "SELECT COUNT(*) FROM patients"),
        ("patient_routes",  "個案路程",  "SELECT COUNT(*) FROM patient_routes"),
        ("orders",          "班表訂單",  "SELECT COUNT(*) FROM orders"),
        ("users",           "使用者帳號", "SELECT COUNT(*) FROM users"),
        ("roles",           "角色",      "SELECT COUNT(*) FROM roles"),
        ("permissions",     "功能點",    "SELECT COUNT(*) FROM permissions"),
        ("role_permissions","角色權限",  "SELECT COUNT(*) FROM role_permissions"),
    ]

    all_ok = True
    for table, label, sql in checks:
        try:
            count = conn.run(sql)[0][0]
            status = "✓" if count > 0 else "⚠"
            if count == 0 and table not in ('sync_queue',):
                all_ok = False
            print(f"  {status}  {label:<12} ({table}): {count} 筆")
        except Exception as e:
            print(f"  ✗  {label:<12} 查詢失敗：{e}")
            all_ok = False

    # 交叉驗證
    print("\n--- 交叉驗證 ---")

    # orders 有 patient_id 的比率
    r = conn.run("SELECT COUNT(*) FROM orders WHERE patient_id IS NOT NULL")[0][0]
    total = conn.run("SELECT COUNT(*) FROM orders")[0][0]
    pct = round(r/total*100, 1) if total else 0
    print(f"  訂單有個案對應：{r}/{total} ({pct}%)")

    # orders 有 driver_id 的比率
    r2 = conn.run("SELECT COUNT(*) FROM orders WHERE driver_id IS NOT NULL")[0][0]
    pct2 = round(r2/total*100, 1) if total else 0
    print(f"  訂單有司機對應：{r2}/{total} ({pct2}%)")

    # patient_routes 有 default_driver 的比率
    rt = conn.run("SELECT COUNT(*) FROM patient_routes")[0][0]
    rd = conn.run("SELECT COUNT(*) FROM patient_routes WHERE default_driver_id IS NOT NULL")[0][0]
    pct3 = round(rd/rt*100, 1) if rt else 0
    print(f"  路程有預設司機：{rd}/{rt} ({pct3}%)")

    # 無法對應的個案姓名（orders 中有名字但 patient_id 為 NULL）
    orphan = conn.run("""
        SELECT DISTINCT o.order_no, o.notes
        FROM orders o
        WHERE o.patient_id IS NULL
        LIMIT 5
    """)
    if orphan:
        print(f"\n  ⚠ 有 {len(orphan)} 筆訂單找不到個案（前5筆）：")
        for row in orphan:
            print(f"    {row[0]}")

    # 使用者角色分佈
    print("\n--- 使用者角色分佈 ---")
    role_dist = conn.run("""
        SELECT r.label, COUNT(u.id)
        FROM roles r
        LEFT JOIN users u ON u.role_id = r.id
        GROUP BY r.id, r.label
        ORDER BY r.id
    """)
    for row in role_dist:
        print(f"  {row[0]}: {row[1]} 人")

    print()
    if all_ok:
        print("✅ 驗證通過，可以切換 API 至新資料表")
    else:
        print("⚠  部分資料表為空，請確認遷移是否正確執行")

    conn.close()

if __name__ == "__main__":
    main()
