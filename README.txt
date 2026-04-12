================================================================
  日照交通服務系統 — 本機管理後台
  架構：Python Flask（前後端合一）+ PostgreSQL 17
  最後更新：2026-04-12
================================================================

【版本歷史】

  v2.0（2026-04-12）
    · 資料庫正規化：JSONB records 表 → 11 張關聯式資料表
    · 新增角色權限系統（roles / permissions / role_permissions）
    · 同步方向改為推送模式（本機 → AppSheet，手動觸發）
    · 移除 APScheduler 自動拉取
    · 新增 sync_engine_v2.py 推送同步引擎
    · 新增 /api/v2/* 正規化 CRUD API
    · 新增 appsheet_assembler.py 資料組裝器

  v1.0（2026-04-06）
    · 初始版本，JSONB 資料表 + APScheduler 每 30 秒拉取
    · Python Flask 取代 Classic ASP 前端

================================================================
【檔案結構】
================================================================

  backend/
    app.py                  主程式（頁面路由 + API + SSE）
    config.py               AppSheet 金鑰、PostgreSQL 連線、資料表設定
    database.py             舊版 JSONB records 表操作（向下相容保留）
    database_v2.py          正規化資料表 CRUD（v2 主要使用）
    appsheet_client.py      AppSheet REST API v2 封裝
    appsheet_assembler.py   將正規化資料組裝成 AppSheet 格式
    sync_engine.py          舊版同步引擎（保留備用）
    sync_engine_v2.py       新版推送同步引擎（本機 → AppSheet）
    requirements.txt        Python 套件清單
    templates/
      base.html             共用版型（CSS、側邊欄、Modal、SSE、共用 JS）
      index.html            總覽儀表板
      today.html            今日班表
      schedule.html         班表總表（日期範圍 / 司機 / 據點篩選）
      map.html              路線圖（Google Maps）
      stats.html            服務統計
      cases.html            個案管理
      table.html            通用資料表瀏覽／CRUD
      synclog.html          同步管理（推送狀態 / 進度 / 日誌）

  sql/
    schema.sql              舊版 JSONB 資料表定義（參考用）
    schema_v2.sql           正規化資料表定義（v2 正式使用）
    migrate_v2.py           JSONB → 正規化資料表遷移腳本
    verify_migration.py     遷移結果驗證腳本

  start_backend.bat         啟動伺服器
  appsheet_schema.txt       AppSheet 欄位結構說明
  README.txt                本文件

================================================================
【啟動步驟】
================================================================

1. 啟動伺服器
   ──────────
   雙擊 start_backend.bat
   或執行：cd backend && python app.py

   → 啟動後直接可用，不再自動同步（v2 改為手動推送）
   → 資料已全數存於本機 PostgreSQL

2. 開啟瀏覽器
   ──────────
   http://localhost:8000

================================================================
【環境需求】
================================================================

  Python 3.10+        ✓
  PostgreSQL 17       ✓（已建立 rizhaodb）
  pip 套件            flask, pg8000, requests
                      （v2 不再需要 apscheduler）

================================================================
【系統架構】
================================================================

  ┌─────────────────────────────────────────────────────┐
  │              AppSheet（雲端）                        │
  │  日照班表 / 司機 / 個案總表 / 日照名單 ...           │
  └─────────────────────┬───────────────────────────────┘
                        │ 手動推送（sync_engine_v2）
                        │ POST /api/sync/trigger
                        ▲ （本機 → AppSheet，dirty 記錄）
  ┌─────────────────────────────────────────────────────┐
  │    Python Flask（Port 8000）                         │
  │    ├── 頁面路由（Jinja2 模板渲染）                   │
  │    │     /            → 總覽儀表板                   │
  │    │     /today       → 今日班表                     │
  │    │     /schedule    → 班表總表                     │
  │    │     /map         → 路線圖（Google Maps）         │
  │    │     /stats       → 服務統計                     │
  │    │     /cases       → 個案管理                     │
  │    │     /table/<名稱> → 通用資料表                  │
  │    │     /synclog     → 同步管理                     │
  │    ├── REST API（/api/* 舊版 + /api/v2/* 新版）      │
  │    ├── SSE 即時推播（/events）                       │
  │    └── Webhook 接收（/webhook）                      │
  └──────────┬──────────────────────────────────────────┘
             │ pg8000
             ▼
  ┌──────────────────────────────────────────────────┐
  │ PostgreSQL 17（rizhaodb）                         │
  │                                                  │
  │  舊版（保留向下相容）                             │
  │    records (JSONB)   sync_log                    │
  │                                                  │
  │  正規化 v2（主要使用）                            │
  │    centers           — 日照中心（5筆）            │
  │    drivers           — 司機（8筆）                │
  │    patients          — 個案人員（61筆）           │
  │    patient_routes    — 個案路程（122筆）          │
  │    orders            — 班表訂單（612筆）          │
  │    roles             — 角色（4種）               │
  │    permissions       — 功能權限（20項）           │
  │    role_permissions  — 角色權限對應              │
  │    users             — 帳號（11筆）              │
  │    sync_queue        — 推送佇列                  │
  │    sync_log          — 同步日誌                  │
  └──────────────────────────────────────────────────┘

================================================================
【API 端點一覽】
================================================================

  系統
    GET    /health                    健康檢查
    GET    /events                    SSE 即時事件串流
    POST   /webhook                   AppSheet Automation 推播

  同步（推送方向：本機 → AppSheet）
    GET    /api/sync/status           推送狀態（dirty 筆數 + 日誌）
    GET    /api/sync/dirty            各資料表待推送筆數
    POST   /api/sync/trigger          全量推送（所有 dirty 記錄）
    POST   /api/sync/one/<table>      單一資料表推送

  資料表（舊版 JSONB，向下相容）
    GET    /api/tables                所有資料表（含筆數）
    GET    /api/<table>               列出資料（?search=關鍵字）
    GET    /api/<table>/<key>         單筆資料
    POST   /api/<table>               新增
    PUT    /api/<table>/<key>         修改
    DELETE /api/<table>/<key>         刪除

  正規化 API v2
    GET    /api/v2/centers            日照中心列表
    POST   /api/v2/centers            新增日照中心
    PUT    /api/v2/centers/<id>       修改日照中心
    DELETE /api/v2/centers/<id>       刪除日照中心

    GET    /api/v2/drivers            司機列表
    POST   /api/v2/drivers            新增司機
    PUT    /api/v2/drivers/<id>       修改司機
    DELETE /api/v2/drivers/<id>       刪除司機

    GET    /api/v2/patients           個案列表（?center_id=N）
    GET    /api/v2/patients/<id>      單筆個案（含路程）
    POST   /api/v2/patients           新增個案
    PUT    /api/v2/patients/<id>      修改個案
    DELETE /api/v2/patients/<id>      刪除個案

    GET    /api/v2/orders             訂單列表
                                      ?from=YYYY-MM-DD&to=YYYY-MM-DD
                                      &driver_id=N&center_id=N&status=狀態
    POST   /api/v2/orders             新增訂單
    PUT    /api/v2/orders/<id>        修改訂單
    DELETE /api/v2/orders/<id>        刪除訂單

    GET    /api/v2/users              使用者帳號列表

  班表專用
    GET    /api/schedule/today        今日班表
    GET    /api/schedule/range        日期範圍查詢
                                      ?from=MM/DD/YYYY&to=MM/DD/YYYY
                                      &driver=司機&center=據點
    GET    /api/schedule/drivers      司機清單
    GET    /api/schedule/centers      據點清單

  統計
    GET    /api/stats/summary         服務統計摘要
                                      ?from=MM/DD/YYYY&to=MM/DD/YYYY

  動作
    POST   /api/action/pickup         登記上車（含 GPS）
    POST   /api/action/dropoff        登記下車（含 GPS）
    POST   /api/action/leave          登記請假
    POST   /api/action/generate       依星期產生班表

================================================================
【同步機制說明】
================================================================

  作業流程
  ────────
  1. 使用者在本機新增 / 修改資料
  2. 資料庫欄位 dirty=TRUE 自動標記
  3. 使用者點選「同步管理」頁面的「推送」按鈕
  4. sync_engine_v2 依 appsheet_assembler 組裝資料
  5. 依 appsheet_key 判斷 edit_row / add_row
  6. 推送成功 → dirty=FALSE（清除旗標）
  7. 推送進度透過 SSE 即時顯示

  dirty 旗標規則
  ──────────────
  · 新增記錄        → dirty=TRUE
  · 修改記錄        → dirty=TRUE
  · 軟刪除記錄      → deleted=TRUE + dirty=TRUE
  · 推送成功後      → dirty=FALSE
  · 推送失敗        → dirty 保持 TRUE，可重試

  AppSheet 資料表對應
  ───────────────────
  日照名單   ← centers
  司機       ← drivers
  司機名單   ← drivers + users
  個案總表   ← patients + patient_routes
  日照班表   ← orders

================================================================
【角色權限系統】
================================================================

  角色（roles）
  ─────────────
  admin       系統管理員（全功能）
  dispatcher  調度員（班表管理、個案管理）
  center      日照中心（檢視本中心資料）
  driver      司機（檢視今日班表、更新行程）

  預設帳號
  ────────
  · admin / admin123
  · 各日照中心：帳號 = 據點名稱拼音，密碼 = center123
  · 各司機：帳號 = 姓名拼音，密碼 = driver123

================================================================
【Google Maps 設定】
================================================================

  地圖 API Key 設定於：
    backend/templates/map.html（最後的 <script> 標籤）

  目前狀態：⚠️ API Key 需更新（地圖無法載入）
    申請步驟：
    1. 前往 https://console.cloud.google.com/
    2. 建立專案 → 啟用「Maps JavaScript API」
    3. 建立 API 金鑰（Credentials）
    4. 更新 map.html 中的 key= 參數

  地圖功能（key 正確後）：
    · 綠色標記 = 上車地點
    · 紅色標記 = 下車地點
    · 藍色虛線 = 接送路線
    · 點擊標記顯示乘客、司機、時間資訊

================================================================
【注意事項】
================================================================

  · backend/config.py 存有 AppSheet API Key，請勿上傳至公開儲存庫
  · app.py 已設定 TEMPLATES_AUTO_RELOAD = True
    修改模板不需重啟伺服器；修改 app.py 本身仍需重啟
  · 舊版 database.py / sync_engine.py 保留供向下相容，
    新功能請使用 database_v2.py / sync_engine_v2.py
  · 遷移腳本 sql/migrate_v2.py 僅需執行一次，勿重複執行
