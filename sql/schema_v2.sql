-- ============================================================
--  日照交通服務系統 — 正規化資料庫 Schema v2
--  建立日期：2026-04-12
--  說明：取代原 records(JSONB) 的扁平化設計
--        新增角色權限、使用者帳號、同步佇列
-- ============================================================

-- 啟用 UUID 擴充（選用）
-- CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
--  ENUM 型別定義
-- ============================================================

DO $$ BEGIN
  CREATE TYPE direction_type    AS ENUM ('去程','回程');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE service_status    AS ENUM ('服務中','待確認','停止服務','自行接送','其他車隊');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE order_status      AS ENUM ('預約','客上','已完成','請假','取消');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE sync_op           AS ENUM ('insert','update','delete');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE sync_status_type  AS ENUM ('pending','syncing','done','failed');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE role_name_type    AS ENUM ('admin','dispatcher','center','driver');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE perm_action_type  AS ENUM ('read','create','update','delete','dispatch','push','pull','manage');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ============================================================
--  1. centers — 日照中心
-- ============================================================
CREATE TABLE IF NOT EXISTS centers (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL UNIQUE,   -- 據點名稱
    phone         VARCHAR(30),
    email         VARCHAR(200),
    address       TEXT,
    appsheet_key  VARCHAR(200),                   -- AppSheet 對應 key（據點名稱）
    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    dirty         BOOLEAN     NOT NULL DEFAULT FALSE,
    deleted       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  centers              IS '日照中心據點';
COMMENT ON COLUMN centers.appsheet_key IS 'AppSheet 日照名單.據點名稱';

-- ============================================================
--  2. drivers — 司機（整合原「司機」+「司機名單」）
-- ============================================================
CREATE TABLE IF NOT EXISTS drivers (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(100) NOT NULL,     -- 姓名
    phone               VARCHAR(30),               -- 聯絡電話
    email               VARCHAR(200),
    vehicle_no          VARCHAR(20),               -- 車號
    fleet               VARCHAR(50),               -- 所屬車隊
    photo_url           TEXT,                      -- 照片路徑
    vehicle_photo_url   TEXT,                      -- 車輛照片路徑
    notes               TEXT,
    appsheet_key        VARCHAR(200),              -- AppSheet 司機.姓名
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    dirty               BOOLEAN     NOT NULL DEFAULT FALSE,
    deleted             BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE drivers IS '司機基本資料（整合 AppSheet 司機 + 司機名單）';

-- ============================================================
--  3. patients — 個案（人，不含路程）
-- ============================================================
CREATE TABLE IF NOT EXISTS patients (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(100) NOT NULL,     -- 乘客姓名
    phone               VARCHAR(30),               -- 乘客電話
    contact_phone       VARCHAR(30),               -- 連絡電話
    emergency_contact   VARCHAR(100),              -- 緊急聯絡人
    emergency_phone     VARCHAR(30),               -- 聯絡人電話
    wheelchair          VARCHAR(20) DEFAULT '無',  -- 輪椅
    status              service_status,            -- 服務狀態
    fleet               VARCHAR(50),               -- 所屬車隊
    notes               TEXT,
    center_id           INTEGER REFERENCES centers(id) ON DELETE SET NULL,
    dirty               BOOLEAN     NOT NULL DEFAULT FALSE,
    deleted             BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  patients          IS '個案（乘客）基本資料';
COMMENT ON COLUMN patients.center_id IS 'FK → centers.id（所屬日照中心）';

-- ============================================================
--  4. patient_routes — 個案路程設定（去程/回程各一筆）
-- ============================================================
CREATE TABLE IF NOT EXISTS patient_routes (
    id                  SERIAL PRIMARY KEY,
    patient_id          INTEGER NOT NULL REFERENCES patients(id) ON DELETE CASCADE,
    direction           direction_type NOT NULL,   -- 去程/回程
    pickup_addr         TEXT,                      -- 上車地點
    pickup_gps          VARCHAR(60),               -- 上車座標 "lat, lng"
    dropoff_addr        TEXT,                      -- 下車地點
    dropoff_gps         VARCHAR(60),               -- 下車座標
    scheduled_time      TIME,                      -- 表定搭乘時間
    vehicle_no          VARCHAR(20),               -- 指定車號
    default_driver_id   INTEGER REFERENCES drivers(id) ON DELETE SET NULL,
    -- 週排班
    mon  BOOLEAN NOT NULL DEFAULT FALSE,           -- 週一
    tue  BOOLEAN NOT NULL DEFAULT FALSE,           -- 週二
    wed  BOOLEAN NOT NULL DEFAULT FALSE,           -- 週三
    thu  BOOLEAN NOT NULL DEFAULT FALSE,           -- 週四
    fri  BOOLEAN NOT NULL DEFAULT FALSE,           -- 週五
    sat  BOOLEAN NOT NULL DEFAULT FALSE,           -- 週六
    sun  BOOLEAN NOT NULL DEFAULT FALSE,           -- 週日
    appsheet_key        VARCHAR(200),              -- AppSheet 個案總表.姓名路程
    dirty               BOOLEAN     NOT NULL DEFAULT FALSE,
    deleted             BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (patient_id, direction)                 -- 每人每方向只有一筆
);

COMMENT ON TABLE  patient_routes              IS '個案路程設定（去程/回程）';
COMMENT ON COLUMN patient_routes.appsheet_key IS 'AppSheet 個案總表.姓名路程（如 徐黃春子去程）';

-- ============================================================
--  5. orders — 日照班表（訂單）
-- ============================================================
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    order_no        VARCHAR(50) UNIQUE,            -- 訂單編號(Task ID)
    patient_id      INTEGER REFERENCES patients(id)      ON DELETE SET NULL,
    route_id        INTEGER REFERENCES patient_routes(id) ON DELETE SET NULL,
    center_id       INTEGER REFERENCES centers(id)       ON DELETE SET NULL,
    driver_id       INTEGER REFERENCES drivers(id)       ON DELETE SET NULL,
    -- 時間
    order_date      DATE NOT NULL,                 -- 搭乘日期
    scheduled_time  TIME,                          -- 搭乘時間
    created_date    DATE,                          -- 建單日期
    -- 狀態
    status          order_status DEFAULT '預約',
    direction       direction_type,                -- 去程/回程（冗餘，方便查詢）
    -- 上車資訊
    pickup_addr     TEXT,
    pickup_gps      VARCHAR(60),
    pickup_time     TIME,
    -- 下車資訊
    dropoff_addr    TEXT,
    dropoff_gps     VARCHAR(60),
    dropoff_time    TIME,
    -- 車輛
    vehicle_no      VARCHAR(20),
    fleet           VARCHAR(50),
    -- 服務結果
    distance_km     DECIMAL(8,2),
    duration_min    INTEGER,
    signature       TEXT,
    passenger_photo TEXT,                          -- 乘客頭像
    center_phone    VARCHAR(30),
    passenger_phone VARCHAR(30),
    notes           TEXT,
    -- 同步控制
    appsheet_key    VARCHAR(200),                  -- AppSheet 日照班表.訂單編號
    dirty           BOOLEAN     NOT NULL DEFAULT FALSE,
    deleted         BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  orders           IS '日照班表（接送訂單）';
COMMENT ON COLUMN orders.order_no  IS 'AppSheet Task ID，格式 T20260120-XXXX';

-- ============================================================
--  6. roles — 角色定義
-- ============================================================
CREATE TABLE IF NOT EXISTS roles (
    id          SERIAL PRIMARY KEY,
    name        role_name_type NOT NULL UNIQUE,
    label       VARCHAR(100)  NOT NULL,
    description TEXT
);

-- 預設角色資料
INSERT INTO roles (name, label, description) VALUES
  ('admin',      '系統管理員', '全部功能，包含帳號管理與同步控制'),
  ('dispatcher', '調度員',     '班表管理、個案管理、路線指派，無帳號管理'),
  ('center',     '日照中心',   '只能查看並管理自己中心的個案與班表'),
  ('driver',     '司機',       '只能查看自己的班表（唯讀）')
ON CONFLICT (name) DO NOTHING;

-- ============================================================
--  7. permissions — 功能權限點
-- ============================================================
CREATE TABLE IF NOT EXISTS permissions (
    id      SERIAL PRIMARY KEY,
    module  VARCHAR(50)       NOT NULL,  -- cases, orders, drivers, centers, sync, stats, users
    action  perm_action_type  NOT NULL,
    label   VARCHAR(100)      NOT NULL,
    UNIQUE (module, action)
);

INSERT INTO permissions (module, action, label) VALUES
  ('cases',   'read',     '查看個案'),
  ('cases',   'create',   '新增個案'),
  ('cases',   'update',   '編輯個案'),
  ('cases',   'delete',   '刪除個案'),
  ('orders',  'read',     '查看班表'),
  ('orders',  'create',   '新增訂單'),
  ('orders',  'update',   '編輯訂單'),
  ('orders',  'delete',   '刪除訂單'),
  ('orders',  'dispatch', '指派司機'),
  ('drivers', 'read',     '查看司機'),
  ('drivers', 'create',   '新增司機'),
  ('drivers', 'update',   '編輯司機'),
  ('drivers', 'delete',   '刪除司機'),
  ('centers', 'read',     '查看日照中心'),
  ('centers', 'create',   '新增日照中心'),
  ('centers', 'update',   '編輯日照中心'),
  ('stats',   'read',     '查看統計報表'),
  ('sync',    'push',     '推送資料至 AppSheet'),
  ('sync',    'pull',     '從 AppSheet 拉取資料'),
  ('users',   'manage',   '帳號與權限管理')
ON CONFLICT (module, action) DO NOTHING;

-- ============================================================
--  8. role_permissions — 角色與功能點對應（N:N）
-- ============================================================
CREATE TABLE IF NOT EXISTS role_permissions (
    role_id       INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id INTEGER NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

-- 預設角色權限配置
DO $$
DECLARE
  r_admin      INTEGER; r_disp INTEGER; r_center INTEGER; r_driver INTEGER;
BEGIN
  SELECT id INTO r_admin  FROM roles WHERE name='admin';
  SELECT id INTO r_disp   FROM roles WHERE name='dispatcher';
  SELECT id INTO r_center FROM roles WHERE name='center';
  SELECT id INTO r_driver FROM roles WHERE name='driver';

  -- admin：全部權限
  INSERT INTO role_permissions (role_id, permission_id)
    SELECT r_admin, id FROM permissions
    ON CONFLICT DO NOTHING;

  -- dispatcher：無帳號管理，其餘全部
  INSERT INTO role_permissions (role_id, permission_id)
    SELECT r_disp, id FROM permissions WHERE module != 'users'
    ON CONFLICT DO NOTHING;

  -- center：查看個案/班表/統計（限自己中心，由 API 層過濾）
  INSERT INTO role_permissions (role_id, permission_id)
    SELECT r_center, id FROM permissions
    WHERE (module='cases'  AND action='read')
       OR (module='orders' AND action IN ('read','update'))
       OR (module='stats'  AND action='read')
    ON CONFLICT DO NOTHING;

  -- driver：只能讀自己班表
  INSERT INTO role_permissions (role_id, permission_id)
    SELECT r_driver, id FROM permissions
    WHERE module='orders' AND action='read'
    ON CONFLICT DO NOTHING;
END $$;

-- ============================================================
--  9. users — 使用者帳號
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,           -- bcrypt hash
    role_id       INTEGER NOT NULL REFERENCES roles(id),
    -- 關聯實體（依角色二選一）
    driver_id     INTEGER REFERENCES drivers(id)  ON DELETE SET NULL,
    center_id     INTEGER REFERENCES centers(id)  ON DELETE SET NULL,
    -- 狀態
    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    last_login    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 限制：driver_id 和 center_id 不可同時有值
    CONSTRAINT chk_user_entity CHECK (
        NOT (driver_id IS NOT NULL AND center_id IS NOT NULL)
    )
);

COMMENT ON TABLE  users              IS '系統使用者帳號（含日照中心、司機登入）';
COMMENT ON COLUMN users.driver_id    IS '若角色為 driver，關聯 drivers.id';
COMMENT ON COLUMN users.center_id    IS '若角色為 center，關聯 centers.id';
COMMENT ON COLUMN users.password_hash IS 'bcrypt hash，不儲存明碼';

-- ============================================================
--  10. sync_queue — 推送佇列
-- ============================================================
CREATE TABLE IF NOT EXISTS sync_queue (
    id            SERIAL PRIMARY KEY,
    source_table  VARCHAR(50)       NOT NULL,      -- 本機資料表名稱
    record_id     INTEGER           NOT NULL,      -- 本機記錄 id
    appsheet_key  VARCHAR(200),                    -- AppSheet 對應 key
    operation     sync_op           NOT NULL,      -- insert/update/delete
    status        sync_status_type  NOT NULL DEFAULT 'pending',
    attempts      SMALLINT          NOT NULL DEFAULT 0,
    error_msg     TEXT,
    payload       JSONB,                           -- 組裝後的 AppSheet 格式快照
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    synced_at     TIMESTAMPTZ
);

COMMENT ON TABLE sync_queue IS '推送至 AppSheet 的待辦佇列';

-- ============================================================
--  11. sync_log — 同步日誌（沿用原有，增加欄位）
-- ============================================================
CREATE TABLE IF NOT EXISTS sync_log (
    id          SERIAL PRIMARY KEY,
    synced_at   BIGINT  NOT NULL,                  -- epoch ms（相容舊資料）
    direction   VARCHAR(10) DEFAULT 'pull',        -- pull / push（新增）
    table_name  TEXT,
    rows_pulled INTEGER NOT NULL DEFAULT 0,
    rows_pushed INTEGER NOT NULL DEFAULT 0,        -- 新增
    error       TEXT
);

-- ============================================================
--  索引
-- ============================================================

-- orders 常用查詢：日期、狀態、司機
CREATE INDEX IF NOT EXISTS idx_orders_date      ON orders (order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_driver    ON orders (driver_id);
CREATE INDEX IF NOT EXISTS idx_orders_center    ON orders (center_id);
CREATE INDEX IF NOT EXISTS idx_orders_patient   ON orders (patient_id);
CREATE INDEX IF NOT EXISTS idx_orders_dirty     ON orders (dirty) WHERE dirty = TRUE;

-- patient_routes 常用
CREATE INDEX IF NOT EXISTS idx_routes_patient   ON patient_routes (patient_id);
CREATE INDEX IF NOT EXISTS idx_routes_dirty     ON patient_routes (dirty) WHERE dirty = TRUE;

-- sync_queue 待推送
CREATE INDEX IF NOT EXISTS idx_syncq_status     ON sync_queue (status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_syncq_table      ON sync_queue (source_table, status);

-- patients
CREATE INDEX IF NOT EXISTS idx_patients_center  ON patients (center_id);
CREATE INDEX IF NOT EXISTS idx_patients_dirty   ON patients (dirty) WHERE dirty = TRUE;

-- users
CREATE INDEX IF NOT EXISTS idx_users_role       ON users (role_id);

-- ============================================================
--  updated_at 自動更新 trigger
-- ============================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['centers','drivers','patients','patient_routes','orders','users'] LOOP
    EXECUTE format('
      DROP TRIGGER IF EXISTS trg_updated_at_%1$s ON %1$s;
      CREATE TRIGGER trg_updated_at_%1$s
        BEFORE UPDATE ON %1$s
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    ', tbl);
  END LOOP;
END $$;

-- ============================================================
--  dirty → sync_queue 自動入隊 trigger
--  當任一業務表的 dirty 被設為 TRUE 時，自動產生 sync_queue 記錄
-- ============================================================
CREATE OR REPLACE FUNCTION enqueue_dirty()
RETURNS TRIGGER AS $$
DECLARE
  op sync_op;
BEGIN
  IF (TG_OP = 'INSERT') THEN
    op := 'insert';
  ELSIF (TG_OP = 'UPDATE') THEN
    IF NEW.deleted = TRUE AND OLD.deleted = FALSE THEN
      op := 'delete';
    ELSE
      op := 'update';
    END IF;
  END IF;

  -- 只在 dirty 為 TRUE 時才入隊
  IF NEW.dirty = TRUE THEN
    INSERT INTO sync_queue (source_table, record_id, appsheet_key, operation)
    VALUES (TG_TABLE_NAME, NEW.id,
            CASE TG_TABLE_NAME
              WHEN 'orders'         THEN NEW.appsheet_key
              WHEN 'patient_routes' THEN NEW.appsheet_key
              WHEN 'patients'       THEN NEW.name
              WHEN 'drivers'        THEN NEW.appsheet_key
              WHEN 'centers'        THEN NEW.name
              ELSE NULL
            END,
            op)
    ON CONFLICT DO NOTHING;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['orders','patient_routes','patients','drivers','centers'] LOOP
    EXECUTE format('
      DROP TRIGGER IF EXISTS trg_enqueue_%1$s ON %1$s;
      CREATE TRIGGER trg_enqueue_%1$s
        AFTER INSERT OR UPDATE ON %1$s
        FOR EACH ROW EXECUTE FUNCTION enqueue_dirty();
    ', tbl);
  END LOOP;
END $$;
