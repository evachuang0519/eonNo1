-- ============================================================
-- migrate_fleet_vehicle.sql
-- 新增 車行（fleets）、車輛（vehicles）資料表
-- 並在 drivers 加入 fleet_id FK
-- ============================================================

-- ── 1. 車行（運輸公司/車隊） ────────────────────────────────

CREATE TABLE IF NOT EXISTS fleets (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    short_name      VARCHAR(30),                   -- 簡稱
    phone           VARCHAR(30),
    fax             VARCHAR(30),
    address         TEXT,
    contact_person  VARCHAR(50),                   -- 聯絡人
    email           VARCHAR(200),
    tax_id          VARCHAR(20),                   -- 統一編號
    bank_account    VARCHAR(50),                   -- 銀行帳號（轉帳用）
    notes           TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    dirty           BOOLEAN NOT NULL DEFAULT FALSE,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fleets_name
    ON fleets(name) WHERE deleted = FALSE;

COMMENT ON TABLE fleets IS '車行（運輸公司）— 管理司機與車輛';


-- ── 2. 車輛 ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS vehicles (
    id              SERIAL PRIMARY KEY,
    plate_no        VARCHAR(20) NOT NULL,          -- 車牌號碼（主要識別）
    brand           VARCHAR(50),                   -- 廠牌（Toyota / Ford …）
    model           VARCHAR(50),                   -- 型號（Hiace / Transit …）
    year            SMALLINT,                      -- 出廠年份
    color           VARCHAR(30),
    seats           SMALLINT DEFAULT 7,            -- 核定座位數
    wheelchair      BOOLEAN NOT NULL DEFAULT FALSE,-- 輪椅空間
    fleet_id        INTEGER REFERENCES fleets(id) ON DELETE SET NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'active',
                    -- active（服役中）/ maintenance（保養中）/ retired（報廢）
    license_expiry  DATE,                          -- 行照到期日
    insurance_expiry DATE,                         -- 保險到期日
    photo_url       TEXT,
    notes           TEXT,
    appsheet_key    VARCHAR(200),
    dirty           BOOLEAN NOT NULL DEFAULT FALSE,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vehicles_plate
    ON vehicles(plate_no) WHERE deleted = FALSE;

CREATE INDEX IF NOT EXISTS idx_vehicles_fleet
    ON vehicles(fleet_id) WHERE deleted = FALSE;

COMMENT ON TABLE vehicles IS '車輛資料 — 歸屬車行，可供班表調度';


-- ── 3. 在 drivers 加入 fleet_id ──────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='drivers' AND column_name='fleet_id'
    ) THEN
        ALTER TABLE drivers
            ADD COLUMN fleet_id INTEGER REFERENCES fleets(id) ON DELETE SET NULL;
        COMMENT ON COLUMN drivers.fleet_id IS '所屬車行 FK';
    END IF;
END $$;


-- ── 4. 在 drivers 加入 vehicle_id（常用車輛） ──────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='drivers' AND column_name='vehicle_id'
    ) THEN
        ALTER TABLE drivers
            ADD COLUMN vehicle_id INTEGER REFERENCES vehicles(id) ON DELETE SET NULL;
        COMMENT ON COLUMN drivers.vehicle_id IS '預設駕駛車輛 FK';
    END IF;
END $$;


-- ── 5. 在 orders 加入 vehicle_id（調度用） ────────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='orders' AND column_name='vehicle_id'
    ) THEN
        ALTER TABLE orders
            ADD COLUMN vehicle_id INTEGER REFERENCES vehicles(id) ON DELETE SET NULL;
        COMMENT ON COLUMN orders.vehicle_id IS '調度車輛 FK';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='orders' AND column_name='fleet_id'
    ) THEN
        ALTER TABLE orders
            ADD COLUMN fleet_id INTEGER REFERENCES fleets(id) ON DELETE SET NULL;
        COMMENT ON COLUMN orders.fleet_id IS '調度車行 FK';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='orders' AND column_name='dispatched_at'
    ) THEN
        ALTER TABLE orders
            ADD COLUMN dispatched_at TIMESTAMPTZ;
        COMMENT ON COLUMN orders.dispatched_at IS '調度時間';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='orders' AND column_name='dispatched_by'
    ) THEN
        ALTER TABLE orders
            ADD COLUMN dispatched_by VARCHAR(50);
        COMMENT ON COLUMN orders.dispatched_by IS '調度人員';
    END IF;
END $$;


-- ── 6. 更新觸發器（updated_at 自動更新） ──────────────────

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_fleets_updated_at') THEN
        CREATE TRIGGER trg_fleets_updated_at
            BEFORE UPDATE ON fleets
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_vehicles_updated_at') THEN
        CREATE TRIGGER trg_vehicles_updated_at
            BEFORE UPDATE ON vehicles
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;
END $$;

-- ── 完成 ────────────────────────────────────────────────────
SELECT 'migrate_fleet_vehicle.sql applied successfully' AS result;
