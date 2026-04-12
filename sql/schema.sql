-- ============================================================
-- 日照交通服務系統 — PostgreSQL Schema
-- Run: psql -U postgres -f schema.sql
-- ============================================================

-- Create database (run as superuser if needed)
SELECT 'CREATE DATABASE rizhaodb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'rizhaodb')\gexec

\c rizhaodb

-- ── Main record store ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS records (
    table_name  TEXT    NOT NULL,
    row_key     TEXT    NOT NULL,
    data        JSONB   NOT NULL,
    synced_at   BIGINT,                        -- Unix ms timestamp
    dirty       BOOLEAN NOT NULL DEFAULT FALSE, -- TRUE = local edit pending write-back
    PRIMARY KEY (table_name, row_key)
);

CREATE INDEX IF NOT EXISTS idx_records_table ON records (table_name);
CREATE INDEX IF NOT EXISTS idx_records_data  ON records USING GIN (data);

-- ── Sync audit log ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sync_log (
    id          SERIAL  PRIMARY KEY,
    synced_at   BIGINT  NOT NULL,
    table_name  TEXT,
    rows_pulled INTEGER NOT NULL DEFAULT 0,
    error       TEXT
);

-- ── Helpful views ─────────────────────────────────────────

-- Quick count per table
CREATE OR REPLACE VIEW v_table_counts AS
SELECT table_name, COUNT(*) AS row_count
FROM records
GROUP BY table_name
ORDER BY table_name;

-- Latest sync per table
CREATE OR REPLACE VIEW v_last_sync AS
SELECT DISTINCT ON (table_name)
    table_name,
    synced_at,
    rows_pulled,
    error
FROM sync_log
ORDER BY table_name, synced_at DESC;

-- Dirty (locally-edited, not yet pushed) records
CREATE OR REPLACE VIEW v_dirty_records AS
SELECT table_name, row_key, data, synced_at
FROM records
WHERE dirty = TRUE;
