import os

# ── AppSheet ──────────────────────────────────────────
# 請至 AppSheet 後台取得 App ID 與 API Key
APPSHEET_APP_ID  = os.getenv("APPSHEET_APP_ID",  "your-appsheet-app-id")
APPSHEET_KEY     = os.getenv("APPSHEET_KEY",     "V2-your-appsheet-api-key")
APPSHEET_API     = f"https://api.appsheet.com/api/v2/apps/{APPSHEET_APP_ID}/tables"

# ── PostgreSQL ────────────────────────────────────────
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB",   "rizhaodb")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "your-postgres-password")

# ── Server ────────────────────────────────────────────
API_PORT       = int(os.getenv("API_PORT", "8000"))
SYNC_INTERVAL  = int(os.getenv("SYNC_INTERVAL_SECONDS", "30"))

# ── Google Maps ───────────────────────────────────────
# 請至 Google Cloud Console 申請 Maps JavaScript API 金鑰
GOOGLE_MAPS_KEY = os.getenv("GOOGLE_MAPS_KEY", "your-google-maps-api-key")
