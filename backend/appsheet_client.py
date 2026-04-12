"""AppSheet REST API v2 client."""
import requests
from urllib.parse import quote
from config import APPSHEET_API, APPSHEET_KEY

HEADERS = {
    "ApplicationAccessKey": APPSHEET_KEY,
    "Content-Type": "application/json",
}
PROPS = {"Locale": "zh-TW", "Timezone": "Asia/Taipei"}


def _request(table: str, action: str, rows: list = None) -> list | dict:
    url = f"{APPSHEET_API}/{quote(table)}/Action"
    body = {"Action": action, "Properties": PROPS, "Rows": rows or []}
    resp = requests.post(url, json=body, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _strip_virtual(row: dict) -> dict:
    """Remove computed/virtual columns that AppSheet rejects on write."""
    return {k: v for k, v in row.items()
            if not k.startswith("Related ") and k != "_RowNumber"}


def find_all(table: str) -> list[dict]:
    result = _request(table, "Find")
    return result if isinstance(result, list) else []


def add_row(table: str, row: dict):
    return _request(table, "Add", [_strip_virtual(row)])


def edit_row(table: str, row: dict):
    return _request(table, "Edit", [_strip_virtual(row)])


def delete_row(table: str, key_field: str, key_value: str):
    return _request(table, "Delete", [{key_field: key_value}])
