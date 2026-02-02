if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime, timezone, timedelta
import base64
import time
import random
import requests


def _qbo_base_url(env: str) -> str:
    env = (env or "").strip().lower()
    return "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_iso_utc(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_access_token() -> str:
    client_id = get_secret_value("QBO_CLIENT_ID")
    client_secret = get_secret_value("QBO_CLIENT_SECRET")
    refresh_token = get_secret_value("QBO_REFRESH_TOKEN")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {basic}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    resp = requests.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        headers=headers,
        data=data,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _request_with_retries(method, url, *, headers, params=None, timeout=60, max_attempts=6):
    for attempt in range(max_attempts):
        resp = requests.request(method, url, headers=headers, params=params, timeout=timeout)
        if resp.status_code == 429 or resp.status_code >= 500:
            time.sleep((2 ** attempt) + random.random())
            continue
        resp.raise_for_status()
        return resp


@data_loader
def load_data(*args, **kwargs):
    env = get_secret_value("QBO_ENV")
    realm_id = get_secret_value("QBO_REALM_ID")

    start_dt = _parse_iso_utc(kwargs["fecha_inicio"])
    end_dt = _parse_iso_utc(kwargs["fecha_fin"])

    base_url = _qbo_base_url(env)
    url = f"{base_url}/v3/company/{realm_id}/query"

    all_rows = []

    chunk_start_iso = _iso_utc(start_dt)
    chunk_end_iso = _iso_utc(end_dt)

    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    start_position = 1
    page_size = 500
    page_number = 1

    while True:
        query = (
            "SELECT * FROM Invoice "
            f"WHERE MetaData.LastUpdatedTime >= '{chunk_start_iso}' "
            f"AND MetaData.LastUpdatedTime < '{chunk_end_iso}' "
            f"STARTPOSITION {start_position} MAXRESULTS {page_size}"
        )

        resp = _request_with_retries("GET", url, headers=headers, params={"query": query})
        data = resp.json()
        invoices = data.get("QueryResponse", {}).get("Invoice", []) or []

        if not invoices:
            break

        for inv in invoices:
            all_rows.append({
                "id": str(inv["Id"]),
                "payload": inv,
                "ingested_at_utc": _iso_utc(datetime.now(timezone.utc)),
                "extract_window_start_utc": chunk_start_iso,
                "extract_window_end_utc": chunk_end_iso,
                "page_number": page_number,
                "page_size": page_size,
                "request_payload": {"query": query},
            })

        if len(invoices) < page_size:
            break

        start_position += page_size
        page_number += 1

    return all_rows
