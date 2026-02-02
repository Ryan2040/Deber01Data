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

    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token returned. keys={list(payload.keys())}")
    return token


def _request_with_retries(method, url, *, headers, params=None, timeout=60, max_attempts=6):
    last = None
    for attempt in range(max_attempts):
        resp = requests.request(method, url, headers=headers, params=params, timeout=timeout)
        if resp.status_code == 401:
            return resp
        if resp.status_code == 429 or resp.status_code >= 500:
            sleep_s = (2 ** attempt) + random.random()
            time.sleep(sleep_s)
            last = resp
            continue
        resp.raise_for_status()
        return resp

    if last is not None:
        last.raise_for_status()
    raise RuntimeError("No response returned from request.")


def _iter_day_windows(start_dt: datetime, end_dt: datetime, chunk_days: int):
    cur = start_dt
    while cur < end_dt:
        nxt = min(cur + timedelta(days=chunk_days), end_dt)
        yield cur, nxt
        cur = nxt


@data_loader
def load_data(*args, **kwargs):
    env = (get_secret_value("QBO_ENV") or "").strip().lower()
    realm_id = (get_secret_value("QBO_REALM_ID") or "").strip()
    if not realm_id:
        raise ValueError("Missing QBO_REALM_ID secret.")

    page_size = int(kwargs.get("page_size", 500))
    page_size = max(1, min(page_size, 1000))

    fecha_inicio = kwargs.get("fecha_inicio")
    fecha_fin = kwargs.get("fecha_fin")

    if fecha_inicio and fecha_fin:
        start_dt = _parse_iso_utc(fecha_inicio)
        end_dt = _parse_iso_utc(fecha_fin)
    else:
        interval_start = kwargs.get("interval_start_datetime")
        interval_end = kwargs.get("interval_end_datetime")
        if not interval_start or not interval_end:
            raise ValueError("Provee (fecha_inicio, fecha_fin) o interval_start_datetime/interval_end_datetime.")
        start_dt = interval_start.astimezone(timezone.utc)
        end_dt = interval_end.astimezone(timezone.utc)

    if start_dt >= end_dt:
        raise ValueError("fecha_inicio debe ser menor que fecha_fin.")

    chunk_days = int(kwargs.get("chunk_days", 1))
    chunk_days = max(1, min(chunk_days, 30))

    base_url = _qbo_base_url(env)
    url = f"{base_url}/v3/company/{realm_id}/query"

    all_rows = []
    pipeline_ingested_at = _iso_utc(datetime.now(timezone.utc))

    for (chunk_start_dt, chunk_end_dt) in _iter_day_windows(start_dt, end_dt, chunk_days):
        t0 = time.time()

        access_token = _get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

        chunk_start_iso = _iso_utc(chunk_start_dt)
        chunk_end_iso = _iso_utc(chunk_end_dt)

        start_position = 1
        page_number = 1
        total_in_chunk = 0

        while True:
            query = (
                "SELECT * FROM Item "
                f"WHERE MetaData.LastUpdatedTime >= '{chunk_start_iso}' "
                f"AND MetaData.LastUpdatedTime < '{chunk_end_iso}' "
                f"STARTPOSITION {start_position} MAXRESULTS {page_size}"
            )

            params = {"query": query}

            resp = _request_with_retries("GET", url, headers=headers, params=params, timeout=60)

            if resp.status_code == 401:
                access_token = _get_access_token()
                headers["Authorization"] = f"Bearer {access_token}"
                resp = _request_with_retries("GET", url, headers=headers, params=params, timeout=60)
                resp.raise_for_status()

            data = resp.json() or {}
            items = data.get("QueryResponse", {}).get("Item", []) or []

            if not items:
                break

            batch = []
            for it in items:
                iid = it.get("Id")
                if iid is None:
                    continue
                batch.append({
                    "id": str(iid),
                    "payload": it,
                    "ingested_at_utc": pipeline_ingested_at,
                    "extract_window_start_utc": chunk_start_iso,
                    "extract_window_end_utc": chunk_end_iso,
                    "page_number": page_number,
                    "page_size": page_size,
                    "request_payload": {"query": query},
                })

            all_rows.extend(batch)
            total_in_chunk += len(batch)

            start_position += len(items)
            page_number += 1

            if len(items) < page_size:
                break

        duration_s = round(time.time() - t0, 3)
        print(
            f"[items] chunk {chunk_start_iso} → {chunk_end_iso} | rows={total_in_chunk} "
            f"| pages={page_number-1} | duration_s={duration_s}"
        )

    print(f"[items] total_rows={len(all_rows)} for range {_iso_utc(start_dt)} → {_iso_utc(end_dt)}")
    return all_rows
