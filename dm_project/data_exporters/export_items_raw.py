if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import psycopg2
from psycopg2.extras import execute_values, Json
from mage_ai.data_preparation.shared.secrets import get_secret_value


@data_exporter
def export_data(rows, *args, **kwargs):
    if not rows:
        print("No rows to export.")
        return

    conn = psycopg2.connect(
        host=get_secret_value('PG_HOST'),
        port=get_secret_value('PG_PORT'),
        dbname=get_secret_value('PG_DB'),
        user=get_secret_value('PG_USER'),
        password=get_secret_value('PG_PASSWORD'),
    )

    sql = """
    INSERT INTO raw.qb_items
    (id, payload, ingested_at_utc, extract_window_start_utc, extract_window_end_utc,
     page_number, page_size, request_payload)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
      payload = EXCLUDED.payload,
      ingested_at_utc = EXCLUDED.ingested_at_utc,
      extract_window_start_utc = EXCLUDED.extract_window_start_utc,
      extract_window_end_utc = EXCLUDED.extract_window_end_utc,
      page_number = EXCLUDED.page_number,
      page_size = EXCLUDED.page_size,
      request_payload = EXCLUDED.request_payload;
    """

    values = []
    for r in rows:
        values.append((
            r["id"],
            Json(r["payload"]),
            r["ingested_at_utc"],
            r["extract_window_start_utc"],
            r["extract_window_end_utc"],
            r["page_number"],
            r["page_size"],
            Json(r["request_payload"]),
        ))

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)

    conn.close()
    print(f"UPSERT OK: {len(rows)}")
