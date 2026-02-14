import os
import json
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

API_KEY = os.getenv("OPENFEC_API_KEY")
BASE = "https://api.open.fec.gov/v1"
ENDPOINT = "/schedules/schedule_a/"
SOURCE = "openfec"


def sf_conn(schema="CONTROL"):
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema,
    )


def get_last_watermark():
    """
    We store a watermark in CONTROL.ingest_runs.
    For Schedule A, we use LOAD_DATE as our incremental watermark.
    """
    conn = sf_conn(schema="CONTROL")
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COALESCE(MAX(last_indexed_date), '1900-01-01'::TIMESTAMP_NTZ)
            FROM CONTROL.ingest_runs
            WHERE source = %s AND endpoint = %s AND status = 'SUCCESS'
            """,
            (SOURCE, ENDPOINT),
        )
        return cur.fetchone()[0]
    finally:
        cur.close()
        conn.close()


def log_run(status, watermark, rows_loaded, notes=""):
    conn = sf_conn(schema="CONTROL")
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO CONTROL.ingest_runs
              (source, endpoint, last_indexed_date, last_run_ts, status, rows_loaded, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                SOURCE,
                ENDPOINT,
                watermark,
                datetime.now(timezone.utc).replace(tzinfo=None),
                status,
                rows_loaded,
                notes,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def fetch_schedule_a_keyset(min_load_date, per_page=100, max_batches=5):
    """
    KEYSET pagination for /schedules/schedule_a/
    DO NOT use page=1,2,3... for this endpoint.
    We use the response pagination.last_indexes to request the next batch.
    """
    headers = {"X-Api-Key": API_KEY}
    rows = []

    # Start params (no "page")
    # Note: Schedule A API requires two_year_transaction_period or specific filters
    current_year = datetime.now().year
    params = {
        "per_page": per_page,
        "sort": "contribution_receipt_date",
        "min_load_date": min_load_date.strftime("%Y-%m-%d"),
        "two_year_transaction_period": current_year,
    }

    for batch_num in range(1, max_batches + 1):
        print(f"Batch {batch_num} params:", params)

        r = requests.get(BASE + ENDPOINT, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            print("Status:", r.status_code)
            print("Response (first 500 chars):", r.text[:500])
        r.raise_for_status()

        data = r.json()
        results = data.get("results", [])
        if not results:
            break

        rows.extend(results)

        # Keyset pagination
        last_indexes = (data.get("pagination") or {}).get("last_indexes") or {}
        if not last_indexes:
            break

        # Update params with returned keyset fields (e.g., last_index, last_load_date, etc.)
        params.update(last_indexes)

    return rows


def insert_raw_schedule_a(rows, commit_every=500):
    if not rows:
        print("No rows to insert.")
        return 0

    ingest_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = sf_conn(schema="RAW")
    cur = conn.cursor()
    sql = """
    INSERT INTO RAW.raw_schedule_a (ingest_ts, source, endpoint, payload)
    SELECT %s, %s, %s, PARSE_JSON(%s)
    """
    try:
        inserted = 0
        for i, row in enumerate(rows, start=1):
            cur.execute(sql, (ingest_ts, SOURCE, ENDPOINT, json.dumps(row)))
            inserted += 1
            if i % commit_every == 0:
                conn.commit()
        conn.commit()
        return inserted
    finally:
        cur.close()
        conn.close()


def compute_new_watermark(rows, fallback):
    """
    Use max(load_date) as watermark if available.
    load_date comes as a string like "2023-07-10T21:05:09".
    We'll keep it as string or parse to datetime safely.
    """
    load_dates = [r.get("load_date") for r in rows if r.get("load_date")]
    if not load_dates:
        return fallback

    # Convert to datetime for robust max, then return datetime (timestamp_ntz)
    parsed = []
    for s in load_dates:
        try:
            # OpenFEC load_date format: YYYY-MM-DDTHH:MM:SS (no timezone)
            dt = datetime.fromisoformat(s)
            parsed.append(dt)
        except Exception:
            pass

    return max(parsed) if parsed else fallback


if __name__ == "__main__":
    if not API_KEY:
        raise RuntimeError("Missing OPENFEC_API_KEY in .env")

    # 1) Get watermark from CONTROL
    watermark = get_last_watermark()
    print(f"Last successful watermark: {watermark}")

    # 2) First run fallback (OpenFEC can reject extremely old filters or it may be too huge)
    if str(watermark).startswith("1900-01-01"):
        watermark = (datetime.now(timezone.utc) - timedelta(days=30)).replace(tzinfo=None)
        print(f"First run fallback watermark (30 days): {watermark}")

    try:
        # 3) Fetch using KEYSET pagination
        rows = fetch_schedule_a_keyset(min_load_date=watermark, per_page=100, max_batches=5)

        # 4) Insert to RAW
        inserted = insert_raw_schedule_a(rows)

        # 5) Update watermark to latest load_date from fetched rows
        new_watermark = compute_new_watermark(rows, watermark)

        # 6) Log success
        log_run("SUCCESS", new_watermark, inserted, notes="Schedule A keyset load (max_batches=5)")
        print(f"Fetched: {len(rows)} | Inserted: {inserted} | New watermark: {new_watermark}")

    except Exception as e:
        log_run("FAILED", watermark, 0, notes=str(e)[:500])
        raise
