import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

API_KEY = os.getenv("OPENFEC_API_KEY")
BASE = "https://api.open.fec.gov/v1"
ENDPOINT = "/candidates/"

def sf_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="RAW",        
    )

def fetch_candidates(per_page=100, max_pages=5):
    headers={
        "X-Api-Key":API_KEY
    }
    params = {"per_page": per_page, "page":1}
    all_rows = []
    for page in range(1, max_pages + 1):
        params['page'] = page
        r = requests.get(BASE + ENDPOINT, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        all_rows.extend(results)
    return all_rows

def insert_raw(rows):
    if not rows:
        print('No rows to insert...')
        return 0
    
    ingest_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    src = "openfec"
    conn = sf_conn()
    cur = conn.cursor()
    try:
        sql = """INSERT INTO RAW.raw_candidates (ingest_ts, source, payload)
        SELECT %s, %s, PARSE_JSON(%s)"""
        count = 0
        for row in rows:
            cur.execute(sql, (ingest_ts, src, json.dumps(row)))
            count +=1
        conn.commit()
        return count
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    if not API_KEY:
        raise RuntimeError("Missing OPENFEC_API_KEY in .env")

    
    rows = fetch_candidates()
    inserted = insert_raw(rows)
    print(f"Fetched: {len(rows)} | Inserted: {inserted}")
