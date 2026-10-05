import json 
import logging 
from datetime import date, datetime, timezone 

from vayu.openaq_client import FatalError, PermanentError, PoisonError, TransientError
from vayu.planning import next_month 

log = logging.getLogger(__name__) 

UPSERT_SQL = """
    INSERT INTO extract_ledger 
        (sensor_id, period_month, status, row_count, object_key, http_status, error_message, attempts, fetched_at) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, 1, now()) 
    ON CONFLICT (sensor_id, period_month) DO UPDATE SET 
        status = EXCLUDED.status, 
        row_count = EXCLUDED.row_count, 
        object_key = EXCLUDED.object_key, 
        http_status = EXCLUDED.http_status, 
        error_message = EXCLUDED.error_message, 
        attempts = extract_ledger.attempts + 1, 
        fetched_at = now() ;
"""

DONE_SQL = "SELECT status, fetched_at FROM extract_ledger WHERE sensor_id = %s AND period_month = %s"

def _done_since(conn, sensor_id, month, since) : 
    if since is None: 
        return False 
    with conn.cursor() as cur :
        cur.execute(DONE_SQL, (sensor_id,month)) 
        row = cur.fetchone() 
    return row is not None and row[0] == "success" and row[1] >= since  

def object_key(sensor_id, month) : 
    return f"openaq/hours/year={month.year}/month={month.month:02d}/sensor_id={sensor_id}.json"

def record(conn, sensor_id, month, status, row_count=None, key=None, https_status=None, error=None) : 
    with conn.cursor() as cur : 
        cur.execute(UPSERT_SQL, (sensor_id, month, status, row_count, key, https_status, error))
    conn.commit()

def extract_sensor(item, client, s3, conn, cfg, done_since=None) : 
    sensor_id = item["sensor_id"] 
    bucket = cfg["storage"]["raw_bucket"] 
    page_limit = cfg["api"]["page_limit"] 
    summary = {"sensor_id": sensor_id, "success": 0, "skipped": 0, "failed_permanent": 0, "poison": 0, "failed_transient": 0, "rows": 0} 

    for month_text in item["months"] : 
        month = date.fromisoformat(month_text) 
        tag = f"[{sensor_id} {month:%Y-%m}]"
        params = {"datetime_from": month.isoformat(), "datetime_to": next_month(month).isoformat()} 

        if _done_since(conn, sensor_id, month, done_since) : 
            log.info("%s already saved during this run; skipping", tag) 
            summary["skipped"] += 1 
            continue

        try: 
            readings = client.get_all(f"sensors/{sensor_id}/hours", params, page_limit=page_limit) 
        except FatalError: 
            raise 
        except PermanentError as e : 
            log.warning("%s permanent failure: %s", tag, e)
            record(conn, sensor_id, month, "failed_permanent", https_status=e.http_status, error=str(e)) 
            summary["failed_permanent"] += 1  
        except PoisonError as e : 
            log.warning("%s poison response: %s", tag, e)
            record(conn, sensor_id, month, "poison", https_status=e.http_status, error=str(e)) 
            summary["poison"] += 1
        except TransientError as e : 
            log.warning("%s still failing after retries: %s. Stopping this sensor.", tag, e)
            record(conn, sensor_id, month, "failed_transient", https_status=e.http_status, error=str(e)) 
            summary["failed_transient"] += 1
            break 

        key = object_key(sensor_id, month) 
        envelope = {
            "source" : "openaq-v3", 
            "endpoint": f"sensors/{sensor_id}/hours", 
            "params": params, 
            "sensor_id": sensor_id, 
            "location_id": item["location_id"], 
            "parameter_id": item["parameter_id"],
            "period_month": month.isoformat(), 
            "fetched_at": datetime.now(timezone.utc).isoformat() ,
            "row_count": len(readings) ,
            "results" : readings 
        }

        body = json.dumps(envelope, ensure_ascii=False).encode("utf-8") 
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json") 

        record(conn, sensor_id, month, "success", row_count=len(readings), key=key, https_status=200) 
        summary["success"] += 1
        summary["rows"] += len(readings) 
        log.info("%s saved %d readings -> s3://%s/%s", tag, len(readings), bucket, key)

    return summary 