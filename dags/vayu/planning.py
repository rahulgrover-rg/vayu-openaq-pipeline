import logging  
from collections import Counter 
from datetime import datetime, date, time, timedelta, timezone 

log = logging.getLogger(__name__) 


def _next_month(d) : 
    return date(d.year + d.month//12, d.month%12 + 1, 1) 

def month_starts(start, end) : 
    current = date(start.year, start.month, 1) 
    last = date(end.year, end.month, 1)
    months = [] 
    while current <= last : 
        months.append(current) 
        current = _next_month(current) 
    return months 

def close_time(month, lookback_days): 
    month_end = datetime.combine(_next_month(month), time.min, tzinfo=timezone.utc) 
    return month_end + timedelta(days=lookback_days) 

def fetch_decision(row, closes_at) : 
    if row is None: 
        return True, "never fetched" 
    status = row["status"] 
    if status == "failed_transient" : 
        return True, "retry transient failure"
    if status in ('failed_permanent', 'poison') : 
        return False, status 
    if row["fetched_at"] < closes_at : 
        return True, "month still open" 
    return False, "final"

def load_ledger(conn) : 
    with conn.cursor() as cur : 
        cur.execute("SELECT sensor_id, period_month, status, fetched_at FROM extract_ledger")
        return {(r[0], r[1]) : {"status": r[2], "fetched_at": r[3]} for r in cur.fetchall()}

def plan_work(sensors, ledger, cfg, now=None) : 
    now = now or datetime.now(timezone.utc) 
    window_start = cfg["history"]["start_date"] 
    lookback = cfg["incremental"]["lookback_days"] 

    work, reasons = [], Counter() 
    for s in sensors: 
        first = datetime.fromisoformat(s["first_utc"]).date() 
        last = datetime.fromisoformat(s["last_utc"]).date() 
        start = max(window_start, first) 
        end  = min(now.date(), last) 

        months = [] 
        for month in month_starts(start,end) : 
            row = ledger.get((s["sensor_id"], month))
            fetch, reason = fetch_decision(row, closes_at=close_time(month=month, lookback_days=lookback))
            reasons[reason] += 1 
            if fetch : 
                months.append(month.isoformat())

        if months : 
            work.append({
                "sensor_id": s["sensor_id"],
                "location_id": s["location_id"], 
                "parameter_id": s["parameter_id"], 
                "months": months
            })
    units = sum(len(w["months"]) for w in work) 
    log.info("Plan: %d sensors, %d sensor-months to fetch", len(work), units) 
    for reason, count in sorted(reasons.items()) : 
        log.info(" %s: %d", reason, count) 

    return {"work": work, "units_to_fetch": units, "decisions": dict(reasons)}