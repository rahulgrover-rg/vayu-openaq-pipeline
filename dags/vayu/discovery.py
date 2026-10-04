import logging 
from datetime import datetime, time, timezone 
from vayu.openaq_client import FatalError, OpenAQError 
from collections import Counter

log = logging.getLogger(__name__) 

def _utc(obj, key) : 
    value = (obj.get(key) or {}).get("utc") 
    if not value : 
        return None 
    return datetime.fromisoformat(value.replace("Z", "+00.00")) 

def discover_sensors(client, cfg, now=None) : 
    now = now or datetime.now(timezone.utc)
    scope = cfg["scope"]
    window_start = datetime.combine(cfg["history"]["start_date"], time.min, tzinfo=timezone.utc)
    wanted_params = {p["id"] for p in cfg["parameters"]} 
    excluded = set(scope.get("exclude_location_ids") or []) 

    locations = client.get_all(
        "locations", 
        {"coordinates": f"{scope['latitude']},{scope['longitude']}","radius":scope["radius_m"]},
        page_limit = cfg["api"]["page_limit"]
    )
    log.info("Locations within %sm: %d", scope["radius_m"], len(locations)) 


    excluded_providers = set(scope.get("exclude_providers") or [])
    stations, dropped = [], [] 
    for loc in locations : 
        provider = (loc.get("provider") or {}).get("name") 
        last = _utc(loc, "datetimeLast")
        if loc["id"] in excluded : 
            reason = "blocklisted in config" 
        elif provider in excluded_providers : 
            reason = f"low-cost provider ({provider})" 
        elif last is None or last < window_start : 
            reason = "no data since window start"  
        else : 
            stations.append(loc)
            continue 

        dropped.append({"location_id" : loc["id"], "name": loc.get("name"), "provider": provider, "reason": reason})
        
    log.info("Active stations in scope: %d", len(stations)) 

    for reason, count in Counter(d["reason"] for d in dropped).items() : 
        log.info("  dropped %d: %s", count, reason) 

    unflagged = [s["id"] for s in stations if not s.get("isMonitor")] 
    if unflagged: 
        log.info("  kept %d stations where OpenAQ's isMonitor is false (F16): %s", len(unflagged), unflagged)

    sensors, skipped = [], []
    for loc in stations: 
        try: 
            station_sensors = client.get(f"locations/{loc['id']}/sensors")["results"] 
        except FatalError: 
            raise 
        except OpenAQError as e : 
            log.warning("Skipping station %s (%s): %s", loc["id"], loc.get("name"), e) 
            skipped.append({"location_id": loc["id"], "name": loc.get("name"), "error": str(e)})
            continue 

        for s in station_sensors : 
            parameter = s.get("parameter") or {} 
            if parameter.get("id") not in wanted_params : 
                continue 
            first, last = _utc(s, "datetimeFirst"), _utc(s, "datetimeLast") 
            if first is None or last is None : 
                log.warning("Sensor %s has no activity dates; skipping", s.get("id"))
                continue 
            if last < window_start or first > now : 
                continue 
            sensors.append({
                "sensor_id": s["id"], 
                "location_id": loc["id"],
                "location_name": loc.get("name"), 
                "parameter_id": parameter["id"], 
                "parameter_name": parameter.get("name"), 
                "units": parameter.get("units"), 
                "first_utc": first.isoformat(), 
                "last_utc": last.isoformat(),
            })

    sensors.sort(key=lambda r: r["sensor_id"]) 
    log.info("Sensors in scope: %d (skipped stations: %d)", len(sensors), len(skipped)) 
    return {"stations": stations, "dropped_stations": dropped, "sensors": sensors, "skipped_stations": skipped}

