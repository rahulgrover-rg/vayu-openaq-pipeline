import json 
import logging 
import subprocess 
from collections import Counter 
from pathlib import Path 
import os 


log = logging.getLogger(__name__)

PROJECT_DIR = Path("/opt/airflow/dbt")
RUN_RESULTS = PROJECT_DIR / "target" / "run_results.json"
FRESHNESS_RESULTS = PROJECT_DIR / "target" / "sources.json"

def run_dbt(args, artifact) : 
    artifact.unlink(missing_ok=True)
    cmd = ["dbt", *args, "--project-dir", str(PROJECT_DIR)]
    log.info("Running: %s", " ".join(cmd))

    env = {**os.environ, "DBT_USE_COLORS": "false"}
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc: 
        for line in proc.stdout: 
            log.info(line.rstrip())
        return proc.wait()

def _read(artifact): 
    if not artifact.exists() : 
        raise RuntimeError(f"dbt did not write {artifact.name}; see the dbt output above")
    return json.loads(artifact.read_text())["results"]

def summarize_build(): 
    results = _read(RUN_RESULTS)
    problems = [
        {"node": r["unique_id"], "status": r["status"], "message": r.get("message")}
        for r in results if r["status"] not in ("success", "pass")
    ]

    return {"statuses":dict(Counter(r["status"] for r in results)), "problems": problems}

def summarize_freshness(): 
    summary = []
    for r in _read(FRESHNESS_RESULTS) : 
        age = r.get("max_loaded_at_time_ago_in_s")
        summary.append({
            "source" : r["unique_id"],
            "status": r["status"],
            "max_loaded_at": r.get("max_loaded_at"),
            "age_hours": round(age/3600, 1) if age is not None else None
        })

    return summary