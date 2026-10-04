import os 
from pathlib import Path 

import yaml 

from vayu.openaq_client import OpenAQClient 

DEFAULT_PATH = "/opt/airflow/pipeline_config/pipeline.yaml"
REQUIRED_SECTIONS = ["api", "scope", "parameters", "history", "incremental", "quality", "storage"]


def load_config(path=None):
    path = Path(path or os.environ.get("VAYU_CONFIG_PATH", DEFAULT_PATH))
    with path.open(encoding="utf-8") as f: 
        cfg = yaml.safe_load(f) or {}

    missing = [s for s in REQUIRED_SECTIONS if s not in cfg]

    if missing: 
        raise ValueError(f"{path} is missing requied sections: {missing}")
    if not cfg["parameters"] : 
        raise ValueError(f"{path}: 'parameters' must list atleast one parameter") 
    return cfg 

def build_client(cfg, api_key) : 
    api = cfg["api"] 
    rate = api["rate_limit"]

    return OpenAQClient(
        api_key=api_key,
        base_url=api["base_url"],
        seconds_between_requests=rate["seconds_between_requests"],
        pause_when_remaining_below=rate["pause_when_remaining_below"],
        max_retries=rate["max_retries"],
        backoff_base_seconds=rate["backoff_base_seconds"]
    )