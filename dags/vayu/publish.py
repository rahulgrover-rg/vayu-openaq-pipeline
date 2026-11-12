import os 
from pathlib import Path 
import duckdb

def publish_warehouse(source_path, target_path): 
    source_path, target_path = Path(source_path), Path(target_path) 
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_suffix('.tmp')
    tmp_path.unlink(missing_ok=True)  # Remove any existing temporary file
    conn = duckdb.connect(str(tmp_path))

    try: 
        conn.execute(f"ATTACH '{source_path}' AS src (READ_ONLY)")
        tables = [row[0] for row in conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_catalog = 'src' AND table_schema = 'main' and table_type = 'BASE TABLE'
        """).fetchall()]
        for name in tables : 
            conn.execute(f'CREATE TABLE main."{name}" AS SELECT * FROM src.main."{name}"')
        conn.execute("DETACH src")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


    os.replace(tmp_path, target_path)  
    return tables