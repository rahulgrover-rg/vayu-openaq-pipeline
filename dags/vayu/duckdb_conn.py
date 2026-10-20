import duckdb 

def _sql_text(value): 
    return "'" + str(value).replace("'", "''") + "'"

def connect(endpoint_url, access_key, secret_key, database=":memory:"): 
    host = endpoint_url.split("://", 1)[-1] 
    con = duckdb.connect(database)
    con.execute("LOAD httpfs")
    con.execute(f"""
        CREATE OR REPLACE SECRET minio ( 
            TYPE s3,
            KEY_ID {_sql_text(access_key)},
            SECRET {_sql_text(secret_key)},
            ENDPOINT {_sql_text(host)},
            URL_STYLE 'path',
            USE_SSL false,
            REGION 'us-east-1'  
        )
    """)

    return con

