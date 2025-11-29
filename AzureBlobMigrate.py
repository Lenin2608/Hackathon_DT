
import os
import re
import io
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from urllib.parse import urlparse
from datetime import datetime


SNOWFLAKE_LOGIN_URL = "https://decisiontree-dt_snowflake_coe.snowflakecomputing.com/console/login"
SNOWFLAKE_USER = "users_team10"
SNOWFLAKE_PASSWORD = "iq-hnwadHNX8qqD5"
SNOWFLAKE_ROLE = "role_team10"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "db_team10"
SNOWFLAKE_SCHEMA = "schema_team10"
# ---------------------------------------------------------------------

def _parse_account_from_login_url(login_url: str) -> str:
    """
    Parse account name for snowflake.connector from the login URL.
    Example input:
      https://decisiontree-dt_snowflake_coe.snowflakecomputing.com/console/login
    Returns:
      decisiontree-dt_snowflake_coe
    """
    parsed = urlparse(login_url)
    host = parsed.netloc  
    account = re.sub(r'\.snowflakecomputing\.com$', '', host, flags=re.IGNORECASE)
    return account

def _sanitize_table_name(filename: str) -> str:
    """
    Convert filename to a safe Snowflake table name: uppercase, alphanumeric+underscore
    e.g. "doctor.csv" -> "DOCTOR"
    if filename starts with digit we prefix with "T_"
    """
    base = os.path.splitext(os.path.basename(filename))[0]
    sanitized = re.sub(r'[^0-9a-zA-Z_]', '_', base)
    sanitized = sanitized.upper()
    if re.match(r'^[0-9]', sanitized):
        sanitized = "T_" + sanitized
    return sanitized

def _df_to_snowflake_types(df: pd.DataFrame) -> dict:
    """
    Simple dtype mapping from pandas dtypes to Snowflake SQL types.
    Returns a dict of column_name -> SQL_TYPE
    """
    mapping = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            mapping[col] = "NUMBER"
        elif pd.api.types.is_float_dtype(dtype):
            mapping[col] = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            mapping[col] = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            mapping[col] = "TIMESTAMP_NTZ"
        else:
            
            mapping[col] = "VARCHAR"
    return mapping

def _create_table_if_not_exists(conn, table_name: str, col_type_map: dict):

    cols_sql = ",\n  ".join([f'"{col.upper()}" {col_type_map[col]}' for col in col_type_map])
    create_sql = f'CREATE TABLE IF NOT EXISTS "{SNOWFLAKE_DATABASE}"."{SNOWFLAKE_SCHEMA}"."{table_name}" (\n  {cols_sql}\n);'
    with conn.cursor() as cur:
        cur.execute(create_sql)

def ingest_file_to_snowflake(local_path: str):

    print(f"[{datetime.now()}] Starting ingestion for {local_path}")
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    
    ext = os.path.splitext(local_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(local_path)
    elif ext == ".tsv":
        df = pd.read_csv(local_path, sep='\t')
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(local_path)
    elif ext == ".parquet":
        df = pd.read_parquet(local_path)
    else:
      
        df = pd.read_csv(local_path)

    if df.empty:
        print(f"[{datetime.now()}] Warning: DataFrame is empty for {local_path}. Creating empty table schema and returning.")

    account = _parse_account_from_login_url(SNOWFLAKE_LOGIN_URL)
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=account,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        client_session_keep_alive=True
    )

  
    with conn.cursor() as cur:
        cur.execute(f'USE ROLE {SNOWFLAKE_ROLE}')
        cur.execute(f'USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}')
        cur.execute(f'USE DATABASE {SNOWFLAKE_DATABASE}')
        cur.execute(f'USE SCHEMA {SNOWFLAKE_SCHEMA}')

  
    table_name = _sanitize_table_name(local_path)
    col_map = _df_to_snowflake_types(df)
    _create_table_if_not_exists(conn, table_name, col_map)
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
    if success:
        print(f"[{datetime.now()}] Ingested {nrows} rows into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} in {nchunks} chunk(s).")
    else:
        print(f"[{datetime.now()}] write_pandas failed for {local_path} into table {table_name}.")

    conn.close()
