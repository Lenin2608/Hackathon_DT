import os
import sys
import json
import math
import time
from typing import List, Dict, Tuple

import snowflake.connector

SRC_CONFIG = {
    "account": os.getenv("SF_SRC_ACCOUNT", "decisiontree-dt_snowflake_coe"),
    "user": os.getenv("SF_SRC_USER", "DT_GEN_USER"),
    "password": os.getenv("SF_SRC_PASSWORD", "DecisionTree@1"),
    "role": os.getenv("SF_SRC_ROLE", "READONLY"),
    "warehouse": os.getenv("SF_SRC_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SF_SRC_DATABASE", "dt_hackathon_source"),
    "schema": os.getenv("SF_SRC_SCHEMA", "source_data"),
}

DST_CONFIG = {
    "account": os.getenv("SF_DST_ACCOUNT", "decisiontree-dt_snowflake_coe"),
    "user": os.getenv("SF_DST_USER", "users_team10"),
    "password": os.getenv("SF_DST_PASSWORD", "iq-hnwadHNX8qqD5"),
    "role": os.getenv("SF_DST_ROLE", "role_team10"),
    "warehouse": os.getenv("SF_DST_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SF_DST_DATABASE", "db_team10"),
    "schema": os.getenv("SF_DST_SCHEMA", "schema_team10"),
}


TABLES_TO_MIGRATE = ["PATIENTVISIT", "PAYMENTMETHOD"]

BATCH_SIZE = 1000


def connect(cfg: Dict):
    return snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        role=cfg.get("role"),
        warehouse=cfg.get("warehouse"),
        database=cfg.get("database"),
        schema=cfg.get("schema"),
        autocommit=False,
    )

def fetch_table_columns(src_conn, src_db: str, src_schema: str, table: str) -> List[Dict]:
    """Return ordered list of dicts with column metadata from information_schema.columns"""
    q = f"""
    SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
    FROM {src_db}.information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    cur = src_conn.cursor()
    try:
        cur.execute(q, (src_schema.upper(), table.upper()))
        rows = cur.fetchall()
        cols = []
        for r in rows:
            cols.append({
                "column_name": r[0],
                "data_type": r[1],
                "char_max_len": r[2],
                "num_precision": r[3],
                "num_scale": r[4],
                "is_nullable": r[5],
            })
        return cols
    finally:
        cur.close()

def make_col_ddl(colmeta: Dict) -> str:
    """Given a column metadata dict, return a DDL fragment like: NAME VARCHAR(100)"""
    name = colmeta["column_name"]
    dt = colmeta["data_type"].upper()
    char_len = colmeta["char_max_len"]
    num_prec = colmeta["num_precision"]
    num_scale = colmeta["num_scale"]
    nullable = colmeta["is_nullable"]

    
    if "CHAR" in dt or "TEXT" in dt:
        if char_len:
            type_ddl = f"{dt}({char_len})"
        else:
            type_ddl = dt
    elif dt.startswith("NUMBER") or dt == "NUMBER":
        if num_prec:
            if num_scale is not None:
                type_ddl = f"NUMBER({num_prec},{num_scale})"
            else:
                type_ddl = f"NUMBER({num_prec})"
        else:
            type_ddl = "NUMBER"
    elif dt.startswith("TIMESTAMP"):
        
        type_ddl = dt
    elif dt in ("VARIANT", "OBJECT", "ARRAY"):
        type_ddl = dt
    else:
        type_ddl = dt

    null_ddl = "" if nullable == "YES" else " NOT NULL"

    return f'"{name}" {type_ddl}{null_ddl}'

def create_table_if_not_exists(dst_conn, dst_db: str, dst_schema: str, table: str, columns_meta: List[Dict]):
    col_ddls = [make_col_ddl(c) for c in columns_meta]
    ddl = f'CREATE OR REPLACE TABLE "{dst_db}"."{dst_schema}"."{table}" (\n  ' + ",\n  ".join(col_ddls) + "\n);"
    cur = dst_conn.cursor()
    try:
        print(f"Creating table {dst_db}.{dst_schema}.{table} ...")
        cur.execute(ddl)
        dst_conn.commit()
    finally:
        cur.close()

def build_insert_sql(dst_db: str, dst_schema: str, table: str, columns_meta: List[Dict]) -> Tuple[str, List[str]]:
    col_names = [c["column_name"] for c in columns_meta]
    cols_quoted = [f'"{c}"' for c in col_names]
    placeholders = ", ".join(["%s"] * len(col_names))
    insert_sql = f'INSERT INTO "{dst_db}"."{dst_schema}"."{table}" ({", ".join(cols_quoted)}) VALUES ({placeholders})'
    return insert_sql, col_names

def row_transform_for_insert(row: Tuple, columns_meta: List[Dict]) -> Tuple:
    transformed = []
    for val, meta in zip(row, columns_meta):
        dt = meta["data_type"].upper()
        if val is None:
            transformed.append(None)
            continue
        
        if dt in ("VARIANT", "OBJECT", "ARRAY"):
            
            if isinstance(val, (dict, list)):
                transformed.append(json.dumps(val))
            else:
                
                transformed.append(val)
        else:
            transformed.append(val)
    return tuple(transformed)

def migrate_table(src_conn, dst_conn, src_db, src_schema, dst_db, dst_schema, table):
    print(f"\n-- Migrating table: {table} --")
    
    cols_meta = fetch_table_columns(src_conn, src_db, src_schema, table)
    if not cols_meta:
        print(f"Table {src_db}.{src_schema}.{table} not found or has no columns. Skipping.")
        return
    print(f"Found {len(cols_meta)} columns.")

    
    create_table_if_not_exists(dst_conn, dst_db, dst_schema, table, cols_meta)

    
    insert_sql, col_names = build_insert_sql(dst_db, dst_schema, table, cols_meta)

    
    cur_src = src_conn.cursor()
    cur_dst = dst_conn.cursor()
    try:
        select_sql = f'SELECT {", ".join([f\'"{c["column_name"]}"\' for c in cols_meta])} FROM "{src_db}"."{src_schema}"."{table}"'
        cur_src.execute(select_sql)
        total_inserted = 0
        batch_num = 0

        while True:
            rows = cur_src.fetchmany(BATCH_SIZE)
            if not rows:
                break
            batch_num += 1
            transformed_rows = [row_transform_for_insert(r, cols_meta) for r in rows]
            # executemany
            cur_dst.executemany(insert_sql, transformed_rows)
            dst_conn.commit()
            total_inserted += len(rows)
            print(f"  Batch {batch_num}: inserted {len(rows)} rows (total {total_inserted})")
        print(f"Finished migrating {table}. Total rows inserted: {total_inserted}")
    finally:
        cur_src.close()
        cur_dst.close()

def get_row_count(conn, db, schema, table) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{db}"."{schema}"."{table}"')
        return cur.fetchone()[0]
    finally:
        cur.close()

def main():
    print("Connecting to Source and Destination Snowflake...")
    src_conn = connect(SRC_CONFIG)
    dst_conn = connect(DST_CONFIG)

    # ensure contexts are set (useful)
    try:
        # Print current user / role for debugging
        print("Source connection info:", SRC_CONFIG["user"], " DB:", SRC_CONFIG["database"], "SCHEMA:", SRC_CONFIG["schema"])
        print("Destination connection info:", DST_CONFIG["user"], " DB:", DST_CONFIG["database"], "SCHEMA:", DST_CONFIG["schema"])

        for table in TABLES_TO_MIGRATE:
            start = time.time()
            try:
                migrate_table(
                    src_conn=src_conn,
                    dst_conn=dst_conn,
                    src_db=SRC_CONFIG["database"],
                    src_schema=SRC_CONFIG["schema"],
                    dst_db=DST_CONFIG["database"],
                    dst_schema=DST_CONFIG["schema"],
                    table=table
                )
                # Validation counts
                src_count = get_row_count(src_conn, SRC_CONFIG["database"], SRC_CONFIG["schema"], table)
                dst_count = get_row_count(dst_conn, DST_CONFIG["database"], DST_CONFIG["schema"], table)
                print(f"Validation: source_count={src_count} dest_count={dst_count}")
                if src_count != dst_count:
                    print(f"  WARNING: row count mismatch for {table} (source {src_count} != dest {dst_count})")
            except Exception as e:
                print(f"Error migrating table {table}: {e}", file=sys.stderr)
            finally:
                print(f"Elapsed time for {table}: {time.time() - start:.1f}s")

    finally:
        src_conn.close()
        dst_conn.close()
        print("Connections closed.")

if __name__ == "__main__":
    main()
