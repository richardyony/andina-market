"""Conexión a Azure SQL y utilidades de carga."""
from __future__ import annotations

import os
import re
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv es opcional
    pass

# Errores transitorios de Azure SQL: base serverless pausada/reanudándose (40613),
# servicio ocupado (40501), timeout de login (HYT00 / 08001).
TRANSIENT = ("40613", "40501", "40197", "HYT00", "08001", "08S01")


def connect(retries: int = 6, wait_s: int = 20):
    import pyodbc

    conn_str = (
        f"DRIVER={{{os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
        f"SERVER=tcp:{os.environ['SQL_SERVER']},1433;"
        f"DATABASE={os.environ['SQL_DATABASE']};"
        f"UID={os.environ['SQL_USER']};PWD={os.environ['SQL_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    for attempt in range(1, retries + 1):
        try:
            return pyodbc.connect(conn_str, autocommit=False)
        except pyodbc.Error as exc:
            if attempt == retries or not any(code in str(exc) for code in TRANSIENT):
                raise
            print(f"  Base no disponible (¿reanudándose tras auto-pausa?). "
                  f"Reintento {attempt}/{retries} en {wait_s}s...")
            time.sleep(wait_s)


def run_sql_file(conn, path: Path):
    """Ejecuta un script T-SQL dividiéndolo por los separadores GO."""
    sql = Path(path).read_text(encoding="utf-8")
    batches = [b.strip() for b in re.split(r"^\s*GO\s*$", sql, flags=re.M | re.I) if b.strip()]
    # ALTER DATABASE no se permite dentro de una transacción: DDL en autocommit.
    previous, conn.autocommit = conn.autocommit, True
    try:
        cur = conn.cursor()
        for batch in batches:
            cur.execute(batch)
            while cur.nextset():  # consumir resultados intermedios (SELECT de verificación)
                pass
    finally:
        conn.autocommit = previous
    print(f"  OK {Path(path).name} ({len(batches)} lotes)")


def _to_db_value(v):
    if v is None or pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        v = v.to_pydatetime()
    if isinstance(v, datetime):
        # Segundos enteros: evita errores de precisión fraccional al enlazar
        # parámetros DATETIME2 con fast_executemany.
        return v.replace(microsecond=0)
    if isinstance(v, (float, np.floating)):
        return Decimal(str(round(float(v), 2)))
    if isinstance(v, np.generic):  # int64, str_ -> tipos nativos
        return v.item()
    return v


def bulk_insert(conn, table: str, df: pd.DataFrame, identity: bool, chunk: int = 5000):
    """Inserta un DataFrame con fast_executemany, preservando los IDs generados."""
    import pyodbc

    cols = list(df.columns)
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT INTO dbo.{table} ({', '.join(cols)}) VALUES ({placeholders})"
    rows = [tuple(_to_db_value(v) for v in rec) for rec in df.itertuples(index=False, name=None)]

    # Tipos declarados de cada columna: fast_executemany infiere el tamaño del
    # parámetro a partir de la primera fila, y un DECIMAL o NVARCHAR más largo
    # en filas posteriores podría truncarse o fallar. Declararlos lo evita.
    meta_cur = conn.cursor()
    meta_cur.execute("""SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                               NUMERIC_PRECISION, NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?""", table)
    meta = {r[0]: r[1:] for r in meta_cur.fetchall()}
    sizes = []
    for col in cols:
        dtype, length, precision, scale = meta[col]
        if dtype == "nvarchar":
            # NVARCHAR(MAX) (length = -1) se enlaza como 4000: los cuerpos de
            # ticket caben y se evita el camino lento de los tipos MAX.
            sizes.append((pyodbc.SQL_WVARCHAR, 4000 if length == -1 else length, 0))
        elif dtype == "decimal":
            sizes.append((pyodbc.SQL_DECIMAL, precision, scale))
        else:
            sizes.append(None)

    if identity:
        meta_cur.execute(f"SET IDENTITY_INSERT dbo.{table} ON")
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.setinputsizes(sizes)
    for i in range(0, len(rows), chunk):
        cur.executemany(sql, rows[i:i + chunk])
        print(f"    {table}: {min(i + chunk, len(rows)):,}/{len(rows):,}", end="\r")
    if identity:
        meta_cur.execute(f"SET IDENTITY_INSERT dbo.{table} OFF")
    conn.commit()
    print(f"    {table}: {len(rows):,} filas cargadas          ")
