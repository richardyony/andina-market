"""Configuración común de las pruebas: rutas de src/ y una sesión de Spark local."""
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for folder in ("src/transform", "src/ingesta", "src/gobierno", "src/agent"):
    sys.path.insert(0, str(ROOT / folder))

# Spark local usa el mismo intérprete que pytest para los workers.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("andina-tests")
        .config("spark.sql.session.timeZone", "UTC")   # fechas en UTC (D-05)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _sql_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, datetime):
        return f"TIMESTAMP '{v:%Y-%m-%d %H:%M:%S}'"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


@pytest.fixture(scope="session")
def make_df(spark):
    """DataFrame de prueba armado con SQL (VALUES), sin workers de Python: así las pruebas corren
    igual en Linux (CI) y en Windows, donde el worker de PySpark local suele fallar.
    Uso: make_df("i INT, v STRING", [(1, "a"), (2, None)])."""
    def build(ddl: str, rows):
        cols = [c.strip().split(None, 1) for c in re.split(r",\s*(?=[A-Za-z_]\w*\s)", ddl)]
        aliases = [f"c{i}" for i in range(len(cols))]
        values = ", ".join("(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in rows)
        select = ", ".join(f"CAST({a} AS {t}) AS {n}" for a, (n, t) in zip(aliases, cols))
        return spark.sql(f"SELECT {select} FROM VALUES {values} AS t({', '.join(aliases)})")
    return build
