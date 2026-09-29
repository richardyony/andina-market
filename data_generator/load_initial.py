"""Carga inicial del histórico de Andina Market en Azure SQL.

Uso:
    python -m data_generator.load_initial --dry-run        # genera CSV en ./out para revisar
    python -m data_generator.load_initial --reset          # crea el esquema, carga todo y activa CT

--reset ejecuta en orden: 01_schema.sql -> inserción -> 02_post_load.sql ->
03_change_tracking.sql. Change Tracking se activa DESPUÉS de la carga: el
histórico representa lo que ya existía en el sistema; a partir de ahí, todo
cambio queda versionado para la ingesta incremental.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .generate import LOAD_ORDER, generate

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "source_db"


def summarize(frames, edge_log):
    print("\nVolumetría generada:")
    for name in LOAD_ORDER:
        print(f"  {name:<15} {len(frames[name]):>8,}")
    o = frames["Orders"]
    print("\nEstados de pedido:", o["Status"].value_counts().to_dict())
    print("Canales:", o["Channel"].value_counts(normalize=True).round(3).to_dict())
    print("Estados de pago:", frames["Payments"]["Status"].value_counts().to_dict())
    print("Segmentos:", frames["Customers"]["Segment"].value_counts().to_dict())
    print("Prioridad tickets:", frames["SupportTickets"]["Priority"].value_counts().to_dict())
    print("\nCasos borde inyectados:")
    for k, v in edge_log.items():
        print(f"  {k:<30} {v:>6}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="No toca la base: escribe CSV en ./out")
    ap.add_argument("--reset", action="store_true", help="Borra y recrea el esquema antes de cargar")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    print("Generando dataset sintético...")
    frames, edge_log = generate(args.seed) if args.seed is not None else generate()
    summarize(frames, edge_log)

    if args.dry_run:
        out = ROOT / "out"
        out.mkdir(exist_ok=True)
        for name, df in frames.items():
            df.to_csv(out / f"{name}.csv", index=False)
        (out / "edge_cases.json").write_text(json.dumps(edge_log, indent=2, ensure_ascii=False))
        print(f"\nCSV escritos en {out}")
        return

    if not args.reset:
        ap.error("La carga inicial recrea la base desde cero: usa --reset (o --dry-run).")

    from .db import bulk_insert, connect, run_sql_file

    conn = connect()
    print("\nCreando esquema (borra las tablas existentes)...")
    run_sql_file(conn, SQL_DIR / "01_schema.sql")
    print("\nInsertando en Azure SQL...")
    for name in LOAD_ORDER:
        bulk_insert(conn, name, frames[name], identity=True)
    print("\nPost-carga y Change Tracking...")
    run_sql_file(conn, SQL_DIR / "02_post_load.sql")
    run_sql_file(conn, SQL_DIR / "03_change_tracking.sql")
    conn.close()
    print("\nListo. La fuente está poblada y con Change Tracking activo.")


if __name__ == "__main__":
    main()
