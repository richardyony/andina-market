"""Configuración compartida de la ingesta desde Azure SQL (andina_oltp).

Un solo lugar define qué tablas se ingieren, su clave primaria y el nombre
que toman en el lakehouse. Lo importan los notebooks de extracción y de bronze.
"""

import re

# Secret scope respaldado por Azure Key Vault (kv-andina-8346) con server, database, user y
# password de Azure SQL. El usuario es databricks_reader, de solo lectura (D-17).
SECRET_SCOPE = "andina-kv"

# Volume de landing (esquema `landing`) y Volume de checkpoints (esquema `ops`).
LANDING_VOLUME = "sqlserver"
CHECKPOINT_VOLUME = "checkpoints"

# source: tabla en dbo · pk: clave primaria (necesaria para CHANGETABLE y los DELETE)
# target: nombre de la carpeta en landing y de la tabla en bronze (snake_case)
TABLES = [
    {"source": "Customers", "pk": "CustomerId", "target": "customers"},
    {"source": "Products", "pk": "ProductId", "target": "products"},
    {"source": "Orders", "pk": "OrderId", "target": "orders"},
    {"source": "OrderItems", "pk": "OrderItemId", "target": "order_items"},
    {"source": "Payments", "pk": "PaymentId", "target": "payments"},
    {"source": "SupportTickets", "pk": "TicketId", "target": "support_tickets"},
]


def select_tables(csv: str) -> list[dict]:
    """Filtra TABLES por una lista separada por comas (source o target). Vacío = todas."""
    wanted = {t.strip().lower() for t in csv.split(",") if t.strip()}
    if not wanted:
        return TABLES
    chosen = [t for t in TABLES if t["source"].lower() in wanted or t["target"] in wanted]
    unknown = wanted - {t["source"].lower() for t in chosen} - {t["target"] for t in chosen}
    if unknown:
        raise ValueError(f"Tablas desconocidas: {sorted(unknown)}")
    return chosen


# Nombre de cada lote en landing: la versión de corte primero, así los lotes se ordenan y la
# versión ya publicada se puede recuperar del nombre si el proceso cayó antes de guardar la marca.
LOT_NAME = re.compile(r"^to_v(\d+)__")


def lot_name(to_v: int, mode: str, batch_id: str) -> str:
    return f"to_v{to_v:012d}__{mode}__run_{batch_id}"


def max_published_version(names) -> int | None:
    """Mayor versión de corte entre nombres de carpetas de landing (None si no hay lotes).
    Ignora lo que no tenga forma de lote, como _staging."""
    versions = [int(m.group(1)) for n in names if (m := LOT_NAME.match(n.rstrip("/")))]
    return max(versions) if versions else None
