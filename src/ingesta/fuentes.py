"""Configuración compartida de la ingesta desde Azure SQL (andina_oltp).

Un solo lugar define qué tablas se ingieren, su clave primaria y el nombre
que toman en el lakehouse. Lo importan los notebooks de extracción y de bronze.
"""

# Secret scope con server, database, user y password de Azure SQL (D-07).
SECRET_SCOPE = "andina-sql"

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
