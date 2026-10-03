"""Máscaras de columna para datos personales (D-25), definidas en un solo lugar.

Las crea la tarea preparar_mascaras del job andina_ingesta antes del pipeline, porque
silver.customers las referencia con la cláusula MASK. Las pruebas de tests/ evalúan cada
expresión con Spark local, como la vería un lector autorizado y uno sin permiso.
"""

# Quien está en este grupo ve el valor real: los service principals del pipeline, que necesitan
# emails y teléfonos reales para detectar cuentas duplicadas.
PII_READERS_GROUP = "andina-pii-readers"

# Cuerpo de cada función. `v` es el valor de la columna; {reader} es la condición de lector.
BODIES = {
    "mask_name": "CASE WHEN {reader} THEN v WHEN v IS NULL THEN NULL ELSE concat(left(v, 1), '***') END",
    "mask_email": "CASE WHEN {reader} THEN v ELSE regexp_replace(v, '^[^@]+', '***') END",
    "mask_phone": "CASE WHEN {reader} THEN v WHEN v IS NULL THEN NULL "
                  "ELSE concat('*** ', right(regexp_replace(v, '[^0-9]', ''), 3)) END",
}

COMMENTS = {
    "mask_name": "Máscara de nombre: solo la inicial (A***).",
    "mask_email": "Máscara de email: oculta la parte local y deja el dominio (***@dominio).",
    "mask_phone": "Máscara de teléfono: solo los 3 últimos dígitos (*** 321).",
}


def body(name: str, group: str = PII_READERS_GROUP) -> str:
    """Expresión que se guarda en Unity Catalog."""
    return BODIES[name].format(reader=f"is_member('{group}')")


def body_for(name: str, is_reader: bool) -> str:
    """La misma expresión con la pertenencia al grupo resuelta, para probarla fuera de Databricks."""
    return BODIES[name].format(reader="TRUE" if is_reader else "FALSE")


def create_statements(catalog: str, group: str = PII_READERS_GROUP) -> list[str]:
    """CREATE OR REPLACE de cada función: idempotente, se puede ejecutar en cada corrida."""
    return [
        f"""CREATE OR REPLACE FUNCTION {catalog}.ops.{name}(v STRING)
RETURNS STRING
COMMENT '{COMMENTS[name]}'
RETURN {body(name, group)}"""
        for name in BODIES
    ]
