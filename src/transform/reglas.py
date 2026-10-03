"""Reglas de limpieza y calidad de silver, separadas del pipeline para poder probarlas.

Son funciones puras sobre columnas o DataFrames de Spark: no usan `dp` ni la sesión que inyecta
el pipeline. Las importan los archivos del pipeline (la carpeta src/transform está en sys.path por
el root_path del pipeline) y las pruebas de tests/, que las ejecutan con Spark local.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

# Variantes de país vistas en la fuente (captura libre) → ISO-2.
COUNTRY_ISO = {
    "PE": "PE", "PERU": "PE",
    "CO": "CO", "COLOMBIA": "CO",
    "CL": "CL", "CHILE": "CL",
    "MX": "MX", "MEX": "MX", "MEXICO": "MX",
    "EC": "EC", "ECUADOR": "EC",
}
EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$"
DOUBLE_CHARGE_WINDOW_S = 60  # dos aprobaciones del mismo monto en menos de un minuto = doble clic


# ---------------------------------------------------------------------------
# Clientes
# ---------------------------------------------------------------------------
def country_iso(col):
    """Mayúsculas, sin espacios ni tildes, y mapeo a ISO-2. Lo que no se reconoce queda NULL."""
    key = F.translate(F.upper(F.trim(col)), "ÁÉÍÓÚ", "AEIOU")
    mapping = F.create_map(*[F.lit(x) for kv in COUNTRY_ISO.items() for x in kv])
    return mapping[key]


def email_norm(col):
    """Minúsculas y sin espacios; vacío pasa a NULL."""
    return F.nullif(F.lower(F.trim(col)), F.lit(""))


def email_valido(norm_col):
    return norm_col.rlike(EMAIL_RE)


def email_key(norm_col, valido_col):
    """Clave para agrupar duplicados por email: solo emails válidos y sin la etiqueta "+...".
    Valores de relleno como "sin-correo" no identifican a nadie y quedan NULL."""
    return F.when(valido_col, F.regexp_replace(norm_col, r"\+[^@]*@", "@"))


def name_phone_key(first_col, last_col, phone_col):
    """Clave por nombre completo y teléfono (solo dígitos). Sin teléfono no hay clave."""
    digits = F.regexp_replace(phone_col, r"[^0-9]", "")
    return F.when(F.length(digits) > 0, F.concat_ws("|", F.lower(first_col), F.lower(last_col), digits))


def email_problem(col):
    """Describe por qué un email es inválido sin copiar el email (dato personal)."""
    return (
        F.when(~col.contains("@"), F.lit("sin @"))
         .when(col.contains("@@"), F.lit("@ repetida"))
         .when(col.rlike(r"@[^.]+$"), F.lit("dominio sin extensión"))
         .otherwise(F.lit("otro formato"))
    )


# ---------------------------------------------------------------------------
# Pedidos, líneas y tickets
# ---------------------------------------------------------------------------
def order_date_corrected(order_date_col, created_at_col):
    """Un OrderDate más de un día posterior a la creación del registro es un error de captura
    (año mal digitado). Devuelve la bandera; nunca NULL."""
    return F.coalesce(order_date_col > created_at_col + F.expr("INTERVAL 1 DAY"), F.lit(False))


def order_date_clean(order_date_col, created_at_col):
    """Fecha de negocio: si la original es un error de captura se usa CreatedAt, que pone la base."""
    return F.when(order_date_corrected(order_date_col, created_at_col), created_at_col).otherwise(order_date_col)


# Una línea sale del estado actual si se borró en la fuente o si llegó con cantidad 0 o negativa
# (error de captura). Es el delete_when de silver.order_items.
ITEM_REMOVED_SQL = "_ct_operation = 'D' OR quantity <= 0"


def body_clean(col):
    """'' y solo espacios son "sin cuerpo": se normalizan a NULL."""
    return F.nullif(F.trim(col), F.lit(""))


# ---------------------------------------------------------------------------
# Pagos
# ---------------------------------------------------------------------------
def double_charges(approved, current, window_s: int = DOUBLE_CHARGE_WINDOW_S):
    """Pagos aprobados con el mismo pedido y monto que otro, con menos de `window_s` segundos de
    diferencia.

    approved: payment_id, order_id, amount, created_at (cada pago que alguna vez estuvo Aprobado)
    current:  payment_id, method, current_status (estado actual del pago)
    """
    w = Window.partitionBy("order_id", "amount").orderBy("created_at", "payment_id")
    return (
        approved.join(current, "payment_id")
        .select(
            "order_id", "amount", "method",
            F.col("payment_id").alias("duplicate_payment_id"),
            F.col("created_at").alias("duplicate_created_at"),
            F.col("current_status").alias("duplicate_current_status"),
            F.lag("payment_id").over(w).alias("original_payment_id"),
            F.lag("created_at").over(w).alias("original_created_at"),
        )
        .withColumn(
            "seconds_apart",
            F.col("duplicate_created_at").cast("long") - F.col("original_created_at").cast("long"),
        )
        .where(f"original_payment_id IS NOT NULL AND seconds_apart <= {window_s}")
    )
