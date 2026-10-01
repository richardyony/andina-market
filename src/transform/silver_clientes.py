"""Silver · clientes: estado actual (SCD1), historial de segmento (SCD2) y duplicados.

Bronze trae cada cambio de dbo.Customers tal cual llegó. Aquí:
  1. una vista normaliza y valida cada cambio (expectations = calidad como código);
  2. AUTO CDC mantiene el estado actual por cliente (SCD1) y el historial del segmento (SCD2),
     ordenando por la versión de Change Tracking y aplicando los DELETE;
  3. una vista materializada agrupa las cuentas duplicadas por email normalizado,
     sin fusionarlas a ciegas.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("andina.catalog")  # noqa: F821 - `spark` lo inyecta el pipeline

# Variantes de país vistas en la fuente (captura libre) → ISO-2.
COUNTRY_ISO = {
    "PE": "PE", "PERU": "PE",
    "CO": "CO", "COLOMBIA": "CO",
    "CL": "CL", "CHILE": "CL",
    "MX": "MX", "MEX": "MX", "MEXICO": "MX",
    "EC": "EC", "ECUADOR": "EC",
}
EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$"


def country_iso(col):
    """Mayúsculas, sin espacios ni tildes, y mapeo a ISO-2. Lo que no se reconoce queda NULL."""
    key = F.translate(F.upper(F.trim(col)), "ÁÉÍÓÚ", "AEIOU")
    mapping = F.create_map(*[F.lit(x) for kv in COUNTRY_ISO.items() for x in kv])
    return mapping[key]


# ---------------------------------------------------------------------------
# 1. Cambios normalizados (vista intermedia, no se publica)
# ---------------------------------------------------------------------------
@dp.temporary_view(comment="Cambios de clientes desde bronze, normalizados y validados")
@dp.expect_or_fail("pk_presente", "customer_id IS NOT NULL")
@dp.expect("email_formato_valido", "email IS NULL OR email_valido")
@dp.expect("pais_reconocido", "country_raw IS NULL OR country_iso IS NOT NULL")
@dp.expect("segmento_conocido", "segment IS NULL OR segment IN ('Nuevo','Regular','Frecuente','VIP')")
def customers_changes():
    email_norm = F.nullif(F.lower(F.trim("Email")), F.lit(""))
    return (
        spark.readStream.table(f"{CATALOG}.bronze.customers")  # noqa: F821
        .select(
            F.col("CustomerId").alias("customer_id"),
            F.trim("FirstName").alias("first_name"),
            F.trim("LastName").alias("last_name"),
            F.col("Email").alias("email"),
            email_norm.alias("email_norm"),
            email_norm.rlike(EMAIL_RE).alias("email_valido"),
            F.col("Phone").alias("phone"),
            # Ciudad nula se reporta como "Desconocida" (catálogo de casos borde).
            F.when(F.col("_ct_operation") != "D", F.coalesce(F.trim("City"), F.lit("Desconocida"))).alias("city"),
            F.col("Country").alias("country_raw"),
            country_iso(F.col("Country")).alias("country_iso"),
            F.col("Segment").alias("segment"),
            F.col("SignupDate").alias("signup_date"),
            F.col("CreatedAt").alias("created_at"),
            F.col("UpdatedAt").alias("updated_at"),
            "_ct_version", "_ct_operation", "_batch_id", "_ingested_at",
        )
    )


# ---------------------------------------------------------------------------
# 2a. Estado actual (SCD1)
# ---------------------------------------------------------------------------
dp.create_streaming_table(
    name="silver.customers",
    comment="Clientes, estado actual (SCD1). Email y país normalizados; banderas de calidad.",
    cluster_by=["customer_id"],
)
dp.create_auto_cdc_flow(
    target="silver.customers",
    source="customers_changes",
    keys=["customer_id"],
    sequence_by=F.col("_ct_version"),          # orden exacto de la fuente, no el reloj de la app
    apply_as_deletes=F.expr("_ct_operation = 'D'"),
    except_column_list=["_ct_operation"],
    stored_as_scd_type=1,
)

# ---------------------------------------------------------------------------
# 2b. Historial del segmento (SCD2)
# ---------------------------------------------------------------------------
# Solo un cambio de `segment` abre una versión nueva. sequence_by es un struct: ordena por la
# versión de CT y deja en __START_AT/__END_AT también `updated_at`, la fecha de negocio del cambio.
dp.create_streaming_table(
    name="silver.customer_segment_history",
    comment="Historial SCD2 del segmento del cliente. Vigencia en __START_AT/__END_AT (versión CT y updated_at).",
    cluster_by=["customer_id"],
)
dp.create_auto_cdc_flow(
    target="silver.customer_segment_history",
    source="customers_changes",
    keys=["customer_id"],
    sequence_by=F.struct("_ct_version", "updated_at"),
    apply_as_deletes=F.expr("_ct_operation = 'D'"),
    column_list=["customer_id", "segment", "created_at", "updated_at"],
    stored_as_scd_type=2,
    track_history_column_list=["segment"],
)


# ---------------------------------------------------------------------------
# 3. Posibles duplicados
# ---------------------------------------------------------------------------
# Dos reglas independientes, porque la segunda cuenta suele crearse en tienda con el email
# escrito de otra forma (mayúsculas, espacios, alias "+tienda") o directamente sin email:
#   - email: válido, en minúsculas, sin espacios y sin la etiqueta "+..." de la parte local;
#   - nombre_telefono: mismo nombre completo y mismo teléfono (solo dígitos).
@dp.materialized_view(
    name="silver.customer_duplicate_groups",
    comment="Grupos de cuentas que parecen la misma persona (por email canónico o por nombre y "
            "teléfono). Se marcan, no se fusionan: la decisión es del negocio.",
)
def customer_duplicate_groups():
    c = spark.read.table("silver.customers")  # noqa: F821
    # Solo emails con formato válido: valores de relleno como "sin-correo" no identifican a nadie.
    email_key = F.when(F.col("email_valido"), F.regexp_replace("email_norm", r"\+[^@]*@", "@"))
    phone_digits = F.regexp_replace("phone", r"[^0-9]", "")
    name_phone_key = F.when(
        F.length(phone_digits) > 0,
        F.concat_ws("|", F.lower("first_name"), F.lower("last_name"), phone_digits),
    )

    def groups(key, rule: str):
        return (
            c.withColumn("group_key", key)
            .where("group_key IS NOT NULL")
            .groupBy("group_key")
            .agg(
                F.min("customer_id").alias("principal_customer_id"),  # la cuenta más antigua
                F.sort_array(F.collect_list("customer_id")).alias("customer_ids"),
                F.count("*").alias("accounts"),
            )
            .where("accounts > 1")
            .withColumn("match_rule", F.lit(rule))
        )

    return groups(email_key, "email").unionByName(groups(name_phone_key, "nombre_telefono"))


@dp.materialized_view(
    name="silver.customer_duplicates",
    comment="Una fila por cuenta que pertenece a algún grupo de duplicados, con su cuenta principal "
            "y las reglas que la detectaron.",
)
def customer_duplicates():
    g = spark.read.table("silver.customer_duplicate_groups")  # noqa: F821
    return (
        g.select(F.explode("customer_ids").alias("customer_id"), "principal_customer_id", "match_rule")
        .groupBy("customer_id")
        .agg(
            F.min("principal_customer_id").alias("principal_customer_id"),
            F.sort_array(F.collect_set("match_rule")).alias("match_rules"),
        )
    )
