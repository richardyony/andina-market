"""Reglas de limpieza de silver contra los casos borde de docs/datos_sinteticos.md."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from reglas import (
    ITEM_REMOVED_SQL,
    body_clean,
    country_iso,
    double_charges,
    email_key,
    email_norm,
    email_problem,
    email_valido,
    name_phone_key,
    order_date_clean,
    order_date_corrected,
)


@pytest.fixture
def one_col(make_df):
    """Aplica una expresión a una columna `v` y devuelve los resultados en orden."""
    def run(values, expr):
        df = make_df("i INT, v STRING", list(enumerate(values)))
        return [r.out for r in df.select("i", expr(F.col("v")).alias("out")).orderBy("i").collect()]
    return run


# ---------------------------------------------------------------------------
# País sucio
# ---------------------------------------------------------------------------
def test_country_iso_normaliza_variantes(one_col):
    values = ["PE", " peru ", "Perú", "colombia", "Chile", "MEX", "México", "ecuador"]
    assert one_col(values, country_iso) == ["PE", "PE", "PE", "CO", "CL", "MX", "MX", "EC"]


def test_country_iso_desconocido_queda_null(one_col):
    # NULL se reporta con la expectation pais_reconocido; no se inventa un país.
    assert one_col(["Argentina", "", None], country_iso) == [None, None, None]


# ---------------------------------------------------------------------------
# Email inválido, nulo o con distinta forma
# ---------------------------------------------------------------------------
def test_email_norm_minusculas_sin_espacios_y_vacio_a_null(one_col):
    assert one_col(["  Ana.Perez@Mail.COM ", "", "   ", None], email_norm) == [
        "ana.perez@mail.com", None, None, None]


@pytest.mark.parametrize("email, valido", [
    ("ana@mail.com", True),
    ("ana+tienda@mail.com.pe", True),
    ("sin-correo", False),
    ("ana@@mail.com", False),
    ("ana@mail", False),
    ("ana mail@mail.com", False),
])
def test_email_valido(one_col, email, valido):
    assert one_col([email], email_valido) == [valido]


def test_email_key_une_variantes_de_la_misma_persona(make_df):
    df = make_df("e STRING", [("Ana@Mail.com ",), ("ana+tienda@mail.com",), ("sin-correo",)])
    norm = email_norm(F.col("e"))
    keys = [r.k for r in df.select(email_key(norm, email_valido(norm)).alias("k")).collect()]
    # Las dos primeras son la misma persona; un valor de relleno no agrupa a nadie.
    assert keys == ["ana@mail.com", "ana@mail.com", None]


def test_name_phone_key_usa_solo_digitos_y_exige_telefono(make_df):
    df = make_df("i INT, f STRING, l STRING, p STRING", [
        (0, "Ana", "Pérez", "+51 987-654-321"), (1, "ANA", "pérez", "51987654321"),
        (2, "Ana", "Pérez", None), (3, "Ana", "Pérez", "---")])
    keys = [r.k for r in df.orderBy("i").select(name_phone_key(F.col("f"), F.col("l"), F.col("p")).alias("k")).collect()]
    assert keys[0] == keys[1] == "ana|pérez|51987654321"
    assert keys[2] is None and keys[3] is None


@pytest.mark.parametrize("email, problema", [
    ("anamail.com", "sin @"),
    ("ana@@mail.com", "@ repetida"),
    ("ana@mail", "dominio sin extensión"),
    ("ana mail@mail.com", "otro formato"),
])
def test_email_problem_describe_sin_copiar_el_email(one_col, email, problema):
    out = one_col([email], email_problem)[0]
    assert out == problema
    assert email not in out


# ---------------------------------------------------------------------------
# OrderDate en 2027 (año mal digitado)
# ---------------------------------------------------------------------------
def test_order_date_futura_se_corrige_con_created_at(make_df):
    df = make_df("i INT, od TIMESTAMP, ca TIMESTAMP", [
        (0, datetime(2027, 3, 10, 12), datetime(2026, 3, 10, 12)),   # año mal digitado
        (1, datetime(2026, 3, 10, 18), datetime(2026, 3, 10, 12)),   # mismo día: no se toca
        (2, None, datetime(2026, 3, 10, 12))])
    rows = df.orderBy("i").select(order_date_corrected(F.col("od"), F.col("ca")).alias("c"),
                                  # como texto en UTC: collect() convertiría a la zona horaria del equipo
                                  F.date_format(order_date_clean(F.col("od"), F.col("ca")), "yyyy-MM-dd HH:mm")
                                  .alias("d")).collect()
    assert [r.c for r in rows] == [True, False, False]   # la bandera nunca es NULL
    assert [r.d for r in rows] == ["2026-03-10 12:00", "2026-03-10 18:00", None]


# ---------------------------------------------------------------------------
# Cantidad 0 y DELETE en líneas de pedido
# ---------------------------------------------------------------------------
def test_linea_sale_del_estado_actual_si_se_borra_o_tiene_cantidad_0(make_df):
    df = make_df("i INT, _ct_operation STRING, quantity INT", [(0, "I", 2), (1, "U", 0), (2, "I", -1), (3, "D", 2)])
    out = [r.x for r in df.orderBy("i").select(F.expr(ITEM_REMOVED_SQL).alias("x")).collect()]
    assert out == [False, True, True, True]


# ---------------------------------------------------------------------------
# Body vacío en tickets
# ---------------------------------------------------------------------------
def test_body_vacio_o_solo_espacios_pasa_a_null(one_col):
    assert one_col(["", "   ", " Hola ", None], body_clean) == [None, None, "Hola", None]


# ---------------------------------------------------------------------------
# Doble cobro
# ---------------------------------------------------------------------------
T0 = datetime(2026, 9, 26, 10, 0, 0)


@pytest.fixture
def double(make_df):
    """rows: (payment_id, order_id, amount, created_at, estado actual)."""
    def run(rows):
        approved = make_df("payment_id INT, order_id INT, amount DECIMAL(12,2), created_at TIMESTAMP",
                           [r[:4] for r in rows])
        current = make_df("payment_id INT, method STRING, current_status STRING",
                          [(r[0], "tarjeta", r[4]) for r in rows])
        return double_charges(approved, current).collect()
    return run


def test_doble_cobro_dentro_de_la_ventana(double):
    out = double([(1, 100, 52.5, T0, "Aprobado"), (2, 100, 52.5, T0.replace(second=12), "Aprobado")])
    assert len(out) == 1
    assert (out[0].original_payment_id, out[0].duplicate_payment_id, out[0].seconds_apart) == (1, 2, 12)
    assert out[0].duplicate_current_status == "Aprobado"   # sigue sin devolverse


def test_doble_cobro_ya_reembolsado_sigue_visible(double):
    out = double([(1, 100, 52.5, T0, "Aprobado"), (2, 100, 52.5, T0.replace(second=5), "Reembolsado")])
    assert [r.duplicate_current_status for r in out] == ["Reembolsado"]


@pytest.mark.parametrize("segundo_pago", [
    (2, 100, 52.5, T0.replace(minute=2)),   # 2 minutos después: otra compra, no doble clic
    (2, 100, 10.0, T0.replace(second=5)),   # otro monto
    (2, 101, 52.5, T0.replace(second=5)),   # otro pedido
])
def test_no_es_doble_cobro(double, segundo_pago):
    assert double([(1, 100, 52.5, T0, "Aprobado"), (*segundo_pago, "Aprobado")]) == []
