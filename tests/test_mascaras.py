"""Máscaras de datos personales (D-25): lo que ve cada lector."""
import pytest

from mascaras import BODIES, PII_READERS_GROUP, body, body_for, create_statements


@pytest.fixture
def masked(make_df):
    def run(name, value, is_reader=False):
        df = make_df("v STRING", [(value,)])
        return df.selectExpr(f"{body_for(name, is_reader)} AS out").first().out
    return run


@pytest.mark.parametrize("name, value, expected", [
    ("mask_name", "Daniela", "D***"),
    ("mask_name", None, None),
    ("mask_email", "daniela.rojas@icloud.com", "***@icloud.com"),
    ("mask_email", None, None),
    ("mask_phone", "+51 987-654-929", "*** 929"),
    ("mask_phone", None, None),
])
def test_lector_sin_permiso_ve_el_valor_enmascarado(masked, name, value, expected):
    assert masked(name, value) == expected


@pytest.mark.parametrize("name, value", [
    ("mask_name", "Daniela"), ("mask_email", "daniela.rojas@icloud.com"), ("mask_phone", "+51 987-654-929")])
def test_lector_del_grupo_ve_el_valor_real(masked, name, value):
    # El pipeline necesita el valor real para detectar cuentas duplicadas.
    assert masked(name, value, is_reader=True) == value


def test_la_mascara_no_deja_ver_el_valor_original(masked):
    assert "daniela" not in masked("mask_email", "daniela.rojas@icloud.com")
    assert "987" not in masked("mask_phone", "+51 987-654-929")


def test_definicion_igual_a_la_desplegada():
    # Lo que devuelve information_schema.routines en andina_dev y andina_prod (02/10/2026).
    assert body("mask_email") == "CASE WHEN is_member('andina-pii-readers') THEN v ELSE regexp_replace(v, '^[^@]+', '***') END"


def test_create_statements_por_catalogo_y_grupo():
    stmts = create_statements("andina_prod")
    assert len(stmts) == len(BODIES) == 3
    for s in stmts:
        assert s.startswith("CREATE OR REPLACE FUNCTION andina_prod.ops.mask_")   # idempotente
        assert f"is_member('{PII_READERS_GROUP}') THEN v" in s
