"""Configuración de la ingesta y nombres de lote (la base de la marca de agua recuperable, D-09)."""
import pytest

from fuentes import TABLES, lot_name, max_published_version, select_tables


def test_todas_las_tablas_tienen_pk():
    # Change Tracking y los DELETE necesitan clave primaria.
    assert all(t["pk"] for t in TABLES)
    assert len({t["target"] for t in TABLES}) == len(TABLES)


def test_select_tables_vacio_son_todas():
    assert select_tables("") == TABLES


def test_select_tables_acepta_nombre_de_fuente_o_destino():
    assert [t["target"] for t in select_tables("Customers, order_items")] == ["customers", "order_items"]


def test_select_tables_rechaza_tablas_desconocidas():
    with pytest.raises(ValueError, match="clientes"):
        select_tables("customers,clientes")


def test_lot_name_ordena_por_version():
    # El orden alfabético de los lotes debe ser el orden de las versiones.
    names = [lot_name(v, "incremental", "1") for v in (9, 10, 123)]
    assert names == sorted(names)
    assert names[0] == "to_v000000000009__incremental__run_1"


def test_max_published_version_recupera_la_marca_de_agua():
    entries = [lot_name(7, "full", "a") + "/", lot_name(42, "incremental", "b") + "/", "_staging/"]
    assert max_published_version(entries) == 42


def test_max_published_version_sin_lotes():
    assert max_published_version([]) is None
    assert max_published_version(["_staging/", "otra_carpeta/"]) is None
