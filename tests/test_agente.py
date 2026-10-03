"""Controles del agente que no dependen del modelo (D-29, D-30).

Se prueban sin Databricks: Spark, el LLM y MLflow se reemplazan por dobles que registran las
llamadas. Lo que interesa es qué SQL y qué parámetros llegarían a Unity Catalog.
"""
import sys
import types

import pytest

# Dobles de mlflow y databricks.sdk: el agente solo usa el decorador de trazas y el cliente HTTP.
_mlflow = types.ModuleType("mlflow")
_mlflow.trace = lambda *a, **k: (a[0] if a and callable(a[0]) else (lambda f: f))
sys.modules.setdefault("mlflow", _mlflow)
_sdk = types.ModuleType("databricks.sdk")
_sdk.WorkspaceClient = object
sys.modules.setdefault("databricks", types.ModuleType("databricks"))
sys.modules.setdefault("databricks.sdk", _sdk)

import andina_agent as aa  # noqa: E402


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def limit(self, n):
        return FakeResult(self.rows[:n])

    def collect(self):
        return self.rows


class FakeRow(dict):
    def asDict(self):
        return dict(self)


class FakeSpark:
    def __init__(self):
        self.calls = []

    def sql(self, query, args=None):
        self.calls.append((query, args))
        return FakeResult([FakeRow(order_id=1)])


class FakeWs:
    """Cliente del endpoint del LLM: devuelve una respuesta fija o falla."""

    def __init__(self, content=None, error=False):
        self.content, self.error, self.calls = content, error, 0
        self.api_client = self

    def do(self, method, path, body):
        self.calls += 1
        if self.error:
            raise TimeoutError("endpoint no disponible")
        return {"choices": [{"message": {"content": self.content}}]}


def tool(name, *params):
    return {"type": "function", "function": {
        "name": name, "description": "", "parameters": {
            "type": "object", "properties": {p: {"type": "string"} for p in params}, "required": list(params)}}}


@pytest.fixture
def agent():
    a = aa.AndinaAgent.__new__(aa.AndinaAgent)   # sin __init__: no consulta Unity Catalog
    a.spark, a.catalog = FakeSpark(), "andina_test"
    a.tools = [tool("get_customer_orders", "p_limit"), tool("get_order_payments", "p_order_id"),
               tool("shipping_quote", "p_city", "p_order_amount"), tool("refund_eta", "p_method"),
               tool("search_policies", "p_question")]
    return a


# ---------------------------------------------------------------------------
# Solo el cliente autenticado
# ---------------------------------------------------------------------------
def test_la_sesion_sobrescribe_el_cliente_que_pide_el_modelo(agent):
    agent.call_tool("get_customer_orders", {"p_customer_id": 1, "p_limit": 3}, customer_id=371)
    query, args = agent.spark.calls[-1]
    assert args["p_customer_id"] == 371
    assert "get_customer_orders(:p_customer_id, :p_limit)" in query


def test_la_sesion_se_aplica_aunque_el_modelo_no_mande_cliente(agent):
    agent.call_tool("get_order_payments", {"p_order_id": 27913}, customer_id=371)
    assert agent.spark.calls[-1][1] == {"p_customer_id": 371, "p_order_id": 27913}


def test_herramientas_sin_datos_del_cliente_no_reciben_el_cliente(agent):
    agent.call_tool("shipping_quote", {"p_city": "Arequipa", "p_order_amount": 45}, customer_id=371)
    assert "p_customer_id" not in agent.spark.calls[-1][1]


# ---------------------------------------------------------------------------
# Lista blanca, parámetros y límites
# ---------------------------------------------------------------------------
def test_herramienta_no_permitida_no_ejecuta_sql(agent):
    out = agent.call_tool("drop_table", {"name": "silver.customers"}, customer_id=371)
    assert "no permitida" in out["error"]
    assert agent.spark.calls == []


def test_argumentos_extra_se_descartan_y_no_se_concatenan(agent):
    agent.call_tool("search_policies", {"p_question": "x'); DROP TABLE t; --", "sql": "DROP"}, customer_id=371)
    query, args = agent.spark.calls[-1]
    assert "DROP" not in query                       # el texto va como parámetro, no dentro del SQL
    assert args == {"p_question": "x'); DROP TABLE t; --"}


def test_limite_de_pedidos_por_consulta(agent):
    agent.call_tool("get_customer_orders", {"p_limit": 500}, customer_id=371)
    assert agent.spark.calls[-1][1]["p_limit"] == 10
    agent.call_tool("get_customer_orders", {"p_limit": -5}, customer_id=371)
    assert agent.spark.calls[-1][1]["p_limit"] == 1


def test_faltan_parametros_no_ejecuta_sql(agent):
    out = agent.call_tool("shipping_quote", {"p_city": "Lima"}, customer_id=371)
    assert "faltan" in out["error"]
    assert agent.spark.calls == []


def test_el_modelo_no_ve_el_parametro_de_sesion():
    class UCSpark:
        def sql(self, query, args=None):
            if "routines" in query:
                return FakeResult([types.SimpleNamespace(routine_name=n, comment="c") for n in aa.ALLOWED_TOOLS])
            return FakeResult([
                types.SimpleNamespace(specific_name="get_customer_orders", parameter_name="p_customer_id",
                                      data_type="INT", comment="sesión"),
                types.SimpleNamespace(specific_name="get_customer_orders", parameter_name="p_limit",
                                      data_type="INT", comment="cuántos"),
            ])

    a = aa.AndinaAgent.__new__(aa.AndinaAgent)
    a.spark, a.catalog = UCSpark(), "andina_test"
    orders = next(t for t in a._tools_from_unity_catalog() if t["function"]["name"] == "get_customer_orders")
    assert list(orders["function"]["parameters"]["properties"]) == ["p_limit"]


# ---------------------------------------------------------------------------
# Filtro de alcance (D-30)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("label, expected", [("FUERA_DE_ALCANCE", False), ("EN_ALCANCE", True), ("en_alcance.", True)])
def test_filtro_de_alcance(agent, label, expected):
    agent.ws = FakeWs(label)
    assert agent.in_scope("¿Cuál es la capital de Francia?") is expected


def test_filtro_falla_abierto(agent):
    agent.ws = FakeWs(error=True)
    assert agent.in_scope("¿Dónde está mi pedido?") is True


def test_pregunta_bloqueada_no_llega_al_modelo_con_herramientas(agent):
    agent.ws = FakeWs("FUERA_DE_ALCANCE")
    agent._llm = lambda messages: pytest.fail("el modelo con herramientas no debía llamarse")
    out = agent.answer("Escríbeme un poema", customer_id=371)
    assert out == {"answer": aa.OUT_OF_SCOPE_ANSWER, "tool_calls": [], "blocked": True}
    assert agent.spark.calls == []
