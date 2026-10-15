from src.data.rows import planted_purchase
from src.engines.registry import ENGINES
from src.semantic.compiler import compile_query
from tests.conftest import run


def _answer(conn, question):
    found = []
    for name, engine in ENGINES.items():
        interpretation = engine(question)
        interpretation.pop("retrieved", None)
        sql, params = compile_query(interpretation)
        if interpretation.get("clarification"):
            found.append((name, interpretation["metric_id"], "", None))
            continue
        value = conn.execute(sql, params).fetchone()[0] if params else conn.execute(sql).fetchone()[0]
        found.append((name, interpretation["metric_id"], sql, float(value)))
    return found


def test_t10_engines_agree(conn):
    run(conn, planted_purchase(), batch_id="ask")
    found = _answer(conn, "What is the net settled amount for GRP-77?")
    metrics = {item[1] for item in found}
    sqls = {item[2] for item in found}
    values = {item[3] for item in found}
    assert metrics == {"net_settled_amount"}
    assert len(sqls) == 1
    assert values == {45.99}


def test_off_topic_is_clarified(conn):
    found = _answer(conn, "What is the weather today?")
    assert {item[1] for item in found} == {None}
    assert {item[2] for item in found} == {""}
