"""Lightweight HTTP API. Health does not open DuckDB."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.catalog.mask import present
from src.catalog.model import ASSETS, PERSONAS, POLICY
from src.engines.registry import ENGINES
from src.paths import DB_PATH, ROOT
from src.pipeline.proof import prove
from src.pipeline.runner import apply_batch
from src.pipeline.spec import default_pipeline, pipeline_from_dict
from src.pipeline.trace import lineage_mermaid, transform_view
from src.semantic.compiler import compile_query, narrate
from src.store import connect, fetch_dicts

app = FastAPI(title="WEX Pipeline Builder", version="1.0")
_conn = None


class RunIn(BaseModel):
    batch_id: str
    source_uri: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None


class AskIn(BaseModel):
    question: str
    engine: str


def jsonable(value):
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def db():
    global _conn
    if _conn is None:
        path = os.environ.get("WEX_DB", str(DB_PATH))
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        _conn = connect(path)
    return _conn


def reset_db() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


def require_persona(x_persona: Optional[str] = Header(default="data_engineer", alias="X-Persona")) -> str:
    if x_persona not in PERSONAS:
        raise HTTPException(status_code=400, detail="unknown persona")
    return x_persona


def _ledger(conn, run_id: str) -> dict:
    rows = fetch_dicts(conn.execute("SELECT * FROM ops.run_ledger WHERE run_id = ?", [run_id]))
    if not rows:
        raise HTTPException(status_code=404, detail="unknown run")
    return rows[0]


def _pipeline_for(conn, name: str):
    rows = fetch_dicts(
        conn.execute("SELECT spec_json FROM ops.pipeline_spec WHERE name = ?", [name])
    )
    if rows:
        return pipeline_from_dict(json.loads(rows[0]["spec_json"]))
    if name == "fleet_transactions_daily":
        return default_pipeline()
    raise HTTPException(status_code=404, detail="unknown pipeline")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    conn = db()
    version = conn.execute("SELECT version FROM catalog.meta").fetchone()[0]
    open_run = conn.execute(
        "SELECT count(*) FROM ops.run_ledger WHERE status = 'accepted'"
    ).fetchone()[0]
    return {"status": "ready", "catalog_version": version, "open_run": bool(open_run)}


@app.post("/v1/pipelines")
def register_pipeline(payload: Dict[str, Any]):
    try:
        pipeline = pipeline_from_dict(payload)
    except (KeyError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    conn = db()
    conn.execute("DELETE FROM ops.pipeline_spec WHERE name = ?", [pipeline.name])
    conn.execute(
        "INSERT INTO ops.pipeline_spec VALUES (?, ?, ?)",
        [pipeline.name, 1, json.dumps(payload)],
    )
    return {"name": pipeline.name, "version": 1}


@app.post("/v1/pipelines/{name}/runs")
def start_run(name: str, body: RunIn):
    conn = db()
    pipeline = _pipeline_for(conn, name)
    if body.rows is not None:
        rows = body.rows
    elif body.source_uri:
        path = Path(body.source_uri)
        if not path.is_absolute():
            path = ROOT / path
        if not path.exists():
            raise HTTPException(status_code=400, detail="landing file not found")
        rows = json.loads(path.read_text())
    else:
        raise HTTPException(status_code=400, detail="rows or source_uri is required")
    try:
        result = apply_batch(conn, pipeline, body.batch_id, rows)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(status_code=202, content=jsonable(result))


@app.get("/v1/runs/latest")
def latest_run():
    rows = fetch_dicts(
        db().execute(
            """
            SELECT * FROM ops.run_ledger
            WHERE status = 'committed'
            ORDER BY rowid DESC
            LIMIT 1
            """
        )
    )
    if not rows:
        return {"run": None}
    return {"run": jsonable(rows[0])}


@app.get("/v1/runs/{run_id}")
def get_run(run_id: str):
    return jsonable(_ledger(db(), run_id))


@app.get("/v1/runs/{run_id}/transform")
def get_transform(run_id: str, persona: str = Depends(require_persona)):
    conn = db()
    ledger = _ledger(conn, run_id)
    pipeline = _pipeline_for(conn, ledger["pipeline_name"])
    view = transform_view(conn, ledger["batch_id"], pipeline, persona)
    view["counts"] = {
        "in_rows": ledger["in_rows"],
        "deduped": ledger["deduped"],
        "rejected": ledger["rejected"],
        "applied": ledger["applied"],
    }
    return jsonable(view)


@app.get("/v1/runs/{run_id}/proof")
def get_proof(
    run_id: str,
    check: str = "column",
    purchase_id: str = "GRP-77",
    persona: str = Depends(require_persona),
):
    if check not in ("column", "purchase"):
        raise HTTPException(status_code=400, detail="unknown proof check")
    conn = db()
    ledger = _ledger(conn, run_id)
    try:
        result = prove(conn, ledger["batch_id"], check, purchase_id, persona)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return jsonable(result)


@app.get("/v1/runs/{run_id}/sample")
def sample_row(run_id: str, persona: str = Depends(require_persona)):
    conn = db()
    _ledger(conn, run_id)
    rows = fetch_dicts(
        conn.execute(
            """
            SELECT source_event_id, cardholder_name, card_token, pan_last4,
                   amount, product_family, purchase_id, event_kind
            FROM curated.transaction
            WHERE purchase_id = 'GRP-77' AND event_kind = 'completion'
            LIMIT 1
            """
        )
    )
    if not rows:
        return {"row": None}
    return {"row": jsonable(present(rows[0], persona))}


@app.get("/v1/catalog/assets")
def list_assets(page: int = 1):
    conn = db()
    assets = fetch_dicts(conn.execute("SELECT * FROM catalog.asset ORDER BY asset_id"))
    known = {asset["asset_id"]: asset for asset in ASSETS}
    start = max(page - 1, 0) * 50
    page_rows = assets[start : start + 50]
    for row in page_rows:
        meta = known.get(row["asset_id"])
        row["row_count"] = 0
        if meta:
            qualified = "%s.%s" % (meta["schema_name"], meta["table_name"])
            row["row_count"] = conn.execute("SELECT count(*) FROM " + qualified).fetchone()[0]
    return {"assets": jsonable(page_rows)}


@app.get("/v1/catalog/assets/{asset_id}")
def asset_detail(asset_id: str):
    conn = db()
    assets = fetch_dicts(
        conn.execute("SELECT * FROM catalog.asset WHERE asset_id = ?", [asset_id])
    )
    if not assets:
        raise HTTPException(status_code=404, detail="unknown asset")
    columns = fetch_dicts(
        conn.execute(
            "SELECT column_name, data_type, class, mask FROM catalog.column WHERE asset_id = ? ORDER BY column_name",
            [asset_id],
        )
    )
    return {"asset": assets[0], "columns": columns}


@app.get("/v1/catalog/assets/{asset_id}/lineage")
def asset_lineage(asset_id: str):
    edges = fetch_dicts(
        db().execute(
            """
            SELECT source_field, op, target_field
            FROM catalog.lineage_edge
            WHERE asset_id = ?
            """,
            [asset_id],
        )
    )
    return {"mermaid": lineage_mermaid(edges)}


@app.get("/v1/catalog/policy")
def catalog_policy():
    return {"personas": list(PERSONAS), "policy": POLICY}


@app.post("/v1/ask")
def ask(body: AskIn, persona: str = Depends(require_persona)):
    if body.engine not in ENGINES:
        raise HTTPException(status_code=400, detail="unknown engine")
    interpretation = ENGINES[body.engine](body.question)
    interpretation.pop("retrieved", None)
    if interpretation.get("clarification"):
        return {
            "answer": interpretation["clarification"],
            "metric": None,
            "sql": "",
            "rows": [],
            "mermaid": "flowchart LR\n  question[Question] --> clarify[Clarification]",
            "row_count": 0,
        }
    sql, params = compile_query(interpretation)
    result = db().execute(sql, params) if params else db().execute(sql)
    value = result.fetchone()[0]
    metric_id = interpretation["metric_id"]
    table = "benefits_claim" if metric_id == "benefits_claim_count" else "transaction"
    return {
        "answer": narrate(metric_id, value, interpretation.get("filters") or {}),
        "metric": metric_id,
        "sql": sql,
        "rows": [{"value": float(value)}],
        "mermaid": "flowchart LR\n  question[Question] --> %s\n  %s --> metric[%s]\n  metric --> %s[%s]"
        % (body.engine, body.engine, metric_id, table, table),
        "row_count": 1,
        "persona": persona,
    }
