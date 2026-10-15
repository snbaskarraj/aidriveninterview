"""Streamlit UI for the pipeline, the catalog, and conversational analytics."""

from __future__ import annotations

import os

import httpx
import streamlit as st
import streamlit.components.v1 as components

API = os.environ.get("WEX_API", "http://127.0.0.1:8000")
PERSONAS = (
    "data_steward",
    "data_engineer",
    "business_analyst",
    "product_manager",
)

ARCHITECTURE = """flowchart LR
  landing[Landing file] --> staging[Staging]
  staging --> dedup[Dedup]
  dedup --> transform[Standardize derive conform protect hash]
  transform --> txn[One DuckDB transaction]
  txn --> curated[Curated tables]
  curated --> proof[Proof queries]
  proof --> ui[Streamlit]
"""


def headers() -> dict:
    return {"X-Persona": st.session_state.get("persona", "data_engineer")}


def request(method: str, path: str, **kwargs):
    try:
        response = httpx.request(method, API + path, headers=headers(), timeout=60, **kwargs)
    except httpx.HTTPError as exc:
        st.error("API is not reachable at %s. Start it with uvicorn. (%s)" % (API, exc))
        return None
    if response.status_code >= 400:
        st.error("%s %s" % (response.status_code, response.text))
        return None
    return response.json()


def show_mermaid(source: str, height: int = 360) -> None:
    if not source:
        return
    safe = source.replace("<", "&lt;")
    components.html(
        """
        <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
        <div class="mermaid">%s</div>
        <script>mermaid.initialize({startOnLoad: true});</script>
        """
        % safe,
        height=height,
    )
    with st.expander("Mermaid source"):
        st.code(source, language="mermaid")


def pipeline_page() -> None:
    st.subheader("Batch pipeline")
    st.write(
        "DuckDB loads the landing file with standardize, derive, conform, protect, and hash, then merge. "
        "A replay of the same batch keeps the committed totals."
    )
    show_mermaid(ARCHITECTURE, 280)
    if st.button("Replay demo batch"):
        request(
            "POST",
            "/v1/pipelines/fleet_transactions_daily/runs",
            json={"batch_id": "demo-batch", "source_uri": "data/landing/fleet_transactions.json"},
        )
    latest = request("GET", "/v1/runs/latest")
    if not latest or not latest.get("run"):
        st.info("No committed run yet. From the project root run: python -m src.demo")
        return
    run = latest["run"]
    cols = st.columns(4)
    cols[0].metric("In rows", run["in_rows"])
    cols[1].metric("Deduped", run["deduped"])
    cols[2].metric("Rejected", run["rejected"])
    cols[3].metric("Inserted", run["inserted"])
    st.caption("Run %s · strategy %s · status %s" % (run["run_id"], run["strategy"], run["status"]))

    proof = request("GET", "/v1/runs/%s/proof?check=purchase&purchase_id=GRP-77" % run["run_id"])
    column = request("GET", "/v1/runs/%s/proof?check=column" % run["run_id"])
    left, right = st.columns(2)
    with left:
        st.markdown("**Purchase GRP-77**")
        if proof:
            st.metric("Proof broken", proof["broken"])
            st.dataframe(proof["rows"], width="stretch")
            st.code(proof["sql"], language="sql")
    with right:
        st.markdown("**Column proof**")
        if column:
            st.metric("Mismatches", column["broken"])
            st.dataframe(column["rows"], width="stretch")

    trace = request("GET", "/v1/runs/%s/transform" % run["run_id"])
    if trace:
        st.markdown("**Source to target**")
        st.write("Steps: " + " → ".join(trace["steps"]))
        show_mermaid(trace["mermaid"], 420)
        st.dataframe(trace["sample"], width="stretch")
        if trace["rejects"]:
            st.markdown("**Rejects**")
            st.dataframe(trace["rejects"], width="stretch")

    sample = request("GET", "/v1/runs/%s/sample" % run["run_id"])
    if sample and sample.get("row"):
        st.markdown("**One completion row as this persona sees it**")
        st.json(sample["row"])


def catalog_page() -> None:
    st.subheader("Governance catalog")
    payload = request("GET", "/v1/catalog/assets")
    if not payload:
        return
    assets = payload["assets"]
    st.dataframe(assets, width="stretch")
    labels = [asset["asset_id"] for asset in assets]
    if not labels:
        return
    selected = st.selectbox("Asset", labels)
    detail = request("GET", "/v1/catalog/assets/%s" % selected)
    lineage = request("GET", "/v1/catalog/assets/%s/lineage" % selected)
    if detail:
        st.dataframe(detail["columns"], width="stretch")
    if lineage:
        show_mermaid(lineage["mermaid"], 420)
    policy = request("GET", "/v1/catalog/policy")
    if policy:
        st.markdown("**Persona policy**")
        rows = []
        classes = list(next(iter(policy["policy"].values())).keys())
        for kind in classes:
            row = {"class": kind}
            for persona in policy["personas"]:
                row[persona] = policy["policy"][persona][kind]
            rows.append(row)
        st.dataframe(rows, width="stretch")


def ask_page() -> None:
    st.subheader("Conversational analytics")
    st.write("LangChain, LangGraph, and RAG bind the question to one governed metric. DuckDB runs the compiled SQL.")
    examples = (
        "What is the net settled amount for GRP-77?",
        "How many transactions?",
        "How many benefits claims?",
        "What is the weather today?",
    )
    choice = st.radio("Engine", ("langchain", "langgraph", "rag"), horizontal=True)
    question = st.text_input("Question", examples[0])
    st.caption("Examples: " + " · ".join(examples))
    if st.button("Ask") and question:
        result = request("POST", "/v1/ask", json={"question": question, "engine": choice})
        if not result:
            return
        st.markdown("**" + result["answer"] + "**")
        if result.get("sql"):
            st.code(result["sql"], language="sql")
        if result.get("rows"):
            st.dataframe(result["rows"], width="stretch")
        show_mermaid(result.get("mermaid") or "", 240)


def main() -> None:
    st.set_page_config(page_title="WEX Pipeline Builder", layout="wide")
    st.title("WEX data pipeline builder")
    st.session_state.persona = st.sidebar.selectbox("Persona", PERSONAS, index=1)
    health = request("GET", "/health")
    ready = request("GET", "/ready")
    if health:
        st.sidebar.success("Health: %s" % health["status"])
    if ready:
        st.sidebar.write("Catalog %s" % ready.get("catalog_version"))
    page = st.sidebar.radio("Page", ("Pipeline", "Catalog", "Ask"))
    if page == "Pipeline":
        pipeline_page()
    elif page == "Catalog":
        catalog_page()
    else:
        ask_page()


main()
