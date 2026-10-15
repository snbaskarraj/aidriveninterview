"""RAG over the glossary and metric definitions."""

from __future__ import annotations

from src.engines.common import interpret, retrieve_docs


def run_rag(question: str) -> dict:
    docs = retrieve_docs(question)
    interpretation = interpret(question)
    if interpretation["metric_id"] is None and docs:
        for doc in docs:
            hinted = doc.metadata.get("metric_id")
            if hinted:
                interpretation = interpret(question)
                break
    interpretation["retrieved"] = len(docs)
    return interpretation
