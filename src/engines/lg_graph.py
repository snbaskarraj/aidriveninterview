"""LangGraph retrieve-then-validate loop."""

from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, StateGraph

from src.engines.common import interpret, retrieve_docs


class GraphState(TypedDict, total=False):
    question: str
    doc_count: int
    interpretation: dict


def _retrieve(state: GraphState) -> dict:
    docs = retrieve_docs(state["question"])
    return {"doc_count": len(docs)}


def _validate(state: GraphState) -> dict:
    return {"interpretation": interpret(state["question"])}


def _graph():
    graph = StateGraph(GraphState)
    graph.add_node("retrieve", _retrieve)
    graph.add_node("validate", _validate)
    graph.set_entry_point("retrieve")
    graph.add_edge("retrieve", "validate")
    graph.add_edge("validate", END)
    return graph.compile()


def run_langgraph(question: str) -> dict:
    result = _graph().invoke({"question": question})
    return result["interpretation"]
