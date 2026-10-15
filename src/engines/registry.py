"""Engine names accepted by the ask API."""

from __future__ import annotations

from src.engines.lc_chain import run_langchain
from src.engines.lg_graph import run_langgraph
from src.engines.rag_engine import run_rag

ENGINES = {
    "langchain": run_langchain,
    "langgraph": run_langgraph,
    "rag": run_rag,
}
