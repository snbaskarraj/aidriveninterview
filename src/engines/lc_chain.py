"""LangChain synonym chain. It returns a catalog interpretation, not SQL."""

from __future__ import annotations

from langchain_core.runnables import RunnableLambda

from src.engines.common import interpret


def run_langchain(question: str) -> dict:
    chain = RunnableLambda(lambda text: text) | RunnableLambda(interpret)
    return chain.invoke(question)
