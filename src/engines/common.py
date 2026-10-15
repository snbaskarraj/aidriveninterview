"""Shared understanding. Engines differ in path and agree after this step."""

from __future__ import annotations

import re

from langchain_core.documents import Document

from src.paths import GLOSSARY_PATH
from src.semantic.compiler import load_metrics


def interpret(question: str) -> dict:
    text = (question or "").lower().strip()
    if not text:
        return _clarify("Enter a question about fleet transactions, benefits, or payments.")
    scored = []
    for metric in load_metrics():
        score = 0
        for phrase in metric["synonyms"]:
            if phrase in text:
                score += len(phrase)
        scored.append((score, metric))
    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best = scored[0]
    if best_score == 0:
        return _clarify(
            "That question is outside the governed fleet, benefits, and payments catalog."
        )
    filters = {}
    match = re.search(r"GRP-\d+", question, re.IGNORECASE)
    if match:
        filters["purchase_id"] = match.group(0).upper()
    if "car wash" in text or "car_wash" in text:
        filters["product_family"] = "car_wash"
    elif "diesel" in text:
        filters["product_family"] = "diesel"
    return {"metric_id": best["id"], "filters": filters, "clarification": None}


def _clarify(message: str) -> dict:
    return {"metric_id": None, "filters": {}, "clarification": message}


def corpus() -> list:
    docs = []
    for metric in load_metrics():
        docs.append(
            Document(
                page_content=metric["description"] + " " + " ".join(metric["synonyms"]),
                metadata={"metric_id": metric["id"]},
            )
        )
    glossary = GLOSSARY_PATH.read_text()
    for block in glossary.split("## "):
        text = block.strip()
        if text and not text.startswith("#"):
            docs.append(Document(page_content=text, metadata={"metric_id": None}))
    return docs


def retrieve_docs(question: str, limit: int = 3) -> list:
    words = set(re.findall(r"[a-z0-9]+", (question or "").lower()))
    scored = []
    for doc in corpus():
        doc_words = set(re.findall(r"[a-z0-9]+", doc.page_content.lower()))
        scored.append((len(words & doc_words), doc))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [doc for score, doc in scored if score > 0][:limit]
