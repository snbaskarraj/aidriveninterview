"""Pipeline spec loaded from YAML. Ops come from a closed list."""

from __future__ import annotations

from dataclasses import dataclass

import yaml

from src.paths import PIPELINE_PATH

ALLOWED_OPS = {
    "trim",
    "upper",
    "lower",
    "cast_decimal",
    "parse_timestamp",
    "month_start",
    "cents_to_decimal",
    "map_ref",
    "tokenize",
    "last4",
    "sha256",
    "drop",
}
ALLOWED_STEPS = ["standardize", "derive", "conform", "protect", "hash"]
ALLOWED_STRATEGIES = {"insert", "update", "merge", "delete"}


@dataclass
class Pipeline:
    name: str
    engine: str
    mode: str
    delivery: str
    scale_min_rows: int
    load_strategy: str
    keys: list
    group_key: list
    dedup: dict
    transforms: list
    columns: list
    tracked_columns: list
    target_table: str

    def validate(self) -> None:
        if self.engine != "duckdb":
            raise ValueError("engine must be duckdb")
        if self.mode != "batch":
            raise ValueError("mode must be batch")
        if self.load_strategy not in ALLOWED_STRATEGIES:
            raise ValueError("unknown load strategy")
        if self.transforms != ALLOWED_STEPS:
            raise ValueError("transforms must be standardize, derive, conform, protect, hash")
        for column in self.columns:
            if column["op"] not in ALLOWED_OPS:
                raise ValueError("unknown transform op: " + column["op"])


def pipeline_from_dict(payload: dict) -> Pipeline:
    pipeline = Pipeline(
        name=payload["name"],
        engine=payload.get("engine", "duckdb"),
        mode=payload.get("mode", "batch"),
        delivery=payload.get("delivery", "at_least_once"),
        scale_min_rows=int(payload.get("scale_min_rows", 6000000)),
        load_strategy=payload.get("load_strategy", "merge"),
        keys=list(payload.get("keys") or []),
        group_key=list(payload.get("group_key") or []),
        dedup=dict(payload.get("dedup") or {}),
        transforms=list(payload.get("transforms") or []),
        columns=list(payload.get("columns") or []),
        tracked_columns=list(payload.get("tracked_columns") or []),
        target_table=payload.get("target", {}).get("table", "transaction"),
    )
    pipeline.validate()
    return pipeline


def default_pipeline(strategy: str = "merge") -> Pipeline:
    payload = yaml.safe_load(PIPELINE_PATH.read_text())
    payload["load_strategy"] = strategy
    return pipeline_from_dict(payload)
