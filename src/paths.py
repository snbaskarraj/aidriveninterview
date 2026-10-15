"""Repository paths."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "wex.duckdb"
LANDING_PATH = DATA_DIR / "landing" / "fleet_transactions.json"
PIPELINE_PATH = ROOT / "pipelines" / "fleet_transactions_daily.yaml"
METRICS_PATH = ROOT / "semantic" / "metrics.yaml"
GLOSSARY_PATH = ROOT / "semantic" / "glossary.md"
