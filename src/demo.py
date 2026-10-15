"""Build the local DuckDB file, run the batch, and print the proof."""

from __future__ import annotations

from src.data.generate import generate_rows, seed_dimensions, write_landing
from src.paths import DB_PATH
from src.pipeline.proof import prove
from src.pipeline.runner import apply_batch
from src.pipeline.spec import default_pipeline
from src.store import connect


def main() -> None:
    rows = generate_rows()
    landing = write_landing(rows)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    for path in (DB_PATH, DB_PATH.with_name(DB_PATH.name + ".wal")):
        if path.exists():
            path.unlink()
    conn = connect(str(DB_PATH))
    pipeline = default_pipeline("merge")
    first = apply_batch(conn, pipeline, "demo-batch", rows)
    second = apply_batch(conn, pipeline, "demo-batch", rows)
    seed_dimensions(conn)
    column_proof = prove(conn, "demo-batch", "column")
    purchase_proof = prove(conn, "demo-batch", "purchase", "GRP-77")
    dup = conn.execute(
        """
        SELECT amount FROM curated.transaction
        WHERE source_event_id = 'TX-DUP'
        """
    ).fetchone()
    conn.close()
    net = purchase_proof["rows"][0]["net_amount"] if purchase_proof["rows"] else None
    print("Landing file: %s" % landing)
    print("DuckDB file: %s" % DB_PATH)
    print(
        "First run: status=%s in=%s deduped=%s rejected=%s inserted=%s"
        % (
            first["status"],
            first["in_rows"],
            first["deduped"],
            first["rejected"],
            first["inserted"],
        )
    )
    print(
        "Replay: status=%s inserted=%s (same committed totals)"
        % (second["status"], second["inserted"])
    )
    print("TX-DUP surviving amount: %s" % (float(dup[0]) if dup else None))
    print("proof_column broken=%s" % column_proof["broken"])
    print("proof_purchase GRP-77 broken=%s net=%s" % (purchase_proof["broken"], net))
    print("")
    print("Next commands, from the project root with the virtualenv active:")
    print("  pytest -q")
    print("  uvicorn src.api.main:app --port 8000")
    print("In a second terminal:")
    print("  streamlit run app/streamlit_app.py")


if __name__ == "__main__":
    main()
