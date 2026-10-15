import pytest

from src.pipeline.runner import apply_batch
from src.pipeline.spec import default_pipeline
from src.store import connect


@pytest.fixture
def conn():
    opened = connect(":memory:")
    try:
        yield opened
    finally:
        opened.close()


def run(conn, rows, strategy="merge", batch_id="b1", fault=None):
    pipeline = default_pipeline(strategy)
    return apply_batch(conn, pipeline, batch_id, rows, fault=fault)
