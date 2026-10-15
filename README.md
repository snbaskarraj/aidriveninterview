# WEX data pipeline builder

A batch pipeline builder for synthetic WEX business data. DuckDB is the engine. The runner is our own code: it does not use dbt or another pipeline framework. Conversational analytics sits on the curated tables through LangChain, LangGraph, and a RAG path. Streamlit draws the lineage as Mermaid diagrams.

The full design is in [design.md](design.md). This file is the problem, the choices, and how to run the app.

## Problem statement

We are building a next-gen Data Pipeline Builder. Pick and choose the runtime engine (SQL or Spark or DuckDB), but do not use existing pipeline frameworks like dbt or others. Formulate a strategy for how the pipeline should work. Call out the role that metadata will play along with how the builder will handle loading strategies like insert, update, merge and delete. Make explicit choices on the data scale, support for batch only or real-time streaming.

## Choices

| Choice | Decision |
| --- | --- |
| Engine | DuckDB |
| Mode | Batch only |
| Scale | At least 6,000,000 transaction events. The demo loads 2,000 plus a planted purchase so it starts quickly. |
| Delivery | At-least-once. A retry of the same batch is deduplicated and idempotent. |
| Load strategies | insert, update, merge, delete. Delete keeps a history row. |
| Transforms | standardize, derive, conform, protect, hash, then the load strategy |
| Metadata | Pipeline spec, run ledger, column classification, persona policy, lineage |
| Analytics | LangChain, LangGraph, and RAG all emit one governed query. One compiler writes the SQL. |
| UI | Streamlit, calling a small HTTP API |

Synthetic data covers three WEX lines of business: fleet fuel cards, employee benefits (HSA, FSA, COBRA), and business payments (virtual card and ACH). Names and card numbers are invented.

A complex purchase, `GRP-77`, is four events that commit in one DuckDB transaction: an auth, a car-wash line, a diesel completion, and a reversal. DuckDB proof queries show the transforms. The net settled amount is `45.99`.

## What a run does

1. Fingerprint the landing file. The same batch id and checksum returns the original result and does not change totals.
2. Land rows in staging. Raw card numbers are replaced with a hash and last four before they are stored.
3. Deduplicate on the event key. The latest event time wins.
4. Transform: trim and cast, derive amount and month, map product and merchant codes, tokenize the card, hash the tracked columns.
5. If any event in a purchase fails, that whole purchase is rejected. Other purchases still commit.
6. Apply insert, update, merge, or delete inside one DuckDB transaction with the run ledger.
7. Read-only proof queries compare landing cents and codes with the curated row.

## Personas

The API applies the mask. The UI only shows what the API returns.

| Class | Data Steward | Data Engineer | Business Analyst | Product Manager |
| --- | --- | --- | --- | --- |
| Public / internal | clear | clear | clear | clear |
| Confidential amounts | clear | clear | aggregate | aggregate |
| Direct PII (name, email) | masked | token | masked | hidden |
| Card data | last four | token | hidden | hidden |
| Benefits health context | label only | token | aggregate count | hidden |

## Prerequisites

- Python 3.9 or newer
- Two terminals
- No API keys. The engines run offline from the metric catalog.

Use the project virtualenv. The `streamlit` command on a Conda `(base)` environment can fail with a protobuf error that does not affect this project.

## How to execute

Run every command from the project root. On macOS and Linux the activate line is `source .venv/bin/activate`.

### 1. Create the virtualenv and install

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Confirm the prompt no longer says `(base)` alone, and that this prints a version:

```bash
which streamlit
streamlit --version
```

`which streamlit` should point inside `.venv`.

### 2. Run the tests

```bash
pytest -q
```

Expect `19 passed, 1 skipped`. The skipped test is the 6,000,000-row load.

### 3. Build the demo database

```bash
python -m src.demo
```

This writes a landing file, loads it with the merge strategy, replays the same batch, and prints the DuckDB proof. You should see:

- `deduped=1` because `TX-DUP` appears twice and the later amount `25.00` is kept
- `rejected=2` for purchase `GRP-88`, which has a bad product code, so the whole purchase stays out of the curated table
- `Replay` with the same inserted count
- `proof_column broken=0`
- `proof_purchase GRP-77` with net `45.99`

### 4. Start the API

Leave this process running.

```bash
source .venv/bin/activate
uvicorn src.api.main:app --port 8000
```

Check it:

```bash
curl -s http://127.0.0.1:8000/health
```

The body is `{"status":"ok"}`. That route does not open DuckDB.

### 5. Start the UI

In a second terminal:

```bash
cd <project-root>
source .venv/bin/activate
streamlit run app/streamlit_app.py
```

Open the local URL Streamlit prints, usually http://localhost:8501.

### 6. What to click

1. **Pipeline.** Counts for the demo batch, the `GRP-77` proof (`4` events, net `45.99`), and the source-to-target table. `4599` cents becomes `45.99`. `DSL` becomes `diesel`. `WASH` becomes `car_wash`. Card values are a token and last four. `GRP-88` is listed under rejects.
2. **Catalog.** Assets, column classes, lineage Mermaid, and the persona policy.
3. **Ask.** Leave the question as `What is the net settled amount for GRP-77?` and press Ask. Switch the engine among LangChain, LangGraph, and RAG. The number stays `45.99`.
4. Try `How many benefits claims?` It returns a count and no member identifier.
5. Try `What is the weather today?` The app clarifies and does not run SQL.
6. Change the sidebar persona to Product Manager and return to Pipeline. The sample completion row hides the name and the card fields.

`Replay demo batch` submits the same batch id again. Inserted totals stay the same.

## Optional scale load

This runs the same SQL transform and merge for the 6,000,000-row contract. It is separate from `pytest -q` and can take several minutes.

```bash
source .venv/bin/activate
WEX_SCALE=1 pytest -q -m scale
```

## API surface

| Method | Path | Role |
| --- | --- | --- |
| GET | `/health` | Liveness. No database. |
| GET | `/ready` | Catalog version only. |
| POST | `/v1/pipelines/{name}/runs` | Start a batch. Returns 202. |
| GET | `/v1/runs/latest` | Latest committed run. |
| GET | `/v1/runs/{id}/transform` | Steps, masked sample, rejects, Mermaid. |
| GET | `/v1/runs/{id}/proof?check=column` | Rows whose transforms do not match. `broken=0` is success. |
| GET | `/v1/runs/{id}/proof?check=purchase&purchase_id=GRP-77` | Purchase net. |
| GET | `/v1/catalog/assets` | Governance catalog. |
| POST | `/v1/ask` | Body: `{"question","engine"}`. Header: `X-Persona`. |

`engine` is `langchain`, `langgraph`, or `rag`.

## Layout

```text
design.md                                 full spec
pipelines/fleet_transactions_daily.yaml   pipeline spec
semantic/metrics.yaml                     governed metrics
semantic/glossary.md                      retrieval text
src/pipeline/                             transform, strategies, runner, proof
src/catalog/                              classification and persona masks
src/engines/                              LangChain, LangGraph, RAG
src/api/main.py                           HTTP API
src/demo.py                               generate, load, print proof
app/streamlit_app.py                      pipeline, catalog, ask
tests/                                    scenario tests T-1 through T-16
```

## Troubleshooting

**`Descriptors cannot be created directly` when starting Streamlit.** The shell is using Anaconda `(base)` Streamlit. Activate `.venv` and run `which streamlit` before `streamlit run`.

**The UI says the API is not reachable.** Start `uvicorn` in the other terminal and leave it running. Streamlit calls http://127.0.0.1:8000.

**Port already in use.** Stop the other process, or pass `--port 8001` to uvicorn and start Streamlit with `WEX_API=http://127.0.0.1:8001 streamlit run app/streamlit_app.py`.
