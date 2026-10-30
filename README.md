# Laya Customer Decisioning POC

Local Python/Ollama inference experiments and a PostgreSQL historical feature pipeline.
The Laya decision policy has **not** been trained or validated for business use.
No cloud API or API key is required.

## Implemented architecture

- **Inference POC:** synthetic customer JSON -> deterministic features -> Qwen note facts -> Laya choice -> JSON outputs.
- **Historical experiments:** DuckDB reads March/April Parquets; separate runners retain real-data, benchmark, robustness and context-utilization experiments.
- **Feature pipeline:** lossless Parquet consolidation -> client-side PostgreSQL binary COPY -> `raw.gold_events` -> SQL aggregates and dictionary.
- **Compact state:** verified aggregates and raw historical events -> `features.laya_state_v1`. Communication/journey sequences are separate. Only `state_json` is model-facing; identifiers and quality/provenance remain outside it. This build makes no model requests.

The local baseline is 9,856,993 raw rows, 172,692 rows / 104 columns in
`features.customer_features`, and 172,692 rows / 42 columns in `features.laya_state_v1`.
The latest verification passed 78 tests and 24 compact-state SQL checks.

## Repository layout

```text
src/
  data/                     Parquet consolidation and provenance
  db/                       Local psql access, binary COPY and loading
  features/                 Formulas, aggregate/state builds, audits and reporting
  run_feature_pipeline.py
  run_one.py                Synthetic Qwen -> Laya integration
  run_real.py               Historical real-data inference
  laya_client.py            System One client and demo-choice validation
  qwen_summary.py           Validated note-fact extraction
  http_client.py            Local HTTP utilities
  state_builder.py          Deterministic synthetic features
  run_benchmark.py           Retained experiments and shared modules
  run_robustness.py
  run_final_poc.py
sql/                        Feature builds, validation and inspection queries
tests/                     Synthetic fixtures and optional rollback-only PG tests
requirements.txt
.env.example                Placeholder PostgreSQL configuration
Modelfile                   Optional existing-model context configuration
```

Local-only `input/`, `data/`, `outputs/` and `reports/` are excluded from Git.
A fresh checkout does not contain datasets, historical results or credentials.

## Requirements and PowerShell setup

Use Windows 11, PowerShell and Python 3.11+. DuckDB is the only Python dependency.
PostgreSQL builds need a running local server and its `psql` client (verified with
PostgreSQL 18.4). No Python PostgreSQL driver is required. Inference additionally
needs local Ollama with existing `qwen2.5:1.5b` and `laya:latest` models and a compatible
`/v1/systemone` endpoint. Scripts never download models. `Modelfile` is optional;
normal setup does not require rebuilding a working model.

```powershell
Set-Location C:\Aryan\Attributics\Laya_POC
# Fresh checkout only; reuse an existing .venv:
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
# Fresh checkout only; do not overwrite working local credentials:
Copy-Item .env.example .env.postgres
```

Edit `.env.postgres` locally with an authorized role/password for `127.0.0.1:5432`,
database `laya_testing`. Prefer a dedicated role authorized to create/own the pipeline
schemas and tables. Creating a missing database requires permission; scripts do not
reset administrator passwords or change global authentication settings.

The original feature pipeline prefers `.env.postgres.app` if present. The compact
state builder/resolver explicitly reads `.env.postgres`. Explicit `PG*` process
variables override either file. Ollama runners read `OLLAMA_BASE_URL` from process
environment (default `http://127.0.0.1:11434`), not dotenv files.

## Tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
# Include local PostgreSQL integration tests (temporary/rolled-back fixtures):
$env:LAYA_TEST_POSTGRES = '1'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Ordinary tests embed synthetic fixtures and need no customer files or running
models. PostgreSQL integration tests require configured local credentials.

## Data and execution

Obtain authorized local copies and place them in `input/`:

- `events_gold_1_bucket_000_20260331.parquet`
- `events_gold_2_bucket_000_20260430.parquet`

Never commit customer JSON, datasets, database exports, generated Parquet, reports,
logs or inference outputs. `input/sample_customer.json` is also excluded; retain the
local synthetic sample or create one from the fixture shape in `tests/test_pipeline.py`.

```powershell
# Fresh data/database build; existing targets are protected:
.\.venv\Scripts\python.exe .\src\run_feature_pipeline.py
# New compact state, after verified raw/features exist:
.\.venv\Scripts\python.exe .\src\features\build_laya_state_v1.py --communication-limit 4 --journey-limit 4
# Fresh checkout: generate an ignored synthetic sample; fail if it already exists:
.\.venv\Scripts\python.exe -c "import json,sys; from pathlib import Path; sys.path.insert(0,'tests'); from test_pipeline import fixture; p=Path('input/sample_customer.json'); p.parent.mkdir(exist_ok=True); p.open('x',encoding='utf-8').write(json.dumps(fixture(),indent=2))"
# Optional inference; needs the local synthetic fixture and working Ollama:
.\.venv\Scripts\python.exe .\src\run_one.py --output-dir .\outputs\synthetic_new_run
```

The feature runner refuses existing consolidated/raw targets unless supplied its
verified `--consolidation-report` and `--reuse-raw` options. `--archive-existing`
explicitly preserves old managed feature tables before replacement. Compact v1
stops if `features.laya_state_v1` exists; there is no implicit replacement. Its
four-event limits are provisional representation parameters, not business rules.
Use each runner's `--help` before experiments: historical inference outputs may be
replaced by a rerun. Keep the machine awake during long batch jobs.

## Inspect in DBeaver

Connect to local `laya_testing` using your configured credentials:

```sql
SELECT count(*) FROM raw.gold_events;
SELECT count(*) FROM features.customer_features;
SELECT count(*) FROM features.laya_state_v1;
SELECT * FROM features.feature_dictionary ORDER BY feature_family, column_name;
SELECT state_json FROM features.laya_state_v1 ORDER BY context_id LIMIT 1;
SELECT latest_non_campaign_event_name, latest_non_campaign_journey_stage,
       recent_communication_events, recent_non_campaign_journey_events, quality_flags
FROM features.laya_state_v1 ORDER BY context_id LIMIT 5;
```

Additional read-only queries: `sql/inspect_features.sql`. Build/verification SQL in
`sql/features/` runs through its Python orchestrator with validated parameters.
Generated evidence and contracts stay locally in `reports/feature_build/` and
`reports/state_build/`.

## Current limitations

- `context_id` is a source grouping key, not verified customer identity. Application lookup reports no/unique/multiple contexts and never chooses the latest ambiguous context or an authoritative current application.
- Source timestamps are timezone-naive. Compact v1 uses the exclusive `2026-05-01 00:00:00` snapshot; it is not a training state for earlier March/April decisions.
- Counts are observed event rows, including potential duplicate payloads, not verified deliveries, unique messages or proof of interest.
- Verified message matching, response-window completeness and telemetry contracts are missing. Matched rates, time-to-open and channel affinity remain unavailable; affinity also needs approved weights, decay and smoothing parameters.
- Business stage, partner encoding and user-initiated activity definitions remain incomplete. AIP spellings remain source observations without inferred approval/payment outcomes.
- The existing inference catalog is demo-only and includes Email, which was not observed in the real data. It remains for historical compatibility. Compact state uses only SMS, WA and GRCS. Real decisioning requires authoritative action/eligibility contracts.
- Laya showed option-order sensitivity. Choice probabilities are not PTP/outcome probabilities. Labelled evaluation is needed before claims of decision accuracy, confidence, business lift or replacement of an RL objective.
