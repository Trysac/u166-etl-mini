# trysac-etl-mini

Declarative in-process ETL: a JSON pipeline of named steps forming a DAG, four built-in transforms, typed retries with injected sleep, and per-step lineage with row fingerprints.

## Problem
Small data jobs grow from one script into a tangle of functions with ad hoc retries and no record of which input produced which output. Established orchestrators like Airflow or Dagster introduce heavy infrastructure overhead unsuitable for jobs that fit in memory.

## Installation
```bash
pip install trysac-etl-mini
```

## Usage
```python
from etl_mini import Pipeline, Registry, run

registry = Registry()
registry.register("source", lambda inps, p: [{"id": 1, "val": 10}])

spec = {"name": "demo", "steps": [{"id": "start", "use": "source"}]}
pipeline = Pipeline.from_dict(spec, registry)
report = run(pipeline)

print(report.status)  # "ok"
```

## Public API Summary
- **Exceptions**: `EtlError` (pipeline/graph errors), `TransientError` (triggers retry).
- **Registry**: `Registry()` with `register(name: str, fn: StepFn)`.
- **Execution**: `run(pipeline, *, clock=..., sleep=..., on_event=...)`.
- **Limits**: Maximum 50 steps per pipeline; maximum 100,000 rows per step output; retry limit of 5 attempts.
- **Options**: `clock` (defaults to UTC now), `sleep` (defaults to `time.sleep`), `on_event` (hook for step telemetry).

## Why not Airflow/Dagster?
Airflow and Dagster require external servers, schedulers, and databases. `etl-mini` provides deterministic order and lineage for single-process jobs without these dependencies.

## Security
See [docs/SECURITY.md](docs/SECURITY.md) for the threat model and isolation guarantees.
