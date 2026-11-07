# Architecture

`trysac-etl-mini` provides a single-process engine for executing ETL pipelines defined as Directed Acyclic Graphs (DAGs).

## Data Flow
1. **Validation**: The `Pipeline` class validates the spec for structure, duplicate IDs, cycle detection, and unknown inputs/steps.
2. **Ordering**: Topological sorting uses Kahn's algorithm, with ties resolved by declaration order.
3. **Execution**: The `run` function processes steps sequentially. 
   - Inputs are deep-copied (`copy.deepcopy`) to prevent mutation.
   - Steps run in isolation; failures skip descendants.
   - Retries trigger after `TransientError` with backoff `backoff_s * 2^(attempt-1)`.
4. **Observability**: Lineage is recorded via `RunReport`, containing `StepRun` entries with SHA-256 row fingerprints and execution timestamps.

## Built-in Steps
- `filter`: Keeps rows where `row[field] == equals`.
- `select`: Keeps specific fields and applies renaming.
- `dedupe`: Removes rows based on key fields.
- `union`: Concatenates all input lists.
