# Project Record

## Identity
- **Package**: `etl-mini`
- **Module**: `etl_mini`

## Objective
Provide a lightweight, declarative in-process DAG for small data jobs, eliminating orchestration overhead.

## Scope
- In-process DAG execution.
- Deterministic step order via topological sort.
- Built-in `filter`, `select`, `dedupe`, `union`.
- Lineage tracking with SHA-256 fingerprints.
- Bounded retry logic.

## Out of Scope
- Scheduling.
- Persistent output storage.
- Distributed or parallel execution.
- Streaming pipelines.
- Specialized database/file connectors.
- YAML configuration support.

## Factory run u166 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | gemini-3.8-flash | yes | 4942 | 15307 | 0 | 0 | 0.0306 |
| fix-1 | gemini-3.8-flash | no | 81870 | 1322 | 0 | 674445 | 0.1169 |
| fix-2 | gemini-3.8-flash | no | 20544 | 287 | 0 | 27856 | 0.0186 |
| audit | gemini-3.1-flash-lite | no | 92814 | 511 | 0 | 365866 | 0.0331 |
| fix-3 | gemini-3.8-flash | no | 91314 | 798 | 0 | 809295 | 0.1322 |
| docs | gemini-3.1-flash-lite | no | 144514 | 1654 | 0 | 0 | 0.0386 |

Total: $0.3700. Prices: Anthropic list prices.
