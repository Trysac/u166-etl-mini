# In-process DAG execution with Kahn's algorithm and cryptographic lineage

Status: accepted (generated, pending human review)

## Context

Small data processing jobs require structured retries, lineage tracking, and dependency management without the overhead of heavy schedulers like Airflow, Dagster, or Prefect.

## Decision

Implement Pipeline with topological sorting using Kahn's algorithm preserving declaration order for ties, bounded retries on TransientError, deep copying of step inputs, and SHA-256 fingerprinting for lineage.

## Consequences

Zero external dependencies, deterministic execution, full testability, and isolated step runs without cross-mutation.
