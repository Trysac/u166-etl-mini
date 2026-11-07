# Testing Strategy

The test suite located in `tests/` covers logical correctness, graph invariants, and edge cases.

- **Acceptance Tests**: A comprehensive suite verifying all 10 defined acceptance items (pipelines, cycles, retries, fingerprint generation).
- **Cycle Detection**: Specific tests for Kahn's algorithm cycle detection and error path reporting.
- **Fingerprint Determinism**: Validation of SHA-256 outputs against fixed inputs.
- **Step Validation**: Tests for invalid outputs, row limits (100,000), and invalid JSON types.
- **Failure Isolation**: Verification that step mutations do not affect siblings and that failed steps prevent execution of downstream dependencies.
