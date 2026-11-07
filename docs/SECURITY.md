# Security

## Threat Model & Guarantees

### Defended
- **Input Isolation**: Steps receive deep copies of upstream data. A step cannot mutate the state of ancestor or sibling steps.
- **Payload redaction**: `on_event` notifications only emit metadata (event type, attempt count, status). Row content never leaves the scope of the step function or report mapping.
- **Error sanitization**: The `StepRun.error` field captures either a fixed error code (e.g., `invalid_output`, `too_many_rows`) or the exception class name (e.g., `exception:ValueError`). It never includes raw exception messages which might leak data.

### Not Defended
- **Code Execution**: The user is responsible for the security of registered step functions. Malicious step code has the same privileges as the process.
- **Fingerprint collisions**: While SHA-256 is used for row fingerprints, the project makes no cryptographic guarantees beyond standard hashing.
