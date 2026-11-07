"""trysac-etl-mini: Declarative in-process ETL with lineage and fingerprinting."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import time
from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__all__ = [
    "EtlError",
    "Pipeline",
    "Registry",
    "RunReport",
    "StepRun",
    "TransientError",
    "lineage_json",
    "run",
]

NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
MAX_STEPS = 50
MAX_ROWS = 100_000
ALLOWED_SPEC_STEP_KEYS = {"id", "use", "inputs", "with", "retries", "backoff_s"}
BUILTIN_NAMES = {"filter", "select", "dedupe", "union"}

StepFn = Callable[[list[list[dict[str, Any]]], Mapping[str, Any]], list[dict[str, Any]]]


class TransientError(Exception):
    """Transient error during step execution that qualifies for retry."""


class EtlError(Exception):
    """Base exception for ETL specification and graph errors."""

    def __init__(
        self,
        code: Literal[
            "invalid_spec",
            "duplicate_step",
            "unknown_step",
            "unknown_input",
            "cycle",
            "too_many_steps",
        ],
        message: str,
    ) -> None:
        super().__init__(message)
        self.code: Literal[
            "invalid_spec",
            "duplicate_step",
            "unknown_step",
            "unknown_input",
            "cycle",
            "too_many_steps",
        ] = code


@dataclass(frozen=True)
class StepRun:
    id: str
    use: str
    status: Literal["ok", "failed", "skipped"]
    attempts: int
    rows_in: int
    rows_out: int
    input_fingerprints: tuple[str, ...]
    output_fingerprint: str | None
    started_at: datetime | None
    ended_at: datetime | None
    error: str | None


@dataclass(frozen=True)
class RunReport:
    pipeline: str
    status: Literal["ok", "failed"]
    steps: tuple[StepRun, ...]
    outputs: Mapping[str, list[dict[str, Any]]]


def _sha256_fingerprint(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _format_iso8601_ms(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    utc_dt = dt.astimezone(timezone.utc)
    ms = int(utc_dt.microsecond / 1000)
    return f"{utc_dt.strftime('%Y-%m-%dT%H:%M:%S')}.{ms:03d}Z"


def lineage_json(report: RunReport) -> str:
    """Serialize lineage data from a RunReport to deterministic JSON."""
    steps_data: list[dict[str, Any]] = []
    for step in report.steps:
        steps_data.append(
            {
                "attempts": step.attempts,
                "ended_at": _format_iso8601_ms(step.ended_at),
                "error": step.error,
                "id": step.id,
                "input_fingerprints": list(step.input_fingerprints),
                "output_fingerprint": step.output_fingerprint,
                "rows_in": step.rows_in,
                "rows_out": step.rows_out,
                "started_at": _format_iso8601_ms(step.started_at),
                "status": step.status,
                "use": step.use,
            }
        )
    data = {
        "pipeline": report.pipeline,
        "status": report.status,
        "steps": steps_data,
    }
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def _builtin_filter(
    inputs: list[list[dict[str, Any]]], params: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if "field" not in params or "equals" not in params:
        raise EtlError("invalid_spec", "Filter step requires 'field' and 'equals' parameters")
    field = params["field"]
    equals = params["equals"]
    result: list[dict[str, Any]] = []
    for row in inputs[0]:
        if field in row and row[field] == equals:
            result.append(row)
    return result


def _builtin_select(
    inputs: list[list[dict[str, Any]]], params: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if "fields" not in params:
        raise EtlError("invalid_spec", "Select step requires 'fields' parameter")
    fields = params["fields"]
    if not isinstance(fields, list):
        raise EtlError("invalid_spec", "'fields' parameter must be a list")
    rename = params.get("rename", {})
    if not isinstance(rename, dict):
        raise EtlError("invalid_spec", "'rename' parameter must be a dict")

    result: list[dict[str, Any]] = []
    for row in inputs[0]:
        new_row: dict[str, Any] = {}
        for f in fields:
            if f in row:
                out_name = str(rename.get(f, f))
                new_row[out_name] = row[f]
        result.append(new_row)
    return result


def _builtin_dedupe(
    inputs: list[list[dict[str, Any]]], params: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if "key" not in params:
        raise EtlError("invalid_spec", "Dedupe step requires 'key' parameter")
    key_fields = params["key"]
    if not isinstance(key_fields, list):
        raise EtlError("invalid_spec", "'key' parameter must be a list")

    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for row in inputs[0]:
        key_values: list[Any] = [row.get(k) for k in key_fields]
        key_repr = json.dumps(key_values, sort_keys=True, separators=(",", ":"))
        if key_repr not in seen:
            seen.add(key_repr)
            result.append(row)
    return result


def _builtin_union(
    inputs: list[list[dict[str, Any]]], _params: Mapping[str, Any]
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for inp in inputs:
        result.extend(inp)
    return result


class Registry:
    """Step function registry."""

    def __init__(self) -> None:
        self._steps: dict[str, StepFn] = {
            "filter": _builtin_filter,
            "select": _builtin_select,
            "dedupe": _builtin_dedupe,
            "union": _builtin_union,
        }

    def register(self, name: str, fn: StepFn) -> None:
        """Register a custom step function."""
        if not isinstance(name, str) or not NAME_PATTERN.match(name):
            raise EtlError("invalid_spec", f"Invalid step name '{name}'")
        if name in BUILTIN_NAMES or name in self._steps:
            raise EtlError(
                "invalid_spec", f"Step '{name}' is already registered or is a built-in step"
            )
        self._steps[name] = fn

    def get(self, name: str) -> StepFn | None:
        """Get a registered step function or None."""
        return self._steps.get(name)


@dataclass(frozen=True)
class StepConfig:
    id: str
    use: str
    inputs: tuple[str, ...]
    params: Mapping[str, Any]
    retries: int
    backoff_s: float


class Pipeline:
    """ETL pipeline definition composed of ordered steps."""

    def __init__(
        self, name: str, steps: list[StepConfig], execution_order: list[str], registry: Registry
    ) -> None:
        self.name = name
        self.steps = steps
        self.steps_by_id = {s.id: s for s in steps}
        self.execution_order = execution_order
        self.registry = registry

    @classmethod
    def from_dict(cls, spec: Mapping[str, Any], registry: Registry) -> Pipeline:
        """Construct a Pipeline from a dictionary spec with validation."""
        Pipeline._validate_structure(spec)
        p_name = spec["name"]
        raw_steps = spec["steps"]
        step_configs, order = Pipeline._validate_and_sort_steps(raw_steps)

        for s in step_configs:
            if registry.get(s.use) is None:
                raise EtlError(
                    "unknown_step", f"Unknown step function '{s.use}' used by step '{s.id}'"
                )
            if s.use in {"filter", "select", "dedupe"} and len(s.inputs) != 1:
                raise EtlError(
                    "invalid_spec", f"Built-in step '{s.use}' requires exactly one input"
                )

        return cls(name=p_name, steps=step_configs, execution_order=order, registry=registry)

    @staticmethod
    def _validate_structure(spec: Mapping[str, Any]) -> None:
        if not isinstance(spec, Mapping):
            raise EtlError("invalid_spec", "Spec must be a mapping")
        if "name" not in spec or not isinstance(spec["name"], str) or not spec["name"]:
            raise EtlError("invalid_spec", "Spec missing valid 'name'")
        if "steps" not in spec or not isinstance(spec["steps"], list):
            raise EtlError("invalid_spec", "Spec missing valid 'steps' list")
        extra = set(spec.keys()) - {"name", "steps"}
        if extra:
            raise EtlError("invalid_spec", f"Unknown keys in spec: {sorted(extra)}")

    @staticmethod
    def _validate_and_sort_steps(raw_steps: list[Any]) -> tuple[list[StepConfig], list[str]]:
        n_steps = len(raw_steps)
        if n_steps == 0:
            raise EtlError("invalid_spec", "Pipeline must have between 1 and 50 steps")
        if n_steps > MAX_STEPS:
            raise EtlError(
                "too_many_steps", f"Pipeline has {n_steps} steps; maximum allowed is {MAX_STEPS}"
            )

        seen_ids: set[str] = set()
        step_configs: list[StepConfig] = []

        for item in raw_steps:
            if not isinstance(item, dict):
                raise EtlError("invalid_spec", "Each step in 'steps' must be a dict")
            extra_keys = set(item.keys()) - ALLOWED_SPEC_STEP_KEYS
            if extra_keys:
                raise EtlError("invalid_spec", f"Unknown keys in step spec: {sorted(extra_keys)}")
            if "id" not in item or not isinstance(item["id"], str) or not item["id"]:
                raise EtlError("invalid_spec", "Step must have a non-empty string 'id'")
            if "use" not in item or not isinstance(item["use"], str) or not item["use"]:
                raise EtlError("invalid_spec", "Step must have a non-empty string 'use'")

            step_id = item["id"]
            if step_id in seen_ids:
                raise EtlError("duplicate_step", f"Duplicate step id '{step_id}'")
            seen_ids.add(step_id)

            inputs = item.get("inputs", [])
            if not isinstance(inputs, list) or not all(isinstance(x, str) for x in inputs):
                raise EtlError(
                    "invalid_spec", f"Step '{step_id}' 'inputs' must be a list of strings"
                )

            params = item.get("with", {})
            if not isinstance(params, dict):
                raise EtlError("invalid_spec", f"Step '{step_id}' 'with' must be a dict")

            retries = item.get("retries", 0)
            if (
                not isinstance(retries, int)
                or isinstance(retries, bool)
                or retries < 0
                or retries > 5
            ):
                raise EtlError(
                    "invalid_spec",
                    f"Step '{step_id}' 'retries' must be an integer between 0 and 5",
                )

            backoff_s = item.get("backoff_s", 1.0)
            if (
                not isinstance(backoff_s, (int, float))
                or isinstance(backoff_s, bool)
                or backoff_s < 0
            ):
                raise EtlError(
                    "invalid_spec", f"Step '{step_id}' 'backoff_s' must be a non-negative float"
                )

            step_configs.append(
                StepConfig(
                    id=step_id,
                    use=item["use"],
                    inputs=tuple(inputs),
                    params=params,
                    retries=retries,
                    backoff_s=float(backoff_s),
                )
            )

        for s in step_configs:
            for inp in s.inputs:
                if inp not in seen_ids:
                    raise EtlError(
                        "unknown_input", f"Step '{s.id}' refers to unknown input '{inp}'"
                    )

        order = Pipeline._topological_sort(step_configs)
        return step_configs, order

    @staticmethod
    def _topological_sort(steps: list[StepConfig]) -> list[str]:
        in_degree: dict[str, int] = {s.id: len(s.inputs) for s in steps}
        dependents: dict[str, list[str]] = defaultdict(list)
        for s in steps:
            for inp in s.inputs:
                dependents[inp].append(s.id)

        # Kahn's algorithm preserving declaration order for ties
        # Maintain queue or list of available nodes in declaration order
        ready = deque([s.id for s in steps if in_degree[s.id] == 0])
        order: list[str] = []

        while ready:
            curr = ready.popleft()
            order.append(curr)
            for dep in dependents[curr]:
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    ready.append(dep)

        if len(order) < len(steps):
            # Cycle detected. Find the cycle for the error message.
            cycle_msg = Pipeline._find_cycle_path(steps)
            raise EtlError("cycle", f"Cycle detected: {cycle_msg}")

        return order

    @staticmethod
    def _find_cycle_path(steps: list[StepConfig]) -> str:
        # Adjacency list: from dependency to dependent (inp -> step.id)
        # A cycle in dependencies inp -> step.id means step.id depends on inp.
        # Acceptance: "A cycle a -> b -> a raises EtlError with code 'cycle'"
        # If b has input 'a' and a has input 'b', the path is a -> b -> a.
        graph: dict[str, list[str]] = {s.id: [] for s in steps}
        for s in steps:
            for inp in s.inputs:
                if inp in graph:
                    graph[inp].append(s.id)

        visited: dict[str, int] = {}
        parent: dict[str, str] = {}
        cycle_nodes: list[str] = []

        def dfs(node: str) -> bool:
            visited[node] = 1
            for neighbor in graph[node]:
                if visited.get(neighbor) == 1:
                    cycle_nodes.append(neighbor)
                    curr = node
                    while curr != neighbor:
                        cycle_nodes.append(curr)
                        curr = parent.get(curr, "")
                    cycle_nodes.append(neighbor)
                    cycle_nodes.reverse()
                    return True
                if visited.get(neighbor) is None:
                    parent[neighbor] = node
                    if dfs(neighbor):
                        return True
            visited[node] = 2
            return False

        for s in steps:
            if visited.get(s.id) is None and dfs(s.id):
                return " -> ".join(cycle_nodes)
        return "cycle"


def _validate_step_output(output: Any) -> tuple[bool, str | None]:
    if not isinstance(output, list):
        return False, "invalid_output"
    if len(output) > MAX_ROWS:
        return False, "too_many_rows"
    for row in output:
        if not isinstance(row, dict):
            return False, "invalid_output"
        for k, _v in row.items():
            if not isinstance(k, str):
                return False, "invalid_output"
    try:
        json.dumps(output, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return False, "invalid_output"
    return True, None


def run(
    pipeline: Pipeline,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> RunReport:
    """Execute a pipeline."""
    # Validate clock returns aware datetime
    test_dt = clock()
    if not isinstance(test_dt, datetime) or test_dt.tzinfo is None or test_dt.utcoffset() is None:
        raise EtlError("invalid_spec", "clock must return an aware datetime")

    outputs: dict[str, list[dict[str, Any]]] = {}
    fingerprints: dict[str, str] = {}
    failed_steps: set[str] = set()
    skipped_steps: set[str] = set()
    step_runs: list[StepRun] = []
    overall_status: Literal["ok", "failed"] = "ok"

    for step_id in pipeline.execution_order:
        config = pipeline.steps_by_id[step_id]
        fn = pipeline.registry.get(config.use)
        if fn is None:
            raise EtlError("unknown_step", f"Unknown step function '{config.use}'")

        # Check if any input step failed or skipped
        has_failed_ancestor = any(
            inp in failed_steps or inp in skipped_steps for inp in config.inputs
        )
        if has_failed_ancestor:
            skipped_steps.add(step_id)
            inp_fps = tuple(fingerprints.get(inp, "") for inp in config.inputs)
            step_runs.append(
                StepRun(
                    id=step_id,
                    use=config.use,
                    status="skipped",
                    attempts=0,
                    rows_in=0,
                    rows_out=0,
                    input_fingerprints=inp_fps,
                    output_fingerprint=None,
                    started_at=None,
                    ended_at=None,
                    error=None,
                )
            )
            continue

        # Prepare inputs with deep copies
        input_rows_list: list[list[dict[str, Any]]] = []
        rows_in = 0
        inp_fps_list: list[str] = []
        for inp in config.inputs:
            rows = outputs[inp]
            rows_in += len(rows)
            inp_fps_list.append(fingerprints[inp])
            input_rows_list.append(copy.deepcopy(rows))
        inp_fps = tuple(inp_fps_list)

        attempts = 0
        success = False
        step_error: str | None = None
        step_output: list[dict[str, Any]] = []
        started_at: datetime | None = None
        ended_at: datetime | None = None

        while attempts <= config.retries:
            attempts += 1
            if attempts == 1:
                started_at = clock()
                if on_event:
                    on_event(
                        {
                            "event": "step_started",
                            "step": step_id,
                            "attempt": attempts,
                            "status": None,
                        }
                    )

            try:
                step_args = [copy.deepcopy(inp) for inp in input_rows_list]
                raw_out = fn(step_args, config.params)
                is_valid, err_code = _validate_step_output(raw_out)
                if not is_valid:
                    step_error = err_code
                    ended_at = clock()
                    break

                step_output = raw_out
                success = True
                ended_at = clock()
                break
            except TransientError:
                if attempts <= config.retries:
                    sleep_time = config.backoff_s * (2.0 ** (attempts - 1))
                    if on_event:
                        on_event(
                            {
                                "event": "step_retry",
                                "step": step_id,
                                "attempt": attempts,
                                "status": None,
                            }
                        )
                    sleep(sleep_time)
                    continue
                else:
                    step_error = "exception:TransientError"
                    ended_at = clock()
                    break
            except Exception as ex:
                step_error = f"exception:{ex.__class__.__name__}"
                ended_at = clock()
                break

        if success:
            out_fp = _sha256_fingerprint(step_output)
            fingerprints[step_id] = out_fp
            outputs[step_id] = step_output
            run_item = StepRun(
                id=step_id,
                use=config.use,
                status="ok",
                attempts=attempts,
                rows_in=rows_in,
                rows_out=len(step_output),
                input_fingerprints=inp_fps,
                output_fingerprint=out_fp,
                started_at=started_at,
                ended_at=ended_at,
                error=None,
            )
            step_runs.append(run_item)
            if on_event:
                on_event(
                    {
                        "event": "step_finished",
                        "step": step_id,
                        "attempt": attempts,
                        "status": "ok",
                    }
                )
        else:
            overall_status = "failed"
            failed_steps.add(step_id)
            run_item = StepRun(
                id=step_id,
                use=config.use,
                status="failed",
                attempts=attempts,
                rows_in=rows_in,
                rows_out=0,
                input_fingerprints=inp_fps,
                output_fingerprint=None,
                started_at=started_at,
                ended_at=ended_at,
                error=step_error,
            )
            step_runs.append(run_item)
            if on_event:
                on_event(
                    {
                        "event": "step_finished",
                        "step": step_id,
                        "attempt": attempts,
                        "status": "failed",
                    }
                )

    return RunReport(
        pipeline=pipeline.name,
        status=overall_status,
        steps=tuple(step_runs),
        outputs=outputs,
    )
