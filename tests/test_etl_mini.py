"""Tests for etl_mini package."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from etl_mini import (
    EtlError,
    Pipeline,
    Registry,
    TransientError,
    lineage_json,
    run,
)
from etl_mini.__main__ import main


def test_acceptance_item_1_and_2() -> None:
    # Item 1: A pipeline extract -> paid -> unique -> out gives report.status 'ok' and outputs['out'] == [{'id': 1, 'amount': 5}]
    # Item 2: StepRun 'paid' rows_in 3, rows_out 2; 'unique' rows_in 2, rows_out 1
    reg = Registry()

    def extract_fn(inputs: list[list[dict[str, Any]]], params: Any) -> list[dict[str, Any]]:
        return [
            {"id": 1, "status": "paid", "total": 5},
            {"id": 2, "status": "open", "total": 7},
            {"id": 1, "status": "paid", "total": 5},
        ]

    reg.register("extract_step", extract_fn)

    spec = {
        "name": "order_etl",
        "steps": [
            {"id": "extract", "use": "extract_step"},
            {
                "id": "paid",
                "use": "filter",
                "inputs": ["extract"],
                "with": {"field": "status", "equals": "paid"},
            },
            {"id": "unique", "use": "dedupe", "inputs": ["paid"], "with": {"key": ["id"]}},
            {
                "id": "out",
                "use": "select",
                "inputs": ["unique"],
                "with": {"fields": ["id", "total"], "rename": {"total": "amount"}},
            },
        ],
    }

    pipeline = Pipeline.from_dict(spec, reg)
    report = run(pipeline)

    assert report.status == "ok"
    assert report.outputs["out"] == [{"id": 1, "amount": 5}]

    runs_by_id = {s.id: s for s in report.steps}
    assert runs_by_id["paid"].rows_in == 3
    assert runs_by_id["paid"].rows_out == 2
    assert runs_by_id["unique"].rows_in == 2
    assert runs_by_id["unique"].rows_out == 1
    assert runs_by_id["out"].rows_in == 1
    assert runs_by_id["out"].rows_out == 1


def test_acceptance_item_3_kahns_algorithm() -> None:
    # Steps declared in the order c (inputs ['a', 'b']), a, b run in the order ['a', 'b', 'c'] and report.steps follows that order
    reg = Registry()
    reg.register("src_a", lambda inps, p: [{"val": "a"}])
    reg.register("src_b", lambda inps, p: [{"val": "b"}])

    spec = {
        "name": "order_test",
        "steps": [
            {"id": "c", "use": "union", "inputs": ["a", "b"]},
            {"id": "a", "use": "src_a"},
            {"id": "b", "use": "src_b"},
        ],
    }
    pipeline = Pipeline.from_dict(spec, reg)
    assert pipeline.execution_order == ["a", "b", "c"]
    report = run(pipeline)
    assert [s.id for s in report.steps] == ["a", "b", "c"]
    assert report.outputs["c"] == [{"val": "a"}, {"val": "b"}]


def test_acceptance_item_4_errors() -> None:
    # A cycle a -> b -> a raises EtlError with code 'cycle' and a message containing 'a -> b -> a';
    # use 'nope' raises 'unknown_step'; inputs ['missing'] raises 'unknown_input'; two steps with id 'a' raise 'duplicate_step'
    reg = Registry()
    reg.register("dummy", lambda inps, p: [])

    cycle_spec = {
        "name": "cycle_test",
        "steps": [
            {"id": "a", "use": "dummy", "inputs": ["b"]},
            {"id": "b", "use": "dummy", "inputs": ["a"]},
        ],
    }
    with pytest.raises(EtlError) as exc_info:
        Pipeline.from_dict(cycle_spec, reg)
    assert exc_info.value.code == "cycle"
    assert "a -> b -> a" in str(exc_info.value)

    # unknown step
    unknown_spec = {
        "name": "unknown_step_test",
        "steps": [
            {"id": "x", "use": "nope"},
        ],
    }
    with pytest.raises(EtlError) as exc_info:
        Pipeline.from_dict(unknown_spec, reg)
    assert exc_info.value.code == "unknown_step"

    # unknown input
    unknown_input_spec = {
        "name": "unknown_input_test",
        "steps": [
            {"id": "x", "use": "dummy", "inputs": ["missing"]},
        ],
    }
    with pytest.raises(EtlError) as exc_info:
        Pipeline.from_dict(unknown_input_spec, reg)
    assert exc_info.value.code == "unknown_input"

    # duplicate step
    dup_spec = {
        "name": "dup_test",
        "steps": [
            {"id": "a", "use": "dummy"},
            {"id": "a", "use": "dummy"},
        ],
    }
    with pytest.raises(EtlError) as exc_info:
        Pipeline.from_dict(dup_spec, reg)
    assert exc_info.value.code == "duplicate_step"


def test_acceptance_item_5_retries_and_failures() -> None:
    # A step with retries 2 and backoff_s 1.0 that raises TransientError twice then succeeds has attempts 3 and the injected sleep receives [1.0, 2.0];
    # a ValueError is not retried: attempts 1, status 'failed', report.status 'failed' and every downstream step has status 'skipped' and attempts 0
    reg = Registry()
    call_count = 0

    def flaking_step(inputs: list[list[dict[str, Any]]], params: Any) -> list[dict[str, Any]]:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise TransientError("try again")
        return [{"success": True}]

    reg.register("flaky", flaking_step)
    sleep_calls: list[float] = []

    spec_flaky = {
        "name": "retry_pipeline",
        "steps": [
            {"id": "f", "use": "flaky", "retries": 2, "backoff_s": 1.0},
        ],
    }
    p1 = Pipeline.from_dict(spec_flaky, reg)
    report1 = run(p1, sleep=sleep_calls.append)
    assert report1.status == "ok"
    assert report1.steps[0].attempts == 3
    assert sleep_calls == [1.0, 2.0]

    # ValueError branch with downstream skipped
    reg2 = Registry()
    reg2.register("fail_step", lambda inps, p: (_ for _ in ()).throw(ValueError("bad value")))
    reg2.register("downstream_step", lambda inps, p: [{"done": True}])

    spec_fail = {
        "name": "fail_pipeline",
        "steps": [
            {"id": "first", "use": "fail_step", "retries": 3},
            {"id": "downstream", "use": "downstream_step", "inputs": ["first"]},
        ],
    }
    p2 = Pipeline.from_dict(spec_fail, reg2)
    report2 = run(p2)
    assert report2.status == "failed"
    step_map = {s.id: s for s in report2.steps}
    assert step_map["first"].attempts == 1
    assert step_map["first"].status == "failed"
    assert step_map["first"].error == "exception:ValueError"
    assert step_map["downstream"].status == "skipped"
    assert step_map["downstream"].attempts == 0


def test_acceptance_item_6_output_fingerprint() -> None:
    # output_fingerprint is the SHA-256 hex of json.dumps(rows, sort_keys=True, separators=(',', ':'), ensure_ascii=False),
    # so [] gives '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945', and input_fingerprints equal the upstream output fingerprints in input order
    reg = Registry()
    reg.register("empty", lambda inps, p: [])
    reg.register("passthrough", lambda inps, p: inps[0])

    spec = {
        "name": "fp_test",
        "steps": [
            {"id": "s1", "use": "empty"},
            {"id": "s2", "use": "passthrough", "inputs": ["s1"]},
        ],
    }
    p = Pipeline.from_dict(spec, reg)
    report = run(p)
    assert (
        report.steps[0].output_fingerprint
        == "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
    )
    assert report.steps[1].input_fingerprints == (
        "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
    )
    assert (
        report.steps[1].output_fingerprint
        == "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
    )


def test_acceptance_item_7_lineage_json_format() -> None:
    # For pipeline 'p' with one step 'a' using 'src' that returns [] and a clock fixed at datetime(2026, 11, 6, tzinfo=timezone.utc),
    # lineage_json(report) == '{"pipeline":"p","status":"ok","steps":[{"attempts":1,"ended_at":"2026-11-06T00:00:00.000Z","error":null,"id":"a","input_fingerprints":[],"output_fingerprint":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","rows_in":0,"rows_out":0,"started_at":"2026-11-06T00:00:00.000Z","status":"ok","use":"src"}]}'
    reg = Registry()
    reg.register("src", lambda inps, p: [])

    spec = {
        "name": "p",
        "steps": [
            {"id": "a", "use": "src"},
        ],
    }
    fixed_time = datetime(2026, 11, 6, tzinfo=timezone.utc)
    pipeline = Pipeline.from_dict(spec, reg)
    report = run(pipeline, clock=lambda: fixed_time)
    expected = '{"pipeline":"p","status":"ok","steps":[{"attempts":1,"ended_at":"2026-11-06T00:00:00.000Z","error":null,"id":"a","input_fingerprints":[],"output_fingerprint":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","rows_in":0,"rows_out":0,"started_at":"2026-11-06T00:00:00.000Z","status":"ok","use":"src"}]}'
    assert lineage_json(report) == expected


def test_acceptance_item_8_step_output_validation() -> None:
    # A step returning a dict, a list containing 1, or a row with a set value fails with StepRun.error == 'invalid_output', and one returning 100_001 rows fails with 'too_many_rows'
    reg = Registry()
    reg.register("ret_dict", lambda inps, p: {"not": "a list"})  # type: ignore
    reg.register("ret_num_list", lambda inps, p: [1])  # type: ignore
    reg.register("ret_set_val", lambda inps, p: [{"s": {1, 2}}])  # type: ignore
    reg.register("ret_huge", lambda inps, p: [{"x": i} for i in range(100_001)])

    for step_use, expected_err in [
        ("ret_dict", "invalid_output"),
        ("ret_num_list", "invalid_output"),
        ("ret_set_val", "invalid_output"),
        ("ret_huge", "too_many_rows"),
    ]:
        spec = {
            "name": "test_validation",
            "steps": [{"id": "step1", "use": step_use}],
        }
        pipeline = Pipeline.from_dict(spec, reg)
        report = run(pipeline)
        assert report.status == "failed"
        assert report.steps[0].status == "failed"
        assert report.steps[0].error == expected_err


def test_acceptance_item_9_limits_and_sentinel() -> None:
    # A spec with 51 steps raises EtlError with code 'too_many_steps', retries 6 raises 'invalid_spec', and no on_event payload contains the row value 'SENTINEL-ROW'
    reg = Registry()
    reg.register("dummy", lambda inps, p: [{"secret": "SENTINEL-ROW"}])

    steps_51 = [{"id": f"s{i}", "use": "dummy"} for i in range(51)]
    with pytest.raises(EtlError) as exc1:
        Pipeline.from_dict({"name": "huge", "steps": steps_51}, reg)
    assert exc1.value.code == "too_many_steps"

    spec_retries_6 = {
        "name": "bad_retries",
        "steps": [{"id": "s1", "use": "dummy", "retries": 6}],
    }
    with pytest.raises(EtlError) as exc2:
        Pipeline.from_dict(spec_retries_6, reg)
    assert exc2.value.code == "invalid_spec"

    events: list[dict[str, Any]] = []
    pipeline = Pipeline.from_dict(
        {"name": "sentinel_test", "steps": [{"id": "s1", "use": "dummy"}]}, reg
    )
    run(pipeline, on_event=events.append)

    assert len(events) == 2  # started, finished
    for evt in events:
        evt_str = str(evt)
        assert "SENTINEL-ROW" not in evt_str


def test_acceptance_item_10_main_cli(tmp_path: Any, capsys: Any) -> None:
    # main(['validate', 'p.json']) prints 'a\nb\nc\n' and returns 0,
    # a cyclic spec prints 'error: cycle: a -> b -> a' to stderr and returns 1,
    # and a missing file returns 2
    p_json = tmp_path / "p.json"
    p_json.write_text(
        '{"name": "p", "steps": [{"id": "c", "use": "union", "inputs": ["a", "b"]}, {"id": "a", "use": "src"}, {"id": "b", "use": "src"}]}',
        encoding="utf-8",
    )
    code = main(["validate", str(p_json)])
    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "a\nb\nc\n"

    cyclic_json = tmp_path / "cycle.json"
    cyclic_json.write_text(
        '{"name": "c", "steps": [{"id": "a", "use": "x", "inputs": ["b"]}, {"id": "b", "use": "x", "inputs": ["a"]}]}',
        encoding="utf-8",
    )
    code_cycle = main(["validate", str(cyclic_json)])
    captured_cycle = capsys.readouterr()
    assert code_cycle == 1
    assert "error: cycle: a -> b -> a" in captured_cycle.err

    missing_file = tmp_path / "non_existent.json"
    code_missing = main(["validate", str(missing_file)])
    assert code_missing == 2


def test_main_cli_usage_error(capsys: Any) -> None:
    code = main(["something_else"])
    assert code == 2
    code2 = main([])
    assert code2 == 2


def test_clock_must_return_aware_datetime() -> None:
    reg = Registry()
    reg.register("dummy", lambda inps, p: [])
    pipeline = Pipeline.from_dict({"name": "p", "steps": [{"id": "s", "use": "dummy"}]}, reg)
    with pytest.raises(EtlError) as exc:
        run(pipeline, clock=lambda: datetime(2026, 1, 1))  # naive datetime
    assert exc.value.code == "invalid_spec"


def test_registry_registration_and_errors() -> None:
    reg = Registry()
    # valid name
    reg.register("valid_step_1", lambda inps, p: [])
    assert reg.get("valid_step_1") is not None

    # duplicate
    with pytest.raises(EtlError) as exc:
        reg.register("valid_step_1", lambda inps, p: [])
    assert exc.value.code == "invalid_spec"

    # built-in name
    with pytest.raises(EtlError) as exc2:
        reg.register("filter", lambda inps, p: [])
    assert exc2.value.code == "invalid_spec"

    # invalid pattern
    with pytest.raises(EtlError) as exc3:
        reg.register("Invalid-Name", lambda inps, p: [])
    assert exc3.value.code == "invalid_spec"

    with pytest.raises(EtlError) as exc4:
        reg.register("1invalid", lambda inps, p: [])
    assert exc4.value.code == "invalid_spec"


def test_pipeline_spec_validation_errors() -> None:
    reg = Registry()
    # Not a mapping
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict("not a map", reg)  # type: ignore
    assert exc.value.code == "invalid_spec"

    # Missing name
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict({"steps": []}, reg)
    assert exc.value.code == "invalid_spec"

    # Extra keys
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict({"name": "test", "steps": [], "extra": 123}, reg)
    assert exc.value.code == "invalid_spec"

    # Empty steps
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict({"name": "test", "steps": []}, reg)
    assert exc.value.code == "invalid_spec"

    # Invalid step item type
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict({"name": "test", "steps": ["not a dict"]}, reg)
    assert exc.value.code == "invalid_spec"

    # Unknown key in step
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict(
            {"name": "test", "steps": [{"id": "s", "use": "filter", "unknown": 1}]}, reg
        )
    assert exc.value.code == "invalid_spec"

    # Step missing use or id
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict({"name": "test", "steps": [{"use": "filter"}]}, reg)
    assert exc.value.code == "invalid_spec"

    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict({"name": "test", "steps": [{"id": "s"}]}, reg)
    assert exc.value.code == "invalid_spec"

    # Invalid inputs type
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict(
            {"name": "test", "steps": [{"id": "s", "use": "filter", "inputs": "str"}]}, reg
        )
    assert exc.value.code == "invalid_spec"

    # Invalid with type
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict(
            {"name": "test", "steps": [{"id": "s", "use": "filter", "with": "str"}]}, reg
        )
    assert exc.value.code == "invalid_spec"

    # Invalid backoff_s
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict(
            {"name": "test", "steps": [{"id": "s", "use": "filter", "backoff_s": -1.0}]}, reg
        )
    assert exc.value.code == "invalid_spec"

    # Builtin single input requirement
    with pytest.raises(EtlError) as exc:
        Pipeline.from_dict(
            {"name": "test", "steps": [{"id": "s", "use": "filter", "inputs": []}]}, reg
        )
    assert exc.value.code == "invalid_spec"


def test_builtin_step_edge_cases() -> None:
    reg = Registry()
    # Filter parameter errors
    with pytest.raises(EtlError):
        reg.get("filter")([[]], {})  # type: ignore

    # Select parameter errors
    with pytest.raises(EtlError):
        reg.get("select")([[]], {})  # type: ignore
    with pytest.raises(EtlError):
        reg.get("select")([[]], {"fields": "not_a_list"})  # type: ignore
    with pytest.raises(EtlError):
        reg.get("select")([[]], {"fields": [], "rename": "not_a_dict"})  # type: ignore

    # Dedupe parameter errors
    with pytest.raises(EtlError):
        reg.get("dedupe")([[]], {})  # type: ignore
    with pytest.raises(EtlError):
        reg.get("dedupe")([[]], {"key": "not_a_list"})  # type: ignore

    # Union step
    res = reg.get("union")([[{"a": 1}], [{"a": 2}]], {})  # type: ignore
    assert res == [{"a": 1}, {"a": 2}]


def test_transient_error_exhaustion() -> None:
    reg = Registry()

    def always_fail(inputs: Any, params: Any) -> list[dict[str, Any]]:
        raise TransientError("always failing")

    reg.register("always_fail", always_fail)
    spec = {
        "name": "exhaustion",
        "steps": [{"id": "s", "use": "always_fail", "retries": 1, "backoff_s": 0.01}],
    }
    p = Pipeline.from_dict(spec, reg)
    report = run(p)
    assert report.status == "failed"
    assert report.steps[0].status == "failed"
    assert report.steps[0].attempts == 2
    assert report.steps[0].error == "exception:TransientError"


def test_deep_copy_isolation() -> None:
    reg = Registry()
    original_rows = [{"k": [1, 2, 3]}]
    reg.register("source", lambda inps, p: original_rows)

    def mutator(inputs: list[list[dict[str, Any]]], params: Any) -> list[dict[str, Any]]:
        rows = inputs[0]
        rows[0]["k"].append(4)
        return rows

    reg.register("mutator", mutator)

    spec = {
        "name": "iso",
        "steps": [
            {"id": "s1", "use": "source"},
            {"id": "s2", "use": "mutator", "inputs": ["s1"]},
        ],
    }
    p = Pipeline.from_dict(spec, reg)
    report = run(p)
    assert report.outputs["s1"][0]["k"] == [1, 2, 3]
    assert report.outputs["s2"][0]["k"] == [1, 2, 3, 4]


def test_nan_rejected_in_output() -> None:
    reg = Registry()
    reg.register("nan_step", lambda inps, p: [{"val": float("nan")}])
    spec = {
        "name": "nan_pipeline",
        "steps": [{"id": "s", "use": "nan_step"}],
    }
    p = Pipeline.from_dict(spec, reg)
    report = run(p)
    assert report.status == "failed"
    assert report.steps[0].error == "invalid_output"


def test_independent_branches_run_after_sibling_failure() -> None:
    reg = Registry()
    reg.register("fail_step", lambda inps, p: (_ for _ in ()).throw(RuntimeError("boom")))
    reg.register("good_step", lambda inps, p: [{"branch": "ok"}])

    spec = {
        "name": "branching",
        "steps": [
            {"id": "branch_a", "use": "fail_step"},
            {"id": "branch_b", "use": "good_step"},
        ],
    }
    p = Pipeline.from_dict(spec, reg)
    report = run(p)
    assert report.status == "failed"
    step_map = {s.id: s for s in report.steps}
    assert step_map["branch_a"].status == "failed"
    assert step_map["branch_b"].status == "ok"
    assert report.outputs["branch_b"] == [{"branch": "ok"}]
