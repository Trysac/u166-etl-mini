"""CLI entry point for etl-mini."""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence

from etl_mini import EtlError, Pipeline


def main(argv: Sequence[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) != 2 or argv[0] != "validate":
        sys.stderr.write("Usage: python -m etl_mini validate <file.json>\n")
        return 2

    filepath = argv[1]
    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        sys.stderr.write(f"error: IO or parse failure: {e}\n")
        return 2

    try:
        Pipeline._validate_structure(data)
        _step_configs, order = Pipeline._validate_and_sort_steps(data["steps"])
        for step_id in order:
            sys.stdout.write(f"{step_id}\n")
        return 0
    except EtlError as e:
        if e.code == "cycle":
            msg = str(e)
            if msg.startswith("Cycle detected: "):
                cycle_part = msg[len("Cycle detected: ") :]
            else:
                cycle_part = msg
            sys.stderr.write(f"error: cycle: {cycle_part}\n")
        else:
            sys.stderr.write(f"error: {e.code}: {e}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
