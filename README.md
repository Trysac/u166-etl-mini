# etl-mini

[![PyPI version](https://img.shields.io/pypi/v/trysac-etl-mini)](https://pypi.org/project/trysac-etl-mini/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-etl-mini)](LICENSE)
[![CI](https://github.com/Trysac/u166-etl-mini/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u166-etl-mini/actions/workflows/ci.yml)

Declarative in-process ETL: a JSON pipeline of named steps forming a DAG, four built-in transforms, typed retries with injected sleep, and per-step lineage with row fingerprints

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-etl-mini
# or
uv add trysac-etl-mini
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import etl_mini

# normalize() trims surrounding whitespace and lower-cases the text.
print(etl_mini.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    etl_mini.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
