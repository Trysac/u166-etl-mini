"""Behavior tests for the public API of etl_mini."""

from __future__ import annotations

import pytest

import etl_mini


def test_normalize_trims_and_lowercases() -> None:
    assert etl_mini.normalize("  Hello World ") == "hello world"


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_normalize_rejects_blank_input(value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        etl_mini.normalize(value)


def test_version_is_semver() -> None:
    parts = etl_mini.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
