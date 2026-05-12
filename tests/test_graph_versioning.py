import pytest

from orion.graph_versioning import (
    DEFAULT_BASE_VERSION,
    format_semver,
    is_semver,
    next_release_version,
    parse_semver,
)


@pytest.mark.parametrize("text,expected", [
    ("1.0.0", (1, 0, 0)),
    ("1.0", (1, 0, 0)),
    ("v2.4.1", (2, 4, 1)),
    ("12.34.56", (12, 34, 56)),
    ("  1.2.3  ", (1, 2, 3)),
    (1.0, (1, 0, 0)),  # yaml floats coerce through str()
])
def test_parse_semver_accepts(text, expected):
    assert parse_semver(text) == expected


@pytest.mark.parametrize("text", ["", "abc", "1", "1.2.3-rc1", "1.2.3.4", None, "1.2.x"])
def test_parse_semver_rejects(text):
    assert parse_semver(text) is None


def test_is_semver():
    assert is_semver("1.2.3") is True
    assert is_semver("not-a-version") is False


def test_format_semver_roundtrip():
    assert format_semver(parse_semver("3.4.5")) == "3.4.5"


def test_next_release_version_no_existing_uses_base():
    assert next_release_version([], "1.0") == "1.0.0"
    assert next_release_version([], "2.5") == "2.5.0"
    # bare two-component base implies patch 0
    assert next_release_version([], DEFAULT_BASE_VERSION) == "1.0.0"


def test_next_release_version_bumps_patch():
    assert next_release_version(["1.0.0"], "1.0") == "1.0.1"
    assert next_release_version(["1.0.0", "1.0.1", "1.0.2"], "1.0") == "1.0.3"


def test_next_release_version_jumps_to_base_when_higher():
    # base is a floor; if no release reaches it, the next release is the floor itself
    assert next_release_version(["1.9.5"], "2.0") == "2.0.0"
    assert next_release_version(["1.0.0", "1.5.0"], "2.0") == "2.0.0"


def test_next_release_version_ignores_base_when_existing_is_higher():
    # if existing releases have already passed the floor, just bump from the highest
    assert next_release_version(["2.0.5"], "1.0") == "2.0.6"
    assert next_release_version(["3.1.0", "1.9.9"], "2.0") == "3.1.1"


def test_next_release_version_ignores_non_semver():
    # non-semver entries (old hash-named graph dirs) don't influence numbering
    assert next_release_version(["abcdef", "1.0.0", "not_a_version"], "1.0") == "1.0.1"
    assert next_release_version(["abcdef"], "1.0") == "1.0.0"


def test_next_release_version_invalid_base_falls_back_to_default():
    # garbage base behaves like the default 1.0 floor rather than raising
    assert next_release_version([], "garbage") == "1.0.0"