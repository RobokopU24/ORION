"""Semantic versioning for graph releases.

ORION distinguishes two versions for every graph it builds:

- **build version** — a deterministic ``xxh64`` hash of the graph's inputs
  (data source versions, normalization schemes, merge strategies, subgraph
  versions). Two builds with identical inputs share a build version, so this is
  what ORION uses to tell whether a graph actually changed.
- **release version** — a human-facing semantic version (``MAJOR.MINOR.PATCH``)
  that is what appears in output directories, URLs, and metadata. It is bumped
  automatically: when a graph is built and its build version differs from every
  previously released build version of that graph, the ``PATCH`` component of
  the highest existing release is incremented. A graph spec may declare a
  ``version:`` floor (e.g. ``"2.0"``) to jump the ``MAJOR``/``MINOR``
  components for a release.
"""

import re

DEFAULT_BASE_VERSION = "1.0"

# Accepts "1.2.3", "1.2" (patch defaults to 0), with an optional leading "v".
_SEMVER_RE = re.compile(r'^v?(\d+)\.(\d+)(?:\.(\d+))?$')


def parse_semver(version) -> tuple[int, int, int] | None:
    """Parse a semantic version string into a (major, minor, patch) tuple, or None if it isn't one."""
    if version is None:
        return None
    match = _SEMVER_RE.match(str(version).strip())
    if not match:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch) if patch is not None else 0


def format_semver(version: tuple[int, int, int]) -> str:
    return f'{version[0]}.{version[1]}.{version[2]}'


def is_semver(version) -> bool:
    return parse_semver(version) is not None


def next_release_version(existing_versions, base_version: str = DEFAULT_BASE_VERSION) -> str:
    """Return the next release version given the versions that already exist for a graph.

    - Entries in ``existing_versions`` that aren't valid semver are ignored.
    - ``base_version`` is a floor: the result is never lower than ``base_version``
      (with its ``PATCH`` treated as 0 unless explicitly given).
    - If nothing valid exists at or above the floor, the result is the floor itself.
    - Otherwise the highest existing version has its ``PATCH`` incremented.

    This never returns a version that already exists in ``existing_versions``;
    reusing an existing release for an unchanged build is handled separately by
    matching build versions before this is called.
    """
    base = parse_semver(base_version) or parse_semver(DEFAULT_BASE_VERSION)
    parsed = [v for v in (parse_semver(s) for s in (existing_versions or [])) if v is not None]
    if not parsed:
        return format_semver(base)
    highest = max(parsed)
    if highest < base:
        return format_semver(base)
    bumped = (highest[0], highest[1], highest[2] + 1)
    return format_semver(max(bumped, base))