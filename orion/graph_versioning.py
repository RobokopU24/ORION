"""Semantic versioning for graph releases.

ORION distinguishes two versions for every graph it builds:

- **build_version** — a deterministic ``xxh64`` hash of the graph's inputs (data
  source versions, normalization schemes, merge strategies, and the build_versions
  of any subgraphs). Two builds with identical inputs share a build_version, so
  this is what ORION uses to tell whether a graph actually changed. It keys the
  directories ORION builds into (``{graphs_dir}/{graph_id}/{build_version}/``),
  which is what lets an unchanged rebuild be found on disk, and what a graph
  downloaded from the registry is stored under.
- **release_version** — a human-facing semantic version (``MAJOR.MINOR.PATCH``)
  that appears in graph metadata and keys everything published: the graph
  registry's endpoints, the ``contentUrl`` a graph's metadata advertises, and the
  directories ``orion-publish`` copies finished builds into. It is bumped
  automatically: when a graph is built and its build_version differs from every
  previously released build_version of that graph, the ``PATCH`` component of
  the highest existing release is incremented. A graph spec may declare a
  ``base_release_version:`` floor (e.g. ``"2.0"``) to jump the
  ``MAJOR``/``MINOR`` components for a release.
"""

import re

DEFAULT_BASE_RELEASE_VERSION = "1.0"

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


def next_release_version(existing_release_versions,
                         base_release_version: str = DEFAULT_BASE_RELEASE_VERSION) -> str:
    """Return the next release_version given the release_versions that already exist for a graph.

    - Entries in ``existing_release_versions`` that aren't valid semver are ignored.
    - ``base_release_version`` is a floor: the result is never lower than
      ``base_release_version`` (with its ``PATCH`` treated as 0 unless explicitly given).
    - If nothing valid exists at or above the floor, the result is the floor itself.
    - Otherwise the highest existing release_version has its ``PATCH`` incremented.

    This never returns a release_version that already exists in
    ``existing_release_versions``; reusing an existing release for an unchanged
    build is handled separately by matching build_versions before this is called.
    """
    base = parse_semver(base_release_version) or parse_semver(DEFAULT_BASE_RELEASE_VERSION)
    parsed = [v for v in (parse_semver(s) for s in (existing_release_versions or [])) if v is not None]
    if not parsed:
        return format_semver(base)
    highest = max(parsed)
    if highest < base:
        return format_semver(base)
    bumped = (highest[0], highest[1], highest[2] + 1)
    return format_semver(max(bumped, base))