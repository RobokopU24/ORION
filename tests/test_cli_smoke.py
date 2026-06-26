"""Smoke tests for every orion-* CLI entry point.

Each test imports the script's main() and invokes it with --help. Argparse exits
with SystemExit(0) on --help; any other failure (import error, broken arg parser,
missing dependency referenced at module import time) surfaces as a test failure.

These are import-level tests, not end-to-end tests. They catch regressions like
"refactored a module and the CLI no longer loads".
"""

import importlib

import pytest


# Mirrors [project.scripts] in pyproject.toml — every script that gets installed
# must load and respond to --help. Add new scripts here when they're added to
# pyproject.toml.
CLI_ENTRY_POINTS = [
    ('orion.graph_pipeline', 'main'),
    ('orion.ingest_pipeline', 'main'),
    ('orion.cli.merge_kgs', 'main'),
    ('orion.cli.generate_meta_kg', 'main'),
    ('orion.cli.generate_redundant_kg', 'main'),
    ('orion.cli.generate_ac_files', 'main'),
    ('orion.cli.neo4j_dump', 'main'),
    ('orion.cli.memgraph_dump', 'main'),
]


@pytest.mark.parametrize("module_path,func_name", CLI_ENTRY_POINTS)
def test_cli_help_runs(module_path, func_name, monkeypatch, capsys):
    module = importlib.import_module(module_path)
    main_func = getattr(module, func_name)
    monkeypatch.setattr('sys.argv', [module_path, '--help'])
    with pytest.raises(SystemExit) as exc_info:
        main_func()
    assert exc_info.value.code == 0, \
        f'{module_path}:{func_name} --help exited with code {exc_info.value.code}'
    captured = capsys.readouterr()
    assert 'usage' in captured.out.lower() or 'usage' in captured.err.lower(), \
        f'{module_path}:{func_name} --help did not print a usage line'