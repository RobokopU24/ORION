"""Build the graphs we publish to the ROBOKOP Atlas graph registry.

For each graph in ATLAS_GRAPH_IDS this builds the graph as its spec defines it, then builds every
parser source underneath it (including sources of graph dependencies like Baseline) as its own
standalone graph with default, unconflated normalization.

Run with: python -m orion.cli.build_for_atlas
"""

import sys

from orion.graph_pipeline import GraphBuilder
from orion.logging import get_orion_logger

logger = get_orion_logger(__name__)

ATLAS_GRAPH_IDS = ['RobokopKG', 
                   'CAMKP_Automat',
                   'COHD_Automat',
                   'OHD_Carolina_Automat']


# Every parser source id a graph is built from, in spec order, recursing into graph dependencies.
def get_parser_source_ids(graph_builder: GraphBuilder, graph_id: str, seen: set = None) -> list[str]:
    if seen is None:
        seen = set()
    parser_source_ids = []
    for source in graph_builder.graph_specs[graph_id].sources:
        if source.id in seen:
            continue
        seen.add(source.id)
        if graph_builder.is_parser_source(source.id):
            parser_source_ids.append(source.id)
        else:
            parser_source_ids.extend(get_parser_source_ids(graph_builder, source.id, seen))
    return parser_source_ids


# Returns the ids of any graphs or sources that failed to build.
def build_for_atlas(graph_builder: GraphBuilder, graph_ids: list[str] = None) -> list[str]:
    graph_ids = graph_ids or ATLAS_GRAPH_IDS
    failed = []
    source_ids = []
    for graph_id in graph_ids:
        if not graph_builder.build_graph(graph_builder.graph_specs[graph_id]):
            failed.append(graph_id)
        source_ids.extend(source_id for source_id in get_parser_source_ids(graph_builder, graph_id)
                          if source_id not in source_ids)

    logger.info(f'Building {len(source_ids)} source graphs for {", ".join(graph_ids)}: {", ".join(source_ids)}')
    for source_id in source_ids:
        if not graph_builder.build_source_graph(source_id):
            failed.append(source_id)
    return failed


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    graph_builder = GraphBuilder()
    failed = build_for_atlas(graph_builder)
    results_path = graph_builder.write_build_results()
    if results_path:
        print(f'Build results written to {results_path}')
    if failed:
        print(f'Failed to build: {", ".join(failed)}')
        sys.exit(1)


if __name__ == '__main__':
    main()