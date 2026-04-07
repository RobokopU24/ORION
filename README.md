# ORION

### Operational Routine for the Ingest and Output of Networks

ORION ingests data from knowledge sources and converts them into [Biolink Model](https://biolink.github.io/biolink-model/) knowledge graphs in [KGX](https://github.com/biolink/kgx) format.

Each data source goes through the following pipeline:

1. **Fetch** - retrieve the original data source
2. **Parse** - transform the data into KGX files
3. **Normalize** - use normalization services to convert identifiers and ontology terms to preferred synonyms
4. **Supplement** - add supplementary knowledge specific to that source

Sources are defined in a Graph Spec yaml file (see examples in the `graph_specs/` directory). ORION automatically runs each specified source through the pipeline and merges them into a Knowledge Graph.

### Installation

ORION requires [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/RobokopU24/ORION.git
cd ORION
uv sync --extra robokop
```

The core library is also available on PyPI (`pip install robokop-orion`), but the full repository is needed to utilize ingest modules from the [ROBOKOP](https://robokop.renci.org/) project.

### CLI Commands

After installation, the following commands are available (prefix with `uv run` if not using a uv-managed shell):

| Command | Description                                           |
|---|-------------------------------------------------------|
| `orion-build` | Build complete knowledge graphs from a Graph Spec     |
| `orion-ingest` | Run the ingest pipeline for individual data sources   |
| `orion-merge` | Merge KGX node/edge files                             |
| `orion-meta-kg` | Generate MetaKG and test data files                   |
| `orion-redundant-kg` | Generate edge files with redundant biolink predicates |
| `orion-ac` | Generate AnswerCoalesce files                         |
| `orion-neo4j-dump` | Generate Neo4j database dumps                         |
| `orion-memgraph-dump` | Generate Memgraph database dumps                      |

### Configuring ORION

ORION is configured via environment variables, which can be set directly or through an `.env` file. 

In most cases, you can simply use this provided script to set up a local environment. It will create directories for ORION outputs next to where ORION was installed and set env vars pointing to them.

```bash
source ./set_up_dev_env.sh
```

For more customization and settings, use an .env file. Copy or rename the `.env.example` file to `.env`.

Then uncommment and edit `.env` as desired to set values for your environment.

| Variable | Purpose                                                    | Default |
|---|------------------------------------------------------------|---|
| `ORION_STORAGE` | Path to a directory for data ingest pipeline storage       | (required) |
| `ORION_GRAPHS` | Path to a directory for Knowledge Graph outputs            | (required) |
| `ORION_LOGS` | Path to a Log file directory (if unset, logs go to stdout) | `None` |
| `ORION_GRAPH_SPEC` | Graph Spec filename from `graph_specs/`                    | `example-graph-spec.yaml` |
| `ORION_GRAPH_SPEC_URL` | URL to a remote Graph Spec file                            | |

Configuration is managed by [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) — environment variables override `.env` file values, and sensible defaults are provided where possible. See `orion/config.py` for the full list of settings.

#### Graph Spec

A Graph Spec yaml file defines which sources to include in a knowledge graph. Set one of the following (not both):

- `ORION_GRAPH_SPEC` - name of a file in the `graph_specs/` directory
- `ORION_GRAPH_SPEC_URL` - URL pointing to a Graph Spec yaml file

Here is a simple Graph Spec example:

```yaml
graphs:
  - graph_id: Example_Graph
    graph_name: Example Graph
    graph_description: A free text description of what is in the graph.
    output_format: neo4j
    sources:
      - source_id: DrugCentral
      - source_id: HGNC
```

See the full list of data sources and their identifiers in the [data sources file](https://github.com/RobokopU24/ORION/blob/master/orion/data_sources.py).

#### Graph Spec Parameters

The following parameters can be set per data source:

- **merge_strategy** - alternative merge strategies
- **strict_normalization** - whether to discard nodes that fail to normalize (true/false)
- **conflation** - whether to conflate genes with proteins and chemicals with drugs (true/false)

The following can be set at the graph level:

- **add_edge_id** - whether to add unique identifiers to edges (true/false)
- **edge_id_type** - if add_edge_id is true, the type of identifier can be specified (uuid or orion)

See the `graph_specs/` directory for more examples.

### Running with Docker

Make sure environment variables are set or an `.env` file is configured with at least `ORION_STORAGE`, and `ORION_GRAPHS` pointing to valid host directories. The compose file reads these env vars and mounts those directories as volumes in the container.

Build the image:

```bash
docker compose build
```

Build all graphs in the configured Graph Spec:

```bash
docker compose up
```

Build a specific graph:

```bash
docker compose run orion orion-build Example_Graph
```

Run the ingest pipeline for a single data source:

```bash
docker compose run orion orion-ingest DrugCentral
```

See available data sources and options:

```bash
docker compose run orion orion-ingest -h
```

### Development

Install dev dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync --extra robokop --group dev
```

Run tests:

```bash
uv run pytest tests/
```

### Contributing

Contributions are welcome, see the [Contributor README](README-CONTRIBUTER.md).