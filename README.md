# ORION (Operational Routine for the Ingest and Output of Networks)

ORION ingests data sets from various sources and converts them into interoperable modular Knowledge Graphs.

Each data source goes through the following pipeline before being included in a graph:

1. Fetch (retrieve an original data source)
2. Parse (convert the data source into KGX files)
3. Normalize (use normalization services to convert identifiers and ontology terms to preferred synonyms)
4. Supplement (add supplementary knowledge specific to that source)

To construct knowledge graphs from a combination of data sources, a simple yaml file is used to specify which data sources should be included in the graph. ORION will automatically run each data source specified through the pipeline and merge them into one knowledge graph.

ORION outputs knowledge graphs in KGX format (jsonl) or ready to use neo4j database backups.

### Installing and Configuring ORION

The following steps walk through a typical installation of ORION, where Docker images are built and deployed locally using Docker Compose.

Alternatively, prebuilt Docker images (on ghcr.io) and helm charts for deploying them are also available (./helm/orion/).

#### Installation

Create a parent directory:

```
mkdir ~/ORION_root
```

Clone the code repository:

```
cd ~/ORION_root
git clone https://github.com/RobokopU24/ORION.git
```

#### Setup environment

There are two ways to set up the environment.

##### Option 1:

Run `setup_env.sh` script to create and configure all the required directories and environment variables. The script asks you to supply paths necessary for ORION, or by default will create ones next to the repository directory.

It will also generate a .env file that will be used by Docker. Modify this .env file for any additional environment variables that will be needed by Docker.

```
cd ~/ORION_root/ORION/
./setup_env.sh
```

##### Option 2:

Use `.env.example` as an example to create your own directories and environment variables.

First, create directories to store data sources, graphs and logs, and export corresponding environment variables.

Copy `.env.example` to `.env` and adjust the environment variables in the file according to your needs.

```
mkdir ~/ORION_root/storage/
export ORION_STORAGE=~/ORION_root/storage/

mkdir ~/ORION_root/graphs/
export ORION_GRAPHS=~/ORION_root/graphs/

mkdir ~/ORION_root/logs/
export ORION_LOGS=~/ORION_root/logs/

export ORION_GRAPH_SPEC=example-graph-spec.yaml
OR
export ORION_GRAPH_SPEC_URL=https://stars.renci.org/var/data_services/graph_specs/default-graph-spec.yaml
```

Refer to the table below for description of acceptable environment variables.

#### Environment variables

| Environment variable               | Description                                                                                                        |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| ORION_STORAGE (Required)           | Location to store data from the data sources                                                                       |
| ORION_GRAPHS (Required)            | Location to store graph databases                                                                                  |
| ORION_LOGS (Required)              | Location to store logs                                                                                             |
| ORION_GRAPH_SPEC (Required)        | Name of the Graph Spec file located in the graph_specs directory. Either use this or ORION_GRAPH_SPEC_URL not both |
| ORION_GRAPH_SPEC_URL (Required)    | URL to graph specification. Either use this or ORION_GRAPH_SPEC not both                                           |
| SHARED_SOURCE_DATA_PATH (Required) | Path for shared data across services - absolute path recommended                                                   |
| OPENAI_API_KEY                     | OpenAI API Key for AI/LLM features                                                                                 |
| OPENAI_API_ORGANIZATION            | Organization ID for OpenAI configuration.                                                                          |
| BAGEL_SERVICE_USERNAME             | Username for Bagel Service Authentication                                                                          |
| BAGEL_SERVICE_PASSWORD             | Password for Bagel Service Authentication                                                                          |
| EDGE_NORMALIZATION_ENDPOINT        | URL to edge normalization service                                                                                  |
| NODE_NORMALIZATION_ENDPOINT        | URL to node noramlization service                                                                                  |
| NAMERES_URL                        | URL for name resolver service                                                                                      |
| SAPBERT_URL                        | URL pointing to SABPERT service                                                                                    |
| LITCOIN_PRED_MAPPING_URL           | URL pointing to Litcoin prediction mapping service                                                                 |
| ORION_OUTPUT_URL                   | URL for Orion                                                                                                      |
| BL_VERSION                         | BL Version?                                                                                                        |

### Building graph

To build a custom graph, alter a Graph Spec file, which is composed of a list of graphs.

For each graph, specify at least:
**graph_id** - a unique identifier string for the graph, with no spaces

**sources** - a list of sources identifiers for data sources to include in the graph

Full list of data sources that can be ingested by ORION and their identifiers are in the [data sources file](https://github.com/RobokopU24/ORION/blob/master/Common/data_sources.py).

Here is a simple example.

```
graphs:
  - graph_id: Example_Graph
    graph_name: Example Graph
    graph_description: A free text description of what is in the graph.
    output_format: neo4j
    sources:
      - source_id: CTD
      - source_id: HGNC
```

Knowledge graph can be customized furthermore through the following parameters. These parameters can be set for a particular data source. Mostly, these parameters are used to indicate that you'd like to use a previously built version of a data source or a specific normalization of a source. If you specify versions that are not the latest, and haven't previously built a data source or graph with those versions, it probably won't work.

**source_version** - the version of the data source, as determined by ORION

**parsing_version** - the version of the parsing code in ORION for this source

**merge_strategy** - used to specify alternative merge strategies

The following are parameters you can set for the entire graph, or for an individual data source:

**node_normalization_version** - the version of the node normalizer API (see: https://nodenormalization-sri.renci.org/openapi.json)

**edge_normalization_version** - the version of biolink model used to normalize predicates and validate the KG

**strict_normalization** - True or False specifying whether to discard nodes, node types, and edges connected to those nodes when they fail to normalize

**conflation** - True or False flag specifying whether to conflate genes with proteins and chemicals with drugs

For example, we could customize the previous example:

```
graphs:
  - graph_id: Example_Graph
    graph_name: Example Graph
    graph_description: A free text description of what is in the graph.
    output_format: neo4j
    sources:
      - source_id: CTD
      - source_id: HGNC
```

See the `graph_specs` directory for more examples.

### Running ORION

Make sure that Docker is installed and configured for these steps.

To build the necessary images run:

```
docker compose build
```

To build every graph in your Graph Spec use the following command. This essentially run the command: `python /ORION/Common/build_manager.py all` on the image.

```
docker compose up
```

#### Building specific graphs

To build an individual graph use `build_manager.py` with a graph_id from the Graph Spec. The script runs the pipelins for each data source and merges them into complete graphs.

Usage: `build_manager.py [-h] graph_id`
positional arguments:
`graph_id` : ID of the graph to build. Must match an ID from the configured Graph Spec.

Example command to create a graph from a Graph Spec with graph_id: Example_Graph:

```
docker compose run --rm orion python /ORION/Common/build_manager.py Example_Graph
```

#### Run ORION Pipeline on a single data source.

To run the ORION pipeline for a single data source and transform it into KGX files, you can use the `load_manager` script.

Optional arguments for the script are:
**-h, --help**: show this help message and exit
**-t, --test_mode**: Test mode will process a small sample version of the data.
**-f, --fresh_start_mode**: Fresh start mode will ignore previous states and overwrite previous data.
**-l, --lenient_normalization**: Lenient normalization mode will allow nodes that do not normalize to persist in the finalized kgx files.

Example command to convert data source CTD to KGX files.

```
docker compose run --rm orion python /ORION/Common/load_manager.py CTD
```

To see the available arguments and a list of supported data sources:

```
docker compose run --rm orion python /ORION/Common/load_manager.py -h
```

#### Testing and Troubleshooting

If you alter the codebase, or if you are experiencing issues or errors you may want to run tests:

```
docker-compose run --rm orion pytest /ORION
```

If you are building the docker image and performance issues occur, setting the correct platform for docker may help:

```
export DOCKER_PLATFORM=linux/arm64
```

#### Contributing to ORION

Contributions are welcome, see the [contrinuting](CONTRIBUTING.md) page.
