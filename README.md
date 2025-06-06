# ORION

### Operational Routine for the Ingest and Output of Networks

This package takes data sets from various sources and converts them into Knowledge Graphs.

Each data source will go through the following pipeline before it can be included in a graph:

1. Fetch (retrieve an original data source)
2. Parse (convert the data source into KGX files)
3. Normalize (use normalization services to convert identifiers and ontology terms to preferred synonyms)
4. Supplement (add supplementary knowledge specific to that source)

To build a graph use a Graph Spec yaml file to specify the sources you want. Some examples live in `graph_specs` folder.

ORION will automatically run each data source specified through the necessary pipeline. Then it will merge the specified sources into a Knowledge Graph.

### Installing and Configuring ORION

Create a parent directory:

```
mkdir ~/ORION_root
```

Clone the code repository:

```
cd ~/ORION_root
git clone https://github.com/RobokopU24/ORION.git
```

Next create directories where data sources, graphs, and logs will be stored.

**ORION_STORAGE** - for storing data sources

**ORION_GRAPHS** - for storing knowledge graphs

**ORION_LOGS** - for storing logs

You can do this manually, or use the script indicated below to set up a default workspace.

Option 1: Use this script to create the directories and set the environment variables:

```
cd ~/ORION_root/ORION/
source ./set_up_test_env.sh
```

Option 2: Create three directories and set environment variables specifying paths to the locations of those directories.

```
mkdir ~/ORION_root/storage/
export ORION_STORAGE=~/ORION_root/storage/

mkdir ~/ORION_root/graphs/
export ORION_GRAPHS=~/ORION_root/graphs/

mkdir ~/ORION_root/logs/
export ORION_LOGS=~/ORION_root/logs/
```

#### Specify Graph Spec file.

Next create or select a Graph Spec yaml file, where the content of knowledge graphs to be built is specified.

Set either of the following environment variables, but not both:

Option 1: ORION_GRAPH_SPEC - the name of a Graph Spec file located in the graph_specs directory of ORION

```
export ORION_GRAPH_SPEC=example-graph-spec.yaml
```

Option 2: ORION_GRAPH_SPEC_URL - a URL pointing to a Graph Spec yaml file

```
export ORION_GRAPH_SPEC_URL=https://stars.renci.org/var/data_services/graph_specs/default-graph-spec.yaml
```

#### Building graph

To build a custom graph, alter a Graph Spec file, which is composed of a list of graphs.

For each graph, specify:

**graph_id** - a unique identifier string for the graph, with no spaces

**sources** - a list of sources identifiers for data sources to include in the graph

See the full list of data sources and their identifiers in the [data sources file](https://github.com/RobokopU24/ORION/blob/master/Common/data_sources.py).

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

There are variety of ways to further customize a knowledge graph. The following are parameters you can set for a particular data source. Mostly, these parameters are used to indicate that you'd like to use a previously built version of a data source or a specific normalization of a source. If you specify versions that are not the latest, and haven't previously built a data source or graph with those versions, it probably won't work.

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

Install Docker to create and run the necessary containers.

Use the following command to build the necessary images.

```
docker compose build
```

To build every graph in your Graph Spec use the following command. This runs the command: `python /ORION/Common/build_manager.py all` on the image.

```
docker compose up
```

#### Building specific graphs

To build an individual graph use `build_manager.py` with a graph_id from the Graph Spec. The script merges data sources into complete graphs.

Usage: `build_manager.py [-h] graph_id`
positional arguments:
`graph_id` : ID of the graph to build. Must match an ID from the configured Graph Spec.

Example command to create a graph from a Graph Spec with graph_id: Example_Graph:

```
docker compose run --rm orion python /ORION/Common/build_manager.py Example_Graph
```

#### Run ORION Pipeline on a single data source.

To run the ORION pipeline for a single data source and transform it into KGX files, you can use the `load_manager` script.

```
optional arguments:
  -h, --help : show this help message and exit
  -t, --test_mode : Test mode will process a small sample version of the data.
  -f, --fresh_start_mode : Fresh start mode will ignore previous states and overwrite previous data.
  -l, --lenient_normalization : Lenient normalization mode will allow nodes that do not normalize to persist in the finalized kgx files.
```

Example command to convert data source CTD to KGX files.

```
docker compose run --rm orion python /ORION/Common/load_manager.py CTD
```

To see the available arguments and a list of supported data sources:

```
docker compose run --rm orion python /ORION/Common/load_manager.py -h
```

#### Testing and Troubleshooting

If you are experiencing issues or errors you may want to run tests:

```
docker-compose run --rm orion pytest /ORION
```

#### Contributing to ORION

Contributions are welcome, see the [Contributer README](README-CONTRIBUTER.md).
