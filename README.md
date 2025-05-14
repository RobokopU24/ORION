# ORION

### Operational Routine for the Ingest and Output of Networks

ORION ingests data sets from various sources and converts them into interoperable modular knowledge graphs.

Each data source will go through the following pipeline before it is included in a graph:

1. Fetch (retrieve an original data source)
2. Parse (convert the data source into KGX files)
3. Normalize (use normalization services to convert identifiers and ontology terms to preferred synonyms)
4. Supplement (add supplementary knowledge specific to that source)

To construct knowledge graphs from a combination of data sources, a simple yaml file is used to specify which data sources should be included in a graph. ORION will automatically run each data source specified through the pipeline and merge them into one knowledge graph.

ORION can output knowledge graphs in KGX format (jsonl) or ready to use neo4j database backups.

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

Use the provided script to create and configure all the required directories and environment variables.

It will ask you to supply paths where directories will be created for ORION, or by default will create ones next to the repository directory.

It will also generate an .env file that will be used by Docker.

```
cd ~/ORION_root/ORION/
./create_env.sh
```

##### Option 2:

Use `.env.example` as an example and create your own directories and environment variables.

Create three new directories where data sources, graphs, and logs will be stored:

**ORION_STORAGE** - for storing data sources

**ORION_GRAPHS** - for storing knowledge graphs

**ORION_LOGS** - for storing logs

Copy the contents of `.env.example` to a new file named `.env`.

Change **ORION_STORAGE**, **ORION_GRAPHS**, **ORION_LOGS** to point to the directories created above.

Alter any of the other example environment variables as needed.

### Configuring a Graph Specification

Next create or select a Graph Specification yaml file, where the content of knowledge graphs to be built is specified.

See the `graph_specs` folder for available Graph Specification files and examples.

Set either of the following values in your .env file, but not both:

**ORION_GRAPH_SPEC** : the name of a Graph Spec file located in the graph_specs directory of ORION (example: example-graph-spec.yaml)

**ORION_GRAPH_SPEC_URL** : a URL pointing to a Graph Spec yaml file online (example: https://stars.renci.org/var/data_services/graph_specs/default-graph-spec.yaml)

#### Building a custom graph

To build a custom graph, alter a Graph Spec yaml file, which is composed of a list of graphs.

For each graph, specify:

**graph_id** - a unique identifier string for the graph, with no spaces

**sources** - a list of data source identifiers designating which data sources to include in a graph

See the full list of data sources and their identifiers in the [data sources file](https://github.com/RobokopU24/ORION/blob/master/Common/data_sources.py).

Here is a simple example.

```
graphs:
  - graph_id: Example_Graph
    graph_name: Example Graph Name
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

For example, we could customize the previous example, turning `conflation` on and `strict_normalization` off:

```
graphs:
  - graph_id: Example_Graph
    graph_name: Example Graph
    graph_description: A free text description of what is in the graph.
    output_format: neo4j
    conflation: True
    strict_normalization: False
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

<<<<<<< HEAD
To build an individual graph use `build_manager.py` with a graph_id from the Graph Spec. The script runs the pipeline for each data source and merges them into complete graphs.
=======
To build an individual graph use `build_manager.py` with a graph_id from the Graph Spec. The script merges data sources into complete graphs.
>>>>>>> bf90b53 (ENH: Update the README file with documentation for the two python scripts run through docker)

Usage: `build_manager.py [-h] graph_id`
positional arguments:
`graph_id` : ID of the graph to build. Must match an ID from the configured Graph Spec.

<<<<<<< HEAD
Example command to create a graph from a Graph Spec with the graph_id Example_Graph:
=======
Example command to create a graph from a Graph Spec with graph_id: Example_Graph:
>>>>>>> bf90b53 (ENH: Update the README file with documentation for the two python scripts run through docker)

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

<<<<<<< HEAD
Example command to ingest the data source CTD and convert it into normalized KGX files:
=======
Example command to convert data source CTD to KGX files.
>>>>>>> bf90b53 (ENH: Update the README file with documentation for the two python scripts run through docker)

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
docker compose run --rm orion pytest /ORION
```

If you are building the docker image and performance issues occur, setting the correct platform for docker may help:

<<<<<<< HEAD
```
export DOCKER_PLATFORM=linux/arm64
```

#### Developers and Contributors

If you would like to contribute to ORION see the [contributing](CONTRIBUTING.md) page.
=======
Contributions are welcome, see the [Contributer README](README-CONTRIBUTER.md).
>>>>>>> bf90b53 (ENH: Update the README file with documentation for the two python scripts run through docker)
