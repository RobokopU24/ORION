#!/usr/bin/env bash

#These environment variables are required by Data Services. See the README for more information.

#DATA_SERVICES_STORAGE - a directory for storing data sources
mkdir -p "$PWD/../Data_services_storage"
export DATA_SERVICES_STORAGE="$PWD/../Data_services_storage/"

#DATA_SERVICES_GRAPHS - a directory for storing knowledge graphs
#mkdir -p "$PWD/../Data_services_graphs"
export DATA_SERVICES_GRAPHS="$PWD/../Data_services_graphs/"

#DATA_SERVICES_LOGS - a directory for storing logs
mkdir -p "$PWD/../Data_services_logs"
export DATA_SERVICES_LOGS="$PWD/../Data_services_logs/"

#Use EITHER of the following, DATA_SERVICES_GRAPH_SPEC or DATA_SERVICES_GRAPH_SPEC_URL

#DATA_SERVICES_GRAPH_SPEC - the name of a Graph Spec file located in the graph_specs directory of Data_services
export DATA_SERVICES_GRAPH_SPEC=testing-graph-spec.yml

#DATA_SERVICES_GRAPH_SPEC_URL - a URL pointing to a Graph Spec file
#export DATA_SERVICES_GRAPH_SPEC_URL=https://raw.githubusercontent.com/RENCI-AUTOMAT/Data_services/helm_deploy/graph_specs/yeast-graph-spec.yml

export DATA_SERVICES_NEO4J_PASSWORD=insecurepasswordexample

export DATA_SERVICES_OUTPUT_URL=https://localhost/

#The following environment variables are optional
export EDGE_NORMALIZATION_ENDPOINT=https://bl-lookup-sri.renci.org/
export NODE_NORMALIZATION_ENDPOINT=https://nodenormalization-sri.renci.org/

