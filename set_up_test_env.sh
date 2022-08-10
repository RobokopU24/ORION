#!/usr/bin/env bash
export PYTHONPATH=$PWD

# These environment variables are required by Data Services. See the README for more information.
#
# DATA_SERVICES_STORAGE - a directory for storing data sources
# DATA_SERVICES_GRAPHS - a directory for storing knowledge graphs
# DATA_SERVICES_GRAPH_SPEC - a file where graphs to be built are specified
# DATA_SERVICES_LOGS - a directory for storing logs

mkdir -p $PWD/../Data_services_storage
export DATA_SERVICES_STORAGE=$PWD/../Data_services_storage/

mkdir -p $PWD/../Data_services_graphs
export DATA_SERVICES_GRAPHS=$PWD/../Data_services_graphs/

export DATA_SERVICES_GRAPH_SPEC=testing-graph-spec.yml
#export DATA_SERVICES_GRAPH_SPEC_URL=https://raw.githubusercontent.com/RENCI-AUTOMAT/Data_services/helm_deploy/graph_specs/yeast-graph-spec.yml

mkdir -p $PWD/../Data_services_logs
export DATA_SERVICES_LOGS=$PWD/../Data_services_logs/

export DATA_SERVICES_NEO4J_PASSWORD=insecurepasswordexample
