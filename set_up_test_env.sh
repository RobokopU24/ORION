#!/usr/bin/env bash
export PYTHONPATH=$PWD

mkdir -p $PWD/../Data_services_storage
export DATA_SERVICES_STORAGE=$PWD/../Data_services_storage/

SOURCES_SPEC_FILE=$PWD/../Data_services_storage/sources-spec.yml
if [ ! -f "$SOURCES_SPEC_FILE" ]; then
    cp $PWD/default-sources-spec.yml "$SOURCES_SPEC_FILE"
fi
export DATA_SERVICES_SOURCES_SPEC=sources-spec.yml

mkdir -p $PWD/../Data_services_graphs
export DATA_SERVICES_GRAPHS=$PWD/../Data_services_graphs/

GRAPH_SPEC_FILE=$PWD/../Data_services_graphs/graph-spec.yml
if [ ! -f "$GRAPH_SPEC_FILE" ]; then
    cp $PWD/default-graph-spec.yml $PWD/../Data_services_graphs/graph-spec.yml
fi
export DATA_SERVICES_GRAPH_SPEC=graph-spec.yml

mkdir -p $PWD/../Data_services_storage/logs
export DATA_SERVICES_LOGS=$PWD/../Data_services_storage/logs

export DATA_SERVICES_REDIS_HOST=redis
export DATA_SERVICES_REDIS_PORT=6379
export DATA_SERVICES_REDIS_PASSWORD=InsecureDevelopmentPassword