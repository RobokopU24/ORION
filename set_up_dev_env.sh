#!/usr/bin/env bash

# ORION requires directories to store data ingest files and graph outputs.
# This script creates those directories and sets the environment variables pointing to them.
# See the README for more information.

# ORION_STORAGE - a directory for storing ingest pipeline files
mkdir -p "$PWD/../ORION_storage"
export ORION_STORAGE="$PWD/../ORION_storage/"

# ORION_GRAPHS - a directory for storing knowledge graph outputs
mkdir -p "$PWD/../ORION_graphs"
export ORION_GRAPHS="$PWD/../ORION_graphs/"

# ORION_LOGS - a directory for storing logs
mkdir -p "$PWD/../ORION_logs"
export ORION_LOGS="$PWD/../ORION_logs/"
