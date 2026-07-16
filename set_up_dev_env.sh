#!/usr/bin/env bash

# Optional helper: ORION will create ~/ORION-workspace/{storage,graphs} on first use if you do
# nothing, so you don't need to run this script unless you want repo-adjacent directories
# (e.g. for development where you want to inspect storage/graphs/logs alongside the cloned repo).
# This script creates those directories and exports the env vars pointing to them.
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
