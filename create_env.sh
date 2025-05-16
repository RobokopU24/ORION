#!/usr/bin/env bash

# Resolve the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

echo "Setup ORION environment variables"
echo "Press Enter to accept the default path shown in brackets."

# Prompt for ORION_STORAGE
read -rp "Path for ORION's Storage (will use [$SCRIPT_DIR/../ORION_storage] if empty): " ORION_STORAGE_INPUT
ORION_STORAGE="${ORION_STORAGE_INPUT:-$SCRIPT_DIR/../ORION_storage}"
mkdir -p "$ORION_STORAGE"

# Prompt for ORION_GRAPHS
read -rp "Path to store ORION's graphs (will use [$SCRIPT_DIR/../ORION_graphs] if empty): " ORION_GRAPHS_INPUT
ORION_GRAPHS="${ORION_GRAPHS_INPUT:-$SCRIPT_DIR/../ORION_graphs}"
mkdir -p "$ORION_GRAPHS"

# Prompt for ORION_LOGS
read -rp "Path to store ORION's logs (will use [$SCRIPT_DIR/../ORION_logs] if empty): " ORION_LOGS_INPUT
ORION_LOGS="${ORION_LOGS_INPUT:-$SCRIPT_DIR/../ORION_logs}"
mkdir -p "$ORION_LOGS"

# Prompt for ORION_GRAPH_SPEC
read -rp "Filename for Graph Specification file from graph_specs directory (will use [example-graph-spec.yaml] if empty): " ORION_GRAPH_SPEC_INPUT
ORION_GRAPH_SPEC="${ORION_GRAPH_SPEC_INPUT:-example-graph-spec.yaml}"

# Prompt for python path
read -rp "Python path (will use [$SCRIPT_DIR] if empty): " PYTHON_PATH_INPUT
PYTHON_PATH_MORE="${PYTHON_PATH_INPUT:-$SCRIPT_DIR}"

# Write environment variables to .env
cat > "$ENV_FILE" <<EOF
ORION_STORAGE=$ORION_STORAGE
ORION_GRAPHS=$ORION_GRAPHS
ORION_LOGS=$ORION_LOGS
ORION_GRAPH_SPEC=$ORION_GRAPH_SPEC
PYTHONPATH=\$PYTHONPATH:$PYTHON_PATH_MORE
EOF

echo "✅ Environment setup complete. Variables saved to $ENV_FILE"
