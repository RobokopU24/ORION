#!/usr/bin/env bash

# Resolve the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setup ORION environment variables${NC}"
echo "Press Enter to accept the default path shown in brackets."

# Function to create directory if it doesn't exist
create_dir() {
    if [ ! -d "$1" ]; then
        echo -e "${YELLOW}Creating directory: $1${NC}"
        mkdir -p "$1"
    fi
}

# Function to check if a variable is set
check_var() {
    if [ -z "${!1}" ]; then
        echo -e "${RED}Error: $1 is not set${NC}"
        return 1
    fi
}

# Prompt for ORION_STORAGE
read -rp "Path for ORION's Storage [${SCRIPT_DIR}/../ORION_storage]: " ORION_STORAGE_INPUT
ORION_STORAGE="${ORION_STORAGE_INPUT:-$SCRIPT_DIR/../ORION_storage}"
create_dir "$ORION_STORAGE"

# Prompt for ORION_GRAPHS
read -rp "Path for ORION's graphs [${SCRIPT_DIR}/../ORION_graphs]: " ORION_GRAPHS_INPUT
ORION_GRAPHS="${ORION_GRAPHS_INPUT:-$SCRIPT_DIR/../ORION_graphs}"
create_dir "$ORION_GRAPHS"

# Prompt for ORION_LOGS
read -rp "Path for ORION's logs [${SCRIPT_DIR}/../ORION_logs]: " ORION_LOGS_INPUT
ORION_LOGS="${ORION_LOGS_INPUT:-$SCRIPT_DIR/../ORION_logs}"
create_dir "$ORION_LOGS"

# Prompt for shared data path
read -rp "Path for shared data [/tmp/shared_data]: " SHARED_DATA_INPUT
SHARED_SOURCE_DATA_PATH="${SHARED_DATA_INPUT:-/tmp/shared_data}"
create_dir "$SHARED_SOURCE_DATA_PATH"

# Prompt for PYTHONPATH 
read -rp "Python path (will use [$SCRIPT_DIR] if empty): " PYTHON_PATH_INPUT
PYTHON_PATH_MORE="${PYTHON_PATH_INPUT:-$SCRIPT_DIR}"

# Prompt for ORION_GRAPH_SPEC with validation
while true; do
    read -rp "Filename for Graph Specification file from graph_specs directory [example-graph-spec.yaml]: " ORION_GRAPH_SPEC_INPUT
    ORION_GRAPH_SPEC="${ORION_GRAPH_SPEC_INPUT:-example-graph-spec.yaml}"
    
    read -rp "Or enter URL for Graph Specification (leave empty if using local file): " ORION_GRAPH_SPEC_URL_INPUT
    ORION_GRAPH_SPEC_URL="${ORION_GRAPH_SPEC_URL_INPUT}"
    
    if [ ! -z "$ORION_GRAPH_SPEC" ] && [ ! -z "$ORION_GRAPH_SPEC_URL" ]; then
        echo -e "${RED}Error: Please specify either a local file OR a URL, not both${NC}"
    else
        break
    fi
done

# Write environment variables to .env
cat > "$ENV_FILE" <<EOF
# Core ORION Configuration
ORION_STORAGE=$ORION_STORAGE
ORION_GRAPHS=$ORION_GRAPHS
ORION_LOGS=$ORION_LOGS
SHARED_SOURCE_DATA_PATH=$SHARED_SOURCE_DATA_PATH
PYTHONPATH=\$PYTHONPATH:$PYTHON_PATH_MORE

# Graph Specification
EOF

if [ ! -z "$ORION_GRAPH_SPEC" ]; then
    echo "ORION_GRAPH_SPEC=$ORION_GRAPH_SPEC" >> "$ENV_FILE"
fi
if [ ! -z "$ORION_GRAPH_SPEC_URL" ]; then
    echo "ORION_GRAPH_SPEC_URL=$ORION_GRAPH_SPEC_URL" >> "$ENV_FILE"
fi

cat >> "$ENV_FILE" <<EOF

# Optional Configuration
# Uncomment and set values as needed

# Biolink Configuration
#BL_VERSION=4.2.1  # biolink version
#EDGE_NORMALIZATION_ENDPOINT=https://bl-lookup-sri.renci.org/
#NODE_NORMALIZATION_ENDPOINT=https://nodenormalization-sri.renci.org/

# Service URLs
#NAMERES_URL=https://name-resolution-sri.renci.org/
#SAPBERT_URL=https://babel-sapbert.apps.renci.org/
#LITCOIN_PRED_MAPPING_URL=https://pred-mapping.apps.renci.org/

# API Keys and Authentication
#OPENAI_API_KEY=your-key-here
#OPENAI_API_ORGANIZATION=your-org-here
#BAGEL_SERVICE_USERNAME=your-username-here
#BAGEL_SERVICE_PASSWORD=your-password-here
EOF

echo -e "${GREEN}✅ Environment setup complete. Variables saved to $ENV_FILE${NC}"
echo -e "${YELLOW}Note: Edit $ENV_FILE to configure optional settings if needed${NC}"

# Validate the configuration
echo -e "\n${GREEN}Validating configuration...${NC}"

# Check required variables
required_vars=(
    "SHARED_SOURCE_DATA_PATH"
    "ORION_STORAGE"
    "ORION_GRAPHS"
    "ORION_LOGS"
)

config_valid=true
for var in "${required_vars[@]}"; do
    if ! check_var "$var"; then
        config_valid=false
    fi
done

if [ "$config_valid" = false ]; then
    echo -e "${RED}Configuration validation failed. Please check your .env file${NC}"
    exit 1
fi

echo -e "${GREEN}Configuration validated successfully!${NC}"
echo "You can now run ORION using: docker compose up"