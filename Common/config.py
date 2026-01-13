# import os
# from pathlib import Path
# from dotenv import dotenv_values

# CONFIG = {
#     **dotenv_values(Path(__file__).parents[1] / '.env'),  # load config variables from .env
#     **os.environ,  # override loaded values with environment variables
# }

import os
from dataclasses import dataclass
from .utils import LoggingUtil
from pathlib import Path

logger = LoggingUtil.init_logging("ORION.Common.config",
                                  line_format='medium')

@dataclass
class Config:
    """
    Set reasonable defaults.
    """
    # Base parent directory for all storage paths (relative to ORION root)
    storage_base_path: str = "Storage"
    base_path: str = ""
    
    # Subdirectories under storage_base_path
    orion_storage_dir_name: str = "ORION_STORAGE"
    orion_graphs_dir_name: str = "ORION_KG"
    shared_source_dir_name: str = "SHARED_DATA"
    orion_logs_dir_name: str = ""

    # Full paths to all subdirectories
    orion_storage_path: str = ""    # Will be set based on storage_base_path
    orion_logs_path: str = ""       # Will be set based on storage_base_path
    orion_graphs_path: str = ""     # Will be set based on storage_base_path
    shared_source_data_path: str = ""    # Will be set based on storage_base_path

    orion_graph_spec: str = "example-graph-spec.yaml"
    orion_graph_spec_url: str = ""
    orion_output_url: str = "https://localhost/"

    orion_test_mode: bool = False

    node_normalization_url: str = "https://nodenormalization-sri.renci.org/"
    edge_normalization_url: str = "https://bl-lookup-sri.renci.org/"
    name_resolver_url: str = "https://name-resolution-sri.renci.org/"
    litcoin_pred_mapping_url: str = "https://pred-mapping.apps.renci.org/"
    sapbert_url: str = "https://babel-sapbert.apps.renci.org/"

    env_vars = {
            "storage_base_path": "STORAGE_BASE_PATH",
            "orion_storage_dir_name": "ORION_STORAGE_DIR_NAME",
            "orion_logs_dir_name": "ORION_LOGS_DIR_NAME",
            "orion_graphs_dir_name": "ORION_GRAPHS_DIR_NAME",
            "shared_source_dir_name": "SHARED_SOURCE_DIR_NAME",
            "orion_graph_spec": "ORION_GRAPH_SPEC",
            "orion_graph_spec_url": "ORION_GRAPH_SPEC_URL",
            "orion_test_mode": "ORION_TEST_MODE",
            "orion_output_url": "ORION_OUTPUT_URL", #Do we need a validator for this?
            "node_normalization_url": "NODE_NORMALIZATION_ENDPOINT",
            "edge_normalization_url": "EDGE_NORMALIZATION_ENDPOINT",
            "name_resolver_url": "NAMERES_URL",
            "litcoin_pred_mapping_url": "LITCOIN_PRED_MAPPING_URL",
            "sapbert_url": "SAPBERT_URL"
        }

    def __post_init__(self):
        """Initialize paths based on storage_base_path if not already set."""
        # Get the ORION root directory (parent of the Common directory)
        orion_root = Path(__file__).parent.parent
        self.base_path = orion_root / self.storage_base_path
        
        # Create all the paths
        self.orion_storage_path = str(self.base_path / self.orion_storage_dir_name)
        self.orion_logs_path = str(self.base_path / self.orion_logs_dir_name) if len(self.orion_logs_dir_name) > 0 else ""
        self.orion_graphs_path = str(self.base_path / self.orion_graphs_dir_name)
        self.shared_source_data_path = str(self.base_path / self.shared_source_dir_name)

    def create_path(self, env_var:str):
        env_var = env_var.upper()
        ## Create necessary directories if needed.
        if env_var == "ORION_STORAGE_DIR_NAME" and len(self.orion_storage_path) > 0:
            logger.info(f"Checking for existence of ORION Storage Path: {self.orion_storage_path}")
            if not Path(self.orion_storage_path).exists():
                logger.info(f"--- Creating ORION Storage Path: {self.orion_storage_path}")
                Path(self.orion_storage_path).mkdir(exist_ok=True, parents=True)
                return self.orion_storage_path
        elif env_var == "ORION_LOGS_DIR_NAME" and len(self.orion_logs_path) > 0:
            logger.info(f"Checking for existence of ORION Logs Path: {self.orion_logs_path}")
            if not Path(self.orion_logs_path).exists():
                logger.info(f"--- Creating ORION Logs Path: {self.orion_logs_path}")
                Path(self.orion_logs_path).mkdir(exist_ok=True, parents=True)
                return self.orion_logs_path
        elif env_var == "ORION_GRAPHS_DIR_NAME" and len(self.orion_graphs_path) > 0:
            logger.info(f"Checking for existence of ORION Knowledge Graph Path: {self.orion_graphs_path}")
            if not Path(self.orion_graphs_path).exists():
                logger.info(f"--- Creating ORION Knowledge Graph Path: {self.orion_graphs_path}")
                Path(self.orion_graphs_path).mkdir(exist_ok=True, parents=True)
                return self.orion_graphs_path
        elif env_var == "SHARED_SOURCE_DIR_NAME" and len(self.shared_source_data_path) > 0:
            logger.info(f"Checking for existence of ORION Shared Source Path: {self.shared_source_data_path}")
            if not Path(self.shared_source_data_path).exists():
                logger.info(f"--- Creating ORION Shared Source Path: {self.shared_source_data_path}")
                Path(self.shared_source_data_path).mkdir(exist_ok=True, parents=True)
                return self.shared_source_data_path
        return None

    def getenv(self, env_name:str):
        if env_name in self. env_vars.values():
            if 'DIR_NAME' in env_name.upper():
                ## This is a directory variable, create the path if needed.
                value = self.create_path(env_name)
            else:
                attr_name = [k for k in self.env_vars if self.env_vars[k] == env_name.upper()]
                value = getattr(self, attr_name) if attr_name is not None else None
            return value
        else:
            return os.environ.get(env_name) 

    @classmethod
    def from_env(cls):
        # Dict with key as variable name, and value as ENV Variable name.
        kwargs = {}
        for kwarg, env_var in cls.env_vars.items():
            env_value = os.environ.get(env_var)
            if env_value:
                kwargs[kwarg] = env_value
                if kwarg == "ORION_TEST_MODE":
                    kwargs[kwarg] = False if (env_value and env_value.lower() == "false") else True
        cls_ret = cls(**kwargs)
        ## Add validation for graph spec vs graph spec url.
        if len(cls_ret.orion_graph_spec) > 0 and len(cls_ret.orion_graph_spec_url) > 0:
            logger.warning("Both Graph Spec URL and Graph Spec were set. Will prioritize using Graph Spec URL")
            cls_ret.orion_graph_spec = ""

        return cls_ret
