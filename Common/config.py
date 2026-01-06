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
    
    # Subdirectories under storage_base_path
    orion_storage_dir_name: str = "ORION_STORAGE"
    orion_logs_dir_name: str = "ORION_LOGS" 
    orion_graphs_dir_name: str = "ORION_KG"
    shared_source_dir_name: str = "SHARED_DATA"

    # Full paths to all subdirectories
    orion_storage_path: str = ""    # Will be set based on storage_base_path
    orion_logs_path: str = ""       # Will be set based on storage_base_path
    orion_graphs_path: str = ""     # Will be set based on storage_base_path
    shared_source_data_path: str = ""    # Will be set based on storage_base_path

    orion_graph_spec: str = "example-graph-spec.yaml"
    orion_graph_spec_url: str = ""
    orion_output_url: str = "https://localhost/"

    def __post_init__(self):
        """Initialize paths based on storage_base_path if not already set."""
        # Get the ORION root directory (parent of the Common directory)
        orion_root = Path(__file__).parent.parent
        base_path = orion_root / self.storage_base_path
        
        # Create all the paths
        self.orion_storage_path = str(base_path / self.orion_storage_dir_name)
        self.orion_logs_path = str(base_path / self.orion_logs_dir_name)
        self.orion_graphs_path = str(base_path / self.orion_graphs_dir_name)
        self.shared_source_data_path = str(base_path / self.shared_source_dir_name)

    @classmethod
    def from_env(cls):
        # Dict with key as variable name, and value as ENV Variable name.
        env_vars = {
            "storage_base_path": "STORAGE_BASE_PATH",
            "orion_storage_dir_name": "ORION_STORAGE_DIR_NAME",
            "orion_logs_dir_name": "ORION_LOGS_DIR_NAME",
            "orion_graphs_dir_name": "ORION_GRAPHS_DIR_NAME",
            "shared_source_dir_name": "SHARED_SOURCE_DIR_NAME",
            "orion_graph_spec": "ORION_GRAPH_SPEC",
            "orion_graph_spec_url": "ORION_GRAPH_SPEC_URL"
        }
        kwargs = {}
        for kwarg, env_var in env_vars.items():
            env_value = os.environ.get(env_var)
            if env_value:
                kwargs[kwarg] = env_value
        cls_ret = cls(**kwargs)
        ## Add validation for graph spec vs graph spec url.
        if len(cls_ret.orion_graph_spec) > 0 and len(cls_ret.orion_graph_spec_url) > 0:
            logger.warning("Both Graph Spec URL and Graph Spec were set. Will prioritize using Graph Spec URL")
            cls_ret.orion_graph_spec = ""

        ## Create necessary directories if needed.
        if len(cls_ret.orion_storage_path) > 0:
            logger.info(f"Checking for existence of ORION Storage Path: {cls_ret.orion_storage_path}")
            if not Path(cls_ret.orion_storage_path).exists():
                logger.info(f"--- Creating ORION Storage Path: {cls_ret.orion_storage_path}")
                Path(cls_ret.orion_storage_path).mkdir(exist_ok=True, parents=True)

        if len(cls_ret.orion_logs_path) > 0:
            logger.info(f"Checking for existence of ORION Logs Path: {cls_ret.orion_logs_path}")
            if not Path(cls_ret.orion_logs_path).exists():
                logger.info(f"--- Creating ORION Logs Path: {cls_ret.orion_logs_path}")
                Path(cls_ret.orion_logs_path).mkdir(exist_ok=True, parents=True)
        
        if len(cls_ret.orion_graphs_path) > 0:
            logger.info(f"Checking for existence of ORION Knowledge Graph Path: {cls_ret.orion_graphs_path}")
            if not Path(cls_ret.orion_graphs_path).exists():
                logger.info(f"--- Creating ORION Knowledge Graph Path: {cls_ret.orion_graphs_path}")
                Path(cls_ret.orion_graphs_path).mkdir(exist_ok=True, parents=True)

        if len(cls_ret.shared_source_data_path) > 0:
            logger.info(f"Checking for existence of ORION Shared Source Path: {cls_ret.shared_source_data_path}")
            if not Path(cls_ret.shared_source_data_path).exists():
                logger.info(f"--- Creating ORION Shared Source Path: {cls_ret.shared_source_data_path}")
                Path(cls_ret.shared_source_data_path).mkdir(exist_ok=True, parents=True)

        return cls_ret
