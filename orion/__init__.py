# Public API
import os as _os
import subprocess as _sp
print(f'[ORION] uid={_os.getuid()} gid={_os.getgid()} ORION_LOGS={_os.environ.get("ORION_LOGS", "not set")}',
      flush=True)

for _name, _path in [('ORION_STORAGE', _os.environ.get('ORION_STORAGE')),
                     ('ORION_GRAPHS',  _os.environ.get('ORION_GRAPHS')),
                     ('ORION_LOGS',    _os.environ.get('ORION_LOGS'))]:
    if _path:
        _out = _sp.run(['ls', '-la', _path], capture_output=True, text=True)
        print(f'[ORION] ls -la {_name} ({_path}):\n{_out.stdout or _out.stderr}', flush=True)
    else:
        print(f'[ORION] {_name} not set', flush=True)
del _os, _sp

from orion.kgx_file_merger import KGXFileMerger
from orion.kgxmodel import GraphSpec, GraphSource
from orion.kgx_metadata import KGXGraphMetadata, KGXKnowledgeSource, generate_schema
from orion.kgx_schema_diff import diff_schemas
from orion.meta_kg import MetaKnowledgeGraphBuilder
from orion.merging import MERGING_CODE_VERSION
from orion.kgx_file_normalizer import KGXFileNormalizer
from orion.normalization import NodeNormalizer, NormalizationScheme, NORMALIZATION_CODE_VERSION

__all__ = [
    "KGXFileMerger",
    "GraphSpec", "GraphSource",
    "KGXGraphMetadata", "KGXKnowledgeSource",
    "generate_schema", "diff_schemas",
    "MetaKnowledgeGraphBuilder",
    "KGXFileNormalizer",
    "NodeNormalizer", "NormalizationScheme", "NORMALIZATION_CODE_VERSION",
    "MERGING_CODE_VERSION"
]