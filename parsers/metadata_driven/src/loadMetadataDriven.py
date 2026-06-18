from pathlib import Path

from orion.metadata_driven_loader import MetadataDrivenLoader


REPO_ROOT = Path(__file__).resolve().parents[3]


class HGNCCroissantLoader(MetadataDrivenLoader):
    parser_spec_path = str(REPO_ROOT / "parser_specs" / "HGNC" / "parser.yaml")


class BINDINGDBCroissantLoader(MetadataDrivenLoader):
    parser_spec_path = str(REPO_ROOT / "parser_specs" / "BINDING-DB" / "parser.yaml")
