import pytest

from parsers.monarchkg.src.loadMonarchKG import MonarchKGFullLoader, MonarchKGLoader


FILTERED_GOA_PRIMARY_SOURCES = (
    "infores:go",
    "infores:ensembl",
    "infores:uniprot",
    "infores:hgnc",
    "infores:uos-mcb",
)


@pytest.mark.parametrize("primary_knowledge_source", FILTERED_GOA_PRIMARY_SOURCES)
def test_monarchkg_skips_replaced_go_annotation_edges(tmp_path, primary_knowledge_source):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert loader.filter_edge(
        subject_id="NCBIGene:10558",
        object_id="GO:0004758",
        predicate="biolink:contributes_to",
        primary_knowledge_source=primary_knowledge_source,
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "go_annotation_edges"},
    )


@pytest.mark.parametrize("primary_knowledge_source", FILTERED_GOA_PRIMARY_SOURCES)
def test_monarchkg_keeps_matching_primary_sources_from_other_monarch_blocks(
    tmp_path, primary_knowledge_source
):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="NCBIGene:10558",
        object_id="GO:0004758",
        predicate="biolink:contributes_to",
        primary_knowledge_source=primary_knowledge_source,
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "some_other_monarch_source"},
    )


def test_monarchkg_keeps_go_annotation_edges_from_unselected_primary_sources(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="NCBIGene:10558",
        object_id="GO:0004758",
        predicate="biolink:contributes_to",
        primary_knowledge_source="infores:monarchinitiative",
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "go_annotation_edges"},
    )


def test_monarchkg_keeps_go_annotation_provider_for_other_predicates(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="MONDO:0000001",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:go",
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "go_annotation_edges"},
    )


@pytest.mark.parametrize("primary_knowledge_source", FILTERED_GOA_PRIMARY_SOURCES)
def test_monarchkg_full_does_not_apply_go_annotation_filter(
    tmp_path, primary_knowledge_source
):
    loader = MonarchKGFullLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="NCBIGene:10558",
        object_id="GO:0004758",
        predicate="biolink:contributes_to",
        primary_knowledge_source=primary_knowledge_source,
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "go_annotation_edges"},
    )
