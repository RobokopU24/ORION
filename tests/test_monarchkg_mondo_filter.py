from parsers.monarchkg.src.loadMonarchKG import MonarchKGFullLoader, MonarchKGLoader


def test_monarchkg_skips_replaced_mondo_phenio_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert loader.filter_edge(
        subject_id="MONDO:0000001",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:mondo",
        aggregator_knowledge_sources=["infores:monarchinitiative", "infores:phenio"],
        monarch_edge={"provided_by": "phenio_edges"},
    )
    assert loader.filter_edge(
        subject_id="MONDO:0000001",
        object_id="HP:0000001",
        predicate="biolink:causes",
        primary_knowledge_source="infores:mondo",
        aggregator_knowledge_sources=["infores:monarchinitiative", "infores:phenio"],
        monarch_edge={"provided_by": "phenio_edges"},
    )


def test_monarchkg_keeps_non_mondo_phenio_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="MONDO:0000001",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:mondo",
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "phenio_edges"},
    )
    assert not loader.filter_edge(
        subject_id="MONDO:0000001",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:mondo",
        aggregator_knowledge_sources=["infores:monarchinitiative", "infores:phenio"],
        monarch_edge={"provided_by": "some_other_source"},
    )
    assert not loader.filter_edge(
        subject_id="MONDO:0000001",
        object_id="HP:0000001",
        predicate="biolink:contributes_to",
        primary_knowledge_source="infores:mondo",
        aggregator_knowledge_sources=["infores:monarchinitiative", "infores:phenio"],
        monarch_edge={"provided_by": "phenio_edges"},
    )


def test_monarchkg_full_keeps_mondo_phenio_edges(tmp_path):
    loader = MonarchKGFullLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="MONDO:0000001",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:mondo",
        aggregator_knowledge_sources=["infores:monarchinitiative", "infores:phenio"],
        monarch_edge={"provided_by": "phenio_edges"},
    )
