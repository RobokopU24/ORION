from parsers.monarchkg.src.loadMonarchKG import MonarchKGFullLoader, MonarchKGLoader


def test_monarchkg_skips_monarch_go_annotation_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert loader.filter_edge(
        subject_id="HGNC:21020",
        object_id="GO:0050567",
        predicate="biolink:contributes_to",
        primary_knowledge_source="infores:go",
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "go_annotation_edges"},
    )


def test_monarchkg_keeps_other_contributes_to_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="HGNC:1",
        object_id="MONDO:1",
        predicate="biolink:contributes_to",
        primary_knowledge_source="infores:example",
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "some_other_monarch_source"},
    )


def test_monarchkg_full_does_not_apply_go_annotation_filter(tmp_path):
    loader = MonarchKGFullLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="HGNC:21020",
        object_id="GO:0050567",
        predicate="biolink:contributes_to",
        primary_knowledge_source="infores:go",
        aggregator_knowledge_sources=["infores:monarchinitiative"],
        monarch_edge={"provided_by": "go_annotation_edges"},
    )
