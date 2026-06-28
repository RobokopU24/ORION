from parsers.monarchkg.src.loadMonarchKG import MonarchKGFullLoader, MonarchKGLoader


def test_monarchkg_skips_hpoa_has_phenotype_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert loader.filter_edge(
        subject_id="OMIM:1",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:hpo-annotations",
        aggregator_knowledge_sources=[],
    )
    assert loader.filter_edge(
        subject_id="OMIM:1",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:omim",
        aggregator_knowledge_sources=["infores:monarchinitiative", "infores:hpo-annotations"],
    )


def test_monarchkg_keeps_non_hpoa_existing_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="NCBIGene:1",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:example",
        aggregator_knowledge_sources=[],
    )


def test_monarchkg_defensively_skips_replaced_gene_disease_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert loader.filter_edge(
        subject_id="NCBIGene:1",
        object_id="OMIM:1",
        predicate="biolink:gene_associated_with_condition",
        primary_knowledge_source="infores:omim",
        aggregator_knowledge_sources=[],
    )


def test_monarchkg_skips_monarch_omim_gene_disease_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert loader.filter_edge(
        subject_id="HGNC:4061",
        object_id="MONDO:0009288",
        predicate="biolink:causes",
        primary_knowledge_source="infores:monarchinitiative",
        aggregator_knowledge_sources=[],
        monarch_edge={"provided_by": "omim_gene_to_disease_edges"},
    )


def test_monarchkg_keeps_other_monarch_causes_edges(tmp_path):
    loader = MonarchKGLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="HGNC:1",
        object_id="MONDO:1",
        predicate="biolink:causes",
        primary_knowledge_source="infores:monarchinitiative",
        aggregator_knowledge_sources=[],
        monarch_edge={"provided_by": "some_other_monarch_source"},
    )


def test_monarchkg_full_does_not_apply_hpoa_filter(tmp_path):
    loader = MonarchKGFullLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="OMIM:1",
        object_id="HP:0000001",
        predicate="biolink:has_phenotype",
        primary_knowledge_source="infores:hpo-annotations",
        aggregator_knowledge_sources=[],
    )


def test_monarchkg_full_does_not_apply_monarch_omim_gene_disease_filter(tmp_path):
    loader = MonarchKGFullLoader(source_data_dir=str(tmp_path))

    assert not loader.filter_edge(
        subject_id="HGNC:4061",
        object_id="MONDO:0009288",
        predicate="biolink:causes",
        primary_knowledge_source="infores:monarchinitiative",
        aggregator_knowledge_sources=[],
        monarch_edge={"provided_by": "omim_gene_to_disease_edges"},
    )
