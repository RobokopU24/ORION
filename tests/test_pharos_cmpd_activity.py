import pytest

from orion.biolink_constants import AFFINITY, AFFINITY_PARAMETER
from parsers.PHAROS.src.loadPHAROS import PHAROSLoader


def test_cmpd_activity_query_does_not_select_assay_type_as_predicate():
    # the assay type (act_type) must only be used as the affinity parameter
    query = " ".join(PHAROSLoader.GENE_TO_CMPD_ACTIVITY_QUERY.lower().split())
    assert "act_type as pred" not in query
    assert "act_type as affinity_parameter" in query


@pytest.mark.parametrize("assay_type", ["IC50", "Ki", "EC50", "AC50", "Kd"])
def test_cmpd_activity_assay_type_does_not_set_direction(assay_type):
    # simulate the row the cmpd_activity query returns: a constant pred plus the assay type as affinity_parameter
    loader = PHAROSLoader.__new__(PHAROSLoader)
    row = {"pred": "binding_agent", "affinity": "6.9", "affinity_parameter": assay_type,
           "dtype": "", "pubmed_ids": None}
    predicate, _, props, _ = loader.get_edge_props(row)
    assert predicate == "RO:0002436"
    assert props[AFFINITY] == 6.9
    assert props[AFFINITY_PARAMETER] == f"p{assay_type}"
