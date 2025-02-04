from Common.biolink_constants import *

ptm_dict = {
            "acetylation": "increased",
            "ADP-ribosylation": "increased",
            "carboxylation": "increased",
            "deacetylation": "decreased",
            "degradation": "increased",  # cleavage
            "deglycosylation": "decreased",
            "demethylation": "decreased",
            "dephosphorylation": "decreased",
            "desumoylation": "decreased",
            "deubiquitination": "decreased",
            "glycosylation": "increased",
            "hydroxylation": "increased",
            "lipidation": "increased",
            "methylation": "increased",
            "monoubiquitination": "increased",
            "neddylation": "increased",
            "oxidation": "increased",
            "palmitoylation": "increased",
            "phosphorylation": "increased",
            "polyubiquitination": "increased",
            "s-nitrosylation": "increased",
            "sumoylation": "increased",
            "trimethylation": "increased",
            "tyrosination": "increased",
            "ubiquitination": "increased"
        }

mechanism_map = {
            "binding": {
                "predicate": f"RO:0002436",  # directly_physically_interacts_with
            },

            "catalytic activity": {
                "predicate": f"RO:0002327",  # catalyzes
            },

            "chemical activation": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: f"RO:0003303",  # causes
                    CAUSAL_MECHANISM_QUALIFIER: "chemical activation"
                }
            },

            "chemical inhibition": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: f"RO:0003303",  # causes
                    CAUSAL_MECHANISM_QUALIFIER: "chemical inhibition"
                }
            },

            "chemical modification": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: f"RO:0003303",  # causes
                    OBJECT_ASPECT_QUALIFIER: "chemical modification",
                }
            },

            "destabilization": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: "RO:0003303",
                    OBJECT_DIRECTION_QUALIFIER: "decreased",
                    OBJECT_ASPECT_QUALIFIER: "stability"
                }
            },

            # This probably needs to be a node property. "is_a"
            "gtpase - activating protein": {
            },

            # This probably needs to be a node property.
            "guanine nucleotide exchange factor": {
                "edge_properties": {
                    CAUSAL_MECHANISM_QUALIFIER: "guanyl_nucleotide_exchange"
                }
            },

            "post transcriptional modification": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: "RO:0003303",
                    OBJECT_ASPECT_QUALIFIER: "post transcriptional modification"
                }
            },

            "post translational modification": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: "RO:0003303",
                    OBJECT_ASPECT_QUALIFIER: "post translation modification"
                }
            },

            # new predicate?
            "precursor of": {
            },

            "relocalization": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: "RO:0003303",
                    OBJECT_ASPECT_QUALIFIER: "relocation"
                }
            },

            "small molecule catalysis": {
                "predicate": "RO:0002327",  # catalyses
            },

            "transcriptional regulation": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    QUALIFIED_PREDICATE: "RO:0003303",
                    CAUSAL_MECHANISM_QUALIFIER: "transcriptional_regulation"
                }
            },

            "translation regulation": {
                "predicate": f"biolink:affects",
                "edge_properties": {
                    OBJECT_ASPECT_QUALIFIER: "translation"
                }
            },

        }

# Effect to Predicate Mapping
effect_mapping = {
    "form complex": {
        "biolink:in_complex_with": {},
        "RO:0002436": {}
    },

    "down-regulates": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "downregulates"
        }
    },

    "down-regulates activity": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "downregulates",
            OBJECT_ASPECT_QUALIFIER: "activity"
        }
    },

    "down-regulates quantity": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "downregulates",
            OBJECT_ASPECT_QUALIFIER: "abundance"
        }
    },

    "down-regulates quantity by destabilization": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "downregulates",
            OBJECT_ASPECT_QUALIFIER: "abundance"
        },

        "biolink:affects": {
            QUALIFIED_PREDICATE: "RO:0003303",
            OBJECT_DIRECTION_QUALIFIER: "decreased",
            OBJECT_ASPECT_QUALIFIER: "stability"
        }
    },

    "down-regulates quantity by repression": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "downregulates",
            OBJECT_ASPECT_QUALIFIER: "abundance"
        },

        "biolink:affects": {
            QUALIFIED_PREDICATE: "RO:0003303",
            OBJECT_DIRECTION_QUALIFIER: "decreased",
            OBJECT_ASPECT_QUALIFIER: "expression"
        }
    },

    "up-regulates": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "upregulates"
        }
    },

    "up-regulates activity": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "upregulates",
            OBJECT_ASPECT_QUALIFIER: "activity"
        }
    },

    "up-regulates quantity": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "upregulates",
            OBJECT_ASPECT_QUALIFIER: "abundance"
        }
    },

    "up-regulates quantity by stabilization": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "upregulates",
            OBJECT_ASPECT_QUALIFIER: "abundance"
        },

        "biolink:affects": {
            QUALIFIED_PREDICATE: "RO:0003303",
            OBJECT_DIRECTION_QUALIFIER: "increased",
            OBJECT_ASPECT_QUALIFIER: "stability"
        }
    },

    "up-regulates quantity by expression": {
        "RO:0002448": {
            OBJECT_DIRECTION_QUALIFIER: "upregulates",
            OBJECT_ASPECT_QUALIFIER: "abundance"
        },

        "biolink:affects": {
            QUALIFIED_PREDICATE: "RO:0003303",
            OBJECT_DIRECTION_QUALIFIER: "increased",
            OBJECT_ASPECT_QUALIFIER: "expression"
        }
    }
}
