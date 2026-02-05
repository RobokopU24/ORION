import requests
import yaml
import os

from bmt import Toolkit
from requests.adapters import HTTPAdapter, Retry
from functools import cache

BIOLINK_MODEL_VERSION = os.environ.get("BL_VERSION", "4.3.4")

def get_biolink_model_toolkit(biolink_version: str = None) -> Toolkit:
    version = biolink_version if biolink_version else BIOLINK_MODEL_VERSION
    schema_url = f"https://raw.githubusercontent.com/biolink/biolink-model/v{version}/biolink-model.yaml"
    predicate_map_url = f"https://raw.githubusercontent.com/biolink/biolink-model/v{version}/predicate_mapping.yaml"
    return Toolkit(schema=schema_url, predicate_map=predicate_map_url)

map_data = {
  "attribute_type_map": {
      "`biolink:primary_knowledge_source`": "biolink:primary_knowledge_source",
      "`biolink:aggregator_knowledge_source`": "biolink:aggregator_knowledge_source",
      "equivalent_identifiers": "biolink:same_as",
      "endogenous": "aragorn:endogenous"
  },
  "value_type_map": {
      "equivalent_identifiers": "metatype:uriorcurie",
      "biolink:primary_knowledge_source": "biolink:InformationResource",
      "biolink:aggregator_knowledge_source": "biolink:InformationResource",
      "endogenous": "xsd:boolean"
  }
}

# set the value type mappings
VALUE_TYPES = map_data['value_type_map']


# Most of this was adapted (stolen) from Plater
class BiolinkUtils:

    def __init__(self, biolink_version: str = None):
        self.toolkit = get_biolink_model_toolkit(biolink_version=biolink_version)

    @cache
    def find_biolink_leaves(self, biolink_concepts: frozenset):
        """
        Given a list of biolink concepts, returns the leaves removing any parent concepts.
        :param biolink_concepts: list of biolink concepts
        :return: leave concepts.
        """
        ancestry_set = set()  # the set of concepts that are parents to concepts in the set
        unknown_elements = set()  # concepts not found in the biolink model
        for x in biolink_concepts:
            current_element = self.toolkit.get_element(x)
            if not current_element:
                unknown_elements.add(x)
            ancestors = set(self.toolkit.get_ancestors(x, mixin=True, reflexive=False, formatted=True))
            ancestry_set = ancestry_set.union(ancestors)
        leaf_set = biolink_concepts - ancestry_set - unknown_elements
        return leaf_set

    def invert_predicate(self, biolink_predicate):
        """Given a biolink predicate, find its inverse, return None if one does not exist"""
        element = self.toolkit.get_element(biolink_predicate)
        if element is None:
            return None
        # If its symmetric return itself
        if 'symmetric' in element and element.symmetric:
            return biolink_predicate
        # If no inverse is found return None
        if 'inverse' not in element or not element['inverse']:
            return None
        # Return the predicate's inverse
        return self.toolkit.get_element(element['inverse']).slot_uri

    def get_attribute_type_id(self, attribute_name):

        # the default if the real biolink attribute type id is not found
        attribute_type_id = "biolink:Attribute"

        # TODO - this part might not work, it was stolen from plater but may not be helpful
        # lookup the biolink info for this attribute
        bl_info = self.toolkit.get_element(attribute_name)
        if not bl_info:
            # check for predicates that should start with biolink curies but don't
            if not attribute_name.startswith('biolink:'):
                bl_info = self.toolkit.get_element(f'biolink:{attribute_name}')
        if bl_info is not None:
            if 'slot_uri' in bl_info:
                attribute_type_id = bl_info['slot_uri']
            elif 'class_uri' in bl_info:
                attribute_type_id = bl_info['class_uri']

        return attribute_type_id

    # !!! DEPRECATED !!!
    # keeping around for neo4j meta kg creation but this was really code geared towards generating TRAPI for plater
    def get_attribute_bl_info(self, attribute_name):
        # set defaults
        new_attr_meta_data = {
            "attribute_type_id": "biolink:Attribute",
            "value_type_id": "EDAM:data_0006",
        }
        # if attribute is meant to be skipped return none
        if attribute_name in ["name", "id"]:
            return None

        # map the attribute type to the list above, otherwise generic default
        new_attr_meta_data["value_type_id"] = VALUE_TYPES.get(attribute_name, new_attr_meta_data["value_type_id"])
        attr_found = None
        if attribute_name in map_data["attribute_type_map"] or f'`{attribute_name}`' in map_data["attribute_type_map"]:
            attr_found = True
            new_attr_meta_data["attribute_type_id"] = map_data["attribute_type_map"].get(attribute_name) \
                                                      or map_data["attribute_type_map"].get(f"`{attribute_name}`")
        if attribute_name in map_data["value_type_map"]:
            new_attr_meta_data["value_type_id"] = map_data["value_type_map"][attribute_name]
        if attr_found:
            return new_attr_meta_data

        # lookup the biolink info, for qualifiers suffix with _qualifier and do lookup.
        bl_info = self.toolkit.get_element(attribute_name) or self.toolkit.get_element(attribute_name + "_qualifier")

        # did we get something
        if bl_info is not None:
            # if there are exact mappings use the first on
            if 'slot_uri' in bl_info:
                new_attr_meta_data['attribute_type_id'] = bl_info['slot_uri']
                # was there a range value
                if 'range' in bl_info and bl_info['range'] is not None:
                    # try to get the type of data
                    new_type = self.toolkit.get_element(bl_info['range'])
                    # check if new_type is not None. For eg. bl_info['range'] = 'uriorcurie' for things
                    # for `relation` .
                    if new_type:
                        if 'uri' in new_type and new_type['uri'] is not None:
                            # get the real data type
                            new_attr_meta_data["value_type_id"] = new_type['uri']
            elif 'class_uri' in bl_info:
                new_attr_meta_data['attribute_type_id'] = bl_info['class_uri']

        # make sure this is not null in case the bmt toolkit lookup returns null
        if not new_attr_meta_data['attribute_type_id']:
            new_attr_meta_data['attribute_type_id'] = "biolink:Attribute"
        return new_attr_meta_data

    def predicate_has_qualifiers(self, predicate):
        # TODO some bmt magic and find out if this predicate has qualifiers
        if predicate in ['biolink:affects', 'biolink:regulates']:
            return True
        return False

    @cache
    def is_qualifier(self, property_name):
        return self.toolkit.is_qualifier(property_name)

    def is_valid_node_type(self, node_type):
        if self.toolkit.is_category(node_type, mixin=True):
            return True
        if self.toolkit.is_mixin(node_type) and not self.toolkit.is_predicate(node_type):
            return True
        return False

    @cache
    def validate_edge(self, subject_types, predicate, object_types):
        for subject_type in subject_types:
            for object_type in object_types:
                if self.toolkit.validate_edge(subject_type, predicate, object_type, ancestors=True):
                    return True
        return False


BIOLINK_MAPPING_CHANGES = {
    'KEGG': 'http://identifiers.org/kegg/',
    'NCBIGene': 'https://identifiers.org/ncbigene/'
}


def get_biolink_prefix_map():
    response = requests.get(f'https://raw.githubusercontent.com/biolink/biolink-model/v{BIOLINK_MODEL_VERSION}/project/prefixmap/biolink_model_prefix_map.json')
    if response.status_code != 200:
        response.raise_for_status()
    biolink_prefix_map = response.json()
    biolink_prefix_map.update(BIOLINK_MAPPING_CHANGES)
    return biolink_prefix_map


INFORES_STATUS_INVALID = 'invalid'
INFORES_STATUS_DEPRECATED = 'deprecated'
INFORES_STATUS_VALID = 'valid'


class BiolinkInformationResources:
    infores_catalog_url = \
        'https://raw.githubusercontent.com/biolink/information-resource-registry/main/infores_catalog.yaml'

    def __init__(self):
        # Fetch the infores catalog from biolink
        s = requests.Session()
        retries = Retry(
            total=8,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={'GET'},
        )
        s.mount('https://', HTTPAdapter(max_retries=retries))
        infores_catalog_response = s.get(self.infores_catalog_url)
        infores_catalog_response.raise_for_status()
        infores_catalog_yaml = infores_catalog_response.text
        infores_catalog = yaml.safe_load(infores_catalog_yaml)

        # store the information as a dictionary with the infores ids as keys
        self.infores_lookup = {infores['id']: infores for infores in infores_catalog['information_resources']}

    def get_infores_status(self, infores_id):
        if infores_id in self.infores_lookup:
            infores_status = self.infores_lookup[infores_id]['status']
            if infores_status == INFORES_STATUS_DEPRECATED:
                return INFORES_STATUS_DEPRECATED
            else:
                return INFORES_STATUS_VALID
        else:
            return INFORES_STATUS_INVALID
