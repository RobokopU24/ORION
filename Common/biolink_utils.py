
from bmt import Toolkit

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

    def __init__(self):
        self.toolkit = Toolkit()

    def find_biolink_leaves(self, biolink_concepts: set):
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
