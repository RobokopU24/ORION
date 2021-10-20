from Common.kgxmodel import kgxnode, kgxedge
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES


class Extractor:
    """
    This is a class so that it can be used to accumulate nodes and edges across multiple files or input streams
    Also so that it can provide a few different interfaces (csv, sql) and keep the guts of the callback code in one
    place.
    """
    def __init__(self):
        #You might thing it would be good to include all the extractors at this level, but they are really file or query
        # level things.  You might want to use the same extractor with two differently formatted files or two different
        # sql queries.

        self.node_ids = set()
        self.nodes = []
        self.edges = []

        self.load_metadata = { 'record_counter': 0, 'skipped_record_counter': 0, 'errors': []}
        self.errors = []

    def csv_extract(self, infile,
                    subject_extractor,
                    object_extractor,
                    predicate_extractor,
                    subject_property_extractor,
                    object_property_extractor,
                    edge_property_extractor,
                    filter_set=set(),
                    filter_field=-1,
                    comment_character="#", delim='\t', has_header_row=False):
        """Read a csv, perform callbacks to retrieve node and edge info per row.
        Assumes that all of the properties extractable for a node occur on the line with the node identifier"""
        for i, line in enumerate(infile, start=1):
            if comment_character is not None and line.startswith(comment_character):
                continue

            if has_header_row and i == 1:
                continue

            if filter_field != -1:
                word_list = line[:-1].split(delim)
                # if(len(word_list) < 14):
                #     continue
                if(word_list[filter_field] not in filter_set):
                    continue

            self.load_metadata['record_counter'] += 1
            try:
                x = line[:-1].split(delim)
                self.parse_row(x, subject_extractor, object_extractor, predicate_extractor, subject_property_extractor, object_property_extractor, edge_property_extractor)
            except Exception as e:
                self.load_metadata['errors'].append(e.__str__())
                self.load_metadata['skipped_record_counter'] += 1

    def sql_extract(self, cursor, sql_query, subject_extractor, object_extractor, predicate_extractor, subject_property_extractor, object_property_extractor, edge_property_extractor):
        """Read a csv, perform callbacks to retrieve node and edge info per row.
        Assumes that all of the properties extractable for a node occur on the line with the node identifier"""

        cursor.execute(sql_query)
        rows = cursor.fetchall()
        for row in rows:
            self.load_metadata['record_counter'] += 1
            try:
                self.parse_row(row, subject_extractor, object_extractor, predicate_extractor, subject_property_extractor, object_property_extractor, edge_property_extractor)
            except Exception as e:
                self.load_metadata['errors'].append(e.__str__())
                self.load_metadata['skipped_record_counter'] += 1

    def json_extract(self,
                     json_array,
                     subject_extractor,
                     object_extractor,
                     predicate_extractor,
                     subject_property_extractor,
                     object_property_extractor,
                     edge_property_extractor):
        for item in json_array:
            self.load_metadata['record_counter'] += 1
            try:
                self.parse_row(item, subject_extractor, object_extractor, predicate_extractor, subject_property_extractor, object_property_extractor, edge_property_extractor)
            except Exception as e:
                self.load_metadata['errors'].append(e.__str__())
                self.load_metadata['skipped_record_counter'] += 1
                return

    def parse_row(self, row, subject_extractor, object_extractor, predicate_extractor, subject_property_extractor, object_property_extractor, edge_property_extractor):
        # print("\n test 5" , "\n")
        # print(row)
        # pull the information out of the edge
        subject_id = subject_extractor(row)
        # print("\n test 51 " + subject_id + "\n")
        object_id = object_extractor(row)
        # print("\n test 52 " + object_id + "\n")
        predicate = predicate_extractor(row)
        # print("\n test 53 " + predicate + "\n")
        subjectprops = subject_property_extractor(row)
        # print("\n test 54 ... \n" )
        objectprops = object_property_extractor(row)
        # print("\n test 55 ... \n" )
        edgeprops = edge_property_extractor(row)

        # print("\n test 56 ... \n" )

        # if we  haven't seen the subject before, add it to nodes
        if subject_id and subject_id not in self.node_ids:
            # print("\n test 57 \n" )
            subject_name = subjectprops.pop('name', None)
            subject_categories = subjectprops.pop('categories', None)
            subject_node = kgxnode(subject_id, name=subject_name, categories=subject_categories, nodeprops=subjectprops)
            self.nodes.append(subject_node)
            self.node_ids.add(subject_id)

        # if we  haven't seen the object before, add it to nodes
        if object_id and object_id not in self.node_ids:
            # print("\n test 58 \n" )
            object_name = objectprops.pop('name', None)
            object_categories = objectprops.pop('categories', None)
            object_node = kgxnode(object_id, name=object_name, categories=object_categories, nodeprops=objectprops)
            self.nodes.append(object_node)
            self.node_ids.add(object_id)

        if subject_id and object_id and predicate:
            # print("\n test 59 \n" )
            original_knowledge_source = edgeprops.pop(ORIGINAL_KNOWLEDGE_SOURCE, None)
            primary_knowledge_source = edgeprops.pop(PRIMARY_KNOWLEDGE_SOURCE, None)
            aggregator_knowledge_sources = edgeprops.pop(AGGREGATOR_KNOWLEDGE_SOURCES, None)
            relation = edgeprops.pop('relation', predicate)
            edge = kgxedge(subject_id,
                           object_id,
                           relation=relation,
                           predicate=predicate,
                           original_knowledge_source=original_knowledge_source,
                           primary_knowledge_source=primary_knowledge_source,
                           aggregator_knowledge_sources=aggregator_knowledge_sources,
                           edgeprops=edgeprops)
            self.edges.append(edge)
            # print("\n test 60 \n" )
