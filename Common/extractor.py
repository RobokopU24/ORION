from Common.kgxmodel import kgxnode, kgxedge

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

        self.load_metadata = { 'record_counter': 0, 'skipped_record_counter': 0 }

    def csv_extract(self, infile, subject_extractor,
                   object_extractor,
                   predicate_extractor,
                   subject_property_extractor,
                   object_property_extractor,
                   edge_property_extractor,
                   comment_character="#",delim='\t'):
        """Read a csv, perform callbacks to retrieve node and edge info per row.
        Assumes that all of the properties extractable for a node occur on the line with the node identifier"""
        for line in infile:
            if comment_character is not None and line.startswith(comment_character):
                continue

            self.load_metadata['record_counter'] += 1
            try:
                x = line[:-1].split(delim)
                self.parse_row(x, subject_extractor, object_extractor, predicate_extractor, subject_property_extractor, object_property_extractor, edge_property_extractor)
            except Exception as e:
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
                print(e)
                print(row)
                exit()
                self.load_metadata['skipped_record_counter'] += 1

    def parse_row(self, row, subject_extractor, object_extractor, predicate_extractor, subject_property_extractor, object_property_extractor, edge_property_extractor):
        # pull the information out of the edge
        subject_id = subject_extractor(row)
        object_id = object_extractor(row)
        predicate = predicate_extractor(row)
        subjectprops = subject_property_extractor(row)
        objectprops = object_property_extractor(row)
        edgeprops = edge_property_extractor(row)

        # if we  haven't seen the subject before, add it to nodes
        if subject_id not in self.node_ids:
            subject_node = kgxnode(subject_id, subjectprops)
            self.nodes.append(subject_node)
            self.node_ids.add(subject_id)

        # if we  haven't seen the subject before, add it to nodes
        if object_id not in self.node_ids:
            object_node = kgxnode(object_id, objectprops)
            self.nodes.append(object_node)
            self.node_ids.add(object_id)

        edge = kgxedge(subject_id, object_id, predicate, edgeprops)
        self.edges.append(edge)

