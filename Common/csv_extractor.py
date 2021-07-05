from Common.kgxmodel import kgxnode, kgxedge

def extract(infile,subject_extractor,
                   object_extractor,
                   predicate_extractor,
                   subject_property_extractor,
                   object_property_extractor,
                   edge_property_extractor,
                   comment_character="#",delim='\t'):
    """Read a csv, perform callbacks to retrieve node and edge info per row.
    Assumes that all of the properties extractable for a node occur on the line with the node identifier"""
    node_ids = set()
    nodes = []
    edges = []

    load_metadata = { 'record_counter': 0, 'skipped_record_counter': 0 }

    for line in infile:
        if comment_character is not None and line.startswith(comment_character):
            continue

        load_metadata['record_counter'] += 1

        try:

            x = line[:-1].split(delim)
            #pull the information out of the edge
            subject_id=subject_extractor(x)
            object_id =object_extractor(x)
            predicate =predicate_extractor(x)
            subjectprops = subject_property_extractor(x)
            objectprops = object_property_extractor(x)
            edgeprops = edge_property_extractor(x)

            #if we  haven't seen the subject before, add it to nodes
            if subject_id not in node_ids:
                subject_node = kgxnode(subject_id,subjectprops)
                nodes.append(subject_node)
                node_ids.add(subject_id)

            #if we  haven't seen the subject before, add it to nodes
            if object_id not in node_ids:
                object_node = kgxnode(object_id,objectprops)
                nodes.append(object_node)
                node_ids.add(object_id)

            edge = kgxedge(subject_id, object_id, predicate, edgeprops)
            edges.append(edge)
        except Exception as e:
            load_metadata['skipped_record_counter'] += 1
    return nodes, edges, load_metadata
