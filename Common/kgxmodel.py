class kgxnode:
    def __init__(self,identifier,nodeprops={}):
        self.identifier = identifier
        self.name = nodeprops.get('name','')
        self.categories = nodeprops.get('categories',['biolink:NamedThing'])
        self.properties = { k:v for k,v in nodeprops.items() if k not in ['name','categories'] }

class kgxedge:
    def __init__(self,subject_id, object_id, predicate, edgeprops):
        self.subjectid = subject_id
        self.objectid = object_id
        self.relation = predicate
        self.properties = edgeprops
