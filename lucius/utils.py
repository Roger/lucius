import time

import couchdb

from flask import g, current_app
from lupyne.engine import documents

field_types = {"default": documents.Field,
               "map": documents.MapField,
               "numeric": documents.NumericField,
               #"format": documents.FormatField,
               "nested": documents.NestedField,
               "datetime": documents.DateTimeField,
               }

class LuceneDocument(documents.document.Document):
    def __init__(self):
        self._related_fields = []
        super(LuceneDocument, self).__init__()

    def add_field(self, name, value, field_type="default", **params):
        cls = field_types[field_type]
        field = cls(name, **params)
        self.add(next(field.items(*[value])))

    def add_related(self, name, docid, doc_field, field_type="default", **params):
        self._related_fields.append([name, docid, doc_field, field_type, params])

def get_field(doc, field_name):
    value = doc
    try:
        for name in field_name.split("."):
            value = value[name]
    except KeyError:
        return None
    return value

def get_designs(database, config=None):
    config = config or current_app.config
    server = config["COUCHDB_SERVER"]
    db = couchdb.Database("%s/%s/" % (server, database))

    view = db.view("_all_docs", startkey="_design", endkey="_design0",
            include_docs=True)
    return view

def get_indexer(database, view, index, start=True):
    # start indexing if not indexing already
    not_exists = start_indexer(database)
    name = "%s/%s/%s" % (database, view, index)

    count = 0
    while True:
        count += 1
        try:
            return g.indexers[name]
        except KeyError:
            if start and not_exists and count <= 5:
                time.sleep(.1)
                continue
            return


def _print_(prefix):
    """
    Restricted Python Printer
    """
    class RestrictedPrint(object):
        def write(self, text):
            print "[%s] %s" % (prefix, text)
    return RestrictedPrint
