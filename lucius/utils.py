import copy
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

class DotDict(dict):
    """
    defaultdict like, with dot notation for key access
    """
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getitem__(self, key):
        try:
            val = dict.__getitem__(self, key)
        except KeyError:
            return None

        if isinstance(val, dict) and not isinstance(val, DotDict):
            val = DotDict(val)
            self[key] = val
        return val
    __getattr__ = __getitem__

    def __deepcopy__(self, memo):
        return DotDict([(copy.deepcopy(k, memo), copy.deepcopy(v, memo))\
                for k, v in self.items()])


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

class GuardError(Exception):
    pass

def _print_(prefix):
    """
    Restricted Python Printer
    """
    class RestrictedPrint(object):
        def write(self, text):
            print "[%s] %s" % (prefix, text)
    return RestrictedPrint

def _getitem_(obj, index):
    """
    Restricted Python getitem guard
    """
    if obj is not None and type(obj) in (list, tuple, dict, DotDict,
            LuceneDocument):
        return obj[index]
    raise GuardError('Key: "%s" in Object Type: "%s"' % (index, type(obj)))
