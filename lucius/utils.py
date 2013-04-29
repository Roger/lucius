from lupyne.engine import documents

field_types = {"default": documents.Field,
               "map": documents.MapField,
               "numeric": documents.NumericField,
               "format": documents.FormatField,
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
