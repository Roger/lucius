from lupyne.engine import documents

field_types = {"default": documents.Field,
               "map": documents.MapField,
               "numeric": documents.NumericField,
               "format": documents.FormatField,
               "nested": documents.NestedField,
               "datetime": documents.DateTimeField,
               }

class LuceneDocument(documents.document.Document):
    def add_field(self, name, value, field_type="default", **params):
        cls = field_types[field_type]
        field = cls(name, **params)
        self.add(next(field.items(*[value])))

def index_doc(doc):
    try:
        data = doc["data"]
        result = LuceneDocument()
        result.add_field("song", data["song"], store=True)
        result.add_field("artist", data["artist"], store=True)
        return result
    except KeyError:
        pass
