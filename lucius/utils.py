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
