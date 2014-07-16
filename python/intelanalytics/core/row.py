from intelanalytics.core.orddict import OrderedDict
from intelanalytics.core.iatypes import supported_types


class Row(object):

    def __init__(self, schema, data=None):
        """
        Expects schema to as list of tuples
        """
        self.schema_dict = OrderedDict(schema)
        self.data = [] if data is None else data  # data is an array of strings right now

    def __getattr__(self, name):
        if name != "_schema" and name in self.schema_dict.keys():
            return self[name]
        return super(Row, self).__getattribute__(name)

    def __getitem__(self, key):
        try:
            if isinstance(key, slice):
                raise TypeError("Index slicing a row is not supported")
            if isinstance(key, list):
                return [self._get_cell_value(k) for k in key]
            if isinstance(key, int):
                return self._get_cell_value_by_index(key)
            return self._get_cell_value(key)
        except KeyError:
            raise KeyError("Column name " + str(key) + " not present.")

    def __len__(self):
        return len(self.schema_dict)

    def __iter__(self):
        return self.items().__iter__()

    def keys(self):
        return self.schema_dict.keys()

    def values(self):
        return [self._get_cell_value(k) for k in self.keys()]

    def types(self):
        return self.schema_dict.values()

    def items(self):
        return zip(self.keys(), self.values())

    def get_cell_type(self, key):
        return self.schema_dict[key]

    def _get_cell_value(self, key):
        index = self.schema_dict.keys().index(key)  # could improve speed here...
        return self._get_cell_value_by_index(index)

    def _get_cell_value_by_index(self, index):
        # converts the value into the proper data type
        dtype = self.schema_dict.values()[index]
        return supported_types.cast(self.data[index], dtype)