
class SimpleDataSource(object):

    annotation = "simple"

    def __init__(self, schema=None, rows=None, columns=None):
        if not ((rows is None) ^ (columns is None)):
            raise ValueError("Either rows or columns must be supplied")
        self.schema = schema
        self.rows = rows
        self.columns = columns
        if columns:
            (names, types) = zip(*self.schema)
            if len(names) != len(self.columns):
                raise ValueError("number of columns in schema not equals number of columns provided")
            for key in self.columns.keys():
                if key not in names:
                    raise ValueError("names in schema do not all match the names in the columns provided")


    def to_pandas_dataframe(self):
        import numpy as np
        from pandas import DataFrame
        if self.rows:
            a = np.array(self.rows, dtype=_schema_as_numpy_dtype(self.schema))
            df = DataFrame(a)
        else:  # columns
            df = DataFrame(self.columns)
        return df

def _schema_as_numpy_dtype(schema):
    return [(c, _get_numpy_dtype_from_core_type(t)) for c, t in schema]

def _get_numpy_dtype_from_core_type(t):
    return object
    # if t in [str, unicode, dict, bytearray, list]:
    #     return object
    # return t

