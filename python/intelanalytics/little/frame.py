##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
LittleFrame object (BigFrame without Big backend, i.e. all Python memory space)

(wraps Pandas DataFrame)
"""

from pandas import Series
from collections import OrderedDict
from intelanalytics.core.frame import BigFrame
from intelanalytics.core.column import BigColumn
from intelanalytics.core.sources import SimpleDataSource
from intelanalytics.core.types import *

_little_frame_backend = None


def _get_little_frame_backend():
    global _little_frame_backend
    if not _little_frame_backend:
        _little_frame_backend = LittleFramePandasBackend()
    return _little_frame_backend


def get_little_frame_names():
    """Gets the names of BigFrame objects available for retrieval"""
    return _get_little_frame_backend().get_frame_names()


def get_little_frame(name):
    """Retrieves the named BigFrame object"""
    return _get_little_frame_backend().get_frame(name)


def delete_little_frame(name):
    """Deletes the frame from backing store"""
    return _get_little_frame_backend().delete_frame(name)


class LittleFrame(BigFrame):
    def __init__(self, source):
        self._backend = _get_little_frame_backend()
        self._df = None
        super(LittleFrame, self).__init__(source)

    def __repr__(self):
        return repr(self.inspect(n=None))


class LittleFramePandasBackend(object):

    _id_gen = 0

    @staticmethod
    def get_next_little_id():
        LittleFramePandasBackend._id_gen =- 1  # todo - make thread safe at some point
        return LittleFramePandasBackend._id_gen

    def _as_json_obj(self, frame):
        raise NotImplementedError

    def add_column(self, frame, schema, row_func):
        wrapped_func = _get_row_func(row_func, frame.schema, out_schema=schema)
        result = frame._df.apply(wrapped_func, axis=1)

        if hasattr(schema, '__iter__'):
            for name in schema:
                _assign_result_col(frame, name, result[name])
        else:
            _assign_result_col(frame, schema, result)

    def append(self, frame, *data):
        for src in data:
            if isinstance(src, SimpleDataSource):
                if frame._df is None:
                    frame._df = src.to_pandas_dataframe()
                    frame._columns = OrderedDict([(n, BigColumn(n, t))
                                                  for n, t in src.schema.items()])
                else:
                    frame._df.append(src.to_pandas_dataframe)
            else:
                raise TypeError("Unsupported data source type %s" % type(src))

    def copy_columns(self, frame, keys, columns):
        raise NotImplementedError

    def count(self, frame):
        return len(frame._df.index)

    def create(self, frame):
        frame._id = LittleFramePandasBackend.get_next_little_id()

    def remove_column(self, frame, name):
        if isinstance(name, basestring):
            name = [name]
        for victim in name:
            del frame._df[victim]

    def drop(self, frame, predicate, rejected_store=None):
        self._drop(frame, predicate, rejected_store, is_filter=False)

    def dropna(self, frame, choice, subset=None, rejected_store=None):
        self.drop(frame, lambda r: r.is_empty(choice, subset), rejected_store)

    def filter(self, frame, predicate, rejected_store=None):
        self._drop(frame, predicate, rejected_store, is_filter=True)

    def _drop(self, frame, predicate, rejected_store, is_filter):
        matching_rows_mask = frame._df.apply(_get_row_func(predicate, frame.schema), axis=1)
        keep_mask = matching_rows_mask if is_filter else ~matching_rows_mask
        frame._df = frame._df[keep_mask]
        if rejected_store:
            rejected_store.append(frame._df[~keep_mask])

    class InspectionTable(object):
        def __init__(self, df):
            self._df = df

        def __repr__(self):
            r = repr(self._df)
            lines = r.split('\n')
            a = []
            for line in lines:
                try:
                    i = line.index(' ')
                except ValueError:
                    a.append(line)
                else:
                    a.append( (' ' * i) + line[i:])
            return "\n".join(a)

        def _repr_html_(self):
            return self._df._repr_html_()

    def inspect(self, frame, n):
        df = frame._df[:n]
        # rewrite the column names to "include" the data type
        df.columns = ["{0}:{1}".format(n, supported_types.get_type_string(t))
                      for n, t in frame.schema.items()]
        return self.InspectionTable(df)

    #def map(self, frame, row_func, out=None):

    def rename_columns(self, frame, name_pairs):
        d = dict(name_pairs)
        columns = list(frame._df.columns)
        for i, c in enumerate(columns):
            try:
                columns[i] = d[c]
            except KeyError:
                pass
        frame._df.columns = columns

    def save(self, frame, name):
        raise NotImplementedError

    def take(self, frame, n, offset):
        t = []
        gen = frame._df.itertuples(index=False)
        try:
            for i in xrange(offset, offset + n):
                t.append(gen.next())
        except StopIteration:
            pass
        return t


def _get_row_func(func, frame_schema, out_schema=None):
    row_wrapper = RowWrapper(frame_schema)
    def row_function(series):
        row_wrapper._series = series
        result = func(row_wrapper)
        if out_schema:
            if hasattr(result, '__iter__'):
                names = _get_names_from_schema(out_schema)
                assert(len(names) == len(result))
                return Series(dict(zip(names, result)))
        return result
    return row_function

# Note, for wrapping a row function to return multiple columns
# ---------------------------------------------------------
#  result \ out  |      x        |  (x, y, z)    |  None
# ---------------+---------------+---------------+---------
#       a        |  frame.x = a  |  Error!       | Error!
# ---------------+---------------+---------------+---------
#   (a, b, c)    |  Error!       | frame.x=a,    | Error!
#                |               | frame.y=b     |
#                |               | frame.z=c     |
# ---------------+---------------+---------------+---------
# ((a, b), c, d) |  Error!       | frame.x=(a,b) | Error!
#                |               | frame.y=c     |
#                |               | frame.z=d     |
# ---------------+---------------+---------------+---------
# ((a, b, c),)   |  frame.x =    | Error!        | Error!
#                |    (a, b, c)  |               |
# -----------------------------------------------+---------


def _get_pairs_from_column_schema(schema):
    #  'x'
    #  ('x',)
    #  ('x', int32)
    #  ('x', 'y')
    #  (('x', int32),)
    #  (('x', int32), 'y')
    #  (('x', int32), ('y', str))
    if isinstance(schema, basestring):
        return [(schema, unknown)]
    if isinstance(schema, tuple) and len(schema) == 2 and isinstance(schema[1], type):
        return [(schema[0], schema[1])]
    if hasattr(schema, '__iter__'):
        result = []
        for item in schema:
            if isinstance(item, basestring):
                result.append((item, unknown))
            if isinstance(item, tuple):
                if len(item) == 1:
                    result.append((item[0], unknown))
                elif len(item) == 2 and isinstance(item[1], type):
                    result.append(item)
                else:
                    raise ValueError('Bad column schema' + str(schema) + " has nested tuple")

        return result
    raise ValueError('Bad column schema' + str(schema))


def _get_names_from_schema(schema):
    names, data_types = zip(*_get_pairs_from_column_schema(schema))
    return names


def _get_types_from_schema(schema):
    names, data_types = zip(*_get_pairs_from_column_schema(schema))
    return names


def _assign_result_col(frame, schema, series):
    if isinstance(schema, tuple):
        name = schema[0]
        if schema[1] in supported_types:
            data_type = schema[1]
        else:
            data_type = supported_types.get_type(series.ix[0])
    else:
        name = schema
        data_type = supported_types.get_type(series.ix[0])
    frame._df[name] = series
    frame._columns[name] = BigColumn(name, data_type)


class RowWrapper(object):
    def __init__(self, schema):
        self._series = None
        self._schema = schema

    def __getattr__(self, name):
        if name != "_schema" and name in self._schema.keys():
            return self[name]
        return super(RowWrapper, self).__getattribute__(name)

    def __getitem__(self, key):
        try:
            if isinstance(key, slice):
                raise TypeError("Slicing on row not supported")
            if isinstance(key, list):
                return [self._series[k] for k in key]
            return self._series[key]
        except KeyError:
            raise KeyError("Column name " + str(key) + " not present.")

    def _is_cell_empty(self, column):
        v = self._series[column]
        t = self._schema[column]
        # todo - flesh this out
        return v is None

    def is_empty(self, choice, subset=None):
        if isinstance(choice, basestring):
            return self._is_cell_empty(choice)
        elif choice is any or choice is all:
            subset = self._schema.keys() if subset is None else subset
            return choice(map(self._is_cell_empty, subset))
        else:
            raise ValueError("Bad choice; must be any, all, or a column name")
