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
    def __init__(self, *source):
        super(LittleFrame, self).__init__()
        self._df = None
        self._backend = _get_little_frame_backend()
        if source:
            self.append(*source)

    def __repr__(self):
        return repr(self.inspect(n=None))


class LittleFramePandasBackend(object):

    def _as_json_obj(self, frame):
        raise NotImplementedError

    def append(self, frame, *data):
        for src in data:
            if isinstance(src, SimpleDataSource):
                if frame._df is None:
                    frame._df = src.to_pandas_dataframe()
                    frame._columns = OrderedDict([(n, BigColumn(n, t))
                                                  for n, t in src.schema])
                else:
                    frame._df.append(src.to_pandas_dataframe)
            # elif isinstance(src, list):
            #     You're in here going... I want to append a list of tuples'
            #      if frame._df is None:
            #         frame._df = src.to_pandas_dataframe()
            #         frame._columns = OrderedDict([(n, BigColumn(n, t))
            #                                       for n, t in src.schema])
            #     else:
            #         frame._df.append(src.to_pandas_dataframe)
            #      list of tuples

            else:
                raise TypeError("Unsupported data source type %s" % type(src))

    def assign(self, frame, dst, value):
        raise NotImplementedError

    def copy_columns(self, frame, keys, columns):
        raise NotImplementedError

    def count(self, frame):
        return len(frame._df.index)

    def drop_columns(self, frame, keys):
        raise NotImplementedError

    def drop(self, frame, predicate, rejected_store=None):
        self._drop(frame, predicate, rejected_store, is_filter=False)

    def dropna(self, frame, choice, subset=None, rejected_store=None):
        self.drop(frame, lambda r: r.is_empty(choice, subset), rejected_store)

    def filter(self, frame, predicate, rejected_store=None):
        self._drop(frame, predicate, rejected_store, is_filter=True)

    def _drop(self, frame, predicate, rejected_store, is_filter):
        matching_rows_mask = frame._df.apply(self._get_row_func(predicate, frame.schema), axis=1)
        keep_mask = matching_rows_mask if is_filter else ~matching_rows_mask
        frame._df = frame._df[keep_mask]
        if rejected_store:
            rejected_store.append(frame._df[~keep_mask])

    def _get_row_func(self, func, schema):
        row_wrapper = RowWrapper(schema)
        def row_function(series):
            row_wrapper._series = series
            return func(row_wrapper)
        return row_function

    class InspectionTable(object):
        def __init__(self, df):
            self._df = df

        def __repr__(self):
            return repr(self._df)

        def _repr_html_(self):
            return self._df._repr_html_()

    def inspect(self, frame, n):
        df = frame._df[:n]
        # rewrite the column names to "include" the data type
        df.columns = ["{0}:{1}".format(n, supported_types.get_type_string(t))
                      for n, t in frame.schema.items()]
        return self.InspectionTable(df)

    def reduce(self, frame, predicate, rejected_store=None):
        raise NotImplementedError

    def rename_columns(self, frame, name_pairs):
        raise NotImplementedError

    def save(self, frame, name):
        raise NotImplementedError

    def take(self, frame, n):
        t = []
        gen = frame._df.itertuples(index=False)
        try:
            for i in xrange(n):
                t.append(gen.next())
        except StopIteration:
            pass
        return t

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
