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
Spark-specific implementation on the client-side
"""
# TODO - remove client knowledge of spark, delete this file

import base64
import os
import itertools
import pstats

spark_home = os.getenv('SPARK_HOME')
if not spark_home:
    spark_home = '~/IntelAnalytics/spark'
    os.environ['SPARK_HOME'] = spark_home

spark_python = os.path.join(spark_home, 'python')
import sys
if spark_python not in sys.path:
    sys.path.append(spark_python)

from serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer, CloudPickleSerializer, write_int

from intelanalytics.core.row import Row
from intelanalytics.core.row import NumpyJSONEncoder
from intelanalytics.core.iatypes import valid_data_types


import json

UdfDependencies = []


def get_file_content_as_str(filename):
    with open(filename, 'rb') as f:
        return f.read()


def _get_dependencies(filenames):
    return [{'file_name': filename, 'file_content':get_file_content_as_str(filename)} for filename in filenames]


def ifiltermap(predicate, function, iterable):
    """creates a generator than combines filter and map"""
    return (function(item) for item in iterable if predicate(item))


def get_add_one_column_function(row_function, data_type):
    """Returns a function which adds a column to a row based on given row function"""
    def add_one_column(row):
        result = row_function(row)
        cast_value = valid_data_types.cast(result, data_type)
        return json.dumps([cast_value], cls=NumpyJSONEncoder)
    return add_one_column


def get_add_many_columns_function(row_function, data_types):
    """Returns a function which adds several columns to a row based on given row function"""
    def add_many_columns(row):
        result = row_function(row)
        data = []
        for i, data_type in enumerate(data_types):
            cast_value = valid_data_types.cast(result[i], data_type)
            data.append(cast_value)
        return json.dumps(data, cls=NumpyJSONEncoder)
    return add_many_columns


def get_copy_columns_function(column_names, from_schema):
    """Returns a function which copies only certain columns for a row"""
    indices = [i for i, column in enumerate(from_schema) if column[0] in column_names]

    def project_columns(row):
        from intelanalytics.core.row import NumpyJSONEncoder
        return json.dumps([row[index] for index in indices], cls=NumpyJSONEncoder)
    return project_columns


class IaPyWorkerError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(base64.urlsafe_b64decode(self.value))


class RowWrapper(Row):
    """
    Wraps row for specific RDD line digestion using the Row object
    """

    def load_row(self, s):
        self._set_data(json.loads(s))


def pickle_function(func):
    """Pickle the function the way Pyspark does"""

    command = (func, None, UTF8Deserializer(), IaBatchedSerializer())
    pickled_function = CloudPickleSerializer().dumps(command)
    return pickled_function


def encode_bytes_for_http(b):
    """
    Encodes bytes using base64, so they can travel as a string
    """
    return base64.urlsafe_b64encode(b)


def _wrap_row_function(frame, row_function):
    """
    Wraps a python row function, like one used for a filter predicate, such
    that it will be evaluated with using the expected 'row' object rather than
    whatever raw form the engine is using.  Ideally, this belong in the engine
    """
    schema = frame.schema  # must grab schema now so frame is not closed over
    def row_func(row):
        try:
            row_wrapper = RowWrapper(schema)
            row_wrapper.load_row(row)
            return row_function(row_wrapper)
        except Exception as e:
            msg = base64.urlsafe_b64encode((u'Exception:%s while processing row:%s' % (repr(e),row)).encode('utf-8'))
            raise IaPyWorkerError(msg)
    return row_func


def get_udf_arg(frame, subject_function, iteration_function):
    """
    Prepares a python row function for server execution and http transmission

    Parameters
    ----------
    frame : Frame
        frame on whose rows the function will execute
    subject_function : function
        a function with a single row parameter
    iteration_function: function
        the iteration function to apply for the frame.  In general, it is
        imap.  For filter however, it is ifilter
    """
    row_ready_function = _wrap_row_function(frame, subject_function)
    def iterator_function(iterator): return iteration_function(row_ready_function, iterator)
    def iteration_ready_function(s, iterator): return iterator_function(iterator)
    return make_http_ready(iteration_ready_function)


def get_udf_arg_for_copy_columns(frame, predicate_function, column_names):
    row_ready_predicate = _wrap_row_function(frame, predicate_function)
    row_ready_map = _wrap_row_function(frame, get_copy_columns_function(column_names, frame.schema))
    def iteration_ready_function(s, iterator): return ifiltermap(row_ready_predicate, row_ready_map, iterator)
    return make_http_ready(iteration_ready_function)


def make_http_ready(function):
    pickled_function = pickle_function(function)
    http_ready_function = encode_bytes_for_http(pickled_function)
    return { 'function': http_ready_function, 'dependencies':_get_dependencies(UdfDependencies)}


class IaBatchedSerializer(BatchedSerializer):
    def __init__(self):
        super(IaBatchedSerializer,self).__init__(PickleSerializer(), 1000)

    def dump_stream(self, iterator, stream):
        self.dump_stream_as_json(self._batched(iterator), stream)

    def dump_stream_as_json(self, iterator, stream):
        for obj in iterator:
            if len(obj) > 0:
                serialized = '[%s]' % ','.join(obj)
                try:
                    s = str(serialized)
                except:
                    s = serialized.encode('utf-8')
                write_int(len(s), stream)
                stream.write(s)
