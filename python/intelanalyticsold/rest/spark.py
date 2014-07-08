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
from intelanalytics.core.types import supported_types

rdd_delimiter = '\0'
rdd_null_indicator = 'YoMeNull'


def make_row(row_data):
    return [unicode(field) if field is not None else unicode(rdd_null_indicator)
            for field in row_data]


def get_add_one_column_function(row_function, data_type):
    """Returns a function which adds a column to a row based on given row function"""
    def add_one_column(row):
        result = row_function(row)
        cast_value = supported_types.cast(result, data_type)
        if cast_value is None:
            cast_value = rdd_null_indicator
        row.data.append(cast_value)
        row_data = make_row(row.data)
        return rdd_delimiter.join(row_data)
    return add_one_column


def get_add_many_columns_function(row_function, data_types):
    """Returns a function which adds several columns to a row based on given row function"""
    def add_many_columns(row):
        result = row_function(row)
        for i, data_type in enumerate(data_types):
            cast_value = supported_types.cast(result[i], data_type)
            if cast_value is None:
                cast_value = rdd_null_indicator
            row.data.append(cast_value)
        row_data = make_row(row.data)
        return rdd_delimiter.join(row_data)
    return add_many_columns


class RowWrapper(Row):
    """
    Wraps row for specific RDD line digestion using the Row object
    """

    def load_row(self, s):
        # todo - will probably change frequently
        #  specific to String RDD, takes a comma-sep string right now...
        self.data = [field if field != rdd_null_indicator else None
                     for field in s.split(rdd_delimiter)]
        #print "row_wrapper.data=" + str(self.data)


def pickle_function(func):
    """Pickle the function the way Pyspark does"""
    command = (func, UTF8Deserializer(), IaBatchedSerializer())
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
    def row_func(row):
        row_wrapper = RowWrapper(frame.schema)
        row_wrapper.load_row(row)
        return row_function(row_wrapper)
    return row_func


def prepare_row_function(frame, subject_function, iteration_function):
    """
    Prepares a python row function for server execution and http transmission

    Parameters
    ----------
    frame : BigFrame
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

    pickled_function = pickle_function(iteration_ready_function)
    http_ready_function = encode_bytes_for_http(pickled_function)
    return http_ready_function


class IaBatchedSerializer(BatchedSerializer):
    def __init__(self):
        super(IaBatchedSerializer,self).__init__(PickleSerializer(), 1)

    def dump_stream(self, iterator, stream):
        self.dump_stream_as_json(self._batched(iterator), stream)

    def dump_stream_as_json(self, iterator, stream):
        for obj in iterator:
            serialized = ",".join(obj)
            try:
                s = str(serialized)
            except UnicodeEncodeError:
                s = unicode(serialized).encode('unicode_escape')
            write_int(len(s), stream)
            stream.write(s)
