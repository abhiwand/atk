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
REST backend for frames
"""
import base64
import logging
logger = logging.getLogger(__name__)
from collections import defaultdict, OrderedDict

from intelanalytics.core.column import BigColumn
from intelanalytics.core.files import CsvFile
from intelanalytics.core.types import *
from intelanalytics.rest.connection import rest_http
from intelanalytics.rest.spark import RowWrapper, pickle_function


def encode_bytes_for_http(b):
    """
    Encodes bytes using base64, so they can travel as a string
    """
    return base64.urlsafe_b64encode(b)


def wrap_row_function(frame, row_function):
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


class FrameBackendREST(object):
    """REST plumbing for BigFrame"""

    def get_frame_names(self):
        logger.info("REST Backend: get_frame_names")
        r = rest_http.get('dataframes')
        payload = r.json()
        return [f['name'] for f in payload]

    def create(self, frame):
        logger.info("REST Backend: create: " + frame.name)
        # hack, steal schema early if possible...
        columns = [[n, supported_types.get_type_string(t)]
                  for n, t in frame.schema.items()]
        if not len(columns):
            try:
                if isinstance(frame._original_source,CsvFile):
                    columns = frame._original_source._schema_to_json()
            except:
                pass
        payload = {'name': frame.name, 'schema': {"columns": columns}}
        r = rest_http.post('dataframes', payload)
        logger.info("REST Backend: create response: " + r.text)
        payload = r.json()
        frame._id = payload['id']

    def append(self, frame, data):
        logger.info("REST Backend: Appending data to frame {0}: {1}".format(frame.name, repr(data)))
        # for now, many data sources requires many calls to append
        if isinstance(data, list):
            for d in data:
                self.append(frame, d)
            return

        payload = {'name': 'load', 'language': 'builtin', 'arguments': {'source': data.file_name, 'separator': data.delimiter, 'skipRows': 1}}
        r = rest_http.post('dataframes/{0}/transforms'.format(frame._id), payload=payload)
        logger.info("Response from REST server {0}".format(r.text))

        if isinstance(data, CsvFile):
            # update the Python object (set the columns)
            # todo - this info should come back from the engine
            for name, data_type in data.fields:
                if data_type is not ignore:
                    frame._columns[name] = BigColumn(name, data_type)
        else:
            raise TypeError("Unsupported append data type "
                            + data.__class__.__name__)

    def filter(self, frame, predicate):
        row_ready_predicate = wrap_row_function(frame, predicate)
        from itertools import ifilter
        def filter_func(iterator): return ifilter(row_ready_predicate, iterator)
        def func(s, iterator): return filter_func(iterator)

        pickled_predicate = pickle_function(func)
        http_ready_predicate = encode_bytes_for_http(pickled_predicate)

        payload = {'name': 'filter', 'language': 'builtin', 'arguments': {'predicate': http_ready_predicate}}
        r = rest_http.post('dataframes/{0}/transforms'.format(frame._id), payload=payload)
        return r

    class InspectionTable(object):
        _align = defaultdict(lambda: 'c')  # 'l', 'c', 'r'
        _align.update([(bool, 'r'),
                       (bytearray, 'l'),
                       (dict, 'l'),
                       (float32, 'r'),
                       (float64, 'r'),
                       (int32, 'r'),
                       (int64, 'r'),
                       (list, 'l'),
                       (string, 'l'),
                       (str, 'l')])

        def __init__(self, schema, rows):
            self.schema = schema
            self.rows = rows

        def __repr__(self):
            # keep the import localized, as serialization doesn't like prettytable
            import intelanalytics.rest.prettytable as prettytable
            table = prettytable.PrettyTable()
            fields = OrderedDict([("{0}:{1}".format(n, supported_types.get_type_string(t)), self._align[t]) for n, t in self.schema.items()])
            table.field_names = fields.keys()
            table.align.update(fields)
            table.hrules = prettytable.HEADER
            table.vrules = prettytable.NONE
            for r in self.rows:
                table.add_row(r)
            return table.get_string()

         #def _repr_html_(self): Add this method for ipython notebooks

    def inspect(self, frame, n, offset):
        # inspect is just a pretty-print of take, we'll do it on the client
        # side until there's a good reason not to
        rows = self.take(frame, n, offset)
        return FrameBackendREST.InspectionTable(frame.schema, rows)

    def take(self, frame, n, offset):
        r = rest_http.get('dataframes/{0}/data?offset={2}&count={1}'.format(frame._id,n, offset))
        return r.json()

    def delete_frame(self, frame):
        logger.info("REST Backend: Delete frame {0}".format(repr(frame)))
        r = rest_http.delete("dataframes/" + str(frame._id))
        return r

