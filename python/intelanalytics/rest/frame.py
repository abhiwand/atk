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


class FrameBackendRest(object):
    """REST plumbing for BigFrame"""

    def __init__(self, http_methods=None):
        self.rest_http = http_methods or rest_http
        # use global connection, auth, etc.  This client does not support
        # multiple connection contexts

    def get_frame_names(self):
        logger.info("REST Backend: get_frame_names")
        r = self.rest_http.get('dataframes')
        payload = r.json()
        return [f['name'] for f in payload]

    def get_frame(self, name):
        """Retrieves the named BigFrame object"""
        raise NotImplemented  # TODO - implement get_frame

    def delete_frame(self, frame):
        logger.info("REST Backend: Delete frame {0}".format(repr(frame)))
        r = self.rest_http.delete("dataframes/" + str(frame._id))
        return r

    def create(self, frame):
        logger.info("REST Backend: create frame: " + frame.name)
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
        r = self.rest_http.post('dataframes', payload)
        logger.info("REST Backend: create frame response: " + r.text)
        payload = r.json()
        frame._id = payload['id']
        frame._uri = "%s/%d" % (self._get_uri(payload), frame._id)

    def _get_uri(self, payload):
        links = payload['links']
        for link in links:
            if link['rel'] == 'self':
               return link['uri']
        raise Exception('Unable to find uri for frame')

    def append(self, frame, data):
        logger.info("REST Backend: Appending data to frame {0}: {1}".format(frame.name, repr(data)))
        # for now, many data sources requires many calls to append
        if isinstance(data, list):
            for d in data:
                self.append(frame, d)
            return

        # TODO - put the frame uri in the frame, as received from create response
        frame_uri = "%sdataframes/%d" % (rest_http.base_uri, frame._id)
        # TODO - abstraction for payload construction
        payload = {'name': 'dataframe/load',
                   'arguments': {'source': data.file_name,
                                 'destination': frame_uri,
                                 'lineParser': { 'operation': { 'name':'builtin/line/separator' },
                                                 'arguments': { 'separator': data.delimiter,
                                                                'skipRows': data.skip_header_lines}}}}

        r = rest_http.post('commands', payload)
        logger.info("Response from REST server {0}".format(r.text))

        if isinstance(data, CsvFile):
            # update the Python object (set the columns)
            # todo - this info should come back from the engine
            for name, data_type in data.fields:
                if data_type is not ignore:
                    self._accept_column(frame, BigColumn(name, data_type))
        else:
            raise TypeError("Unsupported append data type "
                            + data.__class__.__name__)

    @staticmethod
    def _accept_column(frame, column):
        frame._columns[column.name] = column
        column._frame = frame

    def filter(self, frame, predicate):
        row_ready_predicate = wrap_row_function(frame, predicate)
        from itertools import ifilter
        def filter_func(iterator): return ifilter(row_ready_predicate, iterator)
        def func(s, iterator): return filter_func(iterator)

        pickled_predicate = pickle_function(func)
        http_ready_predicate = encode_bytes_for_http(pickled_predicate)

        # TODO - put the frame uri in the frame, as received from create response
        frame_uri = "%sdataframes/%d" % (rest_http.base_uri, frame._id)
        # TODO - abstraction for payload construction
        payload = {'name': 'dataframe/filter',
                   'arguments': {'frame': frame_uri,
                                 'predicate': http_ready_predicate}}
        r = rest_http.post('commands', payload)
        return r

    def remove_column(self, frame, name):
        frame_uri = "%sdataframes/%d" % (rest_http.base_uri, frame._id)
        # TODO - abstraction for payload construction
        payload = {'name': 'dataframe/removecolumn',
                   'arguments': {'frame': frame_uri,
                                 'column': name}}
        r = rest_http.post('commands', payload)
        return r

    def add_column(self, frame, expression, name, type):
        frame_uri = "%sdataframes/%d" % (rest_http.base_uri, frame._id)

        def addColumnLambda(row):
            row.data.append(unicode(supported_types.cast(expression(row),type)))
            return ",".join(row.data)

        row_ready_predicate = wrap_row_function(frame, addColumnLambda)
        from itertools import imap
        def filter_func(iterator): return imap(row_ready_predicate, iterator)
        def func(s, iterator): return filter_func(iterator)

        pickled_predicate = pickle_function(func)
        http_ready_predicate = encode_bytes_for_http(pickled_predicate)

        # TODO - put the frame uri in the frame, as received from create response
        frame_uri = "%sdataframes/%d" % (rest_http.base_uri, frame._id)
        # TODO - abstraction for payload construction
        payload = {'name': 'dataframe/addcolumn',
                   'arguments': {'frame': frame_uri,
                                 'columnname': name,
                                 'columntype': supported_types.get_type_string(type),
                                 'expression': http_ready_predicate}}
        r = rest_http.post('commands', payload)

        # todo - this info should come back from the engine
        self._accept_column(frame, BigColumn(name, type))

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
        return FrameBackendRest.InspectionTable(frame.schema, rows)

    def take(self, frame, n, offset):
        r = self.rest_http.get('dataframes/{0}/data?offset={2}&count={1}'.format(frame._id,n, offset))
        return r.json()
