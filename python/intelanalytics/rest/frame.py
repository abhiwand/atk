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
import uuid
import logging
logger = logging.getLogger(__name__)
from collections import defaultdict, OrderedDict
import json

from intelanalytics.core.frame import BigFrame
from intelanalytics.core.column import BigColumn
from intelanalytics.core.files import CsvFile
from intelanalytics.core.types import *
from intelanalytics.rest.connection import http
from intelanalytics.rest.command import CommandRequest, executor
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
        self.rest_http = http_methods or http
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

    def create(self, frame, source, name):
        logger.info("REST Backend: create frame: " + frame.name)
        if isinstance(source, FrameInfo):
            self._initialize_frame(frame, source)
            return  # early exit here

        frame._name = name or self._get_new_frame_name(source)
        self._create_new_frame(frame)

        # TODO - change project such that server creates the new frame, instead of passing one in
        if isinstance(source, BigFrame):
            self.project_columns(source, frame, source.column_names)
        elif isinstance(source, BigColumn):
            self.project_columns(source.frame, frame, source.name)
        elif (isinstance(source, list) and all(isinstance(iter, BigColumn) for iter in source)):
            self.project_columns(source[0].frame, frame, [s.name for s in source])
        else:
            if source:
                self.append(frame, source)

    def _create_new_frame(self, frame):
        payload = {'name': frame.name }
        r = self.rest_http.post('dataframes', payload)
        logger.info("REST Backend: create frame response: " + r.text)
        payload = r.json()
        #TODO - call _initialize_frame instead
        frame._id = payload['id']
        frame._uri = self._get_uri(payload)

    @staticmethod
    def _initialize_frame(frame, frame_info):
        """Initializes a frame according to given frame_info"""
        frame._id = frame_info.id_number
        # TODO - update uri from result (this is a TODO in the engine)
        #frame._uri = frame_info.uri
        frame._name = frame_info.name
        frame._columns.clear()
        for column in frame_info.columns:
            FrameBackendRest._accept_column(frame, column)

    @staticmethod
    def _get_load_arguments(frame, data):
        if isinstance(data, CsvFile):
            return {'source': data.file_name,
                    'destination': frame.uri,
                    'schema': {'columns': data._schema_to_json()},
                    'skipRows': data.skip_header_lines,
                    'lineParser': {'operation': {'name': 'builtin/line/separator'},
                                   'arguments': {'separator': data.delimiter
                                   }}}
        raise TypeError("Unsupported data source " + type(data).__name__)

    @staticmethod
    def _accept_column(frame, column):
        frame._columns[column.name] = column
        column._frame = frame

    @staticmethod
    def _get_new_frame_name(source=None):
        try:
            annotation = "_" + source.annotation
        except:
            annotation = ''
        return "frame_" + uuid.uuid4().hex + annotation

    @staticmethod
    def _get_uri(payload):
        links = payload['links']
        for link in links:
            if link['rel'] == 'self':
               return link['uri']
        raise Exception('Unable to find uri for frame')


    def add_columns(self, frame, expression, names, types):
        if not names:
            i = 0
            while(True):
                name = "new" + str(i)
                if name not in frame._columns:
                    break
                i += 1
            names = [name]
        elif isinstance(names, basestring):
            names = [names]

        if not hasattr(types, '__iter__'):
            types = [types]
        for t in types:
            supported_types.validate_is_supported_type(t)

        def addColumnLambda(row):
            row.data.append(unicode(supported_types.cast(expression(row),types[0])))
            return ",".join(row.data)

        #def add_columns_func():
        # TODO - write full add multiple columns

        row_ready_predicate = wrap_row_function(frame, addColumnLambda)
        from itertools import imap
        def filter_func(iterator): return imap(row_ready_predicate, iterator)
        def func(s, iterator): return filter_func(iterator)

        pickled_predicate = pickle_function(func)
        http_ready_predicate = encode_bytes_for_http(pickled_predicate)

        arguments = {'frame': frame.uri,
                     'columnname': names[0],
                     'columntype': supported_types.get_type_string(types[0]),
                     'expression': http_ready_predicate}
        command = CommandRequest('dataframe/addcolumn', arguments)
        command_info = executor.issue(command)

        # todo - this info should come back from the engine
        self._accept_column(frame, BigColumn(names[0], types[0]))

        return command_info

    def append(self, frame, data):
        logger.info("REST Backend: append data to frame {0}: {1}".format(frame.name, repr(data)))
        # for now, many data sources requires many calls to append
        if isinstance(data, list) or isinstance(data, tuple):
            for d in data:
                self.append(frame, d)
            return

        arguments = self._get_load_arguments(frame, data)
        command = CommandRequest(name="dataframe/load", arguments=arguments)
        command_info = executor.issue(command)

        if isinstance(data, CsvFile):
            # update the Python object (set the columns)
            # todo - this info should come back from the engine
            for name, data_type in data.schema:
                if data_type is not ignore:
                    self._accept_column(frame, BigColumn(name, data_type))
        else:
            raise TypeError("Unsupported append data type "
                            + data.__class__.__name__)


    def count(self, frame):
        raise NotImplementedError  # TODO - impplement count

    def drop_duplicates(self, frame, columns):
        arguments = {'frame': frame._id, "keyColumns": columns}
        command = CommandRequest("dataframe/drop_duplicates", arguments)
        command_info = executor.issue(command)
        frame_info = FrameInfo(command_info.result)

    def filter(self, frame, predicate):
        row_ready_predicate = wrap_row_function(frame, predicate)
        from itertools import ifilter
        def filter_func(iterator): return ifilter(row_ready_predicate, iterator)
        def func(s, iterator): return filter_func(iterator)

        pickled_predicate = pickle_function(func)
        http_ready_predicate = encode_bytes_for_http(pickled_predicate)

        arguments = {'frame': frame.uri, 'predicate': http_ready_predicate}
        command = CommandRequest(name="dataframe/filter", arguments=arguments)
        executor.issue(command)

    def flatten_column(self, frame, column_name):
        name = self._get_new_frame_name()
        arguments = {'name': name, 'frame': frame._id, 'column': column_name, 'separator': ',' }
        command = CommandRequest("dataframe/flattenColumn", arguments)
        command_info = executor.issue(command)
        frame_info = FrameInfo(command_info.result)
        return BigFrame(frame_info)

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
            fields = OrderedDict([("{0}:{1}".format(n, supported_types.get_type_string(t)), self._align[t]) for n, t in self.schema])
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

    def join(self, left, right, left_on, right_on, how):
        if right_on is None:
            right_on = left_on
        name = self._get_new_frame_name()
        arguments = {'name': name, "how": how, "frames": [[left._id, left_on], [right._id, right_on]] }
        command = CommandRequest("dataframe/join", arguments)
        command_info = executor.issue(command)
        frame_info = FrameInfo(command_info.result)
        return BigFrame(frame_info)

    def project_columns(self, frame, projected_frame, columns, new_names=None):
        # TODO - fix REST server to accept nulls, for now we'll pass an empty list
        if new_names is None:
            new_names = list(columns)
        arguments = {'frame': frame.uri, 'projected_frame': projected_frame.uri, 'columns': columns, "new_column_names": new_names}
        command = CommandRequest("dataframe/project", arguments)
        command_info = executor.issue(command)
        self._initialize_frame(projected_frame, FrameInfo(command_info.result))

        return command_info

    def groupBy(self, frame, group_by_columns, aggregation_list):
        name = self._get_new_frame_name()
        arguments = {'frame': frame.uri,
                    'name': name,
                    'group_by_columns' : group_by_columns,
                    'aggregations': aggregation_list}

        command = CommandRequest("dataframe/groupby", arguments)
        command_info = executor.issue(command)
        frame_info = FrameInfo(command_info.result)

        return BigFrame(frame_info)

    def remove_columns(self, frame, name):
        columns = ",".join(name) if isinstance(name, list) else name
        arguments = {'frame': frame.uri, 'column': columns}
        command = CommandRequest("dataframe/removecolumn", arguments)
        command_info = executor.issue(command)
        # TODO - slurp command_info, instead of the following:
        if isinstance(name, basestring):
            name = [name]
        for victim in name:
            del frame._columns[victim]
        return command_info

    def rename_columns(self, frame, column_names, new_names):
        if isinstance(column_names, basestring) and isinstance(new_names, basestring):
            column_names = [column_names]
            new_names = [new_names]
        if len(column_names) != len(new_names):
            raise ValueError("rename requires name lists of equal length")
        current_names = frame._columns.keys()
        for nn in new_names:
            if nn in current_names:
                raise ValueError("Cannot use rename to '{0}' because another column already exists with that name".format(nn))
        #originalcolumns, renamedcolumns = ",".join(zip(*name_pairs)[0]), ",".join(zip(*name_pairs)[1])
        arguments = {'frame': frame.uri, "original_names": column_names, "new_names": new_names}
        command = CommandRequest("dataframe/rename_column", arguments)
        command_info = executor.issue(command)
        self._initialize_frame(frame, FrameInfo(command_info.result))

        # rename on python side, here in the frame's local columns:
        #values = self._columns.values()  # must preserve order in OrderedDict
        #for p in name_pairs:
            #self._columns[p[0]].name = p[1]
        #self._columns = OrderedDict([(v.name, v) for v in values])

    def rename_frame(self, frame, name):
        arguments = {'frame': frame.uri, "new_name": name}
        command = CommandRequest("dataframe/rename_frame", arguments)
        return executor.issue(command)


    def take(self, frame, n, offset):
        r = self.rest_http.get('dataframes/{0}/data?offset={2}&count={1}'.format(frame._id,n, offset))
        return r.json()


class FrameInfo(object):
    """
    JSON-based Server description of a BigFrame
    """
    def __init__(self, frame_json_payload):
        self._payload = frame_json_payload

    def __repr__(self):
        return json.dumps(self._payload, indent=2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.uri, self.name)

    @property
    def id_number(self):
        return self._payload['id']

    @property
    def name(self):
        return self._payload['name']

    @property
    def uri(self):
        return self._payload['uri']

    @property
    def columns(self):
        return [BigColumn(pair[0], supported_types.get_type_from_string(pair[1]))
                for pair in self._payload['schema']['columns']]

    def update(self, payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, frame ID mismatch %d when expecting %d" \
                  % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload = payload

