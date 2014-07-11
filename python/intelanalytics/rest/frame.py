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
import uuid
import logging
logger = logging.getLogger(__name__)
from ordereddict import OrderedDict
from collections import defaultdict
import json

from intelanalytics.core.frame import BigFrame
from intelanalytics.core.column import BigColumn
from intelanalytics.core.files import CsvFile
from intelanalytics.core.iatypes import *
from intelanalytics.core.aggregation import agg
from intelanalytics.rest.connection import http
from intelanalytics.rest.command import CommandRequest, executor
from intelanalytics.rest.spark import prepare_row_function, get_add_one_column_function, get_add_many_columns_function




class FrameBackendRest(object):
    """REST plumbing for BigFrame"""

    commands_loaded = {}

    def __init__(self, http_methods=None):
        self.rest_http = http_methods or http
        # use global connection, auth, etc.  This client does not support
        # multiple connection contexts
        if not self.__class__.commands_loaded:
            self.__class__.commands_loaded.update(executor.get_command_functions(('dataframe', 'dataframes'),
                                                                        execute_update_frame_command,
                                                                        execute_new_frame_command))
            executor.install_static_methods(self.__class__, self.__class__.commands_loaded)
            BigFrame._commands = self.__class__.commands_loaded

    def get_frame_names(self):
        logger.info("REST Backend: get_frame_names")
        r = self.rest_http.get('dataframes')
        payload = r.json()
        return [f['name'] for f in payload]

    def get_frame(self, name):
        logger.info("REST Backend: get_frame")
        r = self.rest_http.get('dataframes?name='+name)
        payload = r.json()
        return json.dumps(payload, indent=2)

        #raise NotImplemented  # TODO - implement get_frame

    def delete_frame(self, frame):
        logger.info("REST Backend: Delete frame {0}".format(repr(frame)))
        r = self.rest_http.delete("dataframes/" + str(frame._id))
        return r

    def create(self, frame, source, name):
        logger.info("REST Backend: create frame: " + frame.name)
        if isinstance(source, FrameInfo):
            initialize_frame(frame, source)
            return  # early exit here

        frame._name = name or self._get_new_frame_name(source)
        self._create_new_frame(frame)

        # TODO - change project such that server creates the new frame, instead of passing one in
        if isinstance(source, BigFrame):
            self.project_columns(source, frame, source.column_names)
        elif isinstance(source, BigColumn):
            self.project_columns(source.frame, frame, source.name)
        elif isinstance(source, list) and all(isinstance(iter, BigColumn) for iter in source):
            self.project_columns(source[0].frame, frame, [s.name for s in source])
        else:
            if source:
                self.append(frame, source)


    def _create_new_frame(self, frame):
        payload = {'name': frame.name }
        r = self.rest_http.post('dataframes', payload)
        logger.info("REST Backend: create frame response: " + r.text)
        payload = r.json()
        frame._id = payload['id']
        frame._uri = self._get_uri(payload)

    @staticmethod
    def _get_load_arguments(frame, data):
        if isinstance(data, CsvFile):
            return {'destination': frame._id,
                    'source': {
                        "source_type": "file",
                        "uri": data.file_name,
                        "parser":{
                            "name": "builtin/line/separator",
                            "arguments": {
                                "separator": data.delimiter,
                                "skip_rows": data.skip_header_lines,
                                "schema":{
                                    "columns": data._schema_to_json()
                                }
                            }
                        }
                    }
            }
        if isinstance(data, BigFrame):
            return {'source': { 'source_type': 'dataframe',
                                'uri': data.uri},
                    'destination': frame.uri}
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

    @staticmethod
    def _validate_schema(schema):
        for tup in schema:
            if not isinstance(tup[0], basestring):
                raise ValueError("First value in schema tuple must be a string")
            supported_types.validate_is_supported_type(tup[1])

    def add_columns(self, frame, expression, schema):
        if not schema or not hasattr(schema, "__iter__"):
            raise ValueError("add_columns requires a non-empty schema of (name, type)")

        only_one_column = False
        if isinstance(schema[0], basestring):
            only_one_column = True
            schema = [schema]

        self._validate_schema(schema)
        names, data_types = zip(*schema)

        add_columns_function = get_add_one_column_function(expression, data_types[0]) if only_one_column \
            else get_add_many_columns_function(expression, data_types)
        from itertools import imap
        http_ready_function = prepare_row_function(frame, add_columns_function, imap)

        arguments = {'frame': frame.uri,
                     'column_names': names,
                     'column_types': [supported_types.get_type_string(t) for t in data_types],
                     'expression': http_ready_function}

        return execute_update_frame_command('add_columns', arguments, frame)

    def append(self, frame, data):
        logger.info("REST Backend: append data to frame {0}: {1}".format(frame.name, repr(data)))
        # for now, many data sources requires many calls to append
        if isinstance(data, list) or isinstance(data, tuple):
            for d in data:
                self.append(frame, d)
            return

        arguments = self._get_load_arguments(frame, data)
        execute_update_frame_command("load", arguments, frame)

    def count(self, frame):
        raise NotImplementedError  # TODO - implement count

    def drop(self, frame, predicate):
        from itertools import ifilterfalse  # use the REST API filter, with a ifilterfalse iterator
        http_ready_function = prepare_row_function(frame, predicate, ifilterfalse)
        arguments = {'frame': frame.uri, 'predicate': http_ready_function}
        return execute_update_frame_command("filter", arguments, frame)

    def drop_duplicates(self, frame, columns):
        if isinstance(columns, basestring):
            columns = [columns]
        arguments = {'frameId': frame._id, "unique_columns": columns}
        command = CommandRequest("dataframe/drop_duplicates", arguments)
        executor.issue(command)

    def filter(self, frame, predicate):
        from itertools import ifilter
        http_ready_function = prepare_row_function(frame, predicate, ifilter)
        arguments = {'frame': frame.uri, 'predicate': http_ready_function}
        return execute_update_frame_command("filter", arguments, frame)

    def flatten_column(self, frame, column_name):
        name = self._get_new_frame_name()
        arguments = {'name': name, 'frameId': frame._id, 'column': column_name, 'separator': ',' }
        return execute_new_frame_command('flattenColumn', arguments)

    def bin_column(self, frame, column_name, num_bins, bin_type='equalwidth', bin_column_name='binned'):
        import numpy as np
        if num_bins < 1:
            raise ValueError("num_bins must be at least 1")
        if not bin_type in ['equalwidth', 'equaldepth']:
            raise ValueError("bin_type must be one of: equalwidth, equaldepth")
        if bin_column_name.strip() == "":
            raise ValueError("bin_column_name can not be empty string")
        colTypes = dict(frame.schema)
        if not colTypes[column_name] in [np.float32, np.float64, np.int32, np.int64]:
            raise ValueError("unable to bin non-numeric values")
        name = self._get_new_frame_name()
        arguments = {'name': name, 'frame': frame._id, 'columnName': column_name, 'numBins': num_bins, 'binType': bin_type, 'binColumnName': bin_column_name}
        return execute_new_frame_command('binColumn', arguments)

    class InspectionTable(object):
        """
        Inline class used specifically for inspect, where the __repr__ is king
        """
        _align = defaultdict(lambda: 'c')  # 'l', 'c', 'r'
        _align.update([(bool, 'r'),
                       (bytearray, 'l'),
                       (dict, 'l'),
                       (float32, 'r'),
                       (float64, 'r'),
                       (int32, 'r'),
                       (int64, 'r'),
                       (list, 'l'),
                       (unicode, 'l'),
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

         #def _repr_html_(self): TODO - Add this method for ipython notebooks

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
        return execute_new_frame_command('join', arguments)

    def project_columns(self, frame, projected_frame, columns, new_names=None):
        # TODO - change project_columns so the server creates the new frame, like join
        if isinstance(columns, basestring):
            columns = [columns]
        if new_names is not None:
            if isinstance(new_names, basestring):
                new_names = [new_names]
            if len(columns) != len(new_names):
                raise ValueError("new_names list argument must be the same length as the column_names")
        # TODO - create a general method to validate lists of column names, such that they exist, are all from the same frame, and not duplicated
        # TODO - fix REST server to accept nulls, for now we'll pass an empty list
        else:
            new_names = list(columns)
        arguments = {'frame': frame.uri, 'projected_frame': projected_frame.uri, 'columns': columns, "new_column_names": new_names}
        return execute_update_frame_command('project', arguments, projected_frame)

    def groupby(self, frame, groupby_columns, aggregation):
        if groupby_columns is None:
            groupby_columns = []
        elif isinstance(groupby_columns, basestring):
            groupby_columns = [groupby_columns]

        aggregation_list = []
        for arg in aggregation:
            if arg == agg.count:
                aggregation_list.append((agg.count, frame.column_names[0], "count"))
            elif isinstance(arg, dict):
                for k,v in arg.iteritems():
                    if k not in frame._columns:
                        raise ValueError("%s is not a valid column name for this frame" % k)
                    if isinstance(v, list) or isinstance(v, tuple):
                        for j in v:
                            if j not in agg:
                                raise ValueError("%s is not a valid aggregation function, like agg.max.  Supported agg methods: %s" % (j, agg))
                            aggregation_list.append((j, k, "%s_%s" % (k, j)))
                    else:
                        aggregation_list.append((v, k, "%s_%s" % (k, v)))
            else:
                raise TypeError("Bad type %s provided in aggregation arguments; expecting an aggregation function or a dictionary of column_name:[func]" % type(arg))

        name = self._get_new_frame_name()
        arguments = {'frame': frame.uri,
                     'name': name,
                     'group_by_columns': groupby_columns,
                     'aggregations': aggregation_list}

        return execute_new_frame_command("groupby", arguments)

    # def remove_columns(self, frame, name):
    #     columns = ",".join(name) if isinstance(name, list) else name
    #     arguments = {'frame': frame.uri, 'column': columns}
    #     return execute_update_frame_command('removecolumn', arguments, frame)

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
        arguments = {'frame': frame.uri, "original_names": column_names, "new_names": new_names}
        return execute_update_frame_command('rename_column', arguments, frame)

    # def rename_frame(self, frame, name):
    #     r= self.rest_http.get('dataframes')
    #     payload = r.json()
    #     frame_names = [f['name'] for f in payload]
    #     for i in frame_names:
    #         if name==i:
    #             raise ValueError("A frame with this name already exists. Rename failed")
    #
    #         else:
    #             arguments = {'frame': frame.uri, "new_name": name}
    #             return execute_update_frame_command('rename_frame', arguments, frame)

    def take(self, frame, n, offset):
        r = self.rest_http.get('dataframes/{0}/data?offset={2}&count={1}'.format(frame._id,n, offset))
        return r.json()

    def classification_metric(self, frame, metric_type, label_column, pred_column, pos_label, beta):
        if metric_type not in ['accuracy', 'precision', 'recall', 'fmeasure']:
            raise ValueError("metric_type must be one of: 'accuracy'")
        if label_column.strip() == "":
            raise ValueError("label_column can not be empty string")
        if pred_column.strip() == "":
            raise ValueError("pred_column can not be empty string")
        if str(pos_label).strip() == "":
            raise ValueError("invalid pos_label")
        if not label_column in frame.column_names:
            raise ValueError("label_column does not exist in frame")
        if not pred_column in frame.column_names:
            raise ValueError("pred_column does not exist in frame")
        if dict(frame.schema).get(label_column) in ['float32', 'float64']:
            raise ValueError("invalid label_column types")
        if dict(frame.schema).get(pred_column) in ['float32', 'float64']:
            raise ValueError("invalid pred_column types")
        if not beta > 0:
            raise ValueError("invalid beta value for f measure")

        arguments = {'frameId': frame._id, 'metricType': metric_type, 'labelColumn': label_column, 'predColumn': pred_column, 'posLabel': str(pos_label), 'beta': beta}
        return get_command_output_value('classification_metric', arguments).get('metricValue')


class FrameInfo(object):
    """
    JSON-based Server description of a BigFrame
    """
    def __init__(self, frame_json_payload):
        self._payload = frame_json_payload

    def __repr__(self):
        return json.dumps(self._payload, indent=2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.id_number, self.name)

    @property
    def id_number(self):
        return self._payload['id']

    @property
    def name(self):
        return self._payload['name']

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


def initialize_frame(frame, frame_info):
    """Initializes a frame according to given frame_info"""
    frame._id = frame_info.id_number
    # TODO - update uri from result (this is a TODO in the engine)
    frame._uri = http._get_uri("dataframes/" + str(frame._id))
    frame._name = frame_info.name
    frame._columns.clear()
    for column in frame_info.columns:
        FrameBackendRest._accept_column(frame, column)


def execute_update_frame_command(command_name, arguments, frame):
    """Executes command and updates frame with server response"""
    #support for non-plugin methods that may not supply the full name
    if not command_name.startswith('dataframe'):
        command_name = 'dataframe/' + command_name
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    if command_info.result.has_key('name') and command_info.result.has_key('schema'):
        initialize_frame(frame, FrameInfo(command_info.result))
        return None
    return command_info.result


def execute_new_frame_command(command_name, arguments):
    """Executes command and creates a new BigFrame object from server response"""
    #support for non-plugin methods that may not supply the full name
    if not command_name.startswith('dataframe'):
        command_name = 'dataframe/' + command_name
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    frame_info = FrameInfo(command_info.result)
    return BigFrame(frame_info)

def get_command_output_value(command_name, arguments):
    """Executes command and returns the computed value"""
    command_request = CommandRequest('dataframe/' + command_name, arguments)
    command_info = executor.issue(command_request)
    return command_info.result
