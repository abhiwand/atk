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
from intelanalytics.core.orddict import OrderedDict
from collections import defaultdict
import json

from intelanalytics.core.frame import BigFrame
from intelanalytics.core.column import BigColumn
from intelanalytics.core.files import CsvFile
from intelanalytics.core.iatypes import *
from intelanalytics.core.aggregation import agg
from intelanalytics.rest.connection import http
from intelanalytics.rest.command import CommandRequest, CommandInfo, executor
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
        frame_info = FrameInfo(r.json())
        return BigFrame(frame_info)

    def delete_frame(self, frame):
        logger.info("REST Backend: Delete frame {0}".format(repr(frame)))
        r = self.rest_http.delete("dataframes/" + str(frame._id))
        return r

    def create(self, frame, source, name):
        logger.info("REST Backend: create frame with name %s" % name)
        if isinstance(source, FrameInfo):
            initialize_frame(frame, source)
            return  # early exit here

        new_frame_name = self._create_new_frame(frame, name or self._get_new_frame_name(source))

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

        return new_frame_name

    def _create_new_frame(self, frame, name):
        """create helper method to call http and initialize frame with results"""
        payload = {'name': name }
        r = self.rest_http.post('dataframes', payload)
        logger.info("REST Backend: create frame response: " + r.text)
        frame_info = FrameInfo(r.json())
        initialize_frame(frame, frame_info)
        return frame_info.name

    def get_name(self, frame):
        return self._get_frame_info(frame).name

    def get_schema(self, frame):
        return self._get_frame_info(frame).schema

    def get_repr(self, frame):
        frame_info = self._get_frame_info(frame)
        return "\n".join(['"%s"' % frame_info.name] +
                         ["%s:%s" % (name, data_type)
                          for name, data_type in FrameSchema.from_types_to_strings(frame_info.schema)])

    def _get_frame_info(self, frame):
        response = self.rest_http.get_full_uri(self._get_frame_full_uri(frame))
        return FrameInfo(response.json())

    def _get_frame_full_uri(self, frame):
        return self.rest_http.create_full_uri('dataframes/%d' % frame._id)

    def _get_load_arguments(self, frame, data):
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
                                'uri': self._get_frame_full_uri(data)},  # TODO - be consistent about _id vs. uri in these calls
                    'destination': self._get_frame_full_uri(frame)}
        raise TypeError("Unsupported data source " + type(data).__name__)

    @staticmethod
    def _get_new_frame_name(source=None):
        try:
            annotation = "_" + source.annotation
        except:
            annotation = ''
        return "frame_" + uuid.uuid4().hex + annotation

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

        arguments = {'frame': self._get_frame_full_uri(frame),
                     'column_names': names,
                     'column_types': [supported_types.get_type_string(t) for t in data_types],
                     'expression': http_ready_function}

        return execute_update_frame_command('add_columns', arguments, frame)

    def append(self, frame, data):
        logger.info("REST Backend: append data to frame {0}: {1}".format(frame._id, repr(data)))
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
        arguments = {'frame': self._get_frame_full_uri(frame), 'predicate': http_ready_function}
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
        arguments = {'frame': self._get_frame_full_uri(frame), 'predicate': http_ready_function}
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
        elif isinstance(columns, dict):
            if new_names is not None:
                raise ValueError("'new_names' argument must be None since 'columns' argument is a dictionary, ")
            columns, new_names = zip(*columns.items())

        if new_names is not None:
            if isinstance(new_names, basestring):
                new_names = [new_names]
            if len(columns) != len(new_names):
                raise ValueError("new_names list argument must be the same length as the column_names")
        # TODO - fix REST server to accept nulls, for now we'll pass an empty list
        else:
            new_names = list(columns)
        arguments = {'frame': self._get_frame_full_uri(frame),
                     'projected_frame': self._get_frame_full_uri(projected_frame),
                     'columns': columns,
                     'new_column_names': new_names}
        return execute_update_frame_command('project', arguments, projected_frame)

    def groupby(self, frame, groupby_columns, aggregation):
        if groupby_columns is None:
            groupby_columns = []
        elif isinstance(groupby_columns, basestring):
            groupby_columns = [groupby_columns]

        first_column_name = None
        aggregation_list = []

        for arg in aggregation:
            if arg == agg.count:
                if not first_column_name:
                    first_column_name = frame.column_names[0]  #only make this call once, since it goes to http - TODO, ultimately should be handled server-side
                aggregation_list.append((agg.count, first_column_name, "count"))
            elif isinstance(arg, dict):
                for k,v in arg.iteritems():
                    # leave the valid column check to the server
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
        arguments = {'frame': self._get_frame_full_uri(frame),
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
        current_names = frame.column_names
        for nn in new_names:
            if nn in current_names:
                raise ValueError("Cannot use rename to '{0}' because another column already exists with that name".format(nn))
        arguments = {'frame': frame.uri, "original_names": column_names, "new_names": new_names}
        return execute_update_frame_command('rename_column', arguments, frame)

    def take(self, frame, n, offset):
        r = self.rest_http.get('dataframes/{0}/data?offset={2}&count={1}'.format(frame._id,n, offset))
        return r.json()

    def classification_metric(self, frame, metric_type, label_column, pred_column, pos_label, beta):
        # TODO - remove error handling, leave to server (or move to plugin)
        if metric_type not in ['accuracy', 'precision', 'recall', 'fmeasure']:
            raise ValueError("metric_type must be one of: 'accuracy'")
        if label_column.strip() == "":
            raise ValueError("label_column can not be empty string")
        if pred_column.strip() == "":
            raise ValueError("pred_column can not be empty string")
        if str(pos_label).strip() == "":
            raise ValueError("invalid pos_label")
        schema_dict = dict(frame.schema)
        column_names = schema_dict.keys()
        if not label_column in column_names:
            raise ValueError("label_column does not exist in frame")
        if not pred_column in column_names:
            raise ValueError("pred_column does not exist in frame")
        if supported_types.get_type_string(schema_dict[label_column]) in ['float32', 'float64']:
            raise ValueError("invalid label_column types")
        if supported_types.get_type_string(schema_dict[pred_column]) in ['float32', 'float64']:
            raise ValueError("invalid pred_column types")
        if not beta > 0:
            raise ValueError("invalid beta value for f measure")
        arguments = {'frameId': frame._id, 'metricType': metric_type, 'labelColumn': label_column, 'predColumn': pred_column, 'posLabel': str(pos_label), 'beta': beta}
        return get_command_output('classification_metric', arguments).get('metricValue')
    
    def confusion_matrix(self, frame, label_column, pred_column, pos_label):
        if label_column.strip() == "":
            raise ValueError("label_column can not be empty string")
        schema_dict = dict(frame.schema)
        column_names = schema_dict.keys()
        if not label_column in column_names:
            raise ValueError("label_column does not exist in frame")
        if schema_dict.get(label_column) in ['float32', 'float64']:
            raise ValueError("invalid label_column types")
        if pred_column.strip() == "":
            raise ValueError("pred_column can not be empty string")
        if not pred_column in column_names:
            raise ValueError("pred_column does not exist in frame")
        if schema_dict.get(pred_column) in ['float32', 'float64']:
            raise ValueError("invalid pred_column types")
        if str(pos_label).strip() == "":
            raise ValueError("invalid pos_label")
        arguments = {'frameId': frame._id, 'labelColumn': label_column, 'predColumn': pred_column, 'posLabel': str(pos_label)}
        # valueList = (tp, tn, fp, fn)
        valueList = get_command_output('confusion_matrix', arguments).get('valueList')
        # the following output formatting code is ugly, but it works for now...
        maxLength = len(max((str(x) for x in valueList), key=len))
        topRowLen = max([maxLength*2 - 7, 1])
        formattedMatrix = "\n         " + " " * len(str(pos_label)) + "   " + " Predicted" + " " * topRowLen + "  \n"
        formattedMatrix += "         " + " " * len(str(pos_label)) + "  _pos" + "_" * max([maxLength - 2, 1]) + " _neg" + "_" * max([maxLength - 3, 1]) + "_\n"
        formattedMatrix += "Actual   pos | " + str(valueList[0]) + " " * max([maxLength - len(str(valueList[0])), 0]) + "   " + str(valueList[3]) + " " * max([maxLength - len(str(valueList[3])), 0]) + " \n"
        formattedMatrix += "         neg | " + str(valueList[2]) + " " * max([maxLength - len(str(valueList[2])), 0]) + "   " + str(valueList[1]) + " " * max([maxLength - len(str(valueList[1])), 0]) + " \n"

        return formattedMatrix


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
    def schema(self):
        return FrameSchema.from_strings_to_types(self._payload['schema']['columns'])

    def update(self, payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, frame ID mismatch %d when expecting %d" \
                  % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload = payload


class FrameSchema:
    """
    Conversion functions for schema representations

    The standard schema representation is
    1. A list of (name, data_type) tuples of the form (str, type) [types]

    Other representations include:
    2.  A list tuples of the form (str, str)  --used in JSON      [strings]
    3.  A list of BigColumn objects                               [columns]
    4.  An OrderedDict of name: data_type of the form str: type   [dict]
    """

    @staticmethod
    def from_types_to_strings(s):
        return [(name, supported_types.get_type_string(data_type)) for name, data_type in s]

    @staticmethod
    def from_strings_to_types(s):
        return [(name, supported_types.get_type_from_string(data_type)) for name, data_type in s]

    # Add more if necessary

def initialize_frame(frame, frame_info):
    """Initializes a frame according to given frame_info"""
    frame._id = frame_info.id_number


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


def get_command_output(command_name, arguments):
    """Executes command and returns the output"""
    command_request = CommandRequest('dataframe/' + command_name, arguments)
    command_info = executor.issue(command_request)
    return command_info.result
