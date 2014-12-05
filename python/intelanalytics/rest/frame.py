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
from collections import defaultdict, namedtuple
import json
import sys
import pandas
import numpy as np
import intelanalytics.rest.config as config

from intelanalytics.core.frame import Frame
from intelanalytics.core.iapandas import Pandas
from intelanalytics.core.column import Column
from intelanalytics.core.files import CsvFile, LineFile
from intelanalytics.core.iatypes import *
from intelanalytics.core.aggregation import agg

from intelanalytics.rest.connection import http
from intelanalytics.rest.iatypes import get_data_type_from_rest_str, get_rest_str_from_data_type
from intelanalytics.rest.command import CommandRequest, executor
from intelanalytics.rest.spark import prepare_row_function, get_add_one_column_function, get_add_many_columns_function

TakeResult = namedtuple("TakeResult", ['data', 'schema'])
"""
Take result contains data and schema.
data contains only columns based on user specified columns
schema contains only columns baed on user specified columns
the data type under schema is also coverted to IA types
"""


class FrameBackendRest(object):
    """REST plumbing for Frame"""

    def __init__(self, http_methods=None):
        self.rest_http = http_methods or http

    def get_frame_by_id(self, id):
        logger.info("REST Backend: get_frame_by_id")
        if id is None:
            return None
        else:
            r = self.rest_http.get('frames/' + str(id))
            payload = r.json()
            frame = Frame(source=payload)
            return frame

    def create(self, frame, source, name):
        logger.info("REST Backend: create frame with name %s" % name)
        if isinstance(source, dict):
            source = FrameInfo(source)
        if isinstance(source, FrameInfo):
            initialize_frame(frame, source)
        elif isinstance(source, Frame):
            become_frame(frame, source.copy(name=name))
        elif isinstance(source, Column):
            become_frame(frame, source.frame.copy(columns=source.name, name=name))
        elif isinstance(source, list) and all(isinstance(iter, Column) for iter in source):
            become_frame(frame, source[0].frame.copy(columns=[s.name for s in source], name=name))
        else:
            payload = {'name': name }
            r = self.rest_http.post('frames', payload)
            logger.info("REST Backend: create frame response: " + r.text)
            frame_info = FrameInfo(r.json())
            initialize_frame(frame, frame_info)
            if source:
                try:
                    self.append(frame, source)
                except Exception:
                    self.rest_http.delete("frames/%s" % frame_info.id_number)
                    raise
            return frame_info.name
        return frame.name

        return new_frame_name

    def _create_new_frame(self, frame, name):
        """create helper method to call http and initialize frame with results"""
        payload = {'name': name }
        r = self.rest_http.post('frames', payload)
        logger.info("REST Backend: create frame response: " + r.text)
        frame_info = FrameInfo(r.json())
        initialize_frame(frame, frame_info)
        return frame_info.name

    def get_name(self, frame):
        return self._get_frame_info(frame).name

    def get_schema(self, frame):
        return self._get_frame_info(frame).schema

    def get_row_count(self, frame, where):
        if not where:
            return self._get_frame_info(frame).row_count
        # slightly faster generator to only return a list of one item, since we're just counting rows
        # TODO - there's got to be a better way to do this with the RDDs, trick is with Python.
        def icountwhere(predicate, iterable):
           return ("[1]" for item in iterable if predicate(item))
        http_ready_function = prepare_row_function(frame, where, icountwhere)
        arguments = {'frame': self.get_ia_uri(frame), 'where': http_ready_function}
        return executor.get_command_output("frame", "count_where", arguments)

    def get_ia_uri(self, frame):
        return self._get_frame_info(frame).ia_uri

    def get_repr(self, frame):
        frame_info = self._get_frame_info(frame)
        frame_type = "VertexFrame" if frame_info._has_vertex_schema() else "EdgeFrame" if frame_info._has_edge_schema() else "Frame"

        if frame_info._has_vertex_schema():
            frame_type = "VertexFrame"
            graph_data = "\nLabel = %s" % frame_info.label
        elif frame_info._has_edge_schema():
            frame_type = "EdgeFrame"
            graph_data = "\nLabel = %s\nSource Vertex Label = %s\nDestination Vertex Label = %s\nDirected = %s" % (
                frame_info.label, frame_info.src_vertex_label, frame_info.dest_vertex_label, frame_info.directed)
        else:
            frame_type = "Frame"
            graph_data = ""
        return "\n".join(['%s "%s"%s\nrow_count = %d\nschema = ' %
                          (frame_type, frame_info.name, graph_data,  frame_info.row_count)] +
                         ["  %s:%s" % (name, data_type)
                          for name, data_type in FrameSchema.from_types_to_strings(frame_info.schema)])

    def _get_frame_info(self, frame):
        response = self.rest_http.get_full_uri(self._get_frame_full_uri(frame))
        return FrameInfo(response.json())

    def _get_frame_full_uri(self, frame):
        return self.rest_http.create_full_uri('frames/%d' % frame._id)

    def _get_load_arguments(self, frame, source, data=None):
        if isinstance(source, CsvFile):
            return {'destination': frame._id,
                    'source': {
                        "source_type": "file",
                        "uri": source.file_name,
                        "parser":{
                            "name": "builtin/line/separator",
                            "arguments": {
                                "separator": source.delimiter,
                                "skip_rows": source.skip_header_lines,
                                "schema":{
                                    "columns": source._schema_to_json()
                                }
                            }
                        },
                        "data": None
                    }
            }
        if isinstance( source, LineFile):
            return {'destination': frame._id,
                    'source': {"source_type": "linefile",
                            "uri": source.file_name,
                            "parser": {"name": "invalid",
                                        "arguments":{"separator": ',',
                                                    "skip_rows": 0,
                                                    "schema": {"columns": [("data_lines",valid_data_types.to_string(str))]}
                                        }
                                },
                                "data": None
                                },
                    }
        if isinstance(source, Pandas):
            return{'destination': frame._id,
                   'source': {"source_type": "strings",
                              "uri": "pandas",
                              "parser": {"name": "builtin/upload",
                                         "arguments": { "separator": ',',
                                                       "skip_rows": 0,
                                                       "schema":{ "columns": source._schema_to_json()
                                    }
                                }
                              },
                              "data": data
                   }
            }
        if isinstance(source, Frame):
            return {'source': { 'source_type': 'frame',
                                'uri': str(source._id)},  # TODO - be consistent about _id vs. uri in these calls
                    'destination': frame._id}
        raise TypeError("Unsupported data source %s" % type(source))

    @staticmethod
    def _get_new_frame_name(source=None):
        try:
            annotation = "_" + source.annotation
        except:
            annotation = ''
        return "frame_" + uuid.uuid4().hex + annotation

    @staticmethod
    def _format_schema(schema):
        formatted_schema = []
        for name, data_type in schema:
            if not isinstance(name, basestring):
                raise ValueError("First value in schema tuple must be a string")
            formatted_data_type = valid_data_types.get_from_type(data_type)
            formatted_schema.append((name, formatted_data_type))
        return formatted_schema

    def add_columns(self, frame, expression, schema):
        if not schema or not hasattr(schema, "__iter__"):
            raise ValueError("add_columns requires a non-empty schema of (name, type)")

        only_one_column = False
        if isinstance(schema[0], basestring):
            only_one_column = True
            schema = [schema]

        schema = self._format_schema(schema)
        names, data_types = zip(*schema)

        add_columns_function = get_add_one_column_function(expression, data_types[0]) if only_one_column \
            else get_add_many_columns_function(expression, data_types)
        from itertools import imap
        http_ready_function = prepare_row_function(frame, add_columns_function, imap)

        arguments = {'frame': self.get_ia_uri(frame),
                     'column_names': names,
                     'column_types': [get_rest_str_from_data_type(t) for t in data_types],
                     'expression': http_ready_function}

        execute_update_frame_command('add_columns', arguments, frame)

    @staticmethod
    def _handle_error(result):
        if result and result.has_key("error_frame_id"):
            sys.stderr.write("There were parse errors during load, please see frame.get_error_frame()\n")
            logger.warn("There were parse errors during load, please see frame.get_error_frame()")

    def append(self, frame, data):
        logger.info("REST Backend: append data to frame {0}: {1}".format(frame._id, repr(data)))
        # for now, many data sources requires many calls to append
        if isinstance(data, list) or isinstance(data, tuple):
            for d in data:
                self.append(frame, d)
            return

        if isinstance(data, Pandas):
            pan = data.pandas_frame
            if not data.row_index:
                pan = pan.reset_index()
            pan = pan.dropna(thresh=len(pan.columns))
            #number of columns should match the number of columns in the schema, else throw an error
            if len(pan.columns) != len(data.field_names):
                raise ValueError("Number of columns in Pandasframe {0} does not match the number of columns in the "
                                 " schema provided {1}.".format(len(pan.columns), len(data.field_names)))

            begin_index = 0
            iteration = 1
            end_index = config.upload_defaults.rows
            while True:
                pandas_rows = pan[begin_index:end_index].values.tolist()
                arguments = self._get_load_arguments(frame, data, pandas_rows)
                result = execute_update_frame_command("frame:/load", arguments, frame)
                self._handle_error(result)
                if end_index > len(pan.index):
                    break
                iteration += 1
                begin_index = end_index
                end_index = config.upload_defaults.rows * iteration
        else:
            arguments = self._get_load_arguments(frame, data)
            result = execute_update_frame_command("frame:/load", arguments, frame)
            self._handle_error(result)

    def drop(self, frame, predicate):
        from itertools import ifilterfalse  # use the REST API filter, with a ifilterfalse iterator
        http_ready_function = prepare_row_function(frame, predicate, ifilterfalse)
        arguments = {'frame': self.get_ia_uri(frame), 'predicate': http_ready_function}
        execute_update_frame_command("frame:/filter", arguments, frame)

    def filter(self, frame, predicate):
        from itertools import ifilter
        http_ready_function = prepare_row_function(frame, predicate, ifilter)
        arguments = {'frame': self.get_ia_uri(frame), 'predicate': http_ready_function}
        execute_update_frame_command("frame:/filter", arguments, frame)

    def filter_vertices(self, frame, predicate, keep_matching_vertices = True):
        from itertools import ifilter
        from itertools import ifilterfalse

        if(keep_matching_vertices):
            http_ready_function = prepare_row_function(frame, predicate, ifilter)
        else:
            http_ready_function = prepare_row_function(frame, predicate, ifilterfalse)

        arguments = {'frame_id': frame._id, 'predicate': http_ready_function}
        execute_update_frame_command("frame:vertex/filter", arguments, frame)

    def flatten_column(self, frame, column_name):
        name = self._get_new_frame_name()
        arguments = {'name': name, 'frame_id': frame._id, 'column': column_name, 'separator': ',' }
        return execute_new_frame_command('frame:/flatten_column', arguments)

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
        arguments = {'frame': frame._id, 'column_name': column_name, 'num_bins': num_bins, 'bin_type': bin_type, 'bin_column_name': bin_column_name}
        return execute_update_frame_command('bin_column', arguments, frame)


    def column_statistic(self, frame, column_name, multiplier_column_name, operation):
        import numpy as np
        colTypes = dict(frame.schema)
        if not colTypes[column_name] in [np.float32, np.float64, np.int32, np.int64]:
            raise ValueError("unable to take statistics of non-numeric values")
        arguments = {'columnName': column_name, 'multiplierColumnName' : multiplier_column_name,
                     'operation' : operation}
        return execute_update_frame_command('columnStatistic', arguments, frame)

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
            fields = OrderedDict([("{0}:{1}".format(key, valid_data_types.to_string(val)), self._align[val]) for key, val in self.schema])
            table.field_names = fields.keys()
            table.align.update(fields)
            table.hrules = prettytable.HEADER
            table.vrules = prettytable.NONE
            for r in self.rows:
                table.add_row(r)
            return table.get_string().encode('utf8','replace')

         #def _repr_html_(self): TODO - Add this method for ipython notebooks

    def inspect(self, frame, n, offset, selected_columns):
        # inspect is just a pretty-print of take, we'll do it on the client
        # side until there's a good reason not to
        result = self.take(frame, n, offset, selected_columns)
        data = result.data
        schema = result.schema

        return FrameBackendRest.InspectionTable(schema, data)

    def join(self, left, right, left_on, right_on, how):
        if right_on is None:
            right_on = left_on
        name = self._get_new_frame_name()
        arguments = {'name': name, "how": how, "frames": [[left._id, left_on], [right._id, right_on]] }
        return execute_new_frame_command('frame:/join', arguments)

    def copy(self, frame, columns=None, where=None, name=None):
        if where:
            if not columns:
                column_names = frame.column_names
            elif isinstance(columns, basestring):
                column_names = [columns]
            elif isinstance(columns, dict):
                column_names = columns.keys()
            else:
                column_names = columns
            from intelanalytics.rest.spark import prepare_row_function_for_copy_columns
            where = prepare_row_function_for_copy_columns(frame, where, column_names)
        arguments = {'frame': self.get_ia_uri(frame),
                     'columns': columns,
                     'where': where,
                     'name': name}
        return execute_new_frame_command('frame/copy', arguments)

    def group_by(self, frame, group_by_columns, aggregation):
        if group_by_columns is None:
            group_by_columns = []
        elif isinstance(group_by_columns, basestring):
            group_by_columns = [group_by_columns]

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

        arguments = {'frame': self.get_ia_uri(frame),
                     'name': "group_by_" + self._get_new_frame_name(),
                     'group_by_columns': group_by_columns,
                     'aggregations': aggregation_list}

        return execute_new_frame_command("group_by", arguments)

    def rename_columns(self, frame, column_names):
        if not isinstance(column_names, dict):
            raise ValueError("rename_columns requires a dictionary of string pairs")
        new_names = column_names.values()
        column_names = column_names.keys()

        arguments = {'frame': frame._id, "original_names": column_names, "new_names": new_names}
        execute_update_frame_command('rename_columns', arguments, frame)

    def sort(self, frame, columns, ascending):
        if isinstance(columns, basestring):
            columns_and_ascending = [(columns, ascending)]
        elif isinstance(columns, list):
            if isinstance(columns[0], basestring):
                columns_and_ascending = map(lambda x: (x, ascending),columns)
            else:
                columns_and_ascending = columns
        else:
            raise ValueError("Bad type %s provided as argument; expecting basestring, list of basestring, or list of tuples" % type(columns))
        arguments = { 'frame': frame._id, 'column_names_and_ascending': columns_and_ascending }
        execute_update_frame_command("sort", arguments, frame)

    def take(self, frame, n, offset, columns):
        def get_take_result():
            data = []
            schema = None
            while len(data) < n:
                url = 'frames/{0}/data?offset={2}&count={1}'.format(frame._id,n + len(data), offset + len(data))
                result = executor.query(url)
                if not schema:
                    schema = result.schema
                if len(result.data) == 0:
                    break
                data.extend(result.data)
            return TakeResult(data, schema)
        if n < 0:
            raise ValueError("Count value needs to be positive. Provided %s" % n)

        if n == 0:
            return TakeResult([], frame.schema)
        result = get_take_result()

        schema = FrameSchema.from_strings_to_types(result.schema)

        if isinstance(columns, basestring):
            columns = [columns]

        updated_schema = schema
        if columns is not None:
            updated_schema = FrameSchema.get_schema_for_columns(schema, columns)
            indices = FrameSchema.get_indices_for_selected_columns(schema, columns)

        data = result.data
        if columns is not None:
            data = FrameData.extract_data_from_selected_columns(data, indices)

        return TakeResult(data, updated_schema)

    def create_vertex_frame(self, frame, source, label, graph):
        logger.info("REST Backend: create vertex_frame with label %s" % label)
        if isinstance(source, dict):
            source = FrameInfo(source)
        if isinstance(source, FrameInfo):
            self.initialize_graph_frame(frame, source, graph)
            return frame.name  # early exit here

        graph.define_vertex_type(label)
        source_frame = graph.vertices[label]
        self.copy_graph_frame(source_frame, frame)
        return source_frame.name

    def create_edge_frame(self, frame, source, label, graph, src_vertex_label, dest_vertex_label, directed):
        logger.info("REST Backend: create vertex_frame with label %s" % label)
        if isinstance(source, dict):
            source = FrameInfo(source)
        if isinstance(source, FrameInfo):
            self.initialize_graph_frame(frame, source, graph)
            return frame.name  # early exit here

        graph.define_edge_type(label, src_vertex_label, dest_vertex_label, directed)
        source_frame = graph.edges[label]
        self.copy_graph_frame(source_frame, frame)
        return source_frame.name

    def initialize_graph_frame(self, frame, frame_info, graph):
        """Initializes a frame according to given frame_info associated with a graph"""
        frame._ia_uri = frame_info.ia_uri
        frame._id = frame_info.id_number
        frame._error_frame_id = frame_info.error_frame_id
        frame._label = frame_info.label
        frame._graph = graph

    def copy_graph_frame(self, source, target):
        """Initializes a frame from another frame associated with a graph"""
        target._ia_uri = source._ia_uri
        target._id = source._id
        target._error_frame_id = source._error_frame_id
        target._label = source._label
        target._graph = source._graph



class FrameInfo(object):
    """
    JSON-based Server description of a Frame
    """
    def __init__(self, frame_json_payload):
        self._payload = frame_json_payload
        self._validate()

    def __repr__(self):
        return json.dumps(self._payload, indent=2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.id_number, self.name)
    
    def _validate(self):
        try:
            assert self.id_number
        except KeyError:
            raise RuntimeError("Invalid response from server. Expected Frame info.")

    @property
    def id_number(self):
        return self._payload['id']
    
    @property
    def name(self):
        return self._payload['name']
    
    @property
    def ia_uri(self):
        return self._payload['ia_uri']

    @property
    def schema(self):
        return FrameSchema.from_strings_to_types(self._payload['schema']['columns'])

    @property
    def row_count(self):
        return int(self._payload['row_count'])

    @property
    def error_frame_id(self):
        try:
            return self._payload['error_frame_id']
        except:
            return None

    @property
    def label(self):
        try:
            schema = self._payload['schema']
            if self._has_vertex_schema():
                return schema['vertex_schema']['label']
            if self._has_edge_schema():
                return schema['edge_schema']['label']
            return None
        except:
            return None

    @property
    def directed(self):
        if not self._has_edge_schema():
            return None
        try:
            return self._payload['schema']['edge_schema']['directed']
        except:
            return None

    @property
    def dest_vertex_label(self):
        if not self._has_edge_schema():
            return None
        try:
            return self._payload['schema']['edge_schema']['dest_vertex_label']
        except:
            return None

    @property
    def src_vertex_label(self):
        if not self._has_edge_schema():
            return None
        try:
            return self._payload['schema']['edge_schema']['src_vertex_label']
        except:
            return None

    def _has_edge_schema(self):
        return "edge_schema" in self._payload['schema']

    def _has_vertex_schema(self):
        return "vertex_schema" in self._payload['schema']


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
        return [(name, get_rest_str_from_data_type(data_type)) for name, data_type in s]

    @staticmethod
    def from_strings_to_types(s):
        return [(column['name'], get_data_type_from_rest_str(column['data_type'])) for column in s]

    @staticmethod
    def get_schema_for_columns(schema, selected_columns):
        indices = FrameSchema.get_indices_for_selected_columns(schema, selected_columns)
        return [schema[i] for i in indices]

    @staticmethod
    def get_indices_for_selected_columns(schema, selected_columns):
        indices = []
        for selected in selected_columns:
            for column in schema:
                if column[0] == selected:
                    indices.append(schema.index(column))
                    break

        return indices


class FrameData:

    @staticmethod
    def extract_data_from_selected_columns(data_in_page, indices):
        new_data = []
        for row in data_in_page:
            new_data.append([row[index] for index in indices])

        return new_data

def initialize_frame(frame, frame_info):
    """Initializes a frame according to given frame_info"""
    frame._ia_uri = frame_info.ia_uri
    frame._id = frame_info.id_number
    frame._error_frame_id = frame_info.error_frame_id

def become_frame(frame, source_frame):
    """Initializes a frame proxy according to another frame proxy"""
    frame._ia_uri = source_frame._ia_uri
    frame._id = source_frame._id
    frame._error_frame_id = source_frame._error_frame_id

def execute_update_frame_command(command_name, arguments, frame):
    """Executes command and updates frame with server response"""
    #support for non-plugin methods that may not supply the full name
    if not command_name.startswith('frame'):
        command_name = 'frame/' + command_name
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    if command_info.result.has_key('name') and command_info.result.has_key('schema'):
        initialize_frame(frame, FrameInfo(command_info.result))
        return None
    if (command_info.result.has_key('value') and len(command_info.result) == 1):
        return command_info.result.get('value')
    return command_info.result


def execute_new_frame_command(command_name, arguments):
    """Executes command and creates a new Frame object from server response"""
    #support for non-plugin methods that may not supply the full name
    if not command_name.startswith('frame'):
        command_name = 'frame/' + command_name
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    frame_info = FrameInfo(command_info.result)
    return Frame(frame_info)



