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
REST backend for graphs
"""
import json
import logging
import uuid

logger = logging.getLogger(__name__)

from intelanalytics.core.graph import VertexRule, EdgeRule, BigGraph, Rule
from intelanalytics.core.column import BigColumn
from intelanalytics.rest.connection import http
from intelanalytics.rest.command import CommandRequest, executor


def execute_update_graph_command(command_name, arguments, graph):
    #support for non-plugin methods that may not supply the full name
    if not command_name.startswith('graph'):
        command_name = 'graph/' + command_name
    command = CommandRequest(command_name, arguments=arguments)
    command_info = executor.issue(command)
    if (command_info.result.has_key('value') and len(command_info.result) == 1):
        return command_info.result.get('value')
    else:
        return command_info.result

execute_new_graph_command = execute_update_graph_command


def initialize_graph(graph, graph_info):
    """Initializes a graph according to given graph_info"""
    graph._id = graph_info.id_number
    graph._name = graph_info.name
    graph._ia_uri = graph_info.ia_uri
    graph._uri= http.create_full_uri("graphs/"+ str(graph._id))
    return graph

class GraphBackendRest(object):

    commands_loaded = {}

    def __init__(self, http_methods = None):
        self.rest_http = http_methods or http
        if not self.__class__.commands_loaded:
            self.__class__.commands_loaded.update(executor.get_command_functions(('graph', 'graphs'),
                                                                                 execute_update_graph_command,
                                                                                execute_new_graph_command))
            executor.install_static_methods(self.__class__, self.__class__.commands_loaded)
            BigGraph._commands = self.__class__.commands_loaded

    def get_graph_names(self):
        logger.info("REST Backend: get_graph_names")
        r = self.rest_http.get('graphs')
        payload = r.json()
        return [f['name'] for f in payload]

    def get_graph(self,name):
        logger.info("REST Backend: get_graph")
        r = self.rest_http.get('graphs?name='+name)
        graph_info = GraphInfo(r.json())
        return BigGraph(graph_info)

    def delete_graph(self,graph):
        if isinstance(graph,BigGraph):
            return self._delete_graph(graph)
        elif isinstance(graph, basestring):
            #delete by name
            return self._delete_graph(self.get_graph(graph))
        else:
            raise TypeError('Expected argument of type BigGraph or the graph name')

    def _delete_graph(self, graph):
        logger.info("REST Backend: Delete graph {0}".format(repr(graph)))
        r=self.rest_http.delete("graphs/"+str(graph._id))
        return None

    def create(self, graph,rules,name):
        logger.info("REST Backend: create graph with name %s: " % name)
        if isinstance(rules, GraphInfo):
            initialize_graph(graph,rules)
            return  # Early exit here
        new_graph_name=self._create_new_graph(graph,rules,name or self._get_new_graph_name(rules))
        return new_graph_name

    def _create_new_graph(self, graph, rules, name):
        if rules and (not isinstance(rules, list) or not all([isinstance(rule, Rule) for rule in rules])):
            raise TypeError("rules must be a list of Rule objects")
        else:
            payload = {'name': name}
            r=self.rest_http.post('graphs', payload)
            logger.info("REST Backend: create graph response: " + r.text)
            graph_info = GraphInfo(r.json())
            initialized_graph=initialize_graph(graph,graph_info)
            if rules:
                frame_rules = JsonRules(rules)
                if logger.level == logging.DEBUG:
                    import json
                    payload_json = json.dumps(frame_rules, indent=2, sort_keys=True)
                    logger.debug("REST Backend: create graph payload: " + payload_json)
                self.load(initialized_graph,frame_rules, append= False)
            return graph_info.name
    
    def _get_new_graph_name(self,source=None):
        try:
            annotation ="_" + source.annotation
        except:
            annotation= ''
        return "graph_" + uuid.uuid4().hex + annotation

    def rename_graph(self, graph, name):
        arguments = {'graph': self._get_graph_full_uri(graph), "new name": name}
        execute_update_graph_command('rename_graph', arguments,graph)

    def get_name(self, graph):
        return self._get_graph_info(graph).name

    def get_ia_uri(self, graph):
        return self._get_graph_info(graph).ia_uri

    def get_repr(self, graph):
        graph_info = self._get_graph_info(graph)
        return "\n".join(['BigGraph "%s"' % (graph_info.name)])

    def _get_graph_info(self, graph):
        response = self.rest_http.get_full_uri(self._get_graph_full_uri(graph))
        return GraphInfo(response.json())

    def _get_graph_full_uri(self,graph):
        return self.rest_http.create_full_uri('graphs/%d' % graph._id)

    def append(self, graph, rules):
        logger.info("REST Backend: append_frame graph: " + graph.name)
        frame_rules = JsonRules(rules)
        self.load(graph, frame_rules, append=True)

    # def _get_uri(self, payload):
    #     links = payload['links']
    #     for link in links:
    #         if link['rel'] == 'self':
    #             return link['uri']
    #     return "we don't know"
    #     # TODO - bring exception back
    #     #raise Exception('Unable to find uri for graph')

    def als(self, graph, *args, **kwargs):
        logger.info("REST Backend: run als on graph " + graph.name)
        payload = JsonAlsPayload(graph, *args, **kwargs)
        if logger.level == logging.DEBUG:
            import json
            payload_json =  json.dumps(payload, indent=2, sort_keys=True)
            logger.debug("REST Backend: run als payload: " + payload_json)
        r = http.post('commands', payload)
        logger.debug("REST Backend: run als response: " + r.text)

    def recommend(self, graph, vertex_id):
        logger.info("REST Backend: als query on graph " + graph.name)
        cmd_format ='graphs/{0}/vertices?qname=ALSQuery&offset=0&count=10&vertexID={1}'
        cmd = cmd_format.format(graph._id, vertex_id)
        logger.debug("REST Backend: als query cmd: " + cmd)
        r = http.get(cmd)
        json = r.json()
        logger.debug("REST Backend: run als response: " + json)
        return json


class JsonAlsPayload(object):
    def __new__(cls,
                graph,
                input_edge_property_list,
                input_edge_label,
                output_vertex_property_list,
                vertex_type,
                edge_type):
        return {
            "name": "graph/ml/als",
            "arguments" : {
                "graph": graph.uri,
                "lambda": 0.1,
                "max_supersteps": 20,
                "converge_threshold": 0,
                "feature_dimension": 1,
                "input_edge_property_list": input_edge_property_list,
                "input_edge_label": input_edge_label,
                "output_vertex_property_list": output_vertex_property_list,
                "vertex_type": vertex_type,
                "edge_type": edge_type,
            }
        }


# GB JSON Payload objects:

class JsonValue(object):
    def __new__(cls, value):
        if isinstance(value, basestring):
            t, v = "CONSTANT", value
        elif isinstance(value, BigColumn):
            t, v = "VARYING", value.name
        else:
            raise TypeError("Bad graph element source type")
        return {"source": t, "value": v}


class JsonProperty(object):
    def __new__(cls, key, value):
        return {'key': key, 'value': value}


class JsonVertexRule(object):
    def __new__(cls, rule):
        return {'id': JsonProperty(JsonValue(rule.id_key), JsonValue(rule.id_value)),
                'properties': [JsonProperty(JsonValue(k), JsonValue(v))
                               for k, v in rule.properties.items()]}


class JsonEdgeRule(object):
    def __new__(cls, rule):
        return {'label': JsonValue(rule.label),
                'tail': JsonProperty(JsonValue(rule.tail.id_key), JsonValue(rule.tail.id_value)),
                'head': JsonProperty(JsonValue(rule.head.id_key), JsonValue(rule.head.id_value)),
                'properties': [JsonProperty(JsonValue(k), JsonValue(v))
                               for k, v in rule.properties.items()],
                'bidirectional': not rule.is_directed}


class JsonFrame(object):
    def __new__(cls, frame_uri):
        return {'frame': frame_uri,
                'vertex_rules': [],
                'edge_rules': []}


class JsonRules(object):
    """
        We want to keep these objects because we need to do a conversion
        from how the user defines the rules to how our service defines
        these rules.
    """
    def __new__(cls, rules):
        return JsonRules._get_frames(rules)

    @staticmethod
    def _get_frames(rules):
        frames_dict = {}
        for rule in rules:
            frame = JsonRules._get_frame(rule, frames_dict)
            # TODO - capture rule.__repr__ is a creation history for the graph
            if isinstance(rule, VertexRule):
                frame['vertex_rules'].append(JsonVertexRule(rule))
            elif isinstance(rule, EdgeRule):
                frame['edge_rules'].append(JsonEdgeRule(rule))
            else:
                raise TypeError("Non-Rule found in graph create arguments")
        return frames_dict.values()

    @staticmethod
    def _get_frame(rule, frames_dict):
        uri = rule.source_frame._id
        try:
            frame = frames_dict[uri]
        except KeyError:
            frame = JsonFrame(uri)
            frames_dict[uri] = frame
        return frame

class GraphInfo(object):
    """
    JSON based Server description of a BigGraph
    """
    def __init__(self, graph_json_payload):
        self._payload = graph_json_payload

    def __repr__(self):
        return json.dumps(self._payload, indent =2, sort_keys=True)

    def __str__(self):
        return '%s "%s"' % (self.id_number, self.name)

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
    def links(self):
        return self._links['links']

    def update(self,payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, graph ID mismatch %d when expecting %d" \
                % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload=payload
