#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
REST backend for graphs
"""
import json
import logging
import uuid

logger = logging.getLogger(__name__)

from intelanalytics.core.graph import VertexRule, EdgeRule, Rule
from intelanalytics.core.column import Column
from intelanalytics.rest.iaserver import server
from intelanalytics.core.frame import VertexFrame, EdgeFrame
from intelanalytics.rest.command import executor


def initialize_graph(graph, graph_info):
    """Initializes a graph according to given graph_info"""
    graph._id = graph_info.id_number
    graph._name = graph_info.name
    graph._ia_uri = graph_info.ia_uri
    graph._uri= server.create_full_uri("graphs/"+ str(graph._id))
    return graph


class GraphBackendRest(object):

    def __init__(self, http_methods = None):
        self.server = http_methods or server

    def create(self, graph, rules, name, storage_format, _info=None):
        logger.info("REST Backend: create graph with name %s: " % name)
        if isinstance(_info, dict):
            _info = GraphInfo(_info)
        if isinstance(_info, GraphInfo):
            return initialize_graph(graph,_info)._id # Early exit here
        new_graph_id = self._create_new_graph(graph, rules, name, storage_format, True if name else False)
        return new_graph_id

    def _create_new_graph(self, graph, rules, name, storage_format, is_named):
        if rules and (not isinstance(rules, list) or not all([isinstance(rule, Rule) for rule in rules])):
            raise TypeError("rules must be a list of Rule objects")
        else:
            payload = {'name': name, 'storage_format': storage_format, 'is_named': is_named}
            r=self.server.post('graphs', payload)
            logger.info("REST Backend: create graph response: " + r.text)
            graph_info = GraphInfo(r.json())
            initialized_graph=initialize_graph(graph, graph_info)
            if rules:
                frame_rules = JsonRules(rules)
                if logger.level == logging.DEBUG:
                    import json
                    payload_json = json.dumps(frame_rules, indent=2, sort_keys=True)
                    logger.debug("REST Backend: create graph payload: " + payload_json)
                initialized_graph.load(frame_rules, append=False)
            return graph_info.id_number
    
    def _get_new_graph_name(self,source=None):
        try:
            annotation ="_" + source.annotation
        except:
            annotation= ''
        return "graph_" + uuid.uuid4().hex + annotation

    def get_ia_uri(self, graph):
        return self._get_graph_info(graph).ia_uri

    def get_repr(self, graph):
        graph_info = self._get_graph_info(graph)
        return "\n".join(['%s "%s"' % (graph.__class__.__name__, graph_info.name), 'status = %s' % graph_info.status])

    def get_status(self, graph):
        graph_info = self._get_graph_info(graph)
        return graph_info.status

    def _get_graph_info(self, graph):
        response = self.server.get(self._get_graph_full_uri(graph))
        return GraphInfo(response.json())

    def _get_graph_full_uri(self,graph):
        return self.server.create_full_uri('graphs/%d' % graph._id)

    def append(self, graph, rules):
        logger.info("REST Backend: append_frame graph: %s" % graph.name)
        frame_rules = JsonRules(rules)
        graph.load(frame_rules, append=True)

    def get_vertex_frames(self, graphid):
        r = self.server.get('graphs/%s/vertices' % graphid)
        return [VertexFrame(_info=x) for x in r.json()]

    def get_vertex_frame(self,graphid, label):
        r = self.server.get('graphs/%s/vertices?label=%s' % (graphid, label))
        return VertexFrame(_info=r.json())

    def get_edge_frames(self, graphid):
        r = self.server.get('graphs/%s/edges' % graphid)
        return [EdgeFrame(_info=x) for x in r.json()]

    def get_edge_frame(self,graphid, label):
        r = self.server.get('graphs/%s/edges?label=%s' % (graphid, label))
        return EdgeFrame(_info=r.json())

    def get_vertex_count(self, graph):
        arguments = {'graph': self.get_ia_uri(graph)}
        return executor.execute("graph:/vertex_count", graph, arguments)

    def get_edge_count(self, graph):
        arguments = {'graph': self.get_ia_uri(graph)}
        return executor.execute("graph:/edge_count", graph, arguments)



# GB JSON Payload objects:

class JsonValue(object):
    def __new__(cls, value):
        if isinstance(value, basestring):
            t, v = "CONSTANT", value
        elif isinstance(value, Column):
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
                'bidirectional': rule.bidirectional}


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
        #validate the input frames
        from intelanalytics.meta.config import get_frame_backend
        frame_backend = get_frame_backend()

        try:
            frame_backend.get_frame_by_id(uri)
        except:
            raise ValueError("Frame provided to establish VertexRule is no longer available.")

        try:
            frame = frames_dict[uri]
        except KeyError:
            frame = JsonFrame(uri)
            frames_dict[uri] = frame
        return frame


class GraphInfo(object):
    """
    JSON based Server description of a Graph
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
        return self._payload.get('name', None)

    @property
    def entity_type(self):
        return self._payload['entity_type']

    @property
    def ia_uri(self):
        return self._payload['ia_uri']

    @property
    def links(self):
        return self._payload['links']

    @property
    def status(self):
        return self._payload['status']

    def update(self,payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, graph ID mismatch %d when expecting %d" \
                % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload=payload
