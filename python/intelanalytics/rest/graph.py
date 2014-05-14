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
import logging
logger = logging.getLogger(__name__)

from intelanalytics.core.graph import VertexRule, EdgeRule
from intelanalytics.core.column import BigColumn
from intelanalytics.rest.connection import rest_http

# temp adaptor
from intelanalytics.rest.tmp_gb_json import JsonPayloadAdaptor


class GraphBackendRest(object):

    #def __init__(self):
    #    pass

    def get_graph_names(self):
        logger.info("REST Backend: get_graph_names")
        r = rest_http.get('graphs')
        payload = r.json()
        return [f['name'] for f in payload]

    # def get_graph(name):
    #     """Retrieves the named BigGraph object"""
    #     raise NotImplemented
    #
    # def delete_graph(name):
    #     """Deletes the graph from backing store"""
    #     raise NotImplemented

    def create(self, graph, rules):
        logger.info("REST Backend: create graph: " + graph.name)
        #payload = JsonPayload(graph, rules)
        payload = JsonPayloadAdaptor(graph, rules)
        r = rest_http.post('graphs', payload)
        logger.info("REST Backend: create response: " + r.text)
        payload = r.json()
        graph._id = payload['id']


# JSON Payload objects:

class JsonValue(object):
    def __new__(cls, value):
        if isinstance(value, basestring):
            t, v = "static", value
        elif isinstance(value, BigColumn):
            t, v = "column", value.name
        else:
            raise TypeError("Bad graph element source type")
        return {"type": t, "value": v}


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
                'is_directed': rule.is_directed}


class JsonFrame(object):
    def __new__(cls, frame_uri):
        return {'frame_uri': frame_uri,
                'vertex_rules': [],
                'edge_rules': []}


class JsonPayload(object):
    def __new__(cls, graph, rules):
        return {'name': graph.name,
                'frames': JsonPayload._get_frames(rules)}

    @staticmethod
    def _get_frames(rules):
        frames_dict = {}
        for rule in rules:
            frame = JsonPayload._get_frame(rule, frames_dict)
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
        uri = rule.source_frame.uri
        try:
            frame = frames_dict[uri]
        except KeyError:
            frame = JsonFrame(uri)
            frames_dict[uri] = frame
        return frame

