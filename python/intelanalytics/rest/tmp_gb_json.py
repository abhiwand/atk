"""
Temporary adaptor for GB REST API JSON payload
"""

from intelanalytics.core.column import BigColumn
from intelanalytics.core.graph import VertexRule, EdgeRule


# JSON Payload objects:


hacky_frame_id = 0  # global to capture the most recent frame ID


class JsonValue(object):
    def __new__(cls, value):
        if isinstance(value, basestring):
            t, v = "CONSTANT", value
        elif isinstance(value, BigColumn):
            t, v = "VARYING", value.name
            global hacky_frame_id
            hacky_frame_id = value._frame._id
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
                               for k, v in rule.properties.items()]}
                #'is_directed': rule.is_directed}


class JsonFrame(object):
    def __new__(cls, frame_uri):
        return {'frame_uri': frame_uri,
                'vertex_rules': [],
                'edge_rules': []}


canned_output_config = {"storeName": "Titan",
                        "configuration": {}}


class JsonPayloadAdaptor(object):
    def __new__(cls, graph, rules):
        frames = JsonPayloadAdaptor._get_frames(rules)
        return {'graphName': graph.name,
                'dataFrameId': hacky_frame_id,  # use a global to grab frame_id
                'outputConfig': canned_output_config,
                'vertexRules': frames[0]['vertex_rules'],
                'edgeRules': frames[0]['edge_rules'],
                'retainDanglingEdges': False,
                'bidirectional': False}

    @staticmethod
    def _get_frames(rules):
        frames_dict = {}
        for rule in rules:
            frame = JsonPayloadAdaptor._get_frame(rule, frames_dict)
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
