"""
Tests the adaptor
"""
import iatest
iatest.init()
#iatest.set_logging("intelanalytics.rest.connection", 20)

import unittest
from mock import patch, Mock
from collections import OrderedDict
import json

from intelanalytics.core.graph import VertexRule, EdgeRule
from intelanalytics.core.frame import BigFrame
from intelanalytics.core.column import BigColumn
from intelanalytics.rest.tmp_gb_json import JsonPayloadAdaptor
from intelanalytics.tests.test_rest_graph import get_sorted_json_str, get_sorted_json_str_from_str


class TestGraphBackendRest(unittest.TestCase):

    expected_json_str_raw = """
{
    "graphName":  "movieGraph",
    "dataFrameId": 1,
    "outputConfig" : { "storeName" : "Titan",
                       "configuration" : {}
                      },

    "vertexRules" : [

      {"id" : { "key" : { "source" : "CONSTANT", "value" : "user" },
                  "value" : { "source" : "VARYING", "value" : "user" }  },
       "properties" : [{ "key" : { "source" : "CONSTANT", "value" : "vertexType" },
                  "value" : { "source" : "VARYING", "value" : "vertexType" }  }]
       },

      {"id" : { "key" : { "source" : "CONSTANT", "value" : "movie" },
                  "value" : { "source" : "VARYING", "value" : "movie" }  },
       "properties" : []
      }
      ],

    "edgeRules"   : [

     {   "head": { "key" : { "source" : "CONSTANT", "value" : "user" },
                  "value" : { "source" : "VARYING", "value" : "user" }  },
         "tail": { "key" : { "source" : "CONSTANT", "value" : "movie" },
                  "value" : { "source" : "VARYING", "value" : "movie" }  },
         "label": { "source" : "CONSTANT", "value" : "rating" },
         "properties": [ { "key" : { "source" : "CONSTANT", "value" : "splits" },
                  "value" : { "source" : "VARYING", "value" : "splits" }  } ]
     }

    ],


    "retainDanglingEdges" : false,
    "bidirectional" : false
}"""
    expected_json_str = get_sorted_json_str_from_str(expected_json_str_raw)

    @patch("intelanalytics.core.frame._get_backend")
    def create_mock_frame(self, schema, uri, mock_backend):
        frame = BigFrame()
        frame._columns = OrderedDict({k: BigColumn(k, v) for k, v in schema})
        for col in frame._columns.values():
            col._frame = frame
        frame._uri = uri
        return frame

    def test_get_payload(self):
        frame = self.create_mock_frame([('movie', str),
                                        ('user', str),
                                        ('rating', str),
                                        ('splits', str),
                                        ('vertexType', str)],
                                       "hardcoded.com:9999/v1/dataframes/0")

        movie_vertex = VertexRule("movie", frame.movie)
        user_vertex = VertexRule("user", frame.user, {"vertexType": frame.vertexType})
        rules = [user_vertex,
                 movie_vertex,
                 EdgeRule('rating',
                          movie_vertex,
                          user_vertex,
                          {"splits": frame['splits']},
                          is_directed=False)]
        #print "\n".join(repr(r) for r in rules)
        graph = Mock()
        graph.name = "movieGraph"
        frame._id = 1
        json_obj = JsonPayloadAdaptor(graph, rules)
        #print "-" * 70
        #print json_obj
        json_str = get_sorted_json_str(json_obj)
        self.assertEqual(self.expected_json_str, json_str)


if __name__ == "__main__":
    unittest.main()