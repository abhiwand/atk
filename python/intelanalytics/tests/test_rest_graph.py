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
from intelanalytics.rest.graph import GraphBackendRest, JsonPayload


def get_sorted_json_str(json_obj):
    return json.dumps(json_obj, indent=2, sort_keys=True)


def get_sorted_json_str_from_str(json_str):
    return get_sorted_json_str(json.loads(json_str))

#http://localhost:8090/v1/dataframes/17",


class TestGraphBackendRest(unittest.TestCase):

    expected_json_str_raw = """
{
  "name": "movies",
  "frames":
  [
    {
      "frame_uri" : "hardcoded.com:9999/v1/dataframes/0",
      "vertex_rules" :
      [
        {
          "id" :
          {
            "key":    { "type": "static", "value": "movie" },
            "value" : { "type": "column", "value": "movie" }
          },
          "properties" :
          [
            {
              "key":    { "type": "static", "value": "year" },
              "value" : { "type": "column", "value": "released" }
            }
          ]
        },
        {
          "id" :
          {
            "key":    { "type": "static", "value": "user" },
            "value" : { "type": "column", "value": "user" }
          },
          "properties" :
          [
          ]
        }

      ],

      "edge_rules" :
      [
        {
          "label" : { "type": "column", "value": "rating" },
          "tail" :
          {
            "key":    { "type": "static", "value": "user" },
            "value" : { "type": "column", "value": "user" }
          },
          "head" :
          {
            "key":    { "type": "static", "value": "movie" },
            "value" : { "type": "column", "value": "movie" }
          },
          "properties" :
          [
            {
              "key":    { "type": "static", "value": "with_popcorn" },
              "value" : { "type": "column", "value": "popcorn" }
            }
          ],
          "is_directed": false
        }
      ]
    }
  ]
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
                                        ('popcorn', str),
                                        ('released', str)],
                                       "hardcoded.com:9999/v1/dataframes/0")

        movie_vertex = VertexRule("movie", frame.movie, {"year": frame.released})
        user_vertex = VertexRule("user", frame.user)
        rules = [movie_vertex,
                 user_vertex,
                 EdgeRule(frame['rating'],
                          user_vertex,
                          movie_vertex,
                          {"with_popcorn": frame['popcorn']},
                          is_directed=False)]
        #print "\n".join(repr(r) for r in rules)
        graph = Mock()
        graph.name = "movies"
        json_obj = JsonPayload(graph, rules)
        #print "-" * 70
        #print json_obj
        json_str = get_sorted_json_str(json_obj)
        self.assertEqual(self.expected_json_str, json_str)


if __name__ == "__main__":
    unittest.main()
