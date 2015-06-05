##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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

import unittest
import intelanalytics.rest.jsonschema as js
import json


json_schema_cum_dist = """
    {
      "return_schema": {
        "required": [ "name", "status" ],
        "type": "object",
        "properties": {
          "status": {
            "minimum": -9.223372036854776e+18,
            "type": "number",
            "id": "ia:long",
            "multiple_of": 1.0,
            "maximum": 9.223372036854776e+18
          },
          "error_frame_id": {
            "minimum": -9.223372036854776e+18,
            "type": "number",
            "id": "ia:long",
            "multiple_of": 1.0,
            "maximum": 9.223372036854776e+18
          },
          "name": {
            "type": "string"
          }
        },
        "order": [ "name", "error_frame_id", "status" ]
      },
    "name": "frame/cumulative_dist",
    "title": "Cumulative Distribution",
    "description": "Computes the cumulative distribution for a column and eats bags of Cheetos",
    "argument_schema": {
      "required": [
        "name",
        "sample_col"
      ],
      "type": "object",
      "properties": {
        "count_value": {
          "id": "ia:long",
          "type": "number",
          "default": 0
        },
        "name": {
          "type": "string"
        },
        "dist_type": {
          "type": "string",
          "default": "super"
        },
        "sample_col": {
          "type": "string",
          "title": "The name of the column to sample"
        }
      },
      "order": [
        "name",
        "sample_col",
        "dist_type",
        "count_value"
      ]
    }
  }
"""

json_schema_join = """
{
    "return_schema": {
        "required": [ "name", "status" ],
        "type": "object",
        "properties": {
            "status": {
                "minimum": -9.223372036854776e+18,
                "type": "number",
                "id": "ia:long",
                "multiple_of": 1.0,
                "maximum": 9.223372036854776e+18
            },
            "error_frame_id": {
                "minimum": -9.223372036854776e+18,
                "type": "number",
                "id": "ia:long",
                "multiple_of": 1.0,
                "maximum": 9.223372036854776e+18
            },
            "name": {
                "type": "string"
            }
        },
        "order": [ "name", "error_frame_id", "status" ]
    },
    "name": "frame/join",
    "title": "Table join operation",
    "description": "Creates a new frame by joining two frames together",
    "argument_schema": {
        "required": [
            "columns",
            "left_on"
        ],
        "type": "object",
        "properties": {
            "columns": {
                "type": "string"
            },
            "left_on": {
                "type": "string"
            },
            "right_on": {
                "type": "string",
                "default": null
            },
            "how": {
                "type": "string",
                "default": "left",
                "title": "The name of the column to sample"
            }
        },
        "order": [
            "columns",
            "left_on",
            "right_on",
            "how"
        ]
    }
}
"""


class TestJsonSchema(unittest.TestCase):

    def cmd_repr(self, json_str):
        schema = json.loads(json_str)
        cmd = js.get_command_def(schema)
        print "#################################################################"
        print repr(cmd)

    def test1(self):
        # tests only running the code without error
        self.cmd_repr(json_schema_cum_dist)

    def test2(self):
        # tests only running the code without error
        self.cmd_repr(json_schema_join)


if __name__ == '__main__':
    unittest.main()
