//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class FrameMultiLineTests(unittest.TestCase):
    """
    Test loading multiline file formats (XML, JSON)

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        self.json = ia.JsonFile("/datasets/trailblazers.json")
        self.xml = ia.XmlFile("/datasets/foods.xml", "food")
        self.xml_root = ia.XmlFile("/datasets/foods.xml", "menu")

    def test_load_json(self):
        #covers escape double quote and nested json. They are in dataset
        frame = ia.Frame(self.json)
        self.assertEquals(frame.row_count, 15)
        self.assertEquals(frame.column_names, ["data_lines"])


    def test_load_xml(self):
        #covers xml comments
        frame = ia.Frame(self.xml)
        self.assertEquals(frame.row_count, 9)
        self.assertEquals(frame.column_names, ["data_lines"])


    def test_load_xml(self):
        #covers xml comments
        frame = ia.Frame(self.xml_root)
        self.assertEquals(frame.row_count, 1)
        self.assertEquals(frame.column_names, ["data_lines"])




if __name__ == "__main__":
    unittest.main()
