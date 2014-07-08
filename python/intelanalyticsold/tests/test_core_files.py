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

import unittest
from intelanalytics.core.serialize import to_json
from intelanalytics.core.files import *
from intelanalytics.core.types import *


class TestCsvFile(unittest.TestCase):

    def get_csv_and_schema(self):
        schema = [('A', int32), ('B', int64), ('C', float32)]
        return CsvFile("myfile.csv", schema), schema

    def test_csv_attributes(self):
        csv, schema = self.get_csv_and_schema()
        self.assertEqual(csv.file_name, "myfile.csv")
        self.assertEqual(csv.delimiter, ",")
        self.assertEqual(csv.skip_header_lines, 0)
        repr(csv)

    def test_to_ordered_dict(self):
        csv, schema = self.get_csv_and_schema()
        d = csv.to_ordered_dict()
        self.assertEqual(len(schema), len(d))
        for i, (k, v) in enumerate(d.items()):
            self.assertEqual(k, schema[i][0])
            self.assertEqual(v, schema[i][1])

    def test_field_names_and_types(self):
        csv, schema = self.get_csv_and_schema()
        field_names = csv.field_names
        field_types = csv.field_types
        self.assertEqual(len(schema), len(field_names))
        self.assertEqual(len(schema), len(field_types))
        for i in range(len(schema)):
            self.assertEqual(schema[i][0], field_names[i])
            self.assertEqual(schema[i][1], field_types[i])

    def test_bad_schema_bad_name(self):
        try:
            CsvFile("nice.csv", [(int, 'A'), (long, 'B')])
            self.fail()
        except ValueError as e:
            self.assertEqual('First item in CSV schema tuple must be a string', str(e))

    def test_bad_schema_bad_type(self):
        try:
            CsvFile("nice.csv", [('A', int32), ('B', int)])
            self.fail()
        except ValueError as e:
            self.assertTrue(str(e).startswith('Second item in CSV schema tuple must be a supported type:'))

    def test_empty_schema(self):
        try:
            CsvFile("nice.csv", [])
            self.fail()
        except ValueError as e:
            self.assertEqual(str(e), 'schema must be non-empty list of tuples')

    @unittest.skip("json serialization not ready")
    def test_to_json(self):
        csv, schema = self.get_csv_and_schema()
        json_str = to_json(csv)
        self.assertEqual('["csv_file", "myfile.csv", [["A", "int32"], ["B", "int64"], ["C", "float32"]], ",", 0]', json_str)


class TestJsonFile(unittest.TestCase):

    def test_json_file(self):
        file_name = 'new_kid.json'
        x = JsonFile(file_name)
        self.assertEqual(x.file_name, file_name)


class TestXmlFile(unittest.TestCase):

    def test_xml_file(self):
        file_name = 'ugly.xml'
        x = XmlFile(file_name)
        self.assertEqual(x.file_name, file_name)
        self.assertIsNone(x.tag_name)
        tag_name = 'YouKnow'
        y = XmlFile(file_name, tag_name)
        self.assertEqual(y.file_name, file_name)
        self.assertEqual(y.tag_name, tag_name)


if __name__ == "__main__":
    unittest.main()
