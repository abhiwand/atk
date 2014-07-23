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
from intelanalytics.core.iatypes import *


class ValidDataTypes(unittest.TestCase):

    def test_is_frozenset(self):
        self.assertTrue(isinstance(valid_data_types, frozenset))

    def test_contains(self):
        self.assertTrue(int32 in valid_data_types)
        self.assertTrue(float64 in valid_data_types)
        self.assertFalse(dict in valid_data_types)  # not supported yet!
        self.assertFalse(list in valid_data_types)  # not supported yet!
        self.assertFalse(int in valid_data_types)
        self.assertFalse(float in valid_data_types)
        self.assertFalse(ignore in valid_data_types)
        self.assertFalse(unknown in valid_data_types)

    def test_repr(self):
        r = valid_data_types.__repr__()
        self.assertTrue(len(valid_data_types) > 0)
        self.assertEqual(len(r.split(',')), len(valid_data_types))

    def test_get_from_string(self):
        self.assertEqual(int64, valid_data_types.get_from_string("int64"))
        self.assertEqual(int32, valid_data_types.get_from_string("int32"))
        self.assertEqual(str, valid_data_types.get_from_string("str"))
        for bad_str in ["int", "string"]:
            try:
                valid_data_types.get_from_string(bad_str)
            except ValueError:
                pass
            else:
                self.fail("Expected exception!")

    def test_get_from_type(self):
        self.assertEqual(int64, valid_data_types.get_from_type(int64))
        self.assertEqual(float64, valid_data_types.get_from_type(float))
        try:
            valid_data_types.get_from_type(ignore)
        except ValueError:
            pass
        else:
            self.fail("Expected exception!")

    def test_validate(self):
        valid_data_types.validate(float64)
        valid_data_types.validate(int)
        try:
            valid_data_types.validate(ignore)
        except ValueError:
            pass
        else:
            self.fail("Expected exception!")


    def test_to_string(self):
        self.assertEqual('int32', valid_data_types.to_string(int32))
        self.assertEqual('float64', valid_data_types.to_string(float64))
        self.assertEqual('str', valid_data_types.to_string(str))
        try:
            valid_data_types.to_string(ignore)
        except ValueError:
            pass
        else:
            self.fail("Expected exception!")

    def test_cast(self):
        self.assertEqual(float32(1.0), valid_data_types.cast(1.0, float32))
        self.assertEqual('jim', valid_data_types.cast('jim', str))
        self.assertTrue(valid_data_types.cast(None, unicode) is None)
        try:
            valid_data_types.cast(3, set)
        except ValueError:
            pass
        else:
            self.fail("Expected exception!")


if __name__ == '__main__':
    unittest.main()
