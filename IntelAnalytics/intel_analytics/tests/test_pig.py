##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
Unit tests for intel_analytics/pig.py
"""
import unittest
from mock import patch
from intel_analytics.pig import get_pig_args, get_pig_args_with_gb

class TestPig(unittest.TestCase):

    def init_mock_config(self, config):
        # setup mock config to be backed by a dictionary
        my_dict = {}
        config.__getitem__.side_effect = my_dict.__getitem__
        config.__setitem__.side_effect = my_dict.__setitem__

        # initialize default values
        config['local_run']='False'
        config['conf_folder']='/mock/conf/folder'
        config['pig_py_scripts']='/mock/script/location'
        config['graph_builder_jar']='/mock/location/graphbuilder.jar'

    @patch('intel_analytics.pig.config')
    def test_get_pig_args(self, config):
        self.init_mock_config(config) # setup mocks
        result = " ".join(get_pig_args('foo.py')) # invoke method under test
        self.assertEqual('pig -4 /mock/conf/folder/pig_log4j.properties /mock/script/location/foo.py', result)

    @patch('intel_analytics.pig.config')
    def test_get_pig_args_local(self, config):
        self.init_mock_config(config) # setup mocks
        config['local_run']='True'
        result = " ".join(get_pig_args('foo.py')) # invoke method under test
        self.assertEqual('pig -x local -4 /mock/conf/folder/pig_log4j.properties /mock/script/location/foo.py', result)

    @patch('intel_analytics.pig.config')
    def test_get_pig_args_local_bad_value(self, config):
        self.init_mock_config(config) # setup mocks
        config['local_run']=123 # bad non-string value is treated as false
        result = " ".join(get_pig_args('foo.py'))  # invoke method under test
        self.assertEqual('pig -4 /mock/conf/folder/pig_log4j.properties /mock/script/location/foo.py', result)

    @patch('intel_analytics.pig.config')
    def test_get_pig_args_with_additional_pig_args(self, config):
        self.init_mock_config(config) # setup mocks
        result = " ".join(get_pig_args('foo.py', [ '-arg', 'value' ] )) # invoke method under test
        self.assertEqual('pig -4 /mock/conf/folder/pig_log4j.properties' +
                         ' -arg value /mock/script/location/foo.py', result)

    @patch('intel_analytics.pig.config')
    def test_get_pig_args_with_gb(self, config):
        self.init_mock_config(config) # setup mocks
        result = " ".join(get_pig_args_with_gb('foo.py')) # invoke method under test
        self.assertEqual('pig -4 /mock/conf/folder/pig_log4j.properties' +
                         ' -param GB_JAR=/mock/location/graphbuilder.jar /mock/script/location/foo.py', result)

    @patch('intel_analytics.pig.config')
    def test_get_pig_args_with_gb_and_additional_args(self, config):
        self.init_mock_config(config) # setup mocks
        result = " ".join(get_pig_args_with_gb('foo.py', [ '-arg', 'value' ])) # invoke method under test
        self.assertEqual('pig -4 /mock/conf/folder/pig_log4j.properties' +
                     ' -arg value -param GB_JAR=/mock/location/graphbuilder.jar /mock/script/location/foo.py', result)

if __name__ == '__main__':
    unittest.main()