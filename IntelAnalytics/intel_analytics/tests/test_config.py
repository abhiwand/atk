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
Unit tests for intel_analytics/config.py
"""
import unittest
import os
import sys
from string import Template

curdir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(
    os.path.join(os.path.join(curdir, os.pardir), os.pardir)))

# Warning is OK from global config load failure
sys.stderr.write("""
It is SAFE to ignore a config warning message within the *{ }*
*{""")
sys.stderr.flush()

from intel_analytics.config import \
    get_env_vars, get_keys_from_template, dynamic_import, Config

sys.stderr.write("""}*
""")
sys.stderr.flush()


class TestGetEnvVariables(unittest.TestCase):
    def test_get_env_vars(self):
        keys = ['PATH', 'HOSTNAME', 'SHELL']
        results = get_env_vars(keys)
        self.assertEqual(len(keys), len(results))
        for k in keys:
            self.assertTrue(results[k] is not None)

    def test_get_env_vars_neg(self):
        keys = ['PATH', 'OUTRAGEOUS_ENV_VAR', 'OUTLANDISH_ENV_VAR', 'SHELL']
        try:
            get_env_vars(keys)
            self.fail("Expected an error message for unset env variables")
        except Exception as e:
            self.assertTrue(str(e).startswith("Environment vars not set"))


class TestTemplate(unittest.TestCase):
    def test_get_keys_from_template(self):
        testcases = [
            ("There was a ${adj1} ${noun1} who swallowed a ${noun2}",
             ['adj1', 'noun1', 'noun2']),  # output should be sorted
            ("There was an old woman who swallowed a fly", [])]
        for src, expected in testcases:
            template = Template(src)
            results = get_keys_from_template(template)
            self.assertEqual(len(expected), len(results))
            for i in range(len(expected)):
                self.assertEqual(expected[i], results[i])


class TestDynamicImport(unittest.TestCase):
    def test_dynamic_import(self):
        if __name__ == '__main__':
            # This is to allow the tests to execute from both directly executing the script or through test discovery via nosetests
            # nose is unable to find intel_analytics.config.Config but it can find config.Config
            mock_prefix='intel_analytics.'
        else:
            mock_prefix=''
        config_class = dynamic_import("%sconfig.Config" % mock_prefix)
        self.assertEqual("Config", config_class.__name__)


class TestConfig(unittest.TestCase):

    def __compare_dictionaries(self, a, b):
        if len(a) != len(b):
            self.fail("Length mismatch\na={0}\nb={1}".format(str(a), str(b)))
        for k, v in b.items():
            if k not in a or a[k] != v:
                self.fail("Key '{0}' mismatch\na={1}\nb={2}"
                          .format(k, str(a), str(b)))

    def setUp(self):
        self.config =\
            Config(srcfile=os.path.join(curdir, "test_config1.properties"))
        self.expected = {'fruit': 'orange',
                         'sample': 'sample.jar',
                         'dotted.key': '"spaced name"',
                         'double.dotted.key': 'unquoted spaced name',
                         'shell': os.environ.get('SHELL')}

    def test_verify(self):
        self.config.verify(self.expected.keys())
        extra = 'extra'
        self.expected[extra] = 'read all about it'
        try:
            self.config.verify(self.expected.keys())
            self.fail('verify should have thrown an error')
        except Exception as e:
            self.assertTrue(str(e).strip().endswith(extra))

    def test_get_set_del(self):
        key = 'bonus'
        value = 'material'
        self.config[key] = value
        self.assertTrue(value == self.config[key])
        self.expected[key] = value
        self.__compare_dictionaries(self.expected, self.config.props)
        del self.config[key]
        del self.expected[key]
        self.__compare_dictionaries(self.expected, self.config.props)

    def test_reset(self):
        key = 'bonus'
        value = 'material'
        self.config[key] = value
        self.assertTrue(value == self.config[key])
        self.config.reset()
        self.__compare_dictionaries(self.expected, self.config.props)

    def test_load(self):
        self.__compare_dictionaries(self.expected, self.config.props)
        self.config.load(os.path.join(curdir, "test_config2.properties"))
        expected2 = {'fruit': 'pineapple',
                     'pwd': os.environ.get('PWD'),
                     'expired': ''}
        self.__compare_dictionaries(expected2, self.config.props)
        try:
            self.config.load(os.path.join(curdir, "test_config3.properties"))
            self.fail('load 3 should have thrown an error')
        except Exception as e:
            self.assertEqual(str(e), 'Environment vars not set: OUTRAGEOUS_ENV_VAR_THAT_IS_NEVER_DEFINED')
    def test_nested_subs(self):
        self.config.load(os.path.join(curdir, "test_config_nested.properties"))
        expected_nested = {'fruit': 'pineapple',
                           'pwd': os.environ.get("PWD"),
                           'fruitypwd': 'pineapple'+ os.environ.get("PWD"),
                           'nesty':'pineapple'+ os.environ.get("PWD")+'!'}
        self.__compare_dictionaries(expected_nested, self.config.props)

if __name__ == '__main__':
    unittest.main()
