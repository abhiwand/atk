import unittest
from intel_analytics.config import \
    get_env_vars, get_keys_from_template, dynamic_import
from intel_analytics.config import Config

from string import Template
from sets import Set

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
            ("There was an old woman who swallowed a fly", []),
            ]
        for src, expected in testcases:
            template = Template(src)
            results = get_keys_from_template(template)
            self.assertEqual(len(expected), len(results))
            for i in range(len(expected)):
                self.assertEqual(expected[i], results[i])

class TestDynamicImport(unittest.TestCase):
    def test_dynamic_import(self):
        config_class = dynamic_import("intel_analytics.config.Config")
        self.assertEqual("Config", config_class.__name__)

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config = Config(srcfile="test_config1.properties")
    pass

    def test_verify(self):
        expected = {'fruit':'orange',
                    'sample':'sample.jar',
                    'dotted.key':'',
                    'double.dotted.key'}


if __name__ == '__main__':
    unittest.main()
