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
import mock
import intelanalytics.core.iatypes as iatypes
from intelanalytics.core.metaprog import CommandLoadable, load_loadable, get_loadable_class_name_from_command_prefix
from intelanalytics.core.command import CommandDefinition, Parameter, Return
from intelanalytics.core.loggers import loggers

class Numbers(CommandLoadable):
    _command_prefix = 'numbers'
    def __init__(self, name):
        self._id = name
        CommandLoadable.__init__(self)


def get_french(a, b):
    numbers = ['zero', 'un', 'deux']
    return "%s, %s" % (numbers[a], numbers[b])


def get_spanish(a, b):
    numbers = ['zero', 'uno', 'dos']
    return "%s, %s" % (numbers[a], numbers[b])


def get_english(a, b):
    numbers = ['zero', 'one', 'two']
    return "%s, %s" % (numbers[a], numbers[b])


numbers_by_lang = {
    "numbers/lang/spanish": ['zero', 'uno', 'dos'],
    "numbers/lang/english": ['zero', 'one', 'two'],
    "numbers/lang/french": ['zero', 'un', 'deux'],
}

def make_command_def(full_name):
    return CommandDefinition({},
                             full_name,
                             [Parameter('self', object, True, False, None, None),
                              Parameter('a', iatypes.int32, False, False, None, None),
                              Parameter('b', iatypes.int32, False, False, None, None)],
                              Return(list, False, None))

cmd_defs = [make_command_def(name) for name in numbers_by_lang.keys()]

def get_numbers(cmd_name, selfish, **args):
    numbers = numbers_by_lang[cmd_name]
    return "%s, %s" % (numbers[args['a']], numbers[args['b']])


class TestCommandsLoadable(unittest.TestCase):

    @mock.patch("intelanalytics.core.api.check_api_is_loaded", mock.Mock())
    def test_loadable_with_intermediates(self):
        #loggers.set(10, 'intelanalytics.core.metaprog')
        for cmd_def in cmd_defs:
            load_loadable(Numbers, cmd_def, get_numbers)
        n = Numbers('A')
        help(n.lang.spanish)
        self.assertEqual("uno, dos", n.lang.spanish(1, 2))
        self.assertEqual("deux, zero", n.lang.french(2, 0))

class TestNaming(unittest.TestCase):

    def test_upper_first(self):
        from intelanalytics.core.metaprog import upper_first
        self.assertEqual("Apple", upper_first('apple'))
        self.assertEqual("Apple", upper_first('Apple'))
        self.assertEqual('', upper_first(''))
        self.assertEqual('', upper_first(None))

    def test_lower_first(self):
        from intelanalytics.core.metaprog import lower_first
        self.assertEqual("apple", lower_first('apple'))
        self.assertEqual("apple", lower_first('Apple'))
        self.assertEqual('', lower_first(''))
        self.assertEqual('', lower_first(None))

    def test_underscores_to_pascal(self):
        from intelanalytics.core.metaprog import underscores_to_pascal
        self.assertEqual("LogisticRegressionModel", underscores_to_pascal("logistic_regression_model"))

    def test_pascal_to_underscores(self):
        from intelanalytics.core.metaprog import pascal_to_underscores
        self.assertEqual("logistic_regression_model", pascal_to_underscores("LogisticRegressionModel"))

    def test_get_command_prefix_from_class_name(self):
        from intelanalytics.core.metaprog import get_command_prefix_from_class_name
        self.assertEqual("model:logistic_regression", get_command_prefix_from_class_name("LogisticRegressionModel"))
        self.assertEqual("model", get_command_prefix_from_class_name("_BaseModel"))
        with self.assertRaises(ValueError) as cm:
            get_command_prefix_from_class_name("")
        self.assertEqual(str(cm.exception), "Invalid empty class_name, expected non-empty string")

    def test_get_loadable_class_name_from_command_prefix(self):
        from intelanalytics.core.metaprog import get_loadable_class_name_from_command_prefix
        self.assertEqual("LogisticRegressionModel", get_loadable_class_name_from_command_prefix("model:logistic_regression"))
        self.assertEqual("_BaseModel", get_loadable_class_name_from_command_prefix("model"))
        with self.assertRaises(ValueError) as cm:
            get_loadable_class_name_from_command_prefix("")
        self.assertEqual(str(cm.exception), "Invalid empty command_prefix, expected non-empty string")

class TestDocStubs(unittest.TestCase):
    def test_get_loadable_class_name_from_command_prefix(self):
        cases = [
            ('frame', "_BaseFrame"),
            ('frame:', "Frame"),
            ('frame:edge', "EdgeFrame"),
            ('frame:vertex', "VertexFrame"),
            ('graph', "_BaseGraph"),
            ('graph:', "Graph"),
            ('graph:titan', "TitanGraph"),
            ('model', "_BaseModel"),
            ]
        for prefix, expected in cases:
            self.assertEqual(expected, get_loadable_class_name_from_command_prefix(prefix))


if __name__ == '__main__':
    unittest.main()
