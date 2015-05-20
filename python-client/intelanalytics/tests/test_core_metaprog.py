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
import mock
import intelanalytics.core.iatypes as iatypes
#from intelanalytics.meta.metaprog import CommandLoadable, load_loadable, get_loadable_class_name_from_entity_type
from intelanalytics.meta.metaprog2 import CommandInstallable, install_command_def, CommandInstallation, _Constants
from intelanalytics.meta.classnames import entity_type_to_class_name
from intelanalytics.meta.command import CommandDefinition, Parameter, ReturnInfo


class Numbers(CommandInstallable):
    _entity_type = 'numbers'

    def __init__(self, name):
        self._id = name
        CommandInstallable.__init__(self)


setattr(Numbers, _Constants.COMMAND_INSTALLATION, CommandInstallation('numbers', True))

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
                              ReturnInfo(list, False, None))

cmd_defs = [make_command_def(name) for name in numbers_by_lang.keys()]

def get_numbers(cmd_name, selfish, **args):
    numbers = numbers_by_lang[cmd_name]
    return "%s, %s" % (numbers[args['a']], numbers[args['b']])


# class TestCommandsLoadable(unittest.TestCase):
#
#     #@mock.patch("intelanalytics.meta.api.check_api_is_loaded", mock.Mock())
#     def test_loadable_with_intermediates(self):
#         #loggers.set(10, 'intelanalytics.meta.metaprog')
#         for cmd_def in cmd_defs:
#             install_command_def(Numbers, cmd_def, get_numbers)
#         n = Numbers('A')
#         help(n.lang.spanish)
#         self.assertEqual("uno, dos", n.lang.spanish(1, 2))
#         self.assertEqual("deux, zero", n.lang.french(2, 0))

class TestNaming(unittest.TestCase):

    def test_upper_first(self):
        from intelanalytics.meta.classnames import upper_first
        self.assertEqual("Apple", upper_first('apple'))
        self.assertEqual("Apple", upper_first('Apple'))
        self.assertEqual('', upper_first(''))
        self.assertEqual('', upper_first(None))

    def test_lower_first(self):
        from intelanalytics.meta.classnames import lower_first
        self.assertEqual("apple", lower_first('apple'))
        self.assertEqual("apple", lower_first('Apple'))
        self.assertEqual('', lower_first(''))
        self.assertEqual('', lower_first(None))

    def test_underscores_to_pascal(self):
        from intelanalytics.meta.classnames import underscores_to_pascal
        self.assertEqual("LogisticRegressionModel", underscores_to_pascal("logistic_regression_model"))

    def test_pascal_to_underscores(self):
        from intelanalytics.meta.classnames import pascal_to_underscores
        self.assertEqual("logistic_regression_model", pascal_to_underscores("LogisticRegressionModel"))

    def test_get_entity_type_from_class_name(self):
        from intelanalytics.meta.classnames import class_name_to_entity_type
        self.assertEqual("model:logistic_regression", class_name_to_entity_type("LogisticRegressionModel"))
        self.assertEqual("model", class_name_to_entity_type("_BaseModel"))
        with self.assertRaises(ValueError) as cm:
            class_name_to_entity_type("")
        self.assertEqual(str(cm.exception), "Invalid empty class_name, expected non-empty string")

    def test_get_loadable_class_name_from_entity_type(self):
        from intelanalytics.meta.classnames import entity_type_to_class_name
        self.assertEqual("LogisticRegressionModel", entity_type_to_class_name("model:logistic_regression"))
        self.assertEqual("_BaseModel", entity_type_to_class_name("model"))
        with self.assertRaises(ValueError) as cm:
            entity_type_to_class_name("")
        self.assertEqual(str(cm.exception), "Invalid empty entity_type, expected non-empty string")

class TestDocStubs(unittest.TestCase):
    def test_get_loadable_class_name_from_entity_type(self):
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
        for entity_type, expected in cases:
            self.assertEqual(expected, entity_type_to_class_name(entity_type))


if __name__ == '__main__':
    unittest.main()
