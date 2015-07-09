#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import iatest
iatest.init()

import unittest
import taprootanalytics.core.iatypes as iatypes
from taprootanalytics.meta.metaprog import CommandInstallable, CommandInstallation, ATTR_COMMAND_INSTALLATION
from taprootanalytics.meta.names import entity_type_to_class_name
from taprootanalytics.meta.command import CommandDefinition, Parameter, ReturnInfo


class Numbers(CommandInstallable):
    _entity_type = 'numbers'

    def __init__(self, name):
        self._id = name
        CommandInstallable.__init__(self)


setattr(Numbers, ATTR_COMMAND_INSTALLATION, CommandInstallation('numbers', True))

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


class TestNaming(unittest.TestCase):

    def test_upper_first(self):
        from taprootanalytics.meta.names import upper_first
        self.assertEqual("Apple", upper_first('apple'))
        self.assertEqual("Apple", upper_first('Apple'))
        self.assertEqual('', upper_first(''))
        self.assertEqual('', upper_first(None))

    def test_lower_first(self):
        from taprootanalytics.meta.names import lower_first
        self.assertEqual("apple", lower_first('apple'))
        self.assertEqual("apple", lower_first('Apple'))
        self.assertEqual('', lower_first(''))
        self.assertEqual('', lower_first(None))

    def test_underscores_to_pascal(self):
        from taprootanalytics.meta.names import underscores_to_pascal
        self.assertEqual("LogisticRegressionModel", underscores_to_pascal("logistic_regression_model"))

    def test_pascal_to_underscores(self):
        from taprootanalytics.meta.names import pascal_to_underscores
        self.assertEqual("logistic_regression_model", pascal_to_underscores("LogisticRegressionModel"))

    def test_get_entity_type_from_class_name(self):
        from taprootanalytics.meta.names import class_name_to_entity_type
        self.assertEqual("model:logistic_regression", class_name_to_entity_type("LogisticRegressionModel"))
        self.assertEqual("model", class_name_to_entity_type("_BaseModel"))
        with self.assertRaises(ValueError) as cm:
            class_name_to_entity_type("")
        self.assertEqual(str(cm.exception), "Invalid empty class_name, expected non-empty string")

    def test_get_loadable_class_name_from_entity_type(self):
        from taprootanalytics.meta.names import entity_type_to_class_name
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