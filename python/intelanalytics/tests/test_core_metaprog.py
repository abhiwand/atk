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
import intelanalytics.core.iatypes as iatypes
from intelanalytics.core.metaprog import CommandLoadable, load_loadable
from intelanalytics.core.command import CommandDefinition, Parameter, Return
from intelanalytics.core.loggers import loggers

class Numbers(CommandLoadable):
    command_prefixes = ['numbers']
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

def get_numbers(cmd_name, **args):
    numbers = numbers_by_lang[cmd_name]
    return "%s, %s" % (numbers[args['a']], numbers[args['b']])


class TestCommandsLoadable(unittest.TestCase):

    def test_loadable_with_intermediates(self):
        #loggers.set(10, 'intelanalytics.core.metaprog')
        load_loadable(Numbers, cmd_defs, get_numbers)
        n = Numbers('A')
        help(n.lang.spanish)
        self.assertEqual("uno, dos", n.lang.spanish(1, 2))
        self.assertEqual("deux, zero", n.lang.french(2, 0))


if __name__ == '__main__':
    unittest.main()
