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

import sys
import unittest
import mock

from intelanalytics.core.namedobj import add_named_object_support
from intelanalytics.core.errorhandle import errors

errors.show_details = True

class TestNamedObj(unittest.TestCase):

    @mock.patch('intelanalytics.rest.command.execute_command', mock.MagicMock())
    @mock.patch('intelanalytics.rest.connection.http', mock.MagicMock())
    @mock.patch("intelanalytics.core.api.check_api_is_loaded", mock.Mock())
    def test_add_named_object_support(self):
        """
        Validate this module gets the new global methods installed, and they're callable, and the name property shows up
        """
        class Pizza(object):
            _command_prefix = 'pizza'
            def __init__(self, info):
                self._id = 85
                self.info = info
        http = mock.MagicMock()
        execute_command = mock.MagicMock()
        add_named_object_support(Pizza, 'pizza')
        this_module = sys.modules[__name__]
        self.assertTrue(hasattr(this_module, 'get_pizza'))
        self.assertTrue(hasattr(this_module, 'get_pizza_names'))
        self.assertTrue(hasattr(this_module, 'drop_pizzas'))
        self.assertEqual([], get_pizza_names())
        pizza = get_pizza('pepperoni')
        self.assertTrue(str(pizza.info).startswith("<MagicMock name='mock.get().json()'"))
        drop_pizzas('pepperoni')
        pizza2 = Pizza('info')
        self.assertTrue(hasattr(pizza2, 'name'))
        pizza2.name = 'olives'
        print "pizza2's name = " + str(pizza2.name)
        drop_pizzas(pizza2)
        try:
            drop_pizzas(2)
        except Exception as e:
            self.assertEqual("Excepted argument of type pizza or else the pizza's name", str(e))
        else:
            self.fail("Expected exception for bad type passed to drop_pizzas")

        # could sophisticate the mocks to validate rest activity, but better to leave to the integrations

if __name__ == '__main__':
    unittest.main()