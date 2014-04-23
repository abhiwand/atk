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

from intelanalytics.rest.serialize import IAPickle
import unittest
from StringIO import StringIO
import pickle


class TestIAPickle(unittest.TestCase):
    def pickle_and_unpickle(self, obj):
        pickled_stream = StringIO()
        i = IAPickle(pickled_stream)
        i.dump(obj)
        return pickle.loads(pickled_stream.getvalue())

    def pickle_and_unpickle_with_dependencies(self, obj):
        pickled_stream = StringIO()
        i = IAPickle(pickled_stream)
        i.dump(obj)
        return pickle.loads(pickled_stream.getvalue()), i.get_dependent_modules()

    def test_builtin_type(self):
        x = 5
        y = self.pickle_and_unpickle(x)
        self.assertEqual(x, y)

    def test_lambda(self):
        x = lambda t: t > 4
        y = self.pickle_and_unpickle(x)
        self.assertEqual(x(4), y(4))

    def test_class_instance(self):
        class A(object):
            def __init__(self):
                self.msg = 'Instance of A'

        x = A()
        y = self.pickle_and_unpickle(x)
        self.assertEqual(x.msg, y.msg)

    def test_func_def(self):
        def x(a, b):
            return a + b

        y = self.pickle_and_unpickle(x)
        self.assertEqual(x(1, 2), y(1, 2))

    def test_closure(self):
        def x(n):
            def nth_power(m):
                return m ** n

            return nth_power

        y = self.pickle_and_unpickle(x)
        self.assertEqual(x(3)(5), y(3)(5))

    def test_numpy_import(self):
        import numpy as np

        x = np.array([1, 2, 3])
        y, dependencies = self.pickle_and_unpickle_with_dependencies(x)
        self.assertItemsEqual(x, y)
        self.assertIn('numpy 1.8.0', dependencies)

    def test_dependent_lambdas(self):
        import numpy as np

        a = np.array([1, 2, 3])
        x = lambda t: t < a.sum()
        y, dependencies = self.pickle_and_unpickle_with_dependencies(x)
        self.assertEqual(x(6), y(6))
        self.assertItemsEqual(['numpy 1.8.0', 'testiapickle', '__builtin__', 'cloud 2.8.5'], dependencies)

    def test_complex_lambdas(self):
        import numpy as np

        def lfunc(a):
            return a*np.sum([1,2,3,4])
        def rfunc(a):
            return a*100
        def func(a,b):
            return lfunc(a) + rfunc(b)
        x = lambda t: func(t,10)
        y = self.pickle_and_unpickle(x)
        self.assertEqual(x(4), y(4))


if __name__ == '__main__':
    unittest.main()