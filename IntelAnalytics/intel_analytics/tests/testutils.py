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
from mock import MagicMock


class MockRegistry(dict):
    def __init__(self, *args, **kw):
        super(MockRegistry, self).__init__(*args, **kw)

    def initialize(self, d):
        for k, v in d.items():
            self[k] = v

    def get_key(self, value):
        try:
            return (k for k, v in self.items() if v == value).next()
        except StopIteration:
            raise ValueError

    def register(self, key, value, *args, **kwargs):
        self[key] = value

    def unregister_key(self, key):
        del self[key]

    def unregister_value(self, value):
        try:
            key = self.get_key(value)
        except ValueError:
            pass
        else:
            del self[key]


def create_mock_registry():
    m = MagicMock()
    m._mr = MockRegistry()
    m.get_key.side_effect = m._mr.get_key
    m.items.side_effect = m._mr.items
    m.keys.side_effect = m._mr.keys
    m.values.side_effect = m._mr.values
    m.__getitem__.side_effect = m._mr.__getitem__
    m.__setitem__.side_effect = m._mr.__setitem__
    m.__delitem__.side_effect = m._mr.__delitem__
    m.__iter__.side_effect = m._mr.__iter__
    m.initialize.side_effect = m._mr.initialize
    m.register.side_effect = m._mr.register
    m.unregister_key.side_effect = m._mr.unregister_key
    m.unregister_value.side_effect = m._mr.unregister_value
    return m


class RegistryCallableFactory(object):

    class Helper(object):
        def __init__(self, dictionary, key):
            self.dictionary = dictionary
            self.key = key

        def callable(self):
            if self.key not in self.dictionary:
                self.dictionary[self.key] = create_mock_registry()
            return self.dictionary[self.key]

    def __init__(self):
        self._registries = {}

    def get_registry_callable(self, name):
        # print "get_registry_callable called"
        return RegistryCallableFactory.Helper(self._registries, name).callable

def get_diff_str(a, b):
    from difflib import ndiff
    results = list(ndiff(a.splitlines(), b.splitlines()))
    return '\n'.join(results)


if __name__ == "__main__":
    x = create_mock_registry()
