# import os
# import sys
#
# print "import mock_hbase_table"
# curdir = os.path.dirname(__file__)
# sys.path.append(os.path.abspath(
#     os.path.join(os.path.join(curdir, os.pardir), os.pardir)))
#
# from intel_analytics.config import Registry
# hbase_registry = Registry(os.path.join(curdir, 'mock_hbase_registry.txt'))

from mock import MagicMock

_registry = {}
def get_key(value):
    try:
        return (k for k, v in _registry.items() if v == value).next()
    except StopIteration:
        return None

hbase_registry = MagicMock()
hbase_registry.get_key.side_effect = get_key
hbase_registry.items.side_effect = _registry.items
hbase_registry.keys.side_effect = _registry.keys
hbase_registry.values.side_effect = _registry.values
hbase_registry.__getitem__.side_effect = _registry.__getitem__
hbase_registry.__setitem__.side_effect = _registry.__setitem__
hbase_registry.__delitem__.side_effect = _registry.__delitem__
