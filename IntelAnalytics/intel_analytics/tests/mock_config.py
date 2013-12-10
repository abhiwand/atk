# todo: expand this later, when config settles down

from collections import defaultdict
from mock import Mock, MagicMock

dynamic_import = Mock()
get_time_str = Mock()

dd = defaultdict(lambda: 'mocked')
def getitem(key):
    return dd[key]

def setitem(key, value):
    dd[key] = value

def delitem(key):
    del dd[key]

global_config = MagicMock()
global_config.__getitem__.side_effect = getitem
global_config.__setitem__.side_effect = setitem
global_config.__delitem__.side_effect = delitem
global_config.verify.return_value = True
