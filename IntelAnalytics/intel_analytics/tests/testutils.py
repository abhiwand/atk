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

