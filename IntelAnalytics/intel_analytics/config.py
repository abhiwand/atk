"""
Global configuration class

Provides the 'global_config' singleton
"""

from pyjavaproperties import Properties
import os

__all__ = ['global_config', 'Config', "get_keys_from_template"]


# todo: figure out the correct way to get these next two:
id = 'user0'

properties_file = '/'.join([os.getenv('INTEL_ANALYTICS_HOME', os.getcwd()),
                            'intel_analytics',
                            'intel_analytics.properties'])

class Config(object):

    def __init__(self, srcfile=properties_file):
        self.srcfile = srcfile
        self.load_defaults(srcfile)

    def load_defaults(self, srcfile=None):
        if srcfile is None:
            srcfile=self.srcfile
        self.default_props = Properties()
        with open(srcfile, 'r') as src:
            self.default_props.load(src)
        self.reset()

    def reset(self):
        self.props = dict(self.default_props)

    def verify(self, keys):
        missing = []
        for k in keys:
            if k not in self.props:
                missing.append(k)
        if len(missing) > 0:
            self.raise_missing_parameters_error(missing)

    def verify_template(self, template):
        keys = get_keys_from_template(template)
        self.verify(keys)

    def __getitem__(self, item):
        return self.props[item]

    def __setitem__(self, key, value):
        self.props[key] = value

    def __repr__(self):
        r = []
        items = self.props.items()
        items.sort()
        for k,v in items:
            r.append(k + "=" + v)
        return "\n".join(r)

    def raise_missing_parameters_error(self, missing):
        m = missing if isinstance(missing, str) else ", ".join(missing)
        raise Exception("Configuration file '" + self.srcfile +
                        "' is missing parameters: " + m )

global_config = Config()

def get_keys_from_template(template):
    # pull the required keys from the template
    from collections import defaultdict
    d = defaultdict(lambda : None)
    template.substitute(d)
    keys = d.keys()
    keys.sort()
    return keys

