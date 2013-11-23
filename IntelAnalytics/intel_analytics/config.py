"""
Global configuration class

Provides the 'global_config' singleton
"""

from pyjavaprops import Properties
from string import Template
import os

__all__ = ['global_config', 'Config', "get_keys_from_template"]

properties_file = os.path.join(
    os.getenv('INTEL_ANALYTICS_PYTHON', os.path.dirname(__file__)),
    'intel_analytics.properties')


def get_env_vars(names):
    """
    returns a dict of requested os env variables
    """
    vars = {}
    missing = []
    for name in names:
        value = os.environ.get(name)
        if value is None:
            missing.append(name)
        else:
            vars[name] = value
    if len(missing) > 0:
        raise Exception("Environment vars not set: " + ", ".join(missing))
    return vars

def get_keys_from_template(template):
    # pull the required keys from the template
    from collections import defaultdict
    d = defaultdict(lambda : None)
    template.substitute(d)
    keys = d.keys()
    keys.sort()
    return keys


class Config(object):

    def __init__(self, srcfile=properties_file):
        self.srcfile = srcfile
        self.load_defaults(srcfile)

    def load_defaults(self, srcfile=None):
        if srcfile is None:
            srcfile=self.srcfile
        self.default_props = Properties()
        #with open(srcfile, 'r') as src: Pig uses jython 2.5, so with is not there!
        src = open(srcfile, 'r')
        try:
            template = Template(src.read())
            env_keys = get_keys_from_template(template)
            env_vars = get_env_vars(env_keys)
            lines = template.substitute(env_vars).split(os.linesep)
            self.default_props.load_lines(lines)
        finally:
            src.close()
        
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

