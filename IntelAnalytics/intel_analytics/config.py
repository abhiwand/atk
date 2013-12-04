"""
The global configuration class.

Provides the 'global_config' singleton.
"""

from pyjavaprops import Properties
from string import Template
import os
import importlib

__all__ = ['global_config', 'Config', "get_keys_from_template"]

# todo: figure out the correct way to get this:
properties_file = '/'.join([os.getenv('INTEL_ANALYTICS_HOME', os.getcwd()),
                            'intel_analytics',
                            'intel_analytics.properties'])

def get_env_vars(names):
    """
    Returns a dictionary of the requested OS env variables.
    """
    env_vars = {}
    missing = []
    for name in names:
        value = os.environ.get(name)
        if value is None:
            missing.append(name)
        else:
            env_vars[name] = value
    if len(missing) > 0:
        raise Exception("Environment vars not set: " + ", ".join(missing))
    return env_vars

# todo: move get_keys_from_template to a more general utils module:
def get_keys_from_template(template):
    """
    Screens a template for all of the keys required for substitution.
    """
    from collections import defaultdict
    d = defaultdict(lambda : None)
    template.substitute(d)
    keys = d.keys()
    keys.sort()
    return keys

# todo: move dynamic_import to a more general utils module:
def dynamic_import(attr_path):
    """
    Dynamically imports and returns an attribute according to the given path.
    """
    module_path, attr_name = attr_path.rsplit(".", 1)

    try:
        module = importlib.import_module(module_path)
    except ImportError:
        raise ValueError("Could not import module '{0}'".format(module_path))
    try:
        attr = getattr(module, attr_name)
    except ImportError:
        raise ValueError("Error trying to find '{0}' in module '{1}'"
        .format(attr_name, module_path))
    return attr


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

