"""
Global configuration class

Provides the 'global_config' singleton
"""

from pyjavaprops import Properties
from string import Template
import os
import importlib

__all__ = ['get_global_config', 'Config', "get_keys_from_template"]

# todo: figure out the correct way to get this:
properties_file = '/'.join([os.getenv('INTEL_ANALYTICS_HOME', os.getcwd()),
                            'intel_analytics',
                            'intel_analytics.properties'])


def get_env_vars(names):
    """
    returns a dict of requested os env variables
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
    Screens a template for all the keys requires for substitution
    """
    from collections import defaultdict
    d = defaultdict(lambda: None)
    template.substitute(d)
    keys = d.keys()
    keys.sort()
    return keys


# todo: move dynamic_import to a more general utils module:
def dynamic_import(attr_path):
    """
    Dynamically imports and returns a attribute according to the given path
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

    def __init__(self, srcfile=None):
        self.srcfile = srcfile
        if srcfile is not None:
            self.load(srcfile)

    def load(self, srcfile=None):
        srcfile = srcfile or self.srcfile
        if srcfile is None:
            raise Exception('Configuration source file not specified')

        self.default_props = Properties()
        lines = []
        #with open(srcfile, 'r') as src: Pig uses jython 2.5, so can't use with
        src = open(srcfile, 'r')
        try:
            while 1:
                line = src.readline()
                if not line:
                    break
                line = line.strip()
                if len(line) > 0 and line[0] != '!' and line[0] != '#':
                    lines.append(line)
        finally:
            src.close()
        template = Template(os.linesep.join(lines))
        env_keys = get_keys_from_template(template)
        env_vars = get_env_vars(env_keys)
        lines = template.substitute(env_vars).split(os.linesep)
        self.default_props.load_lines(lines)
        self.srcfile = srcfile
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

    def __delitem__(self, key):
        del self.props[key]

    def __repr__(self):
        r = []
        items = self.props.items()
        items.sort()
        for k, v in items:
            r.append(k + "=" + v)
        return "\n".join(r)

    def raise_missing_parameters_error(self, missing):
        m = missing if isinstance(missing, str) else ", ".join(missing)
        raise Exception("Configuration file '" + self.srcfile +
                        "' is missing parameters: " + m)

# Global Config Singleton
try:
    global_config = Config(properties_file)
except Exception as e:
    import sys
    sys.stderr.write("""
WARNING - could not load default properties file {0} because:
  {1}

Global Configuration will be empty until property loaded.
Try global_config.load()
""".format(properties_file, str(e)))
    sys.stderr.flush()
    global_config = Config()
