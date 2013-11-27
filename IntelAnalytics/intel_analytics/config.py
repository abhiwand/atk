"""
Global configuration class

Provides the 'global_config' singleton
"""

from pyjavaprops import Properties
from StringIO import StringIO
from string import Template
import os
import time
import datetime

__all__ = ['get_global_config', 'Config', "get_keys_from_template"]

properties_file = os.path.join(
    os.getenv('INTEL_ANALYTICS_PYTHON', os.path.dirname(__file__)),
    'intel_analytics.properties')


def get_time_str():
    """
    get current time stamp
    """
    ts = time.time()
    time_str =\
        datetime.datetime.fromtimestamp(ts).strftime('_%Y-%m-%d-%H-%M-%S')
    return time_str


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
        # import importlib
        # module = importlib.import_module(module_path) --requires 2.7
        module = __import__(module_path, fromlist=[attr_name])
    except ImportError:
        raise ValueError("Could not import module '{0}'".format(module_path))
    try:
        attr = getattr(module, attr_name)
    except ImportError:
        raise ValueError("Error trying to find '{0}' in module '{1}'"
                         .format(attr_name, module_path))
    return attr


class Registry(object):
    """
    Maintains key-value string map, persisted to a file

    A dictionary that is persisted to a file, with function names
    to guide usage
    """
    def __init__(self, filename):
        self.filename = filename
        self._d = {}
        filedir = os.path.dirname(filename)
        if filedir and not os.path.exists(filedir):
            os.makedirs(filedir)
        try:
            src = open(self.filename, 'r')
        except:
            #todo: log...
            self._persist()
            return
        try:
            while 1:
                line = src.readline()
                if not line:
                    break
                line = line.strip()
                if len(line) > 0 and line[0] != '!' and line[0] != '#':
                    (k, v) = line.split('=', 1)
                    self._d[k] = v
        finally:
            src.close()

    def __repr__(self):
        out = StringIO()
        try:
            self._write_dictionary(out)
            return out.getvalue()
        finally:
            out.close()
            return ""

    def __getitem__(self, key):
        return self.get_value(key)

    def __setitem__(self, key, value):
        self.register(key, value)

    def __delitem__(self, key):
        self.unregister_key(key)

    def get_value(self, key):
        try:
            return self._d[key]
        except:
            return None

    def get_key(self, value):
        try:
            return (k for k, v in self._d.items() if v == value).next()
        except StopIteration:
            return None

    def register(self, key, value):
        self._d[key] = value
        self._persist()

    def unregister_key(self, key):
        del self._d[key]
        self._persist()

    def unregister_value(self, value):
        key = self.get_key(value)
        if key is not None:
            del self._d[key]
            self._persist()

    def replace_value(self, old_value, new_value):
        key = self.get_key(old_value)
        self.register(key, new_value)

    def keys(self):
        return self._d.keys()

    def values(self):
        return self._d.values()

    def items(self):
        return self._d.items()

    # todo: make persist_map and load_map thread-safe
    def _persist(self):
        try:
            dst = open(self.filename, 'w')
            try:
                self._write_dictionary(dst)
            finally:
                dst.close()
        except IOError:
            #todo: log...
            raise Exception("Could not open names file for writing.  " +
                            "Check permissions for: " + self.filename)

    def _write_dictionary(self, out):
        for k, v in sorted(self._d.items()):
            out.write(k)
            out.write('=')
            out.write(v)
            out.write(os.linesep)


class Config(object):

    def __init__(self, srcfile=None):
        self.srcfile = srcfile
        self.default_props = {}
        self.props = {}
        if srcfile is not None:
            self.load(srcfile)

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

    def load(self, srcfile=None):
        """
        Initializes the config from a file

        If srcfile is None, the previously loaded file is reloaded.
        """
        srcfile = srcfile or self.srcfile
        if srcfile is None:
            raise Exception('Configuration source file not specified')

        lines = []
        #with open(srcfile, 'r') as src: Pig uses jython 2.5, so can't use with
        src = open(srcfile, 'r')
        try:
            while 1:
                # not the most efficient algo, but need to strip comments first
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
        default_props = Properties()
        default_props.load_lines(lines)
        # delay assignment to self until now, after error possibilities
        self.default_props = default_props
        self.srcfile = srcfile
        self.reset()

    def reset(self):
        """
        Restores the config to the last file load
        """
        self.props = dict(self.default_props)

    def verify(self, keys):
        """
        Verifies the config contains the given keys; raises Exception otherwise
        """
        missing = []
        for k in keys:
            if k not in self.props:
                missing.append(k)
        if len(missing) > 0:
            raise Exception("Configuration " +
                            "based on file '" + (self.srcfile or "(None)") +
                            "' is missing parameters: " + ", ".join(missing))

    def verify_template(self, template):
        """
        Verifies config contains the keys necessary to satisfy given template
        """
        keys = get_keys_from_template(template)
        self.verify(keys)


# Global Config Singleton
try:
    global_config = Config(properties_file)
except Exception, e:
    import sys
    sys.stderr.write("""
WARNING - could not load default properties file {0} because:
  {1}

Global Configuration will be empty until property loaded.
Try global_config.load()
""".format(properties_file, str(e)))
    sys.stderr.flush()
    global_config = Config()
