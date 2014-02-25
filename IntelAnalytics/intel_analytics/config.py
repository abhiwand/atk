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
"""

Provides the 'global_config' singleton.
"""

from pyjavaprops import Properties
from StringIO import StringIO
from string import Template
import os
import time
import datetime
import platform
import sys

__all__ = ['get_global_config', 'Config', "get_keys_from_template"]

_here_folder = os.path.abspath(os.path.join(os.path.dirname(__file__)))

if not os.getenv('INTEL_ANALYTICS_PYTHON'):
    #If this file is running, we must know where to find our python files...
    os.environ['INTEL_ANALYTICS_PYTHON'] = _here_folder

if not os.getenv('INTEL_ANALYTICS_HOME'):
    #If we can find a conf folder from here, that's probably home.
    maybe_home = os.path.abspath(os.path.join(_here_folder, ".."))
    maybe_conf = os.path.join(maybe_home, "conf")
    if os.path.exists(maybe_conf):
        os.environ['INTEL_ANALYTICS_HOME'] = maybe_home
    elif "virtpy" in maybe_home:
        maybe_home = maybe_home[:maybe_home.index("virtpy")]
        if os.path.exists(maybe_home):
            os.environ['INTEL_ANALYTICS_HOME'] = maybe_home

if not os.getenv('HOSTNAME'):
    os.environ['HOSTNAME'] = platform.node()

if not os.getenv('HADOOP_HOME'):
    if os.path.exists('/home/hadoop/IntelAnalytics/hadoop'):
        os.environ['HADOOP_HOME'] = '/home/hadoop/IntelAnalytics/hadoop'

if not os.getenv('TITAN_HOME'):
    if os.path.exists('/home/hadoop/IntelAnalytics/titan-server'):
        os.environ['TITAN_HOME'] = '/home/hadoop/IntelAnalytics/titan-server'

if not os.getenv('PIG_OPTS'):
    os.environ['PIG_OPTS'] = "-Dpython.verbose=error"#to get rid of Jython logging

if not os.getenv('SPARK_HOME'):
    os.environ['SPARK_HOME'] = '/home/hadoop/IntelAnalytics/spark'

if not os.getenv('MASTER'):
    os.environ['MASTER'] = 'spark://master:7077'

properties_file = os.path.join(
        os.getenv('INTEL_ANALYTICS_HOME', _here_folder),
        'conf',
        'intel_analytics.properties')

sys.path.append(os.path.join(os.getenv('SPARK_HOME'), 'python'))

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
    d = defaultdict(lambda: None)
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
    module = __import__(module_path, fromlist=[attr_name])
    attr = getattr(module, attr_name)
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
        return self._d[key]

    def get_key(self, value):
        try:
            return (k for k, v in self._d.items() if v == value).next()
        except StopIteration:
            raise ValueError

    def has_value(self, value):
        return value in self._d.values()

    def register(self, key, value):
        self._d[key] = value
        self._persist()

    def unregister_key(self, key):
        if key in self._d[key]:
            del self._d[key]
            self._persist()

    def unregister_value(self, value):
        try:
            key = self.get_key(value)
        except ValueError:
            pass
        else:
            del self._d[key]
            self._persist()

    def replace_value(self, old_value, new_value):
        key = self.get_key(old_value)
        self._d[key] = new_value
        self._persist()

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

    os.environ["JYTHONPATH"] = global_config['pig_jython_path']#required to ship jython scripts with pig
except Exception, e:
    sys.stderr.write("""
WARNING - could not load default properties file %s because:
  %s

Global Configuration will be empty until property loaded.
Try global_config.load()
""" % (properties_file, e))
    sys.stderr.flush()
    global_config = Config()

