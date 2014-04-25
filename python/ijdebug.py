
# settings must match IJ's Python Remote Debug
host = 'localhost'
port = 9119

# use the 'requests' package as a reference to "site-packages"
# pycharm-debug.egg must be in the same folder as the 'requests' package
import requests
import os
dirname = os.path.dirname
pycharm_debug_egg =\
    os.path.join(dirname(dirname(requests.__file__)), 'pycharm-debug.egg')
print "Using " + pycharm_debug_egg


import sys
if pycharm_debug_egg not in sys.path:
    sys.path.append(pycharm_debug_egg)
import pydevd


def start(host_name=None, port_number=None):
    host_name = host if host_name is None else host_name
    port_number = port if port_number is None else port_number
    pydevd.settrace(host_name, port=port_number, stdoutToServer=True, stderrToServer=True)
