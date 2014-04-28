
# settings must match IJ's Python Remote Debug
host = 'localhost'
port = 9119

# use the 'requests' package as a reference to "site-packages"
# pycharm-debug.egg must be in the same folder as the 'requests' package
#import requests
#import os
#dirname = os.path.dirname
#pycharm_debug_egg =\
#    os.path.join(dirname(dirname(requests.__file__)), 'pycharm-debug.egg')
pycharm_debug_egg = "/home/blbarker/ij13/pycharm-3.1.1/pycharm-debug.egg"



import sys
if pycharm_debug_egg not in sys.path:
    sys.path.append(pycharm_debug_egg)
import pydevd
print "Using " + pycharm_debug_egg
import string


def start(host_name=None, port_number=None):
    host_name = host if host_name is None else host_name
    port_number = port if port_number is None else port_number
    print "Connecting to debugger at {0}:{1}".format(host_name, port_number)
    pydevd.settrace(host_name, port=port_number, stdoutToServer=True, stderrToServer=True)


def show_hex(byte_array, out=None):
    """
    Prints byte array in hex display format, matching that of xxd
    (try :%!xxd in vi editor)
    """
    out = out if out else sys.stdout
    line = ['.'] * 16
    i = 0
    for by in byte_array:
        if i % 16 == 0:
            out.write(" {0}\n{1:07x}: ".format("".join(line), i))
        out.write("{0:02x}".format(by))
        c = chr(by)
        line[i % 16] = c if (c in string.printable and (not c.isspace() or c == ' ')) else '.'
        if i % 2 == 1:
            out.write(" ")
        i += 1
    leftover = i % 16
    if leftover != 0:
        out.write(" " * (41 - leftover*2 - (leftover >> 1)))
        out.write("".join(line[:leftover]))
    out.write("\n")
    out.flush()
