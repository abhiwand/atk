##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
