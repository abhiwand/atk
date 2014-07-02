##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
Command objects
"""


def docstub(f):
    """
    Marks the function as a documentation stub that exists only to facilitate
    generation of static Python html docs. The implementation, if any, is replaced
    at runtime by the standard command dispatch logic.
    :param f: the function
    :return: the function, annotated so that the command dispatch logic knows it is
                safe to replace it.
    """
    f.docstub = True
    return f

class CommandSupport(object):

    def __init__(self):
        functions = getattr(self.__class__, "_commands", dict())
        for ((intermediates, name), function) in functions.items():
            current = self.__class__
            for inter in intermediates:
                if not hasattr(current, inter):
                    def make(current = current, inter = inter):
                        class Holder:
                            pass
                        return Holder
                    holder = make()
                    setattr(current, inter, holder)
                else:
                    holder = getattr(current, inter)
                current = holder
            if not hasattr(current, name):
                print "Installing", name
                setattr(current, name, function)
            else:
                f = getattr(current, name)
                if hasattr(f, "docstub"):
                    function.__doc__ = f.__doc__
                    delattr(current, name)
                    setattr(current, name, function)
                    print "Installing (with documentation copied from stub):", name
                else:
                    print "Skipping installation of", name, "method already exists and is not a stub"
