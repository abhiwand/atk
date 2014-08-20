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

import logging
logger = logging.getLogger(__name__)

from collections import namedtuple
from intelanalytics.core.npdoc import get_numpy_doc


Parameter = namedtuple("Parameter", ['name', 'data_type', 'use_self', 'optional', 'default', 'doc'])

Return = namedtuple("Return", ['data_type', 'use_self', 'doc'])

Version = namedtuple("Version", ['added', 'changed', 'deprecated', 'doc'])

Doc = namedtuple("Doc", ['one_line_summary', 'extended_summary'])


class CommandDefinition(object):
    """Defines a Command"""

    def __init__(self, json_schema, full_name, parameters, return_type, doc=None, version=None):
        self.json_schema = json_schema
        self.full_name = full_name
        parts = self.full_name.split('/')
        self.prefix = parts[0]
        self.intermediates = tuple(parts[1:-1])
        self.name = parts[-1]
        self.parameters = parameters
        self.return_type = return_type
        self.version = version
        # do doc last, so we can send populated self to create CommandNumpydoc
        self.doc = '' if doc is None else get_numpy_doc(self, doc.one_line_summary, doc.extended_summary)


    def __repr__(self):
        return "\n".join([self.full_name,
                          "\n".join([repr(p) for p in self.parameters]),
                          repr(self.return_type),
                          repr(self.version),
                          self.doc])


def validate_arguments(arguments, parameters):
    """
    Returns validated and possibly re-cast arguments

    Use parameter definitions to make sure the arguments conform.  This function
    is closure over in the dynamically generated execute command function
    """
    validated = {}
    for (k, v) in arguments.items():
        try:
            parameter = [p for p in parameters if p.name == k][0]
        except IndexError:
            raise ValueError("No parameter named '%s'" % k)
        validated[k] = v
        if parameter.use_self:
            validated[k] = v._get_id()
        if parameter.data_type is list:
            if isinstance(v, basestring) or not hasattr(v, '__iter__'):
                validated[k] = [v]
    return validated


# TODO - Remove the rest of this file (it moved to metaprog.py)

from types import MethodType


class Holder(object):
    """
    Base class for intermediate objects created for method namespacing
    """
    pass


class CommandSupport(object):
    """
    Base class for classes that need to have methods dynamically installed
    based on server plugins. Available commands are converted to methods and
    installed into the class.
    """

    def __init__(self):
        functions = getattr(self.__class__, "_commands", dict())
        for ((intermediates, name), function) in functions.items():
            added_to_class = None
            current = self
            for inter in intermediates:
                if not hasattr(current, inter):
                    setattr(current, inter, Holder())
                holder = getattr(current, inter)
                current = holder
            if current == self:
                if not hasattr(self.__class__, name):
                    setattr(self.__class__, name, function)
                    added_to_class = "new function"
                else:
                    f = getattr(self.__class__, name)
                    if hasattr(f, "doc_stub"):
                        function.__doc__ = f.__doc__
                        delattr(self.__class__, name)
                        setattr(self.__class__, name, function)
                        added_to_class = "doc_stub replacement"
            else:
                method = MethodType(function, self, self.__class__)
                current.__dict__[name] = method
                added_to_class = "new function for intermediate"
            if added_to_class:
                logger.debug("(Old Way) Added function %s to class %s as %s", name, current.__class__, added_to_class)
