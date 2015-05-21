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
"""
Command Definition and subservient objects
"""

import logging
logger = logging.getLogger('meta')
from collections import namedtuple

from intelanalytics.meta.installpath import InstallPath
from intelanalytics.meta.names import default_value_to_str, is_entity_constructor_command_name


Parameter = namedtuple("Parameter", ['name', 'data_type', 'use_self', 'optional', 'default', 'doc'])

ReturnInfo = namedtuple("Returns", ['data_type', 'use_self', 'doc'])

Version = namedtuple("Version", ['added', 'changed', 'deprecated', 'doc'])


class Doc(object):
    """Represents descriptive text for an object, but not its individual pieces"""

    def __init__(self, one_line='<Missing Doc>', extended=''):  # todo add examples!!
        self.one_line = one_line.strip()
        self.extended = extended

    def __str__(self):
        r = self.one_line
        if self.extended:
            r += ("\n\n" + self.extended)
        return r

    @staticmethod
    def _pop_blank_lines(lines):
        while lines and not lines[0].strip():
            lines.pop(0)

    @staticmethod
    def get_from_str(doc_str):
        if doc_str:
            lines = doc_str.split('\n')

            Doc._pop_blank_lines(lines)
            summary = lines.pop(0).strip() if lines else ''
            Doc._pop_blank_lines(lines)
            if lines:
                margin = len(lines[0]) - len(lines[0].lstrip())
                extended = '\n'.join([line[margin:] for line in lines])
            else:
                extended = ''

            if summary:
                return Doc(summary, extended)

        return Doc("<Missing Doc>", doc_str)


class CommandDefinition(object):
    """Defines a Command"""

    def __init__(self, json_schema, full_name, parameters=None, return_info=None, is_property=False, doc=None, maturity=None, version=None):
        self.json_schema = json_schema
        self.full_name = full_name
        parts = self.full_name.split('/')
        self.entity_type = parts[0]
        if not self.entity_type:
            raise ValueError("Invalid empty entity_type, expected non-empty string")
        self.intermediates = tuple(parts[1:-1])
        self.name = parts[-1]
        self.install_path = InstallPath(full_name[:-(len(self.name)+1)])
        self.parameters = parameters if parameters else []
        self.return_info = return_info
        self.is_property = is_property
        self.maturity = maturity
        self.version = version
        self._doc = None  # handle type conversion in the setter, next line
        self.doc = doc

    @property
    def doc(self):
        return self._doc

    @doc.setter
    def doc(self, value):
        if value is None:
            self._doc = Doc()
        elif isinstance(value, basestring):
            self._doc = Doc.get_from_str(value)
        elif isinstance(value, Doc):
            self._doc = value
        else:
            raise TypeError("Received bad type %s for doc, expected type %s or string" % (type(value), Doc))

    @property
    def is_constructor(self):
        return is_entity_constructor_command_name(self.name) or self.name == "__init__"

    @property
    def function_name(self):
        return "__init__" if self.is_constructor else self.name

    @property
    def doc_name(self):
        return '.'.join(list(self.intermediates) + [self.function_name])

    def __repr__(self):
        return "\n".join([self.full_name,
                          "\n".join([repr(p) for p in self.parameters]) if self.parameters else "<no parameters>",
                          repr(self.return_info),
                          repr(self.version),
                          repr(self.doc)])

    def get_return_type(self):
        return None if self.return_info is None else self.return_info.data_type

    def get_function_args_text(self):
        if self.parameters:
            return ", ".join(['self' if param.use_self else
                              param.name if not param.optional else
                              "%s=%s" % (param.name, default_value_to_str(param.default))
                              for param in self.parameters])
        else:
            return ''
