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
from intelanalytics.meta.npdoc import get_numpy_doc


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
        self.entity_type = parts[0]
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
