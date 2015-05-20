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
Static Program Analysis (SPA) docstring lib
"""

from intelanalytics.meta.classnames import indent, get_type_name


def get_spa_docstring(command_def, override_rtype=None):
    """return text for a docstring needed for SPA, uses classic rst format"""
    doc_str = str(command_def.doc)

    params = command_def.parameters
    if params:
        params_text = "\n".join([get_parameter_text(p) for p in params if not p.use_self])
        doc_str += ("\n\n" + params_text)
    if command_def.return_info:
        doc_str += ("\n\n" + get_returns_text(command_def.return_info, override_rtype))

    return indent(doc_str)


def get_parameter_text(p):
    description = indent(p.doc)[4:]  # indents, but grabs the first line's space back
    if p.optional:
        description = "(default=%s)  " % (p.default if p.default is not None else "None") + description

    return ":param {name}: {description}\n:type {name}: {data_type}".format(name=p.name,
                                                                            description=description,
                                                                            data_type=get_type_name(p.data_type))


def get_returns_text(return_info, override_rtype):
    description = indent(return_info.doc)[4:]  # indents, but grabs the first line's space back
    return ":returns: {description}\n:rtype: {data_type}".format(description=description,
                                                                 data_type=get_type_name(override_rtype
                                                                                         or return_info.data_type))

