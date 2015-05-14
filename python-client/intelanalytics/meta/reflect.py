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

from intelanalytics.meta.classnames import is_name_private
import inspect


def get_args_text_from_function(function, ignore_self=False, ignore_private_args=False):
    args, kwargs, varargs, varkwargs = get_args_spec_from_function(function, ignore_self, ignore_private_args)
    args_text = ", ".join(args + ["%s=%s" % (name, default_value_to_str(value)) for name, value in kwargs])
    if varargs:
        args_text += (', *' + varargs)
    if varkwargs:
        args_text += (', **' + varkwargs)
    return args_text


def get_args_spec_from_function(function, ignore_self=False, ignore_private_args=False):
    args, varargs, varkwargs, defaults = inspect.getargspec(function)
    if ignore_self:
        args = [a for a in args if a != 'self']

    num_defaults = len(defaults) if defaults else 0
    if num_defaults:
        kwargs = zip(args[-num_defaults:], defaults)
        args = args[:-num_defaults]
    else:
        kwargs = []

    if ignore_private_args:
        args = [name for name in args if not is_name_private(name)]
        kwargs = [(name, value) for name, value in kwargs if not is_name_private(name)]
    return args, kwargs, varargs, varkwargs


def default_value_to_str(value):
    return value if value is None or type(value) not in [str, unicode] else "'%s'" % value

