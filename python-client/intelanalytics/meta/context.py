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
API context
"""

import logging
logger = logging.getLogger('meta')

from decorator import decorator

from intelanalytics.core.errorhandle import IaError
from intelanalytics.core.loggers import log_api_call
from intelanalytics.core.api import api_status


class _ApiCallStack(object):
    """Tracks call depth in the call from the API"""

    def __init__(self):
        self._depth = 0

    @property
    def is_empty(self):
        return self._depth == 0

    def inc(self):
        self._depth += 1

    def dec(self):
        self._depth -= 1
        if self._depth < 0:
            self._depth = 0
            raise RuntimeError("Internal error: API call stack tracking went below zero")

_api_call_stack = _ApiCallStack()


class ApiCallLoggingContext(object):

    def __init__(self, call_stack, call_logger, call_depth, function, *args, **kwargs):
        self.logger = None
        self.call_stack = call_stack
        if self.call_stack.is_empty:
            self.logger = call_logger
            self.depth = call_depth
            self.function = function
            self.args = args
            self.kwargs = kwargs

    def __enter__(self):
        if self.call_stack.is_empty:
            log_api_call(self.depth, self.function, *self.args, **self.kwargs)
        self.call_stack.inc()

    def __exit__(self, exception_type, exception_value, traceback):
        self.call_stack.dec()
        if exception_value:
            error = IaError(self.logger)


            raise error  # see intelanalytics.errors.last for details



def api_context(logger, depth, function, *args, **kwargs):
    global _api_call_stack
    return ApiCallLoggingContext(_api_call_stack, logger, depth, function, *args, **kwargs)


def get_api_context_decorator(execution_logger, call_depth=4):
    """Parameterized decorator which will wrap function for execution in the api context"""

    depth = call_depth
    exec_logger = execution_logger
    verify_api_installed = api_status.verify_installed

    # Note: extra whitespace lines in the code below is intentional for pretty-printing when error occurs
    def execute_in_api_context(function, *args, **kwargs):
        with api_context(logger, depth, function, *args, **kwargs):
            try:
                verify_api_installed()
                return function(*args, **kwargs)
            except:
                error = IaError(exec_logger)



                raise error  # see intelanalytics.errors.last for python details




    def execute_in_api_context_decorator(function):
        return decorator(execute_in_api_context, function)

    return execute_in_api_context_decorator
