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
Intel Analytics API management
"""
import logging
logger = logging.getLogger(__name__)

import inspect
from intelanalytics.core.errorhandle import IaError
from intelanalytics.core.loggers import log_api_call
from decorator import decorator


class _ApiGlobals(set):
    def __init__(self, *args, **kwargs):
        set.__init__(self, *args, **kwargs)

api_globals = _ApiGlobals()  # holds generated items that should be added to the API


class _ApiCallStack(object):

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


def get_api_decorator(logger):
    """Provides an API decorator which will wrap functions designated as an API"""

    # Note: extra whitespace lines in the code below is intentional for pretty-printing when error occurs
    def _api(function, *args, **kwargs):
        with api_context(logger, 4, function, *args, **kwargs):
            try:
                check_api_is_loaded()
                return function(*args, **kwargs)
            except:
                error = IaError(logger)



                raise error  # see intelanalytics.errors.last for details



    def api(item):
        if inspect.isclass(item):
            api_globals.add(item)
            return item
        else:
            # must be a function, wrap with logging, error handling, etc.
            d = decorator(_api, item)
            d.is_api = True  # add 'is_api' attribute for meta query
            return d
    return api


class _Api(object):
    __commands_from_backend = None

    @staticmethod
    def load_api():
        """
        Download API information from the server, once.

        After the API has been loaded, it cannot be changed or refreshed.  Also, server connection
        information cannot change.  User must restart Python in order to change connection info.

        Subsequent calls to this method invoke no action.
        """
        if not _Api.is_loaded():
            from intelanalytics.rest.connection import http
            from intelanalytics.rest.jsonschema import get_command_def
            from intelanalytics.meta.metaprog import install_command_defs, delete_docstubs
            logger.info("Requesting available commands from server")
            response = http.get("commands/definitions")
            commands_json_schema = response.json()
            # ensure the assignment to __commands_from_backend is the last line in this 'if' block before the fatal try:
            _Api.__commands_from_backend = [get_command_def(c) for c in commands_json_schema]
            try:
                install_command_defs(_Api.__commands_from_backend)
                delete_docstubs()
                from intelanalytics import _refresh_api_namespace
                _refresh_api_namespace()
            except Exception as e:
                _Api.__commands_from_backend = None
                raise FatalApiLoadError(e)

    @staticmethod
    def is_loaded():
        return _Api.__commands_from_backend is not None


load_api = _Api.load_api  # create alias for export


class FatalApiLoadError(RuntimeError):
    def __init__(self, e):
        self.details = str(e)
        RuntimeError.__init__(self, """
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Fatal error: processing the loaded API information failed and has left the
client in a state of unknown compatibility with the server.

Please contact support.

Details:%s
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""" % self.details)


class ApiAlreadyLoadedError(RuntimeError):
    def __init__(self):
        RuntimeError.__init__(self, "Invalid operation because the client is already connected to "
                                    "the intelanalytics server.  Restarting Python is required "
                                    "if you need to perform this operation")


class ApiNotYetLoadedError(RuntimeError):
    def __init__(self):
        RuntimeError.__init__(self, "Please connect to the server by calling 'connect()' before proceeding")


def check_api_is_loaded():
    if not _Api.is_loaded():
        raise ApiNotYetLoadedError()


# decorator for methods that are invalid after api load, like changing server config
def _error_if_api_already_loaded(function, *args, **kwargs):
    if _Api.is_loaded():
        raise ApiAlreadyLoadedError()
    return function(*args, **kwargs)


def error_if_api_already_loaded(function):
    return decorator(_error_if_api_already_loaded, function)


# methods to dump the names in the API (driven by QA coverage)
def get_api_names(obj, prefix=''):
    from intelanalytics.meta.metaprog import get_intermediate_property_class
    found = []
    for name in dir(obj):
        if not name.startswith("_"):
            a = getattr(obj, name)
            suffix = _get_inheritance_suffix(obj, name)
            if isinstance(a, property):
                intermediate_class = get_intermediate_property_class(a)
                if intermediate_class:
                    found.extend(get_api_names(intermediate_class, prefix + name + "."))
                a = a.fget
            if hasattr(a, "is_api"):
                found.append(prefix + name + suffix)
            elif inspect.isclass(a):
                found.extend(get_api_names(a, prefix + name + "."))
    return found


def _get_inheritance_suffix(obj, member):
    if inspect.isclass(obj):
        if member in obj.__dict__:
            return ''
        heritage = inspect.getmro(obj)[1:]
        for base_class in heritage:
            if member in base_class.__dict__:
                return "   (inherited from %s)" % base_class.__name__
    return ''
