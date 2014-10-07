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


def get_api_decorator(logger):

    # Note: extra whitespace lines in the code below is intentional for pretty-printing when error occurs
    def _api(function, *args, **kwargs):
        log_api_call(function)
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
            return decorator(_api, item)
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
            from intelanalytics.core.metaprog import install_command_defs
            logger.info("Requesting available commands from server")
            response = http.get("commands/definitions")
            commands_json_schema = response.json()
            # ensure the assignment to __commands_from_backend is the last line in this 'if' block before the fatal try:
            _Api.__commands_from_backend = [get_command_def(c) for c in commands_json_schema]
            try:
                install_command_defs(_Api.__commands_from_backend)
                from intelanalytics import _refresh_api_namespace
                _refresh_api_namespace()
            except Exception as e:
                raise FatalApiLoadError(e)

    @staticmethod
    def is_loaded():
        return _Api.__commands_from_backend is not None


load_api = _Api.load_api  # create alias for export


class FatalApiLoadError(RuntimeError):
    def __init__(self, e):
        self.details = str(e)
        RuntimeError(self, """
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
