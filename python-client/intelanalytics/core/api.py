#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
API
"""

import datetime
import threading


class _ApiStatus(object):

    #_api_lock = threading.RLock()

    """tracks whether the API has been installed yet"""
    def __init__(self):
        self.__api_installed_timestamp = None
        self.__build_id = None
        self.__server_uri = None
        self.__user = None

    @property
    def is_installed(self):
        return bool(self.installed_time)

    @property
    def installed_time(self):
        #with _ApiStatus._api_lock:
        return self.__api_installed_timestamp

    @property
    def server_build_id(self):
        return str(self.__build_id)

    @property
    def user(self):
        return str(self.__user)

    @property
    def server_uri(self):
        return self.__server_uri

    def declare_installed(self, server, server_build_id):
        """declares the API as installed for the package, no turning back."""
        #with _ApiStatus._api_lock:
        self.__api_installed_timestamp = datetime.datetime.now()
        self.__build_id = server_build_id
        self.__server_uri = server._get_base_uri()
        self.__user = server.user

    def __repr__(self):
        if not self.is_installed:
            return "This client has not connected to the server yet.  Use connect() to enable this client instance."
        else:
            return "This client instance connected to server %s (build_id=%s) as user %s at %s."\
                   % (self.server_uri, self.server_build_id, self.user, self.installed_time)

    def verify_installed(self):
        if not self.is_installed:
            raise ApiNotInstalledError()

    def verify_not_installed(self):
        if self.is_installed:
            raise ApiInstalledError()


def error_if_api_installed(function):
    """decorator for methods which should raise an error if called after the API is installed"""
    from decorator import decorator

    def _error_if_api_installed(func, *args, **kwargs):
        api_status.verify_not_installed()
        return func(*args, **kwargs)

    return decorator(_error_if_api_installed, function)


class ApiNotInstalledError(RuntimeError):
    def __init__(self):
        RuntimeError.__init__(self, "API is not properly installed.  Try connecting to the server with 'connect()'.")


class ApiInstalledError(RuntimeError):
    def __init__(self):
        RuntimeError.__init__(self, "Illegal operation, API is already installed.  Must restart session.")


api_status = _ApiStatus()  # singleton

api_globals = set()  # holds all the public objects for the API
