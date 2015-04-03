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

import intelanalytics.rest.http as http
import intelanalytics.rest.config as config
import intelanalytics.connections as connections
import json


class Server(object):
    """
    Base class for server objects
    """
    _unspecified = object()  # sentinel

    @staticmethod
    def _get_value_from_config(server_name, value_name, default=_unspecified):
        try:
            server_config = getattr(connections, server_name)
            value = getattr(server_config, value_name)
        except AttributeError:
            try:
                value = getattr(config.server_defaults, value_name)
            except AttributeError:
                if default is Server._unspecified:
                    raise RuntimeError("Unable to find value '%s' for server '%s' in configuration" % (value_name, server_name))
                value = default
        return value

    def __init__(self, host, port, scheme, headers, user_name=None, user_password=None):
        self._host = host
        self._port = port
        self._scheme = scheme
        self._headers = headers
        self._user_name = user_name
        self._user_password = user_password
        self._conf_frozen = None

    def __repr__(self):
        return '{"host": "%s", "port": "%s", "scheme": "%s", "headers": "%s", "user_name": "%s", "user_password": "%s"}' \
               % (self.host, self.port, self.scheme, self.headers, self.user_name, self.user_password)

    def _error_if_conf_frozen(self):
        if self._conf_frozen:
            raise RuntimeError("This server's connection settings can no longer be modified.")

    def freeze_configuration(self):
        self._conf_frozen = True

    @property
    def host(self):
        """server host name"""
        return self._host

    @host.setter
    def host(self, value):
        self._error_if_conf_frozen()
        self._host = value

    @property
    def port(self):
        """server port number - can be None for no specification"""
        return self._port

    @port.setter
    def port(self, value):
        self._error_if_conf_frozen()
        self._port = value

    @property
    def scheme(self):
        """connection scheme, like http or https"""
        return self._scheme

    @scheme.setter
    def scheme(self, value):
        self._error_if_conf_frozen()
        self._scheme = value

    @property
    def headers(self):
        """scheme headers"""
        return self._headers

    @headers.setter
    def headers(self, value):
        self._error_if_conf_frozen()
        self._headers = value

    @property
    def user_name(self):
        return self._user_name

    @user_name.setter
    def user_name(self, value):
        self._error_if_conf_frozen()
        self._user_name = value

    @property
    def user_password(self):
        return self._user_password

    @user_password.setter
    def user_password(self, value):
        self._error_if_conf_frozen()
        self._user_password = value

    def _get_scheme_and_authority(self):
        uri = "%s://%s" % (self.scheme, self.host)
        if self.port:
            uri += ":%s" % self.port
        return uri

    def _get_base_uri(self):
        """Returns the base uri used by client as currently configured to talk to the server"""
        return self._get_scheme_and_authority()

    def create_full_uri(self, uri_path=''):
        base = self._get_base_uri()
        if uri_path.startswith(base):
            return uri_path
        if len(uri_path) > 0 and uri_path[0] != '/':
            uri_path = '/' + uri_path
        return base + uri_path

    def _check_response(self, response):
        if 400 <= response.status_code < 600 or response.status_code == 202:
                raise RuntimeError(response.text)

    def get(self, url):
        response = http.get(self, url)
        self._check_response(response)
        return response

    def post(self, url, data):
        if not isinstance(data, basestring):
            data = json.dumps(data)
        response = http.post(self, url, data)
        self._check_response(response)
        return response

    def delete(self, url):
        response = http.delete(self, url)
        self._check_response(response)
        return response

