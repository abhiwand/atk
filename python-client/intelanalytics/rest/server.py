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

import intelanalytics.rest.http as http
import intelanalytics.rest.config as config
import json


_connections_module = None # global singleton, indicating the module to use for connection config


class Server(object):
    """
    Base class for server objects
    """
    _unspecified = object()  # sentinel

    @staticmethod
    def _get_value_from_config(server_name, value_name, default=_unspecified):
        global _connections_module
        if not _connections_module:
            try:
                import intelanalytics.connections as _connections_module
            except:
                _connections_module = config
        try:
            server_config = getattr(_connections_module, server_name)
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

    def _repr_attrs(self):
        return ["host", "port", "scheme", "headers", "user_name", "user_password"]

    def __repr__(self):
        d = dict([(a, getattr(self, a)) for a in self._repr_attrs()])
        return json.dumps(d, cls=ServerJsonEncoder, sort_keys=True, indent=2)

    @staticmethod
    def _str_truncate_value(v):
        """helper method for __str__"""
        s = str(v)
        if len(s) > 58:
            return s[:58] + "..."
        return s

    def __str__(self):
        """This is a friendlier str representation, which truncates long lines"""
        import json
        d = json.loads(repr(self))
        line = "\n------------------------------------------------------------------------------\n"
        return line + "\n".join(["%-15s: %s" % (k, self._str_truncate_value(v)) for k, v in sorted(d.items())]) + line

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
        if uri_path.startswith('http'):
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


class ServerJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Server):
            return json.loads(repr(obj))
        return json.JSONEncoder.default(self, obj)
