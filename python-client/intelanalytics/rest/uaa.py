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

import logging
logger = logging.getLogger(__name__)
import intelanalytics.rest.http as http
from intelanalytics.rest.server import Server


class UaaServer(Server):
    """
    Handles communication with CF UAA and keeping an active token
    """
    _server_name = "oauth_server"

    _scheme_default = "https"
    _headers_default = {"Accept": "application/json"}

    def __init__(self, host, port, scheme, headers, user_name, user_password, client_name, client_password):
        super(UaaServer, self).__init__(host, port, scheme, headers, user_name=user_name, user_password=user_password)
        self.scheme = scheme or self._scheme_default
        self.headers = headers or self._headers_default
        self.client_name = client_name
        self.client_password = client_password

    def _repr_attrs(self):
        """overrides base indicating all the attributes which should be printed for the repr"""
        return super(UaaServer, self)._repr_attrs() + ["client_name", "client_password"]

    def is_enabled(self):
        """Indicates if the server info has enough config to be considered "enabled"."""
        return bool(self.host)

    def get_token(self):
        """
        Connect to the cloudfoundry uaa server and acquire token

        Calling this method is required before invoking any ATK operations. This method connects to UAA server
        and validates the user and the client and returns an token that will be passed in the rest server header
        """
        # Authenticate to UAA as client (this client)
        # Tell UAA to grant us (the client) a token by authenticating with the user's password
        if self.is_enabled():
            data = {'grant_type': "password", 'username': self.user_name, 'password': self.user_password}
            auth = (self.client_name, self.client_password)
            response = http.post(self, "/oauth/token", headers=self.headers, auth=auth, data=data)
            self._check_response(response)
            token = response.json()['access_token']
            return token
        else:
            return None
