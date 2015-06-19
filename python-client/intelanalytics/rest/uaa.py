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


def _get_uaa_server(uaa_uri):
    scheme = Server._get_value_from_config('uaa_scheme')
    headers = Server._get_value_from_config('uaa_headers')
    return Server(uaa_uri, scheme, headers)


def get_oauth_token(uaa_uri, user_name, user_password):
    """
    Connect to the cloudfoundry uaa server and acquire token

    Calling this method is required before invoking any ATK operations. This method connects to UAA server
    and validates the user and the client and returns an token that will be passed in the rest server header
    """
    # Authenticate to UAA as client (this client)
    # Tell UAA to grant us (the client) a token by authenticating with the user's password
    if uaa_uri:
        uaa_server = _get_uaa_server(uaa_uri)
        client_name = Server._get_value_from_config('client_name')
        client_password = Server._get_value_from_config('client_password')
        auth = (client_name, client_password)
        data = {'grant_type': "password", 'username': user_name, 'password': user_password}
        response = http.post(uaa_server, "/oauth/token", auth=auth, data=data)
        uaa_server._check_response(response)
        token_type = response.json()['token_type']
        token = response.json()['access_token']
        refresh_token = response.json()['refresh_token']
        #print "refresh_token=%s" % refresh_token
        #print "token_type=%s" % token_type
        return token_type, token, refresh_token
    return None, None, None


def get_refreshed_oauth_token(uaa_uri, refresh_token):
    """
    Connect to the cloudfoundry uaa server and acquire access token using a refresh token
    """
    # Authenticate to UAA as client (this client)
    # Tell UAA to grant us (the client) a token using a refresh token
    if uaa_uri:
        uaa_server = _get_uaa_server(uaa_uri)
        client_name = Server._get_value_from_config('client_name')
        client_password = Server._get_value_from_config('client_password')
        auth = (client_name, client_password)
        data = {'grant_type': 'refresh_token', 'refresh_token': refresh_token }
        response = http.post(uaa_server, "/oauth/token", auth=auth, data=data)
        uaa_server._check_response(response)
        token_type = response.json()['token_type']
        token = response.json()['access_token']
        refresh_token = response.json()['refresh_token']
        return token_type, token, refresh_token
    return None, None, None
