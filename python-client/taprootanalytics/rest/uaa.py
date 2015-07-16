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
import taprootanalytics.rest.http as http
from taprootanalytics.rest.server import Server


def get_oauth_credentials(uri, user_name, user_password):
    """
    Get the credentials needed to communicate using OAuth

    uri: URI of ATK server or the UAA server itself
    """
    try:
        oauth_info = _get_oauth_server_info(uri)  # try using ATK URI first
    except:
        oauth_info = {'uri': uri}
        logger.info("Using UAA uri %s provided explicitly." % uri)
    else:
        logger.info("Using UAA uri %s obtained from ATK server." % oauth_info['uri'])

    token_type, token, refresh_token = _get_oauth_token(oauth_info, user_name, user_password)

    return {'user': user_name,
            'oauth_uri': oauth_info['uri'],
            'token_type': token_type,
            'token': token,
            'refresh_token': refresh_token}


def get_refreshed_oauth_token(uaa_uri, refresh_token):
    """
    Connect to the cloudfoundry uaa server and acquire access token using a refresh token
    """
    # Authenticate to UAA as client (this client)
    # Tell UAA to grant us (the client) a token using a refresh token
    data = {'grant_type': 'refresh_token', 'refresh_token': refresh_token }
    return _get_token(_get_oauth_server_info(uaa_uri), data)


# supporting objects and functions....

class UaaServer(Server):
    """Server object for communicating with the UAA server"""

    def __init__(self, uaa_info):
        scheme = Server._get_value_from_config('uaa_scheme')
        headers = Server._get_value_from_config('uaa_headers')
        super(UaaServer, self).__init__(uaa_info['uri'], scheme, headers)
        self.client_name=uaa_info.get('client_name', Server._get_value_from_config('client_name'))
        self.client_password=uaa_info.get('client_password', Server._get_value_from_config('client_password'))


def _get_oauth_server_info(atk_uri):
    """Get the UAA server information from the ATK Server"""
    server = _get_simple_atk_server(atk_uri)
    response = http.get(server, "oauth_server")
    server._check_response(response)
    return response.json()


def _get_simple_atk_server(atk_uri):
    """Gets ATK Server object for very simple HTTP"""
    scheme = Server._get_value_from_config('scheme')
    headers = Server._get_value_from_config('headers')
    return Server(atk_uri, scheme, headers)


def _get_oauth_token(uaa_info, user_name, user_password):
    """
    Connect to the cloudfoundry uaa server and acquire token

    Calling this method is required before invoking any ATK operations. This method connects to UAA server
    and validates the user and the client and returns an token that will be passed in the rest server header
    """
    # Authenticate to UAA as client (this client)
    # Tell UAA to grant us (the client) a token by authenticating with the user's password
    data = {'grant_type': "password", 'username': user_name, 'password': user_password}
    return _get_token(uaa_info, data)


def _get_token(uaa_info, data):
    """worker to get the token tuple from the UAA server"""
    if uaa_info:
        uaa_server = UaaServer(uaa_info)
        auth = (uaa_server.client_name, uaa_server.client_password)
        response = http.post(uaa_server, "/oauth/token", auth=auth, data=data)
        uaa_server._check_response(response)
        token_type = response.json()['token_type']
        token = response.json()['access_token']
        refresh_token = response.json()['refresh_token']
        return token_type, token, refresh_token
    return None, None, None
