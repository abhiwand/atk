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

"""
Connection to the Intel Analytics REST Server
"""
import requests
import json
import logging
logger = logging.getLogger(__name__)

from decorator import decorator
from intelanalytics.core.api import api_status
import intelanalytics.rest.config as config
from intelanalytics.rest.server import Server
from intelanalytics.rest.uaa import get_oauth_token, get_refreshed_oauth_token
from intelanalytics.meta.installapi import install_api


class InvalidAuthTokenError(RuntimeError):
    """Error raise when an invalid auth token is used"""
    pass


def _with_retry_on_token_error(function, *args, **kwargs):
    """wrapper for decorator which will refresh token on InvalidAuthTokenError"""
    try:
        return function(*args, **kwargs)
    except InvalidAuthTokenError:
        ia_server = args[0]
        if ia_server.retry_on_token_error:
            ia_server.refresh_oauth_token()
            return function(*args, **kwargs)
        else:
            raise


def with_retry_on_token_error(function):
    """decorator for http methods which call the server, opportunity for retry with a refreshed token"""
    return decorator(_with_retry_on_token_error, function)


class IaServer(Server):
    """
    Server object for talking with ATK server

    Defaults from intelanalytics/rest/config.py are used but
    they can be overridden by setting the values in this class.

    Properties can be overridden manually can be overridden manually

    Example::
        ia.server.uri = 'your.hostname.com'

        ia.server.ping()  # test server connection
    """

    def __init__(self):
        user=self._get_conf('user', None)
        super(IaServer, self).__init__(
            uri= self._get_conf('uri'),
            scheme=self._get_conf('scheme'),
            headers=self._get_conf('headers'),
            user=user)
        # specific to IA Server
        self._version = self._get_conf('version')
        self._oauth_uri = None
        self._oauth_token = None
        self.oauth_refresh_token = None
        self.retry_on_token_error = self._get_conf('retry_on_token_error', True)
        self.refresh_authorization_header()

    @property
    def version(self):
        """API version to connect to"""
        return self._version

    @version.setter
    def version(self, value):
        self._error_if_conf_frozen()
        self._version = value

    @property
    def oauth_uri(self):
        """API version to connect to"""
        return self._oauth_uri

    @oauth_uri.setter
    def oauth_uri(self, value):
        self._error_if_conf_frozen()
        self._oauth_uri = value

    @property
    def oauth_token(self):
        return self._oauth_token

    @oauth_token.setter
    def oauth_token(self, value):
        self._oauth_token = value
        self.refresh_authorization_header()
        logger.info("Client oauth token updated to %s", self._oauth_token)

    def refresh_authorization_header(self):
        if self._oauth_token:
            self.headers['Authorization'] = self._oauth_token
        else:
            # If no oauth, we just pass the user for the token
            self.headers['Authorization'] = self.user

    def _get_base_uri(self):
        """Returns the base uri used by client as currently configured to talk to the server"""
        return super(IaServer, self)._get_base_uri() + ("/%s" % self.version)

    def _check_response(self, response):
        """override base check response to check for an invalid token error"""
        if response.status_code == 400 and "CF-InvalidAuthToken" in response.text:
            raise InvalidAuthTokenError(response.text)
        super(IaServer, self)._check_response(response)

    def refresh_oauth_token(self):
        token_type, self.oauth_token, self.oauth_refresh_token = get_refreshed_oauth_token(self.oauth_uri, self.oauth_refresh_token)

    def _load_connect_file(self, connect_file):
        with open(connect_file, "r") as f:
            creds = json.load(f)
        self.uri = creds.get('uri', self.uri)
        self.oauth_uri = creds.get('oauth_uri', self.oauth_uri)
        self.user = creds.get('user', self.user)
        self.oauth_token = creds.get('token', self.oauth_token)
        self.oauth_refresh_token = creds.get('refresh_token', self.oauth_token)

    def ping(self):
        """
        Ping the server, throw exception if unable to connect
        """
        uri = ""
        try:
            uri = self._get_scheme_and_authority() + "/info"
            logger.info("[HTTP Get] %s", uri)
            r = requests.get(uri, timeout=config.requests_defaults.ping_timeout_secs)
            logger.debug("[HTTP Get Response] %s\n%s", r.text, r.headers)
            self._check_response(r)
            if "Intel Analytics" != r.json()['name']:
                raise Exception("Invalid response payload: " + r.text)
            print "Successful ping to Intel Analytics at " + uri
        except Exception as e:
            message = "Failed to ping Intel Analytics at %s\n%s" % (uri, e)
            logger.error(message)
            raise IOError(message)

    def connect(self, connect_file=None):
        """
        Connect to the intelanalytics server.

        This method calls the server, downloads its API information and dynamically generates and adds
        the appropriate Python code to this Python package.  Calling this method is required before
        invoking any server activity.

        After the client has connected to the server, the server config cannot be changed or refreshed.
        User must restart Python in order to change connection info.

        Subsequent calls to this method invoke no action.

        If a credentials file is supplied, they will override the settings in the server's configuration

        There is no "connection" object or notion of being continuously "connected".  The call to
        connect is just a one-time process to download the API and prepare the client.  If the server
        goes down and comes back up, this client will not recognize any difference from a connection
        point of view, and will still be operating with the API information originally downloaded.
        """

        #with api_status._api_lock:
        if api_status.is_installed:
            print "Already connected.  %s" % api_status
        else:
            if connect_file:
                self._load_connect_file(connect_file)

            install_api(self)
            msg = "Connected.  %s" % api_status
            logger.info(msg)
            print msg

    @staticmethod
    def _get_conf(name, default=Server._unspecified):
        return Server._get_value_from_config(name, default)

    # HTTP calls to server

    @with_retry_on_token_error
    def get(self, url):
        return super(IaServer, self).get(url)

    @with_retry_on_token_error
    def post(self, url, data):
        return super(IaServer, self).post(url, data)

    @with_retry_on_token_error
    def delete(self, url):
        return super(IaServer, self).delete(url)


server = IaServer()


def create_connect_file(filename):
    import json
    import getpass
    with open(filename, "w"):
        pass

    uri = raw_input("server URI: ")
    oauth_uri = raw_input("OAuth server URI: ") or None
    user = raw_input("user name: ")
    if oauth_uri:
        password = getpass.getpass()
        token_type, token, refresh_token = get_oauth_token(oauth_uri, user, password)
    else:
        token_type, token, refresh_token = None, None, None

    creds = dict({ 'uri': uri,
                   'user': user,
                   'oauth_uri': oauth_uri,
                   'token_type': token_type,
                   'token': token,
                   'refresh_token': refresh_token })
    # call uaa and get the token...
    # entanglement with server
    with open(filename, "w") as f:
        f.write(json.dumps(creds, indent=2))
    print "\nCredentials created at '%s'" % filename


