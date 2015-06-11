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
import os
import requests
import logging
logger = logging.getLogger(__name__)

from decorator import decorator
from intelanalytics.core.api import api_status
import intelanalytics.rest.config as config
from intelanalytics.rest.server import Server
from intelanalytics.rest.uaa import UaaServer
from intelanalytics.meta.installapi import install_api


class InvalidAuthTokenError(RuntimeError):
    """Error raise when an invalid auth token is used"""
    pass


def _with_retry_on_token_error(function, *args, **kwargs):
    """wrapper for decorator which will refresh token on InvalidAuthTokenError"""
    try:
        return function(*args, **kwargs)
    except InvalidAuthTokenError:
        self = args[0]
        if self.retry_on_token_error:
            self._refresh_token()
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

    If environment variable INTELANALYTICS_HOST is set, it will use for the host
    If environment variable INTELANALYTICS_PORT is set, it will use for the port

    Properties can be overridden manually can be overridden manually

    Example::
        ia.server.host = 'your.hostname.com'
        ia.server.port = None

        ia.server.ping()  # test server connection
    """

    def __init__(self):
        user_name=self._get_conf('user_name', None)
        user_password=self._get_conf('user_password', None)
        super(IaServer, self).__init__(
            host=os.getenv('INTELANALYTICS_HOST', None) or self._get_conf('host'),
            port=os.getenv('INTELANALYTICS_PORT', None) or self._get_conf('port'),
            scheme=self._get_conf('scheme'),
            headers=self._get_conf('headers'),
            user_name=user_name,
            user_password=user_password)
        # specific to IA Server
        self._version = self._get_conf('version')
        self._oauth_token = None
        self._oauth_server = None
        self.oauth_server = self._get_oauth_server(user_name, user_password)
        self.retry_on_token_error = self._get_conf('retry_on_token_error', True)

    def _repr_attrs(self):
        return super(IaServer, self)._repr_attrs() + ["oauth_server"]  # add oauth to repr

    @property
    def version(self):
        """API version to connect to"""
        return self._version

    @version.setter
    def version(self, value):
        self._error_if_conf_frozen()
        self._version = value

    @property
    def oauth_server(self):
        return self._oauth_server

    @oauth_server.setter
    def oauth_server(self, value):
        """
        Sets the oauth server, where value can be a string which references an entry in the rest/config.py
        or it can be a UaaServer instance.  If None, an "empty" server instance is created, but oauth is
        not considered enabled.  This empty instance is provided to allow easier script configuration for
        referencing the oauth_server directly instead of needing to instantiate one.
        """
        self._error_if_conf_frozen()
        if value is None:
            value = UaaServer(None, None, None, None, None, None, None, None)  # just an empty server
        elif isinstance(value, basestring):
            # pull from config
            value = UaaServer(host=Server._get_value_from_config(value, "host"),
                              port=Server._get_value_from_config(value, "port"),
                              scheme=Server._get_value_from_config(value, "scheme"),
                              headers=Server._get_value_from_config(value, "headers"),
                              user_name=self.user_name,
                              user_password=self.user_password,
                              client_name=Server._get_value_from_config(value, "client_name"),
                              client_password=Server._get_value_from_config(value, "client_password"))
        elif not isinstance(value, UaaServer):
            raise ValueError("Unexpected value for oauth_server: %s.  Expected server object or valid config reference." % type(value))
        self._oauth_server = value
        self.oauth_token = None

    @property
    def oauth_token(self):
        return self._oauth_token

    @oauth_token.setter
    def oauth_token(self, value):
        self._oauth_token = value
        if value:
            self.headers['Authorization'] = self._oauth_token
        else:
            # If no oauth, we just pass the user_name for the token
            self.headers['Authorization'] = self.user_name
        logger.info("Client oauth token updated to %s", self._oauth_token)


    def _get_base_uri(self):
        """Returns the base uri used by client as currently configured to talk to the server"""
        return super(IaServer, self)._get_base_uri() + ("/%s" % self.version)

    def _check_response(self, response):
        """override base check response to check for an invalid token error"""
        if response.status_code == 400 and "CF-InvalidAuthToken" in response.text:
            raise InvalidAuthTokenError(response.text)
        super(IaServer, self)._check_response(response)

    def _refresh_token(self):
        if self.oauth_server:
            self.oauth_token = self.oauth_server.get_token()

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

    def freeze_configuration(self):
        super(IaServer, self).freeze_configuration()
        if self.oauth_server:
            self.oauth_server.freeze_configuration()

    def connect(self, user_name=None, user_password=None):
        """
        Connect to the intelanalytics server.

        This method calls the server, downloads its API information and dynamically generates and adds
        the appropriate Python code to this Python package.  Calling this method is required before
        invoking any server activity.

        After the client has connected to the server, the server config cannot be changed or refreshed.
        User must restart Python in order to change connection info.

        Subsequent calls to this method invoke no action.

        If user credentials are supplied, they will override the settings in the server's configuration

        There is no "connection" object or notion of being continuously "connected".  The call to
        connect is just a one-time process to download the API and prepare the client.  If the server
        goes down and comes back up, this client will not recognize any difference from a connection
        point of view, and will still be operating with the API information originally downloaded.
        """

        if api_status.is_installed:
            print "Already connected.  %s" % api_status
        else:
            if user_name or user_password:
                if user_name is None or user_password is None:
                    raise ValueError("Both name and password required if specifying connect credentials")
                self.user_name = user_name
                self.user_password = user_password

            if self.oauth_server:
                self.oauth_server.user_name = self.user_name
                self.oauth_server.user_password = self.user_password
                self.oauth_token = self.oauth_server.get_token()
                # a couple tokens for dev/test
                # token_eu = "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiI4OTUxNjZjNC1hYTgxLTRjZTUtYmM1OS05OTQ0NWVhNTQ3Y2QiLCJzdWIiOiIyNWFiMDgwYy1jMjI4LTRjZDktODg2YS1jZGY1YWQ0Nzg5M2MiLCJzY29wZSI6WyJjbG91ZF9jb250cm9sbGVyLndyaXRlIiwiY2xvdWRfY29udHJvbGxlcl9zZXJ2aWNlX3Blcm1pc3Npb25zLnJlYWQiLCJvcGVuaWQiLCJjbG91ZF9jb250cm9sbGVyLnJlYWQiXSwiY2xpZW50X2lkIjoiYXRrLWNsaWVudCIsImNpZCI6ImF0ay1jbGllbnQiLCJhenAiOiJhdGstY2xpZW50IiwiZ3JhbnRfdHlwZSI6InBhc3N3b3JkIiwidXNlcl9pZCI6IjI1YWIwODBjLWMyMjgtNGNkOS04ODZhLWNkZjVhZDQ3ODkzYyIsInVzZXJfbmFtZSI6ImFkbWluIiwiZW1haWwiOiJhZG1pbiIsImlhdCI6MTQyNjc5Mzk1NiwiZXhwIjoxNDI2ODM3MTU2LCJpc3MiOiJodHRwczovL3VhYS5nb3RhcGFhcy5jb20vb2F1dGgvdG9rZW4iLCJhdWQiOlsiYXRrLWNsaWVudCIsImNsb3VkX2NvbnRyb2xsZXIiLCJjbG91ZF9jb250cm9sbGVyX3NlcnZpY2VfcGVybWlzc2lvbnMiLCJvcGVuaWQiXX0.xJ5IlPsrHXutETZgdbTtiDTkliyIs26UYf_2oP-cwRWuxdsxAcphOXZEgHZXgNECr591Ts9R1-v8e6EihwW5x5CQ_7_BzmIYM0Z2IfYm220ZPktDkEryoKjujG5eqhUqjVGnj1og1ro6HX7ANu-HAXPhZ-USKu2eh_hR02EeZUU"
                # token_or = "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiJmMjM4OTEyYS1mOGM4LTQ0ZmItYmRlZi05MDU4N2JiNTljNjgiLCJzdWIiOiIyNWFiMDgwYy1jMjI4LTRjZDktODg2YS1jZGY1YWQ0Nzg5M2MiLCJzY29wZSI6WyJzY2ltLnJlYWQiLCJjbG91ZF9jb250cm9sbGVyLmFkbWluIiwicGFzc3dvcmQud3JpdGUiLCJzY2ltLndyaXRlIiwib3BlbmlkIiwiY2xvdWRfY29udHJvbGxlci53cml0ZSIsImNsb3VkX2NvbnRyb2xsZXIucmVhZCIsImRvcHBsZXIuZmlyZWhvc2UiXSwiY2xpZW50X2lkIjoiY2YiLCJjaWQiOiJjZiIsImF6cCI6ImNmIiwiZ3JhbnRfdHlwZSI6InBhc3N3b3JkIiwidXNlcl9pZCI6IjI1YWIwODBjLWMyMjgtNGNkOS04ODZhLWNkZjVhZDQ3ODkzYyIsInVzZXJfbmFtZSI6ImFkbWluIiwiZW1haWwiOiJhZG1pbiIsImlhdCI6MTQyNzk4MTA0NCwiZXhwIjoxNDI3OTgxNjQ0LCJpc3MiOiJodHRwczovL3VhYS5nb3RhcGFhcy5jb20vb2F1dGgvdG9rZW4iLCJhdWQiOlsiZG9wcGxlciIsInNjaW0iLCJvcGVuaWQiLCJjbG91ZF9jb250cm9sbGVyIiwicGFzc3dvcmQiLCJjZiJdfQ.IgmW_srXaQeCdIrg6YQtKNDiE7I5INoXnYs_Hu0F8U_VL3XIgi9hh2L3b5V032WSETLaeB-Z3Mrwl_lDRclB59xAT44_Jg3CvGOASInBAK_HGS0iREUti5TLFjkpY6WXCTvZ0Kt-UI7QL3kfj-hftyPiFmLlhwJS5rpXBqbkNtY"
                #self.oauth_token = token_or
                #print "token = %s" % self.oauth_token
            install_api(self)
            self.freeze_configuration()
            msg = "Connected.  %s" % api_status
            logger.info(msg)
            print msg

    @staticmethod
    def _get_conf(name, default=Server._unspecified):
        return Server._get_value_from_config('ia_server', name, default)

    @staticmethod
    def _get_oauth_server(user_name, user_password):
        o = IaServer._get_conf(UaaServer._server_name, None)
        if not o:
            return None
        try:
            return UaaServer(host=o.host,
                             port=o.port,
                             scheme=o.scheme,
                             headers=o.headers,
                             user_name=user_name,
                             user_password=user_password,
                             client_name=o.client_name,
                             client_password=o.client_password)
        except AttributeError as e:
            raise AttributeError("Missing one or more configuration settings for %s: %s" % (UaaServer._server_name, str(e)))

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

# Note: at one point we verified the build_id.  Should we want it back, here it.  It may make sense to have
# it turned on by default, but can be overridden to be ignored, since QA still runs into this problem from time to time.
#
#     @staticmethod
#     def _check_response_for_build_id(response):
#         # verify server and client are from the same build
#         if hasattr(config, "build_id") and config.build_id:
#             try:
#                 build_id = response.headers['build_id']
#             except KeyError:
#                 raise RuntimeError("Server response did not provide a build ID.  " + build_id_help_msg)
#             else:
#                 if str(config.build_id) != build_id:
#                     raise RuntimeError("Client build ID '%s' does not match server build ID '%s'.  "
#                                        % (config.build_id, build_id) + build_id_help_msg)
#
#
# build_id_help_msg = """
# Double check your client and server installation versions.
#
# To turn this client/server build check OFF, change the value of 'build_id' to
# be None in the intelanalytics/rest/config.py file --OR-- run this code:
#
# import intelanalytics.rest.config
# intelanalytics.rest.config.build_id = None
# """
