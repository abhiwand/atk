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
Connection to the Intel Analytics REST Server
"""
import os
import sys
import json
import requests
import logging
logger = logging.getLogger(__name__)

import intelanalytics.rest.config as config
from intelanalytics.core.api import error_if_api_already_loaded, _Api


class Server(object):
    """
    Configuration for the client to talk to the server.

    Defaults from rest/config.py are used but they can be overridden by setting the values
    in this class.

    If environment variable INTELANALYTICS_HOST is set, it will use for the host
    If environment variable INTELANALYTICS_PORT is set, it will use for the port

    host : str
        host name
    port : int or str
        port number
    scheme : str
        protocol scheme, like "http"
    version : str
        server API version to use, like "v1"
    headers : dict of str pairs
        http headers

    Example::
        ia.server.host = 'your.hostname.com'
        ia.server.port = None

        ia.server.ping()  # test server connection

        ia.server.reset()  # configuration restored to defaults
    """

    def __init__(self):
        self._host = os.getenv('INTELANALYTICS_HOST', config.server_defaults.host)
        self._port = os.getenv('INTELANALYTICS_PORT', config.server_defaults.port)
        self._scheme = config.server_defaults.scheme
        self._version = config.server_defaults.version
        self._headers = config.server_defaults.headers

    @property
    def host(self):
        """server host name"""
        return self._host

    @host.setter
    @error_if_api_already_loaded
    def host(self, value):
        self._host = value

    @property
    def port(self):
        """server port number - can be None for no specification"""
        return self._port

    @port.setter
    @error_if_api_already_loaded
    def port(self, value):
        self._port = value

    @property
    def scheme(self):
        """connection scheme, like http or https"""
        return self._scheme

    @scheme.setter
    @error_if_api_already_loaded
    def scheme(self, value):
        self._scheme = value

    @property
    def version(self):
        """API version to connect to"""
        return self._version

    @version.setter
    @error_if_api_already_loaded
    def version(self, value):
        self._version = value

    @property
    def headers(self):
        """scheme headers"""
        return self._headers

    @headers.setter
    @error_if_api_already_loaded
    def headers(self, value):
        self._headers = value

    def reset(self):
        """Restores the server configuration to defaults"""
        self.__init__()

    def __repr__(self):
        return '{"host": "%s", "port": "%s", "scheme": "%s", "version": "%s", "headers": "%s"}' \
               % (self.host, self.port, self.scheme, self.version, self.headers)

    def __str__(self):
        return """
------------------------------------------------------------------------------
             Configuration to reach IntelAnalytics Server

host:     %s
port:     %s
scheme:   %s
version:  %s
------------------------------------------------------------------------------
""" % (self.host, self.port, self.scheme, self.version)

    def _get_scheme_and_authority(self):
        uri = "%s://%s" % (self.scheme, self.host)
        if self.port:
            uri += ":%s" % self.port
        return uri

    def get_base_uri(self):
        """Returns the base uri used by client as currently configured to talk to the server"""
        return "%s/%s/" % (self._get_scheme_and_authority(), self.version)

    def ping(self):
        """
        Ping the server, throw exception if unable to connect
        """
        uri = ""
        try:
            uri = self._get_scheme_and_authority() + "/info"
            logger.info("[HTTP Get] %s", uri)
            r = requests.get(uri)
            logger.debug("[HTTP Get Response] %s\n%s", r.text, r.headers)
            HttpMethods._check_response(r)
            if "Intel Analytics" != r.json()['name']:
                raise Exception("Invalid response payload: " + r.text)
            print "Successful ping to Intel Analytics at " + uri
        except Exception as e:
            message = "Failed to ping Intel Analytics at %s\n%s" % (uri, e)
            logger.error(message)
            raise IOError(message)

    @staticmethod
    def connect():
        """
        Connect to the intelanalytics server.

        Calling this method is required before invoking any server activity.  After the client has
        connected to the server, the server config cannot be changed or refreshed.  User must restart
        Python in order to change connection info.

        Subsequent calls to this method invoke no action.
        """
        if _Api.is_loaded():
            print "Already connected to intelanalytics server."
        else:
            _Api.load_api()
            print "Connected to intelanalytics server."


class HttpMethods(object):
    """
    HTTP methods to the REST server
    """
    def __init__(self, server):
        self.server = server

    def create_full_uri(self, path):
        return self.server.get_base_uri() + path

    @staticmethod
    def _check_response(response, ignore=None):

        HttpMethods._check_response_for_build_id(response)

        try:
            response.raise_for_status()
        except Exception as e:
            if not ignore or response.status_code not in ignore:
                raise requests.exceptions.HTTPError(str(e) + " "+ response.text)
            else:
                m = "Ignoring HTTP Response ERROR probably due to {0}:\n\t{1}". \
                    format(ignore[response.status_code], e)
                logger.warn(m)
                sys.stderr.write(m)
                sys.stderr.flush()

    @staticmethod
    def _check_response_for_build_id(response):
        # verify server and client are from the same build
        if hasattr(config, "build_id") and config.build_id:
            try:
                build_id = response.headers['build_id']
            except KeyError:
                raise RuntimeError("Server response did not provide a build ID.  " + build_id_help_msg)
            else:
                if str(config.build_id) != build_id:
                    raise RuntimeError("Client build ID '%s' does not match server build ID '%s'.  "
                                       % (config.build_id, build_id) + build_id_help_msg)


    @property
    def base_uri(self):
        return self.server.get_base_uri()

   # HTTP commands

    def get(self, uri_path):
        uri = self.create_full_uri(uri_path)
        return self.get_full_uri(uri)

    def get_full_uri(self, uri):
        if logger.level <= logging.INFO:
            logger.info("[HTTP Get] %s", uri)
        r = requests.get(uri, headers=self.server.headers)
        if logger.level <= logging.DEBUG:
            logger.debug("[HTTP Get Response] %s\n%s", r.text, r.headers)

        self._check_response(r)
        return r

    def delete(self, uri_path):
        uri = self.create_full_uri(uri_path)
        return self.delete_full_uri(uri)

    def delete_full_uri(self, uri):
        logger.info("[HTTP Delete] %s", uri)
        r = requests.delete(uri, headers=self.server.headers)
        if logger.level <= logging.DEBUG:
            logger.debug("[HTTP Delete Response] %s", r.text)
        self._check_response(r)
        return r

    def post(self, uri_path, payload):
        uri = self.create_full_uri(uri_path)
        return self.post_full_uri(uri, payload)

    def post_full_uri(self, uri, payload):
        data = json.dumps(payload)
        if logger.level <= logging.INFO:
            pretty_data = json.dumps(payload, indent=2)
            logger.info("[HTTP Post] %s\n%s", uri, pretty_data)
        r = requests.post(uri, data=data, headers=self.server.headers)
        if logger.level <= logging.DEBUG:
            logger.debug("[HTTP Post Response] %s", r.text)
            self.reason = r.text

        self._check_response(r, {406: 'long initialization time'})
        return r

build_id_help_msg = """
Double check your client and server installation versions.

To turn this client/server build check OFF, change the value of 'build_id' to
be None in the intelanalytics/rest/config.py file --OR-- run this code:

import intelanalytics.rest.config
intelanalytics.rest.config.build_id = None
"""

server = Server()
http = HttpMethods(server)
