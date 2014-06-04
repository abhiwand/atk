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
import sys
import json
import requests
import logging
logger = logging.getLogger(__name__)

__all__ = ['Server', 'HttpMethods']

# default connection config
_host = "localhost"
_port = 9099
_scheme = "http"
_version = "v1"
_headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

_default = object()


class Server(object):

    def __init__(self, host=None, port=_default, scheme=None, version=None):
        self.host = host or _host
        self.port = port if port is not _default else _port
        self.scheme = scheme or _scheme
        self.version = version or _version
        self.headers = _headers

        # Currently credentials only contain the api key, and that will be
        # passed in the Authorization header of every request sent to the backend
        # TODO: switch to real credentials
        self.headers['Authorization'] = "test_api_key_1"

    def __repr__(self):
        return '{"host": "%s", "port": "%s", "scheme": "%s", "version": "%s"}' \
               % (self.host, self.port, self.scheme, self.version)

    def __str__(self):
        return """host:    %s
port:    %s
scheme:  %s
version: %s""" % (self.host, self.port, self.scheme, self.version)

    def _get_scheme_and_authority(self):
        uri = "%s://%s" % (self.scheme, self.host)
        if self.port:
            uri += ":%s" % self.port
        return uri

    def get_base_uri(self):
        return "%s/%s/" % (self._get_scheme_and_authority(), self.version)

    def ping(self):
        """
        Ping the server, throw exception if not there
        """
        uri = ""
        try:
            uri = self._get_scheme_and_authority() + "/info"
            logger.info("[HTTP Get] %s", uri)
            r = requests.get(uri)
            logger.debug("[HTTP Get Response] %s", r.text)
            r.raise_for_status()
            if "Intel Analytics" != r.json()['name']:
                raise Exception("Invalid response payload: " + r.text)
            print "Successful ping to Intel Analytics at " + uri
        except Exception as e:
            message = "Failed to ping Intel Analytics at " + uri + "\n" + str(e)
            #print (message)
            logger.error(message)
            raise IOError(message)


class HttpMethods(object):
    """
    HTTP methods to the REST server
    """
    def __init__(self, server):
        self.server = server

    def _get_uri(self, path):
        return self.server.get_base_uri() + path

    @staticmethod
    def _check_response(response, ignore=None):
        if not ignore or response.status_code not in ignore:
            response.raise_for_status()
        else:
            try:
                response.raise_for_status()
            except Exception as e:
                m = "Ignoring HTTP Response ERROR probably due to {0}:\n\t{1}".\
                    format(ignore[response.status_code], e)
                logger.warn(m)
                sys.stderr.write(m)
                sys.stderr.flush()

    @property
    def base_uri(self):
        return self.server.get_base_uri()

   # HTTP commands

    def get(self, uri_path):
        uri = self._get_uri(uri_path)
        return self.get_full_uri(uri)

    def get_full_uri(self, uri):
        if logger.level <= logging.INFO:
            logger.info("[HTTP Get] %s", uri)
        r = requests.get(uri, headers=self.server.headers)
        if logger.level <= logging.DEBUG:
            logger.debug("[HTTP Get Response] %s", r.text)
        self._check_response(r)
        return r

    def delete(self, uri_path):
        uri = self._get_uri(uri_path)
        return self.delete_full_uri(uri)

    def delete_full_uri(self, uri):
        logger.info("[HTTP Delete] %s", uri)
        r = requests.delete(uri, headers=self.server.headers)
        if logger.level <= logging.DEBUG:
            logger.debug("[HTTP Delete Response] %s", r.text)
        self._check_response(r)
        return r

    def post(self, uri_path, payload):
        uri = self._get_uri(uri_path)
        return self.post_full_uri(uri, payload)

    def post_full_uri(self, uri, payload):
        data = json.dumps(payload)
        if logger.level <= logging.INFO:
            pretty_data = json.dumps(payload, indent=2)
            logger.info("[HTTP Post] %s\n%s", uri, pretty_data)
        r = requests.post(uri, data=data, headers=self.server.headers)
        if logger.level <= logging.DEBUG:
            logger.debug("[HTTP Post Response] %s", r.text)
        self._check_response(r, {406: 'long initialization time'})
        return r


server = Server()
http = HttpMethods(server)
