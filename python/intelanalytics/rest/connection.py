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
import json
import requests
import logging
logger = logging.getLogger(__name__)

__all__ = ['rest_connection']

# default connection config
_host = "localhost"
_port = 8090
_scheme = "http"
_version = "v1"
_headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

_default = object()


class Connection(object):

    def __init__(self, host=None, port=_default, scheme=None, version=None):
        self.host = host or _host
        self.port = port if port is not _default else _port
        self.scheme = scheme or _scheme
        self.version = version or _version
        self.headers = _headers

    def __repr__(self):
        return '{"host": "%s", "port": "%s", "scheme": "%s", "version": "%s"}' \
               % (self.host, self.port, self.scheme, self.version)

    def get_url(self):
        url = "%s://%s" % (self.scheme, self.host)
        if self.port:
            url += ":%s" % self.port
        return url

    def ping(self):
        """
        Ping the server, throw exception if not there
        """
        try:
            r = requests.get(self.get_url() + "/info")
            if "Intel Analytics" != r.json()['name']:
                raise Exception("invalid response payload: " + r.text)
        except Exception as e:
            #print ("Ping failed: " + e.message)
            logger.error("Ping failed: " + e.message)
            raise IOError("Unable to find Intel Analytics server at " + self.get_url())


class HttpMethods(object):
    """
    HTTP methods to the REST server
    """
    def __init__(self, connection):
        self.connection = connection

    def _get_base_uri(self):
        return "%s/%s/" % (self.connection.get_url(), self.connection.version)

   # HTTP commands

    def get(self, uri_path):
        uri = self._get_base_uri() + uri_path
        logger.info("Get: " + uri)
        return requests.get(uri)

    def delete(self, uri_path):
        uri = self._get_base_uri() + uri_path
        logger.info("Delete: " + uri)
        return requests.delete(uri)

    def post(self, uri_path, payload):
        data = (json.dumps(payload))
        uri = self._get_base_uri() + uri_path
        logger.info("Post to: " + uri + " with payload: " + data)
        return requests.post(uri, data=data, headers=self.connection.headers)


rest_connection = Connection()
rest_http = HttpMethods(rest_connection)
