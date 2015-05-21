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
"""
HTTP methods
"""
import json
import requests
import logging
import urllib3
logger = logging.getLogger(__name__)

import ssl

from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

class Tlsv1HttpAdapter(HTTPAdapter):
    """"Transport adapter" that allows us to use TLSv1."""
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       ssl_version=ssl.PROTOCOL_TLSv1)

# A simple http session wrapper over requests given a scheme (http or https)
class httpSession(object):
    def __init__(self, scheme):
        self.scheme = scheme
        self.session = requests.Session()
        self.session.mount('%s://' % self.scheme, Tlsv1HttpAdapter())
    def __enter__(self):
        return self.session
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

# Helper methods

def _get_arg(key, from_kwargs, default):
    """Remove value from kwargs and return it, else return default value"""
    if key in from_kwargs:
        arg = from_kwargs[key]
        del from_kwargs[key]
        return arg
    return default


def _get_headers(server, from_kwargs):
    """Helper function to collect headers from kwargs when the http caller wants to override them"""
    return _get_arg('headers', from_kwargs, server.headers)


# HTTP methods

def get(server, uri_path, **kwargs):
    uri = server.create_full_uri(uri_path)
    headers = _get_headers(server, kwargs)
    timeout = _get_arg('timeout', kwargs, None)
    if logger.level <= logging.INFO:
        details = uri
        if logger.level <= logging.DEBUG:
            details += "\nheaders=%s" % headers
        logger.info("[HTTP Get] %s", details)
    with httpSession(server.scheme) as session:
        response = session.get(uri, headers=headers, timeout=timeout, verify=False, **kwargs)
    if logger.level <= logging.DEBUG:
        logger.debug("[HTTP Get Response] %s\nheaders=%s", response.text, response.headers)
    return response


def delete(server, uri_path, **kwargs):
    uri = server.create_full_uri(uri_path)
    headers = _get_headers(server, kwargs)
    logger.info("[HTTP Delete] %s", uri)
    with httpSession(server.scheme) as session:
        response = session.delete(uri, headers=headers, verify=False, **kwargs)
    if logger.level <= logging.DEBUG:
        logger.debug("[HTTP Delete Response] %s", response.text)
    return response


def post(server, uri_path, data, **kwargs):
    uri = server.create_full_uri(uri_path)
    headers = _get_headers(server, kwargs)
    if logger.level <= logging.INFO:
        try:
            pretty_data = json.dumps(json.loads(data), indent=2)
        except:
            pretty_data = data
        logger.info("[HTTP Post] %s\n%s\nheaders=%s", uri, pretty_data, headers)
    with httpSession(server.scheme) as session:
            response = session.post(uri, headers=headers, data=data, verify=False, **kwargs)
    if logger.level <= logging.DEBUG:
        logger.debug("[HTTP Post Response] %s", response.text)
    return response

