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
config file for rest client
"""


# default connection config
class server_defaults:
    host="localhost"
    port=9099
    scheme = 'http'
    headers = {'Content-type': 'application/json',
               'Accept': 'application/json,text/plain'}
    version = 'v1'
    user_name = "test_api_key_1"


class upload_defaults:
    rows = 10000


class requests_defaults:
    ping_timeout_secs = 10
    request_timeout_secs = None  # None means no timeout


class polling_defaults:
    start_interval_secs = 1
    max_interval_secs = 20
    backoff_factor = 1.02


build_id = None


# Oregon UAA
class oauth_or:  # (see server_defaults to disable entirely)
    scheme = "https"
    host = "login.gotapaas.com"
    port = None
    headers = {"Accept": "application/json"}
    # user_name and user_password come from ia_server
    # Must specify client name and password as well:
    client_name = "atk-client"
    client_password = "c1oudc0w"


# Ireland UAA
class oauth_eu:  # (see server_defaults to disable entirely)
    scheme = "https"
    host = "uaa.run.gotapaas.eu"
    port = None
    headers = {"Accept": "application/json"}
    # user_name and user_password come from ia_server login
    # Must specify client name and password as well:
    client_name = "atk-client"
    client_password = "c1oudc0w"



class ia_server_local:
    """Standard, local server"""
    scheme = "http"
    host = "localhost"
    port = 9099
    user_name = "test_api_key_1"
    version = "v1"
    oauth_server = oauth_eu
    #oauth_server = None  #oauth_eu


class ia_server_eu:
    """Ireland ATK server"""
    scheme = "http"
    host = "atk-bryn2.apps.gotapaas.eu"
    port = None
    user_name = "admin"
    user_password="c1oudc0w"
    version = "v1"
    oauth_server = oauth_eu
    #user_name = "admin"   # oregon
    #user_password = "WQXng43TEfj"
    #oauth_server=oauth_or


ia_server = ia_server_local
#ia_server = ia_server_eu