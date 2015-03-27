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

class cf:
    # config specific to cloud foundry

    class oauth:  # (see server_defaults o disable entirely)
        url = "https://login.gotapaas.com/oauth/token"
        user_name = "admin"  # uaa user name
        user_password = "WQXng43TEfj"  # uaa user password
        client_name = "atk-client"
        client_password = "c1oudc0w"  # for gotapaas.eu
        headers = {"Accept": "application/json"}


# default connection config
class server_defaults:
    host = "localhost"
    port = 9099
    scheme = "http"
    version = "v1"
    headers = {'Content-type': 'application/json',
               'Accept': 'application/json,text/plain',
               'Authorization': "test_api_key_1"}
    #oauth = cf.oauth  # to disable oauth, set to None
    oauth = None


class upload_defaults:
    rows = 10000


class requests_defaults:
    ping_timeout_secs = 10
    get_timeout_secs = None  # None means no timeout


class polling_defaults:
    start_interval_secs = 1
    max_interval_secs = 20
    backoff_factor = 1.02


build_id = None

