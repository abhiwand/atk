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
import logging
logger = logging.getLogger(__name__)
import intelanalytics.rest.http as http
from intelanalytics.rest.server import Server


class UaaServer(Server):
    """
    Handles communication with CF UAA and keeping an active token
    """
    _server_name = "oauth_server"

    headers = {"Accept": "application/json"}

    def __init__(self, host, port, scheme, headers, user_name, user_password, client_name, client_password):
        super(UaaServer, self).__init__(host, port, scheme, headers, user_name=user_name, user_password=user_password)
        self.client_name = client_name
        self.client_password = client_password

    def get_token(self):
        """
        Connect to the cloudfoundry uaa server and acquire token

        Calling this method is required before invoking any ATK operations. This method connects to UAA server
        and validates the user and the client and returns an token that will be passed in the rest server header
        """
        # Authenticate to UAA as client (this client)
        # Tell UAA to grant us (the client) a token by authenticating with the user's password
        data = {'grant_type': "password", 'username': self.user_name, 'password': self.user_password}
        auth = (self.client_name, self.client_password)
        response = http.post(self, "/oauth/token", headers=self.headers, auth=auth, data=data)
        self._check_response(response)
        token = response.json()['access_token']
        return token

