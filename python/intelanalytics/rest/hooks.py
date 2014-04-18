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

from bottle import request
from bottle import Bottle, HeaderDict
from message import Message


class WebhookServer(object):
    def __init__(self):
        self._app = Bottle()

        self._route()
        self.response_dict = HeaderDict()

    def _route(self):
        self._app.route('/notify/<id>', method="POST", callback=self.__notify)

    def start(self, port, message_queue):
        self.message_queue = message_queue
        self._app.run(host='localhost', port=port, quiet=True)

    def stop(self):
        self._app.close()

    def __notify(self, id):
        m = Message(id, request.json)
        self.message_queue.put(m)

