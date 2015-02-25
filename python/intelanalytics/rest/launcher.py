//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

import atexit
from multiprocessing import Process
import multiprocessing
from hooks import WebhookServer

message_queue = multiprocessing.Queue()
web_server = WebhookServer()

class _Launcher(object):
    """
    Webhook launcher.
    """

    def __del__(self):
        """
        Shutdown the webhook server on object delete
        """
        self.shutdown()

    def launch(self, port):
        """
        Launch webhook server with the specified port

        Parameters
        ----------
        port: int
            The port that the webhook server uses to serve the request


        Examples
        --------
        >>> webhook_launcher.launch(10050)
        """
        self.server_process = Process(target=web_server.start, args=(port, message_queue))
        self.server_process.start()

    def shutdown(self):
        """
        Shutdown the webhook server

        Examples
        --------
        >>> webhook_launcher.shutdown()
        """
        self.server_process.terminate()


webhook_launcher = _Launcher()
atexit.register(webhook_launcher.shutdown)
