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

from mock import Mock
import iatest

iatest.init()
from intelanalytics.rest import launcher
from intelanalytics.rest.message import Message
import unittest
from intelanalytics.rest.launcher import *



class WebHook(unittest.TestCase):
    def test_message_immutable(self):
        m = Message(123, "content")
        self.assertEqual(m.job_id, 123)
        self.assertEqual(m.content, "content")

        def modifiy_job_id():
            m.job_id = 1

        def modifiy_content():
            m.job_id = 1

        self.assertRaises(Exception, modifiy_job_id)
        self.assertRaises(Exception, modifiy_content)

    def test_launch(self):
        launcher.web_server = Mock()

        content = """{
                    "first": "John",
                    "last": "Doe",
                    "age": 39,
                    "sex": "M",
                    "salary": 70000,
                    "registered": true,
                    "favorites": {
                        "color": "Blue",
                        "sport": "Soccer",
                        "food": "Spaghetti"
                    }
                    }"""

        def start(port, message_queue):
            m = Message(12345, content)
            m.port = port
            message_queue.put(m)

        launcher.web_server.start = start
        webhook_launcher.launch(100)

        m = launcher.message_queue.get(timeout=0.5)
        self.assertEqual(content, m.content)
        self.assertEqual(100, m.port)



if __name__ == '__main__':
    unittest.main()
