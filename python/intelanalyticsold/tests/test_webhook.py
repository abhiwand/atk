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
