import logging
import unittest

import datetime
import time


class FakeFileHandler:
    def __init__(self, filename, mode='a', encoding=None, delay=0):
        pass

logging.FileHandler = FakeFileHandler
from intel_analytics.logger import stdout_logger

class LoggingTest(unittest.TestCase):

    def test_propagation(self):
        self.assertEqual(0, stdout_logger.propagate)

    def test_only_file_handler(self):
        self.assertEqual(1, len(stdout_logger.handlers))
        self.assertTrue(isinstance(stdout_logger.handlers[0], logging.FileHandler))

    def test_distinct_file_name(self):
        from intel_analytics.logger import get_file_name_from_datetime
        filename1 = get_file_name_from_datetime(datetime.datetime.now())
        time.sleep(0.001)
        filename2 = get_file_name_from_datetime(datetime.datetime.now())
        self.assertNotEqual(filename1, filename2)



if __name__ == '__main__':
    unittest.main()
