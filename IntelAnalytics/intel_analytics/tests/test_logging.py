from logging import FileHandler
import time

import unittest
from intel_analytics.logger import stdout_logger, get_file_name_from_datetime


class LoggingTest(unittest.TestCase):
    def test_propagation(self):
        self.assertEqual(0, stdout_logger.propagate)

    def test_only_file_handler(self):
        for handler in stdout_logger.handlers:
            self.assertTrue(isinstance(handler, FileHandler))

    def test_distinct_file_name(self):
        filename1 = get_file_name_from_datetime()
        time.sleep(0.001)
        filename2 = get_file_name_from_datetime()
        self.assertNotEqual(filename1, filename2)



if __name__ == '__main__':
    unittest.main()
