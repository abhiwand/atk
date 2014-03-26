import unittest

import datetime
import time
import os
os.environ['IN_UNIT_TESTS'] = 'true'


from intel_analytics.logger import stdout_logger


class LoggingTest(unittest.TestCase):

    def test_propagation(self):
        self.assertEqual(0, stdout_logger.propagate)

    def test_distinct_file_name(self):
        from intel_analytics.logger import get_file_name_from_datetime
        filename1 = get_file_name_from_datetime(datetime.datetime.now())
        time.sleep(0.001)
        filename2 = get_file_name_from_datetime(datetime.datetime.now())
        self.assertNotEqual(filename1, filename2)



if __name__ == '__main__':
    unittest.main()
