import os
import unittest


os.environ['IN_UNIT_TESTS'] = 'true'

from intel_analytics.report import PigJobReportStrategy
from intel_analytics.table.pig.pig_helpers import get_generate_key_statements


class TestPigDataImport(unittest.TestCase):

    def test_get_pig_status_noise(self):
        strategy = PigJobReportStrategy()

        # receiving anything before 'Pig job status report-Start:' will not be included in the report
        strategy.report('input:5000')
        strategy.report('output:6000')

        isEmpty = True;
        if strategy.content:
            isEmpty = False

        self.assertEqual(True, isEmpty)

    def test_get_pig_status(self):
        strategy = PigJobReportStrategy()
        strategy.report('Pig job status report-Start:')
        isEmpty = True;
        if strategy.content:
            isEmpty = False

        self.assertEqual(True, isEmpty)
        strategy.report('input:5000')
        strategy.report('output:6000')

        if strategy.content:
            isEmpty = False

        self.assertEqual(2, len(strategy.content))
        strategy.report('Pig job status report-End:')
        #anything after pig job status report-end will not be included
        strategy.report('table:xyz')
        self.assertEqual(2, len(strategy.content))
        self.assertEqual('5000', strategy.content['input'])
        self.assertEqual('6000', strategy.content['output'])

    def test_get_key_assignment_statements_original_import(self):
        statements = get_generate_key_statements('in', 'out', 'f1, f2, f3')
        self.assertEqual(1, len(statements))
        self.assertEqual('out = rank in;', statements[-1])


    def test_get_key_assignment_statements_append(self):
        statements = get_generate_key_statements('in', 'out', 'f1, f2, f3', 1000)
        self.assertEqual(2, len(statements))
        self.assertEqual('temp = rank in;', statements[0])
        self.assertEqual('out = foreach temp generate $0 + 1000 as key, f1, f2, f3;', statements[1])

    def test_get_log_file_location(self):
        line = 'Details at logfile: /user/lib/IntelAnalytics/pig_1395809054698.log'
        strategy = PigJobReportStrategy()
        log_file = strategy.get_log_file(line)
        self.assertEqual(log_file, '/user/lib/IntelAnalytics/pig_1395809054698.log')

    def test_get_log_file_none(self):
        line = 'ERROR org.apache.pig.Main  - ERROR 1121: Python Error. Traceback (most recent call last):'
        strategy = PigJobReportStrategy()
        log_file = strategy.get_log_file(line)
        self.assertEqual(log_file, None)

    def test_get_error_code(self):
        line = 'org.apache.pig.backend.executionengine.ExecException: ERROR 2118: Input Pattern hdfs://localhost:19010/user/hadoop/worldban111k* matches 0 files'
        strategy = PigJobReportStrategy()
        error_code = strategy.get_error_code(line)
        self.assertEqual('2118', error_code)

    def test_get_error_code_none(self):
        line = 'Details at logfile: /user/lib/IntelAnalytics/pig_1395809054698.log'
        strategy = PigJobReportStrategy()
        error_code = strategy.get_error_code(line)
        self.assertEqual(None, error_code)







if __name__ == '__main__':
    unittest.main()
