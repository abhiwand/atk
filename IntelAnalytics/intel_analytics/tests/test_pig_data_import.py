import unittest
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






if __name__ == '__main__':
    unittest.main()
