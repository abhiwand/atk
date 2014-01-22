import unittest
from intel_analytics.report import PigJobReportStrategy


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





if __name__ == '__main__':
    unittest.main()
