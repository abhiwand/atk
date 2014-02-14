import unittest
from mock import MagicMock
from intel_analytics.graph.biggraph import GraphWrapper
from intel_analytics.report import FaunusProgressReportStrategy


class TestGraph(unittest.TestCase):
    def test_convert_query_statements_to_xml(self):
        statements = []
        statements.append("g.V('_gb_ID','11').out")
        statements.append("g.V('_gb_ID','11').outE")
        wrapper = GraphWrapper(MagicMock())
        xml = wrapper._get_query_xml(statements)
        expected = "<query><statement>g.V('_gb_ID','11').out.transform('{[it,it.map()]}')</statement><statement>g.V('_gb_ID','11').outE.transform('{[it,it.map()]}')</statement></query>"
        self.assertEqual(xml, expected)


    def test_receive_faunus_job_complete_signal_no_signal(self):
        reportStrategy = FaunusProgressReportStrategy()
        self.assertFalse(reportStrategy.is_job_complete_signaled("14:20:50 INFO mapred.JobClient:  map 100% reduce 8%"))

    def test_receive_faunus_job_complete_signal_no_signal_at_100_percent(self):
        reportStrategy = FaunusProgressReportStrategy()
        self.assertFalse(reportStrategy.is_job_complete_signaled("14:21:50 INFO mapred.JobClient:  map 100% reduce 100%"))

    def test_receive_faunus_job_complete_signal_with_signal(self):
        reportStrategy = FaunusProgressReportStrategy()
        self.assertTrue(reportStrategy.is_job_complete_signaled("14:21:51 INFO mapred.JobClient: Job complete: job_201402121231_0117"))

    def test_get_progress_mapper_half_way(self):
        reportStrategy = FaunusProgressReportStrategy()
        reportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 50% reduce 0%")
        progressbar = reportStrategy.job_progress_bar_list[-1]
        self.assertEquals(25, progressbar.value)

    def test_get_progress_complete(self):
        reportStrategy = FaunusProgressReportStrategy()
        reportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 100% reduce 100%")
        progressbar = reportStrategy.job_progress_bar_list[-1]
        self.assertEquals(100, progressbar.value)

    def test_get_progress_mapper_finished(self):
        reportStrategy = FaunusProgressReportStrategy()
        reportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 100% reduce 0%")
        progressbar = reportStrategy.job_progress_bar_list[-1]
        self.assertEquals(50, progressbar.value)

    def test_get_progress_mapper_received_job_complete_signal(self):
        reportStrategy = FaunusProgressReportStrategy()
        reportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 100% reduce 0%")
        reportStrategy.report("14:21:51 INFO mapred.JobClient: Job complete: job_201402121231_0117")
        progressbar = reportStrategy.job_progress_bar_list[-1]
        self.assertEquals(100, progressbar.value)

if __name__ == '__main__':
    unittest.main()
