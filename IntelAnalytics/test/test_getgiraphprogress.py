import unittest
from intel_analytics.giraphprogressreportstrategy import GiraphProgressReportStrategy

class TestGetGiraphProgress(unittest.TestCase):

    def test_start(self):
        strategy = GiraphProgressReportStrategy()
        self.assertEquals(0, strategy.get_total_map_reduce_job_count())

    def test_first_phase_start(self):
        strategy = GiraphProgressReportStrategy()
        strategy.report("13/11/21 11:34:03 INFO mapred.JobClient:  map 0% reduce 0%")
        progress = strategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(1, strategy.get_total_map_reduce_job_count())
        self.assertEquals(0, progress.mapper_progress)
        self.assertEquals(0, progress.reducer_progress)
        self.assertEquals(0, progress.total_progress)

    def test_first_phase_middle(self):
        strategy = GiraphProgressReportStrategy()
        strategy.report("13/11/21 11:34:03 INFO mapred.JobClient:  map 0% reduce 0%")
        strategy.report("13/11/21 11:34:21 INFO mapred.JobClient:  map 33% reduce 0%")
        progress = strategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(1, strategy.get_total_map_reduce_job_count())
        self.assertEquals(33, progress.mapper_progress)
        self.assertEquals(0, progress.reducer_progress)
        self.assertEquals(33, progress.total_progress)

    def test_first_phase_complete(self):
        strategy = GiraphProgressReportStrategy()
        strategy.report("13/11/21 11:34:21 INFO mapred.JobClient:  map 33% reduce 0%")
        strategy.report("13/11/21 11:34:23 INFO mapred.JobClient:  map 100% reduce 0%")
        progress = strategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(2, strategy.get_total_map_reduce_job_count())
        self.assertEquals(100, progress.mapper_progress)
        self.assertEquals(0, progress.reducer_progress)
        self.assertEquals(100, progress.total_progress)

        progress2 = strategy.get_all_map_reduce_jobs_progress_list()[1]
        self.assertEquals(0, progress2.mapper_progress)
        self.assertEquals(0, progress2.reducer_progress)
        self.assertEquals(0, progress2.total_progress)

    def test_second_phase_complete(self):
        strategy = GiraphProgressReportStrategy()
        strategy.report("13/11/21 11:34:21 INFO mapred.JobClient:  map 33% reduce 0%")
        strategy.report("13/11/21 11:34:23 INFO mapred.JobClient:  map 100% reduce 0%")
        strategy.report("13/11/21 11:34:26 INFO mapred.JobClient: Job complete: job_201311191412_0063")
        progress = strategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(2, strategy.get_total_map_reduce_job_count())
        self.assertEquals(100, progress.mapper_progress)
        self.assertEquals(0, progress.reducer_progress)
        self.assertEquals(100, progress.total_progress)

        progress2 = strategy.get_all_map_reduce_jobs_progress_list()[1]
        self.assertEquals(100, progress2.mapper_progress)
        self.assertEquals(100, progress2.reducer_progress)
        self.assertEquals(100, progress2.total_progress)

    def test_get_giraph_complete_message(self):
        strategy = GiraphProgressReportStrategy()
        self.assertFalse(strategy._is_computation_complete("13/11/21 11:34:23 INFO mapred.JobClient:  map 100% reduce 0%"))
        self.assertFalse(strategy._is_computation_complete("13/11/21 11:34:02 INFO mapred.JobClient: Running job: job_201311191412_0063"))
        self.assertFalse(strategy._is_computation_complete("13/11/21 11:34:02 INFO common.GiraphTitanUtils: opened Titan Graph"))
        self.assertTrue(strategy._is_computation_complete("13/11/21 11:34:26 INFO mapred.JobClient: Job complete: job_201311191412_0063"))

if __name__ == '__main__':
    unittest.main()
