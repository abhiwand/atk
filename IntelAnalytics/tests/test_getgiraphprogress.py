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
        self.assertEquals("Step 2", strategy.get_next_step_title())

    def test_first_phase_middle(self):
        strategy = GiraphProgressReportStrategy()
        strategy.report("13/11/21 11:34:03 INFO mapred.JobClient:  map 0% reduce 0%")
        strategy.report("13/11/21 11:34:21 INFO mapred.JobClient:  map 33% reduce 0%")
        progress = strategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(1, strategy.get_total_map_reduce_job_count())
        self.assertEquals(33, progress.mapper_progress)
        self.assertEquals(0, progress.reducer_progress)
        self.assertEquals(33, progress.total_progress)
        self.assertEquals("Step 2", strategy.get_next_step_title())

    def test_first_phase_complete(self):
        strategy = GiraphProgressReportStrategy()
        strategy.report("13/11/21 11:34:21 INFO mapred.JobClient:  map 33% reduce 0%")
        strategy.report("13/11/21 11:34:23 INFO mapred.JobClient:  map 100% reduce 0%")
        progress = strategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(2, strategy.get_total_map_reduce_job_count())
        self.assertEquals(100, progress.mapper_progress)
        self.assertEquals(100, progress.total_progress)

        # in giraph, progress reaches 100% will automatically generate the next progress bar
        # therefore, progress bar 1 finishes will generate progress bar 2,
        # the next set will therefore be progress bar 3
        self.assertEquals("Step 3", strategy.get_next_step_title())

    def test_second_phase_complete(self):
        strategy = GiraphProgressReportStrategy()
        strategy.report("13/11/21 11:34:21 INFO mapred.JobClient:  map 33% reduce 0%")
        strategy.report("13/11/21 11:34:23 INFO mapred.JobClient:  map 100% reduce 0%")
        strategy.report("13/11/21 11:34:26 INFO mapred.JobClient: Job complete: job_201311191412_0063")
        progress_step_1 = strategy.get_all_map_reduce_jobs_progress_list()[0]
        progress_step_2 = strategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(2, strategy.get_total_map_reduce_job_count())
        self.assertEquals(100, progress_step_1.mapper_progress)
        self.assertEquals(100, progress_step_1.total_progress)
        self.assertEquals(100, progress_step_2.mapper_progress)
        self.assertEquals(100, progress_step_2.total_progress)
        self.assertEquals("Step 3", strategy.get_next_step_title())


    def test_get_giraph_complete_message(self):
        strategy = GiraphProgressReportStrategy()
        self.assertFalse(strategy._is_computation_complete("13/11/21 11:34:23 INFO mapred.JobClient:  map 100% reduce 0%"))
        self.assertFalse(strategy._is_computation_complete("13/11/21 11:34:02 INFO mapred.JobClient: Running job: job_201311191412_0063"))
        self.assertFalse(strategy._is_computation_complete("13/11/21 11:34:02 INFO common.GiraphTitanUtils: opened Titan Graph"))
        self.assertTrue(strategy._is_computation_complete("13/11/21 11:34:26 INFO mapred.JobClient: Job complete: job_201311191412_0063"))

if __name__ == '__main__':
    unittest.main()
