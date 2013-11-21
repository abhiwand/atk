import unittest
from intel_analytics.progressreportstrategy import ProgressReportStrategy


class TestProgressReportStrategy(unittest.TestCase):


    def test_start_with_0_job(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.assertEquals(0, self.progressReportStrategy.get_total_map_reduce_job_count())

    def test_start_with_0_job_in_list(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.assertEquals(0, len(self.progressReportStrategy.get_all_map_reduce_jobs_progress_list()))

    def test_1_job_with_progress(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:35:58 INFO mapred.JobClient:  map 66% reduce 0%")
        self.assertEquals(1, self.progressReportStrategy.get_total_map_reduce_job_count())
        progress = self.progressReportStrategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(66, progress.get_mapper_progress())
        self.assertEquals(0, progress.get_reducer_progress())
        self.assertEquals(33, progress.get_total_progress())

    def test_1_job_completion(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.assertEquals(1, self.progressReportStrategy.get_total_map_reduce_job_count())
        progress = self.progressReportStrategy.get_all_map_reduce_jobs_progress_list()[0]
        self.assertEquals(100, progress.get_mapper_progress())
        self.assertEquals(100, progress.get_reducer_progress())
        self.assertEquals(100, progress.get_total_progress())

    def test_second_job_start(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.progressReportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 0% reduce 0%")
        self.assertEquals(2, self.progressReportStrategy.get_total_map_reduce_job_count())
        progress = self.progressReportStrategy.get_all_map_reduce_jobs_progress_list()[-1]
        self.assertEquals(0, progress.get_mapper_progress())
        self.assertEquals(0, progress.get_reducer_progress())
        self.assertEquals(0, progress.get_total_progress())

    def test_second_job_with_progress(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.progressReportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 33% reduce 0%")
        self.assertEquals(2, self.progressReportStrategy.get_total_map_reduce_job_count())
        progress = self.progressReportStrategy.get_all_map_reduce_jobs_progress_list()[-1]
        self.assertEquals(33, progress.get_mapper_progress())
        self.assertEquals(0, progress.get_reducer_progress())
        self.assertEquals(16.5, progress.get_total_progress())

    def test_second_job_completion(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.assertEquals(2, self.progressReportStrategy.get_total_map_reduce_job_count())
        progress = self.progressReportStrategy.get_all_map_reduce_jobs_progress_list()[-1]
        self.assertEquals(100, progress.get_mapper_progress())
        self.assertEquals(100, progress.get_reducer_progress())
        self.assertEquals(100, progress.get_total_progress())

if __name__ == '__main__':
    unittest.main()
