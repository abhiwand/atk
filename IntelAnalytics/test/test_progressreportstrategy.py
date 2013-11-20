import unittest
from intel_analytics.progressreportstrategy import ProgressReportStrategy


class TestProgressReportStrategy(unittest.TestCase):


    def test_start_with_0_job(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.assertEquals(0, self.progressReportStrategy.getTotalMapReduceJobCounts())

    def test_start_with_0_job_in_list(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.assertEquals(0, len(self.progressReportStrategy.getAllMapReduceJobsProgressList()))

    def test_1_job_with_progress(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:35:58 INFO mapred.JobClient:  map 66% reduce 0%")
        self.assertEquals(1, self.progressReportStrategy.getTotalMapReduceJobCounts())
        progress = self.progressReportStrategy.getAllMapReduceJobsProgressList()[0]
        self.assertEquals(66, progress.getMapperProgress())
        self.assertEquals(0, progress.getReducerProgress())
        self.assertEquals(33, progress.getTotalProgress())

    def test_1_job_completion(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.assertEquals(1, self.progressReportStrategy.getTotalMapReduceJobCounts())
        progress = self.progressReportStrategy.getAllMapReduceJobsProgressList()[0]
        self.assertEquals(100, progress.getMapperProgress())
        self.assertEquals(100, progress.getReducerProgress())
        self.assertEquals(100, progress.getTotalProgress())

    def test_second_job_start(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.progressReportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 0% reduce 0%")
        self.assertEquals(2, self.progressReportStrategy.getTotalMapReduceJobCounts())
        progress = self.progressReportStrategy.getAllMapReduceJobsProgressList()[-1]
        self.assertEquals(0, progress.getMapperProgress())
        self.assertEquals(0, progress.getReducerProgress())
        self.assertEquals(0, progress.getTotalProgress())

    def test_second_job_with_progress(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.progressReportStrategy.report("13/11/14 14:35:53 INFO mapred.JobClient:  map 33% reduce 0%")
        self.assertEquals(2, self.progressReportStrategy.getTotalMapReduceJobCounts())
        progress = self.progressReportStrategy.getAllMapReduceJobsProgressList()[-1]
        self.assertEquals(33, progress.getMapperProgress())
        self.assertEquals(0, progress.getReducerProgress())
        self.assertEquals(16, progress.getTotalProgress())

    def test_second_job_completion(self):
        self.progressReportStrategy = ProgressReportStrategy()
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.progressReportStrategy.report("13/11/14 14:36:07 INFO mapred.JobClient:  map 100% reduce 100%")
        self.assertEquals(2, self.progressReportStrategy.getTotalMapReduceJobCounts())
        progress = self.progressReportStrategy.getAllMapReduceJobsProgressList()[-1]
        self.assertEquals(100, progress.getMapperProgress())
        self.assertEquals(100, progress.getReducerProgress())
        self.assertEquals(100, progress.getTotalProgress())

if __name__ == '__main__':
    unittest.main()
