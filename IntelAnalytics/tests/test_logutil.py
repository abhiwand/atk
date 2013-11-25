import unittest
from intel_analytics.mapreducelogutil import find_progress


class TestLogUtil(unittest.TestCase):
    def test_invalid_line(self):
        progress = find_progress("13/11/14 14:35:52 INFO mapred.JobClient: Running job: job_201311121330_0046")
        self.assertEqual(progress, None)

    def test_empty_line(self):
        progress = find_progress("")
        self.assertEquals(progress, None)

    def test_valid_line_1(self):
        progress = find_progress("13/11/14 14:36:05 INFO mapred.JobClient:  map 100% reduce 33%")
        self.assertEquals(100, progress.mapper_progress)
        self.assertEquals(33, progress.reducer_progress)

    def test_valid_line_2(self):
        progress = find_progress("13/11/14 14:36:05 INFO mapred.JobClient:  map 0% reduce 0%")
        self.assertEquals(0, progress.mapper_progress)
        self.assertEquals(0, progress.reducer_progress)

    def test_reading_from_file(self):
        currentProgress = None
        with open("../tests/MapReduceLogSample", "r") as logFile:
            for line in logFile:
                progress = find_progress(line)
                if(progress != None):
                    currentProgress = progress
                    #print("map:" + str(progress.getMapperProgress()) + ", reduce:" + str(progress.getReducerProgress()))

        self.assertEquals(100, currentProgress.mapper_progress)
        self.assertEquals(100, currentProgress.reducer_progress)





if __name__ == '__main__':
    unittest.main()
