from intel_analytics.mapreducelogutil import MapReduceProgress
from mapreducelogutil import find_progress
from progressreportstrategy import ProgressReportStrategy
import re
from threading import Thread
import time

job_completion_pattern = re.compile(r".*?mapred.JobClient: Job complete")


class GiraphProgressReportStrategy(ProgressReportStrategy):

    def __init__(self):
        self.job_progress_bar_list = []
        self.progress_list = []
        self.job_progress_bar_list.append(self.get_new_progress_bar("Progress"))
        self.progress_list.append(MapReduceProgress(0, 0))

    def report(self, line):
        progress = find_progress(line)
        if progress:
            # giraph is a mapper only job
            progressGiraph = MapReduceProgress(progress.mapper_progress * 0.5, 0)
            self.job_progress_bar_list[-1].update(progressGiraph.mapper_progress)
            progressGiraph.total_progress = progressGiraph.mapper_progress
            self.progress_list[-1] = progressGiraph

        if self._is_computation_complete(line):
            self.progress_list[-1] = MapReduceProgress(100, 100)
            self.job_progress_bar_list[-1].update(100)

    def _is_computation_complete(self, line):
        match = re.match(job_completion_pattern, line)
        if match:
            return True
        else:
            return False



