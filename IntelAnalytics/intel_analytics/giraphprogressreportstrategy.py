from intel_analytics.mapreducelogutil import MapReduceProgress
from mapreducelogutil import find_progress
from progressreportstrategy import ProgressReportStrategy
import re

job_completion_pattern = re.compile(r".*?mapred.JobClient: Job complete")
class GiraphProgressReportStrategy(ProgressReportStrategy):

    def report(self, line):
        progress = find_progress(line)
        if progress:
            if len(self.job_progress_bar_list) == 0:
                self.job_progress_bar_list.append(self.get_new_progress_bar("Progress"))
                self.progress_list.append(progress)

            # giraph is a mapper only job
            self.job_progress_bar_list[-1].update(progress.mapper_progress)
            progress.total_progress = progress.mapper_progress
            self.progress_list[-1] = progress

            # mapper job finishes, create second progress bar automatically since
            # giraph does not print any message indicating beginning of the second phase
            if progress.mapper_progress == 100:
                self.job_progress_bar_list.append(self.get_new_progress_bar("Progress"))
                self.progress_list.append(MapReduceProgress(0, 0))

        if self._is_computation_complete(line):
            self.progress_list[-1] = MapReduceProgress(100, 100)
            self.job_progress_bar_list[-1].update(100)

    def _is_computation_complete(self, line):
        match = re.match(job_completion_pattern, line)
        if match:
            return True
        else:
            return False