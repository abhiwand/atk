from intel_analytics.mapreducelogutil import MapReduceProgress
from intel_analytics.jobreportservice import ReportStrategy
from mapreducelogutil import find_progress
from progress import Progress

"""
Subclass of ReportStrategy which captures map reduce job progress
and displays it in progress bar
"""
class ProgressReportStrategy(ReportStrategy):
    def __init__(self):
        self.job_progress_list = []

    def report(self, line):
        progress = find_progress(line)
        if progress:
            if len(self.job_progress_list) == 0 or self.job_progress_list[-1].value >= 100:
                self.job_progress_list.append(self.get_new_progress_bar("Progress"))

            self.job_progress_list[-1].update(progress.total_progress)

    def get_total_map_reduce_job_count(self):
        """
        :return currently known map reduce job count in the current
        job submission. For example, graphbuilder runs two map reduce jobs
        in the first phase, this method will return 1. In the second phase, this
        method will return 2:
        """
        return len(self.job_progress_list)

    def get_all_map_reduce_jobs_progress_list(self):
        """
        :return list of progress for all the known map reduce job in the
        current job submission:
        """
        progress_list = []

        for bar in self.job_progress_list:
            total_progress_value = bar.value
            mapper_progress = min(100, 2 * total_progress_value)
            reducer_progress = 2 * total_progress_value - mapper_progress
            progress = MapReduceProgress(mapper_progress, reducer_progress)
            progress_list.append(progress)

        return progress_list

    def get_new_progress_bar(self, title):
        """
        Generate new progress bar instance
        :param title: setting the progress bar title
        :return: progress bar instance
        """
        progress_bar = Progress(title)
        progress_bar._repr_html_()
        return progress_bar
