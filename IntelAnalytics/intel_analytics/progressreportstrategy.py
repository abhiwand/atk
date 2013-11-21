from reportstrategy import ReportStrategy
from mapreducelogutil import MapReduceLogUtil
from progress import Progress

from intel_analytics.mapreduceprogress import MapReduceProgress

"""
Subclass of ReportStrategy which captures map reduce job progress
and displays it in progress bar
"""
class ProgressReportStrategy(ReportStrategy):
    def __init__(self):
        self.job_progress_list = []
        self.log_util = MapReduceLogUtil()

    def report(self, line):
        progress = self.log_util.find_progress(line)
        if progress is not None:
            mapper_progress = progress.get_mapper_progress()
            reducer_progress = progress.get_reducer_progress()

            if len(self.job_progress_list) == 0 or self.job_progress_list[-1].value == 100:
                self.job_progress_list.append(self.get_new_progress_bar("Progress"))

            total_progress_value = (mapper_progress + reducer_progress) * 0.5
            self.job_progress_list[-1].update(total_progress_value)

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
