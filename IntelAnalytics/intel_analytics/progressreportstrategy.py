from intel_analytics.jobreportservice import ReportStrategy
from mapreducelogutil import find_progress
from progress import Progress

"""
Subclass of ReportStrategy which captures map reduce job progress
and displays it in progress bar
"""
class ProgressReportStrategy(ReportStrategy):
    def __init__(self):
        self.job_progress_bar_list = []
        self.progress_list = []

        # show this bar for initialization
        self.initialization_progressbar = self.get_new_progress_bar("Initialization")
        self.initialization_progressbar._enable_animation()
        self.initialization_progressbar.update(100)

    def get_next_step_title(self):
        return "Step " + str(len(self.job_progress_bar_list) + 1)

    def report(self, line):
        progress = find_progress(line)
        if progress:
            if len(self.progress_list) == 0:
                self.initialization_progressbar._disable_animation()

            if len(self.progress_list) == 0 or self.job_progress_bar_list[-1].value >= 100:
                self.job_progress_bar_list.append(self.get_new_progress_bar(self.get_next_step_title()))
                self.progress_list.append(progress)

            self.job_progress_bar_list[-1].update(progress.total_progress)
            self.progress_list[-1] = progress

    def get_total_map_reduce_job_count(self):
        """
        :return currently known map reduce job count in the current
        job submission. For example, graphbuilder runs two map reduce jobs
        in the first phase, this method will return 1. In the second phase, this
        method will return 2:
        """
        return len(self.job_progress_bar_list)

    def get_all_map_reduce_jobs_progress_list(self):
        """
        :return list of progress for all the known map reduce job in the
        current job submission:
        """
        return self.progress_list

    def get_new_progress_bar(self, title):
        """
        Generate new progress bar instance
        :param title: setting the progress bar title
        :return: progress bar instance
        """
        progress_bar = Progress(title)
        progress_bar._repr_html_()
        return progress_bar
