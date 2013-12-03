import re

from intel_analytics.report import ReportStrategy, get_pig_progress
from progress import Progress

job_completion_pattern = re.compile(r".*?MapReduceLauncher - 100% complete")

class PigProgressReportStrategy(ReportStrategy):
    def __init__(self):
        # show this bar for initialization
        self.initialization_progressbar = Progress("Initialization")
        self.initialization_progressbar._repr_html_()
        self.initialization_progressbar._enable_animation()
        self.initialization_progressbar.update(100)

        self.progress_bar = None
                
    def report(self, line):
        progress = get_pig_progress(line)

        if progress:
            if not self.progress_bar:
                self.initialization_progressbar._disable_animation()
                progress_bar = Progress("Progress")
                progress_bar._enable_animation()
                progress_bar._repr_html_()
                self.progress_bar = progress_bar

            self.progress_bar.update(progress)
            
        if self._is_computation_complete(line):
            self.progress_bar._disable_animation()

    def _is_computation_complete(self, line):
        match = re.match(job_completion_pattern, line)
        if match:
            return True
        else:
            return False



