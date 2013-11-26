import re

from mapreducelogutil import get_pig_progress
from intel_analytics.jobreportservice import ReportStrategy
from progress import Progress

job_completion_pattern = re.compile(r".*?MapReduceLauncher - 100% complete")

class PigProgressReportStrategy(ReportStrategy):
    def __init__(self):
        progress_bar = Progress("Progress")
        progress_bar._enable_animation()
        progress_bar._repr_html_()
        self.progress_bar = progress_bar
                
    def report(self, line):
        progress = get_pig_progress(line)

        if progress:
            self.progress_bar.update(progress)
            
        if self._is_computation_complete(line):
            self.progress_bar._disable_animation()

    def _is_computation_complete(self, line):
        match = re.match(job_completion_pattern, line)
        if match:
            return True
        else:
            return False



