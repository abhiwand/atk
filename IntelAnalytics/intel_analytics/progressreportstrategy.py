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
        self.jobProgressList = []
        self.logUtil = MapReduceLogUtil()

    def report(self, line):
        progress = self.logUtil.findProgress(line)
        if progress is not None:
            mapperProgress = progress.getMapperProgress()
            reducerProgress = progress.getReducerProgress()

            if len(self.jobProgressList) == 0 or self.jobProgressList[-1].value == 100:
                self.jobProgressList.append(self.getNewProgressBar("Progress"))

            totalProgressValue = (mapperProgress + reducerProgress) * 0.5
            self.jobProgressList[-1].update(totalProgressValue)

    def getTotalMapReduceJobCounts(self):
        """
        :return currently known map reduce job count in the current
        job submission. For example, graphbuilder runs two map reduce jobs
        in the first phase, this method will return 1. In the second phase, this
        method will return 2:
        """
        return len(self.jobProgressList)

    def getAllMapReduceJobsProgressList(self):
        """
        :return list of progress for all the known map reduce job in the
        current job submission:
        """
        progressList = []

        for bar in self.jobProgressList:
            totalProgressValue = bar.value
            mapperProgress = min(100, 2 * totalProgressValue)
            reducerProgress = 2 * totalProgressValue - mapperProgress
            progress = MapReduceProgress(mapperProgress, reducerProgress)
            progressList.append(progress)

        return progressList

    def getNewProgressBar(self, title):
        """
        Generate new progress bar instance
        :param title: setting the progress bar title
        :return: progress bar instance
        """
        progressBar = Progress(title)
        progressBar._repr_html_()
        return progressBar
