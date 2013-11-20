from reportstrategy import ReportStrategy
from mapreducelogutil import MapReduceLogUtil
from progress import Progress

from intel_analytics.mapreduceprogress import MapReduceProgress

class ProgressReportStrategy(ReportStrategy):

    def __init__(self):
        self.jobProgressList = []
        self.logUtil = MapReduceLogUtil()

    def report(self, line):
        progress = self.logUtil.findProgress(line)
        if(progress != None):
            mapperProgress = progress.getMapperProgress()
            reducerProgress = progress.getReducerProgress()

            if(len(self.jobProgressList) == 0 or self.jobProgressList[-1].value == 100):
                self.jobProgressList.append(self.getNewProgressBar("Progress"))

            totalProgressValue = (mapperProgress + reducerProgress) * 0.5
            self.jobProgressList[-1].update(totalProgressValue)

    def getTotalMapReduceJobCounts(self):
        return len(self.jobProgressList)

    def getAllMapReduceJobsProgressList(self):
        progressList = []

        for bar in self.jobProgressList:
            totalProgressValue = bar.value
            mapperProgress = min(100, 2 * totalProgressValue)
            reducerProgress = 2 * totalProgressValue - mapperProgress
            progress = MapReduceProgress(mapperProgress, reducerProgress)
            progressList.append(progress)

        return progressList

    def getNewProgressBar(self, title):
        progressBar = Progress(title)
        progressBar._repr_html_()
        return progressBar
