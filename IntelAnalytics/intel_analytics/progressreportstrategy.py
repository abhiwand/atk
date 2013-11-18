from reportstrategy import ReportStrategy
from mapreducelogutil import MapReduceLogUtil
from progress import Progress
from intel_analytics.mapreduceprogressbar import MapReduceProgressBar

from intel_analytics.mapreduceprogress import MapReduceProgress
MINIMUM_PROGRESS = 1

class ProgressReportStrategy(ReportStrategy):


    def __init__(self):
        self.jobProgressList = []
        self.logUtil = MapReduceLogUtil()
        progressBar = MapReduceProgressBar()

        # make the progress bar starts from 1 percent so it is visible
        progressBar.mapperProgressbar.update(MINIMUM_PROGRESS)
        progressBar.reducerProgressbar.update(MINIMUM_PROGRESS)
        self.jobProgressList.append(progressBar)

    def report(self, line):
        progress = self.logUtil.findProgress(line)
        if(progress != None):
            mapperProgress = progress.getMapperProgress()
            reducerProgress = progress.getReducerProgress()

            # make the progress bar starts from 1 percent so it is visible
            if(mapperProgress == 0):
                mapperProgress = MINIMUM_PROGRESS
            if(reducerProgress == 0):
                reducerProgress = MINIMUM_PROGRESS

            if(self.jobProgressList[-1].getMapperProgressBarValue() == 100
            and self.jobProgressList[-1].getReducerProgressBarValue() == 100):
                self.jobProgressList.append(MapReduceProgressBar())


            self.jobProgressList[-1].mapperProgressbar.update(mapperProgress)
            self.jobProgressList[-1].reducerProgressbar.update(reducerProgress)

    def getTotalMapReduceJobCounts(self):
        return len(self.jobProgressList)

    def getAllMapReduceJobsProgressList(self):
        progressList = []

        for bar in self.jobProgressList:
            progress = MapReduceProgress(bar.getMapperProgressBarValue(), bar.getReducerProgressBarValue())
            progressList.append(progress)

        return progressList