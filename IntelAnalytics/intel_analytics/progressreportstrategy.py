from reportstrategy import ReportStrategy
from mapreducelogutil import MapReduceLogUtil
from progress import Progress
MINIMUM_PROGRESS = 1

class ProgressReportStrategy(ReportStrategy):


    def __init__(self):
        self.logUtil = MapReduceLogUtil()
        self.mapperProgressbar = Progress("mapper progress")
        self.mapperProgressbar._repr_html_()
        self.reducerProgressbar = Progress("reducer progress")
        self.reducerProgressbar._repr_html_()

        # make the progress bar starts from 1 percent so it is visible
        self.mapperProgressbar.update(MINIMUM_PROGRESS)
        self.reducerProgressbar.update(MINIMUM_PROGRESS)

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

            self.mapperProgressbar.update(mapperProgress)
            self.reducerProgressbar.update(reducerProgress)