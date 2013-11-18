from progress import Progress
MINIMUM_PROGRESS = 1

class MapReduceProgressBar():
    def __init__(self):
        self.mapperProgressbar = Progress("mapper progress")
        self.mapperProgressbar._repr_html_()
        self.reducerProgressbar = Progress("reducer progress")
        self.reducerProgressbar._repr_html_()

    def getMapperProgressBarValue(self):
        value = self.mapperProgressbar.value
        return self.getRealProgressValue(value)

    def getReducerProgressBarValue(self):
        value =  self.reducerProgressbar.value
        return self.getRealProgressValue(value)


    def getRealProgressValue(self, value):
        if (value == MINIMUM_PROGRESS):
            return 0
        else:
            return value

