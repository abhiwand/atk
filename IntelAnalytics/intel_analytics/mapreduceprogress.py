class MapReduceProgress:
    def __init__(self, mapperProgress, reducerProgress):
        self.mapperProgress = mapperProgress
        self.reducerProgress = reducerProgress

    def getMapperProgress(self):
        return self.mapperProgress

    def getReducerProgress(self):
        return self.reducerProgress

    def getTotalProgress(self):
        return int((self.mapperProgress + self.reducerProgress) * 0.5)

