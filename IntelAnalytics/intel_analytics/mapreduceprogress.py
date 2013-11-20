"""
representation of map reduce job progress
"""
class MapReduceProgress:

    def __init__(self, mapperProgress, reducerProgress):
        """
        initialize map reduce progress instance
        :param mapperProgress:
        :param reducerProgress:
        """
        self.mapperProgress = mapperProgress
        self.reducerProgress = reducerProgress

    def getMapperProgress(self):
        """
        :return mapper progress:
        """
        return self.mapperProgress

    def getReducerProgress(self):
        """
        :return reducer progress:
        """
        return self.reducerProgress

    def getTotalProgress(self):
        """
        Calculate total progress based on current mapper
        and reducer progress
        :return total progress:
        """
        return (self.mapperProgress + self.reducerProgress) * 0.5

