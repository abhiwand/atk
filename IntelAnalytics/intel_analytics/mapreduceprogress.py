"""
representation of map reduce job progress
"""
class MapReduceProgress:

    def __init__(self, mapper_progress, reducer_progress):
        """
        initialize map reduce progress instance
        :param mapper_progress:
        :param reducer_progress:
        """
        self.mapper_progress = mapper_progress
        self.reducer_progress = reducer_progress

    @property
    def total_progress(self):
        """
        Calculate total progress based on current mapper
        and reducer progress
        :return total progress:
        """
        return (self.mapper_progress + self.reducer_progress) * 0.5

