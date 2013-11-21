import re

mr_progress_output_pattern = re.compile(r".*?mapred.JobClient:.*?map.*?([0-9]*?%).*?reduce.*?([0-9]*?%)")
def find_progress(line_value):
    """
    explore map reduce output based on input line
    :param line_value: a input line. can be from stdout
    :return: map reduce progress object
    """
    match = re.match(mr_progress_output_pattern, line_value)
    if not match:
        return None
    else:
        map_progress = int(match.group(1)[:-1])
        reduce_progress = int(match.group(2)[:-1])
        return MapReduceProgress(map_progress, reduce_progress)

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