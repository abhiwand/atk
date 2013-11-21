import re

from mapreduceprogress import MapReduceProgress

"""Utility class for exploring map reduce job log"""
class MapReduceLogUtil:

    def __init__(self):
        self.pattern = re.compile(r".*?mapred.JobClient:.*?map.*?([0-9]*?%).*?reduce.*?([0-9]*?%)")

    def find_progress(self, line_value):
        """
        explore map reduce output based on input line
        :param line_value: a input line. can be from stdout
        :return: map reduce progress object
        """
        match = re.match(self.pattern, line_value)
        if not match:
            return None
        else:
            map_progress = int(match.group(1)[:-1])
            reduce_progress = int(match.group(2)[:-1])
            return MapReduceProgress(map_progress, reduce_progress)


