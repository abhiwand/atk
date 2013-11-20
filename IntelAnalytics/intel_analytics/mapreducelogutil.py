import re

from mapreduceprogress import MapReduceProgress

"""Utility class for exploring map reduce job log"""
class MapReduceLogUtil:

    def findProgress(self, lineValue):
        """
        explore map reduce output based on input line
        :param lineValue: a input line. can be from stdout
        :return: map reduce progress object
        """
        pattern = re.compile(r".*?mapred.JobClient:.*?map.*?([0-9]*?%).*?reduce.*?([0-9]*?%)")
        match = re.match(pattern, lineValue)
        if not match:
            return None
        else:
            mapProgress = int(match.group(1)[:-1])
            reduceProgress = int(match.group(2)[:-1])
            return MapReduceProgress(mapProgress, reduceProgress)


