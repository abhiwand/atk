##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
import re
from intel_analytics.logger import stdout_logger
from progress import Progress


class ReportStrategy(object):
    """
    Base report strategy class. It defines the signature
    of reporting job status for input
    """
    def report(self, line):
        pass

    def handle_error(self, error_code, error_message):
        pass


class PrintReportStrategy(ReportStrategy):
    def report(self, line):
        print line



class ProgressReportStrategy(ReportStrategy):
    """
    Subclass of ReportStrategy which captures map reduce job progress
    and displays it in progress bar
    """
    def __init__(self):
        self.job_progress_bar_list = []
        self.progress_list = []

        # show this bar for initialization
        self.initialization_progressbar = self.get_new_progress_bar("Initialization")
        self.initialization_progressbar._enable_animation()
        self.initialization_progressbar.update(100)

    def get_next_step_title(self):
        return "Step " + str(len(self.job_progress_bar_list) + 1)

    def report(self, line):
        progress = find_progress(line)
        if progress:
            if len(self.progress_list) == 0:
                self.initialization_progressbar._disable_animation()

            if len(self.progress_list) == 0 or self.progress_list[-1].total_progress > progress.total_progress:
                self.job_progress_bar_list.append(self.get_new_progress_bar(self.get_next_step_title()))
                self.progress_list.append(progress)

            self.job_progress_bar_list[-1].update(progress.total_progress)
            self.progress_list[-1] = progress

    def get_total_map_reduce_job_count(self):
        """
        :return currently known map reduce job count in the current
        job submission. For example, graphbuilder runs two map reduce jobs
        in the first phase, this method will return 1. In the second phase, this
        method will return 2:
        """
        return len(self.job_progress_bar_list)

    def get_all_map_reduce_jobs_progress_list(self):
        """
        :return list of progress for all the known map reduce job in the
        current job submission:
        """
        return self.progress_list

    def get_new_progress_bar(self, title):
        """
        Generate new progress bar instance
        :param title: setting the progress bar title
        :return: progress bar instance
        """
        progress_bar = Progress(title)
        progress_bar._repr_html_()
        return progress_bar

    def handle_error(self, error_code, error_message):
        if(len(self.job_progress_bar_list) == 0):
            self.initialization_progressbar.alert()
        else:
            self.job_progress_bar_list[-1].alert()

class FaunusProgressReportStrategy(ProgressReportStrategy):
    def is_job_complete_signaled(self, line):
        pattern = '.*Job complete.*'
        matched = re.match(pattern, line) != None
        return matched

    def report(self, line):
        if self.is_job_complete_signaled(line) and self.job_progress_bar_list[-1].value < 100:
            self.job_progress_bar_list[-1].update(100)
        else:
            super(FaunusProgressReportStrategy, self).report(line)


class MapOnlyProgressReportStrategy(ProgressReportStrategy):
    def report(self, line):
        progress = find_progress(line)
        if progress:
            if len(self.progress_list) == 0:
                self.initialization_progressbar._disable_animation()

            if len(self.progress_list) == 0 or self.progress_list[-1].mapper_progress > progress.mapper_progress:
                self.job_progress_bar_list.append(self.get_new_progress_bar(self.get_next_step_title()))
                self.progress_list.append(progress)

            self.job_progress_bar_list[-1].update(progress.mapper_progress)
            self.progress_list[-1] = progress

log_pattern = 'Details at logfile: (.*)'
class PigJobReportStrategy(ReportStrategy):

    def __init__(self):
        self.content = {}
        self.is_during_recording = False

    def report(self, line):

        log_file = self.get_log_file(line)
        if log_file:
            file = open(log_file, "r")
            stdout_logger.debug(file.read())

        line = line.strip()
        if line == 'Pig job status report-Start:':
            self.is_during_recording = True
            return
        elif line == 'Pig job status report-End:':
            self.is_during_recording = False
            return

        if not self.is_during_recording:
            return

        splits = line.split(':')
        if len(splits) == 2:
            self.content[splits[0]] = splits[1]

    def get_log_file(self, line):
        match = re.match(log_pattern, line)
        if not match:
            return None
        else:
            return match.group(1)




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


class MapReduceProgress(object):
    """
    representation of map reduce job progress
    """

    def __init__(self, mapper_progress, reducer_progress):
        """
        initialize map reduce progress instance
        :param mapper_progress:
        :param reducer_progress:
        """
        self.mapper_progress = mapper_progress
        self.reducer_progress = reducer_progress
        self._total_progress = (self.mapper_progress + self.reducer_progress) * 0.5

    @property
    def total_progress(self):
        """
        Calculate total progress based on current mapper
        and reducer progress
        :return total progress:
        """
        return self._total_progress

    @total_progress.setter
    def total_progress(self, val):
        self._total_progress = val

#Pig doesn't log JobClient's output, instead the MapReduceLauncher class prints the progress
pig_progress_output_pattern = re.compile(r".*?MapReduceLauncher.*?([0-9]*?%)")

def get_pig_progress(line_value):
    match = re.match(pig_progress_output_pattern, line_value)
    if not match:
        return None
    else:
        pig_progress = int(match.group(1)[:-1])
        return pig_progress


class JobReportService:
    """
    Class for giving report based on input string values,
    which can be from for example stdout or stderr from executing
    commands.
    """

    def __init__(self):
        self.report_strategy_list = []

    def add_report_strategy(self, report_strategy):
        """
        assign a strategy instance to be use
        :param report_strategy:
        """
        self.report_strategy_list.append(report_strategy)

    def report_line(self, line):
        """
        giving report with the assigned strategy for single input line
        :param line:
        """
        for strategy in self.report_strategy_list:
            if strategy:
                strategy.report(line)

    def report_lines(self, lines):
        """
        giving reports for multiple input lines
        :param lines:
        """
        for line in lines:
            self.report_line(line)

    def handle_error(self, error_code, error_message):
        """
        handle error condition
        :param line:
        """
        for strategy in self.report_strategy_list:
            if strategy:
                strategy.handle_error(error_code, error_message)

