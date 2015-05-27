##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
"""
Progress bar printing
"""
import sys
import time
import datetime


class ProgressPrinter(object):

    def __init__(self):
        self.job_count = 0
        self.last_progress = []
        self.job_start_times = []
        self.initializing = True

    def print_progress(self, progress, finished):
        """
        Print progress information on progress bar.

        Parameters
        ----------
        progress : list of dictionary
            The progresses of the jobs initiated by the command

        finished : bool
            Indicate whether the command is finished
        """
        if progress == False:
            return

        total_job_count = len(progress)
        new_added_job_count = total_job_count - self.job_count

        # if it was printing initializing, overwrite initializing in the same line
        # therefore it requires 1 less new line
        number_of_new_lines = new_added_job_count if not self.initializing else new_added_job_count - 1

        if total_job_count > 0:
            self.initializing = False

        for i in range(0, new_added_job_count):
            self.job_start_times.append(time.time())

        self.job_count = total_job_count
        self.print_progress_as_text(progress, number_of_new_lines, self.job_start_times, finished)

    def print_progress_as_text(self, progress, number_of_new_lines, start_times, finished):
        """
        Print progress information on command line progress bar

        Parameters
        ----------
        progress : List of dictionary
            The progresses of the jobs initiated by the command
        number_of_new_lines: int
            number of new lines to print in the command line
        start_times: List of time
            list of observed starting time for the jobs initiated by the command
        finished : boolean
            Indicate whether the command is finished
        """
        if not progress:
            initializing_text = "\rinitializing..."
            sys.stdout.write(initializing_text)
            sys.stdout.flush()
            return len(initializing_text)

        progress_summary = []

        for index in range(0, len(progress)):
            p = progress[index]['progress']
            # Check if the Progress has tasks_info field
            message = ''
            if 'tasks_info' in progress[index].keys():
                retried_tasks = progress[index]['tasks_info']['retries']
                message = "Tasks retries:%s" %(retried_tasks)

            total_bar_length = 25
            factor = 100 / total_bar_length

            num_star = int(p / factor)
            num_dot = total_bar_length - num_star
            number = "%3.2f" % p

            time_string = datetime.timedelta(seconds = int(time.time() - start_times[index]))
            progress_summary.append("\r[%s%s] %6s%% %s Time %s" % ('=' * num_star, '.' * num_dot, number, message, time_string))

        for i in range(0, number_of_new_lines):
            # calculate the index for fetch from the list from the end
            # if number_of_new_lines is 3, will need to take progress_summary[-4], progress_summary[-3], progress_summary[-2]
            # index will be calculated as -4, -3 and -2 respectively
            index = -1 - number_of_new_lines + i
            previous_step_progress = progress_summary[index]
            previous_step_progress = previous_step_progress + "\n"
            sys.stdout.write(previous_step_progress)

        current_step_progress = progress_summary[-1]

        if finished:
            current_step_progress = current_step_progress + "\n"

        sys.stdout.write(current_step_progress)
        sys.stdout.flush()