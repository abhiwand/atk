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
Command objects.
"""
import datetime
import time
import json
import logging
import sys
import re

from requests import HTTPError

logger = logging.getLogger(__name__)

import intelanalytics.rest.config as config
from intelanalytics.rest.iaserver import server
from collections import namedtuple

def _explicit_garbage_collection(age_to_delete_data = None, age_to_delete_meta_data = None):
    """
    Execute garbage collection out of cycle age ranges specified using the typesafe config duration format.
    :param age_to_delete_data: Minimum age for data deletion. Defaults to server config.
    :param age_to_delete_meta_data: Minimum age for meta data deletion. Defaults to server config.
    """
    execute_command("_admin:/_explicit_garbage_collection", None,
                    age_to_delete_data=age_to_delete_data,
                    age_to_delete_meta_data=age_to_delete_meta_data)


def execute_command(command_name, selfish, **arguments):
    """Executes command and returns the output."""
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    from intelanalytics.meta.results import get_postprocessor
    is_frame = command_info.result.has_key('schema')
    parent = None
    if is_frame:
        parent = command_info.result.get('parent')
        if parent and parent == getattr(selfish, '_id'):
            #print "Changing ID for existing proxy"
            selfish._id = command_info.result['id']
    postprocessor = get_postprocessor(command_name)
    if postprocessor:
        result = postprocessor(selfish, command_info.result)
    elif command_info.result.has_key('value') and len(command_info.result) == 1:
        result = command_info.result.get('value')
    elif is_frame:
        # TODO: remove this hack for plugins that return data frame
        from intelanalytics import get_frame
        if parent:
            result = selfish
        else:
            #print "Returning new proxy"
            result = get_frame(command_info.result['id'])
    else:
        result = command_info.result
    return result


class OperationCancelException(Exception):
    pass


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

class CommandRequest(object):
    @staticmethod
    def validate_arguments(parameter_types, arguments):
        validated = {}
        for (k, v) in arguments.items():
            if k not in parameter_types:
                raise ValueError("No parameter named '%s'" % k)
            validated[k] = v
            schema = parameter_types[k]
            if schema.get('type') == 'array':
                if isinstance(v, basestring) or not hasattr(v, '__iter__'):
                    validated[k] = [v]
        return validated

    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments

    def to_json_obj(self):
        """
        Returns json for REST payload.
        """
        return self.__dict__


class CommandInfo(object):
    __commands_regex = re.compile("""^http.+/(queries|commands)/\d+""")

    @staticmethod
    def is_valid_command_uri(uri):
        return CommandInfo.__commands_regex.match(uri) is not None

    def __init__(self, response_payload):
        self._payload = response_payload
        if not self.is_valid_command_uri(self.uri):
            raise ValueError("Invalid command URI: " + self.uri)

    def __repr__(self):
        return json.dumps(self._payload, indent=2, sort_keys=True)

    def __str__(self):
        return 'commands/%s "%s"' % (self.id_number, self.name)

    @property
    def id_number(self):
        return self._payload['id']

    @property
    def correlation_id(self):
        return self._payload.get('correlation_id', None)

    @property
    def name(self):
        return self._payload.get('name', None)

    @property
    def uri(self):
        try:
            return self._payload['links'][0]['uri']
        except KeyError:
            return ""

    @property
    def error(self):
        try:
            return self._payload['error']
        except KeyError:
            return None

    @property
    def complete(self):
        try:
            return self._payload['complete']
        except KeyError:
            return False

    @property
    def result(self):
        try:
            return self._payload['result']
        except KeyError:
            return False

    @property
    def progress(self):
        try:
            return self._payload['progress']
        except KeyError:
            return False

    def update(self, payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, command ID mismatch %d when expecting %d"\
                  % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload = payload


class Polling(object):

    @staticmethod
    def poll(uri, predicate=None, start_interval_secs=None, max_interval_secs=None, backoff_factor=None):
        """
        Issues GET methods on the given command uri until the response
        command_info cause the predicate to evalute True.  Exponential retry
        backoff

        Parameters
        ----------
        uri : str
            The uri of the command
        predicate : function
            Function with a single CommandInfo parameter which evaluates to True
            when the polling should stop.
        start_interval_secs : float
            Initial sleep interval for the polling, in seconds
        max_interval_secs : float
            Maximum sleep interval for the polling, in seconds
        backoff_factor : float
            Factor to increase the sleep interval on subsequent retries
        """
        if predicate is None:
            predicate = Polling._get_completion_status
        if start_interval_secs is None:
            start_interval_secs = config.polling_defaults.start_interval_secs
        if backoff_factor is None:
            backoff_factor = config.polling_defaults.backoff_factor
        if max_interval_secs is None:
            max_interval_secs = config.polling_defaults.max_interval_secs
        if not CommandInfo.is_valid_command_uri(uri):
            raise ValueError('Cannot poll ' + uri + ' - a /commands/{number} uri is required')
        interval_secs = start_interval_secs

        command_info = Polling._get_command_info(uri)

        printer = ProgressPrinter()
        if not predicate(command_info):
            last_progress = []

            next_poll_time = time.time()
            start_time = time.time()
            while True:
                if time.time() < next_poll_time:
                    time.sleep(start_interval_secs)
                    continue

                command_info = Polling._get_command_info(command_info.uri)
                finish = predicate(command_info)

                next_poll_time = time.time() + interval_secs
                progress = command_info.progress
                printer.print_progress(progress, finish)

                if finish:
                    break

                if last_progress == progress and interval_secs < max_interval_secs:
                    interval_secs = min(max_interval_secs, interval_secs * backoff_factor)

                last_progress = progress
            end_time = time.time()
            logger.info("polling %s completed after %0.2f seconds" % (uri, end_time - start_time))
        return command_info

    @staticmethod
    def _get_command_info(uri):
        response = server.get(uri)
        return CommandInfo(response.json())

    @staticmethod
    def _get_completion_status(command_info):
        return command_info.complete


class CommandServerError(Exception):
    """
    Error for errors reported by the server when issuing a command
    """
    def __init__(self, command_info):
        self.command_info = command_info
        try:
            message = command_info.error['message']
        except KeyError:
            message = "(Server response insufficient to provide details)"
        message = message + (" (command: %s, corId: %s)" % (command_info.id_number, command_info.correlation_id))
        Exception.__init__(self, message)

QueryResult = namedtuple("QueryResult", ['data', 'schema'])
"""
QueryResult contains the data and schema directly returned from the rest server
"""

class Executor(object):
    """
    Executes commands
    """

    __commands = []


    def get_query_response(self, id, partition):
        """
        Attempt to get the next partition of data as a CommandInfo Object. Allow for several retries
        :param id: Query ID
        :param partition: Partition number to pull
        """
        max_retries = 20
        for i in range(max_retries):
            try:
                info = CommandInfo(server.get("queries/%s/data/%s" % (id, partition)).json())
                return info
            except HTTPError as e:
                time.sleep(5)
                if i == max_retries - 1:
                    raise e

    def issue(self, command_request):
        """
        Issues the command_request to the server
        """
        logger.info("Issuing command " + command_request.name)
        response = server.post("commands", command_request.to_json_obj())
        return self.poll_command_info(response)

    def poll_command_info(self, response):
        """
        poll command_info until the command is completed and return results
        :param response: response from original command
        :return: :raise CommandServerError:
        """
        command_info = CommandInfo(response.json())
        # For now, we just poll until the command completes
        try:
            if not command_info.complete:
                command_info = Polling.poll(command_info.uri)
                # Polling.print_progress(command_info.progress)

        except KeyboardInterrupt:
            self.cancel(command_info.id_number)
            raise OperationCancelException("command cancelled by user")

        if command_info.error:
            raise CommandServerError(command_info)
        return command_info

    def query(self, query_url):
        """
        Issues the query_request to the server
        """
        logger.info("Issuing query " + query_url)
        try:
            response = server.get(query_url)
        except:
            # do a single retry
            response = server.get(query_url)

        response_json = response.json()

        schema = response_json["result"]["schema"]['columns']

        if response_json["complete"]:
            data = response_json["result"]["data"]
            return QueryResult(data, schema)
        else:
            command = self.poll_command_info(response)

            #retreive the data
            printer = ProgressPrinter()
            total_pages = command.result["total_pages"] + 1

            start = 1
            data = []
            for i in range(start, total_pages):
                next_partition = self.get_query_response(command.id_number, i)
                data_in_page = next_partition.result["data"]

                data.extend(data_in_page)

                #if the total pages is greater than 10 display a progress bar
                if total_pages > 5:
                    finished = i == (total_pages - 1)
                    if not finished:
                        time.sleep(.5)
                    progress = [{
                        "progress": (float(i)/(total_pages - 1)) * 100,
                        "tasks_info": {
                            "retries": 0
                        }
                    }]
                    printer.print_progress(progress, finished)
        return QueryResult(data, schema)


    def cancel(self, command_id):
        """
        Tries to cancel the given command
        """
        logger.info("Executor cancelling command " + str(command_id))

        arguments = {'status': 'cancel'}
        command_request = CommandRequest("", arguments)
        server.post("commands/%s" %(str(command_id)), command_request.to_json_obj())

    def fetch(self):
        """
        Obtains a list of all the available commands from the server
        :return: the commands available
        """
        logger.info("Requesting available commands")
        response = server.get("commands/definitions")
        commands = response.json()
        return commands

    @property
    def commands(self):
        """
        The available commands
        """
        if not self.__commands:
            self.__commands = self.fetch()
        return self.__commands

    def get_command_output(self, command_type, command_name, arguments):
        """Executes command and returns the output."""
        command_request = CommandRequest( "%s/%s" % (command_type, command_name), arguments)
        command_info = executor.issue(command_request)
        if command_info.result.has_key('value') and len(command_info.result) == 1:
            return command_info.result.get('value')
        return command_info.result



executor = Executor()