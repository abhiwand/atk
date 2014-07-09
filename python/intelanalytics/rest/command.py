##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
Command objects
"""
import datetime

import time
import json
import logging
import sys
import re
import collections

logger = logging.getLogger(__name__)

import intelanalytics.rest.config as config
from intelanalytics.rest.connection import http
from intelanalytics.core.errorhandle import IaError


def print_progress(progress, progressMessage, make_new_line, start_times, finished):
    if not progress:
        initializing_text = "\rinitializing..."
        sys.stdout.write(initializing_text)
        sys.stdout.flush()
        return len(initializing_text)

    progress_summary = []

    for index in range(0, len(progress)):
        p = progress[index]
        message = progressMessage[index] if(index < len(progressMessage)) else ''

        num_star = int(p / 2)
        num_dot = 50 - num_star
        number = "%3.2f" % p

        time_string = datetime.timedelta(seconds = int(time.time() - start_times[index]))
        progress_summary.append("\r%6s%% [%s%s] %s [Elapsed Time %s]" % (number, '=' * num_star, '.' * num_dot, message, time_string))

    if make_new_line:
        previous_step_progress = progress_summary[-2]
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
        returns json for REST payload
        """
        return self.__dict__


class CommandInfo(object):
    __commands_regex = re.compile("""^http.+/commands/\d+""")

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
    def name(self):
        return self._payload['name']

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

    @property
    def progressMessage(self):
        try:
            return self._payload['progressMessage']
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
            start_interval_secs = config.polling.start_interval_secs
        if backoff_factor is None:
            backoff_factor = config.polling.backoff_factor
        if not CommandInfo.is_valid_command_uri(uri):
            raise ValueError('Cannot poll ' + uri + ' - a /commands/{number} uri is required')
        interval_secs = start_interval_secs

        command_info = Polling._get_command_info(uri)

        if not predicate(command_info):
            job_count = 0
            last_progress = []

            next_poll_time = time.time()

            start_time = time.time()
            job_start_times = []
            while True:
                if time.time() < next_poll_time:
                    time.sleep(start_interval_secs)
                    continue

                command_info = Polling._get_command_info(command_info.uri)
                finish = predicate(command_info)

                next_poll_time = time.time() + interval_secs
                progress = command_info.progress
                print_new_line = len(progress) > 1 and job_count < len(progress)

                if job_count < len(progress):
                    job_start_times.append(time.time())
                    job_count = len(progress)

                print_progress(progress, command_info.progressMessage, print_new_line, job_start_times, finish)

                if finish:
                    break

                if last_progress == progress and interval_secs < max_interval_secs:
                    interval_secs = min(max_interval_secs, interval_secs * backoff_factor)

                last_progress = progress
                end_time = time.time()
                logger.info("polling %s completed after %0.2f seconds" % (uri, end_time - start_time))
        return command_info

        # if predicate(command_info):
        #     return command_info
        #
        # job_count = 1
        # last_progress = []
        # while True:
        #     time.sleep(interval_secs)
        #     wait_time = time.time() - job_start_times
        #     command_info = Polling._get_command_info(command_info.uri)
        #     progress = command_info.progress
        #
        #     print_new_line = job_count < len(progress)
        #     print_progress(progress, print_new_line)
        #
        #     if(print_new_line):
        #         job_count = len(progress)
        #
        #     if predicate(command_info):
        #         return command_info
        #     if wait_time > timeout_secs:
        #         msg = "Polling timeout for %s after ~%d seconds" \
        #               % (str(command_info), wait_time)
        #         logger.error(msg)
        #         raise RuntimeError(msg)
        #
        #
        #     if last_progress == progress:
        #         interval_secs *= backoff_factor
        #         if interval_secs > 30:
        #             interval_secs = 30
        #
        #     last_progress = progress

    @staticmethod
    def _get_command_info(uri):
        response = http.get_full_uri(uri)
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
        Exception.__init__(self, message)


class Executor(object):
    """
    Executes commands
    """

    __commands = []

    def issue(self, command_request):
        """
        Issues the command_request to the server
        """
        logger.info("Issuing command " + command_request.name)
        response = http.post("commands", command_request.to_json_obj())
        command_info = CommandInfo(response.json())
        # For now, we just poll until the command completes
        try:
            if not command_info.complete:
                command_info = Polling.poll(command_info.uri, max_interval_secs=30, backoff_factor=1.1)
                # Polling.print_progress(command_info.progress)

        except KeyboardInterrupt:
            self.cancel(command_info.id_number)

        if command_info.error:
            raise CommandServerError(command_info)
        return command_info

    def cancel(self, command_id):
        """
        Tries to cancel the given command
        """
        logger.info("Executor cancelling command " + str(command_id))
        # TODO - implement command cancellation (like a DELETE to commands/id?)

    def fetch(self):
        """
        Obtains a list of all the available commands from the server
        :return: the commands available
        """
        logger.info("Requesting available commands")
        response = http.get("commands/definitions")
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

    def install_static_methods(self, clazz, functions):
        for ((intermediates, name), v) in functions.items():
            current = clazz
            for inter in intermediates:
                if not hasattr(current, inter):
                    holder = object()
                    setattr(current, inter, holder)
                    current = holder
            if not hasattr(current, name):
                print name, "installed statically on ", clazz
                setattr(clazz, name, staticmethod(v))

    def get_command_functions(self, prefixes, update_function, new_function):
        functions = dict()
        for cmd in executor.commands:
            full_name = cmd['name']
            parts = full_name.split('/')
            if parts[0] not in prefixes:
                continue
            args = cmd['argument_schema']
            intermediates = parts[1:-1]
            command_name = parts[-1]
            parameters = args.get('properties', {})
            print "ARG schema:", args
            self_parameter_name = ([k for k, v in parameters.items()
                                    if isinstance(v, dict) and v.has_key('self')] or [None])[0]
            print "self arg: ", self_parameter_name

            return_props = cmd['return_schema'].setdefault('properties', {})
            return_self_parameter = ([k for k, v in return_props.items()
                                      if isinstance(v, dict) and v.has_key('self')] or [None])[0]
            possible_args = args.get('order', [])[:]
            if self_parameter_name:
                possible_args.remove(self_parameter_name)

            #Create a new function scope to bind variables properly
            # (see, e.g. http://eev.ee/blog/2011/04/24/gotcha-python-scoping-closures )
            #Need make and not just invoke so that invoke won't have
            #kwargs that include command_name et. al.
            def make(full_name = full_name,
                     command_name = command_name,
                     cmd = cmd,
                     self_name = self_parameter_name,
                     return_self = return_self_parameter,
                     possible_args = possible_args,
                     parameters = parameters):

                def invoke(s, *args, **kwargs):
                    try:
                        if self_name:
                            print "Setting", self_name, "to", s._id
                            kwargs[self_name] = s._id
                        for (k,v) in zip(possible_args, args):
                            if k in kwargs:
                                raise ValueError("Argument " + k +
                                                 " supplied as a positional argument and as a keyword argument")
                            print "Assigning", k, "to", v
                            kwargs[k] = v
                        validated = CommandRequest.validate_arguments(parameters, kwargs)
                        if return_self:
                            return new_function(full_name, validated, s)
                        else:
                            return update_function(full_name, validated, s)
                    except:
                        raise IaError(logger)
                invoke.command = cmd
                invoke.parameters = parameters
                invoke.func_name = str(command_name)
                return invoke
            f = make()
            functions[(tuple(intermediates), command_name)] = f
        return functions



executor = Executor()
