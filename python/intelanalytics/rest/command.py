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

import time
import json
import logging
import sys
logger = logging.getLogger(__name__)

import intelanalytics.rest.config as config
from intelanalytics.rest.connection import http


def print_progress(progress, make_new_line):
    if not progress:
        sys.stdout.write("\rinitializing...")
        sys.stdout.flush()
        return

    progress_summary = []
    for p in progress:
        num_star = int(p / 2)
        num_dot = 50 - num_star
        number = "%3.2f" % p
        progress_summary.append("\r%6s%% [%s%s]" % (number, '=' * num_star, '.' * num_dot))

    if make_new_line:
        print progress_summary[-2]

    sys.stdout.write(progress_summary[-1])
    sys.stdout.flush()


class CommandRequest(object):
    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments

    def to_json_obj(self):
        """
        returns json for REST payload
        """
        return self.__dict__


class CommandInfo(object):
    def __init__(self, response_payload):
        self._payload = response_payload

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
        interval_secs = start_interval_secs
        start_time = time.time()

        command_info = Polling._get_command_info(uri)

        if not predicate(command_info):
            job_count = 1
            last_progress = []
            while True:
                time.sleep(interval_secs)
                command_info = Polling._get_command_info(command_info.uri)

                progress = command_info.progress
                new_job_progress_exists = job_count < len(progress)
                print_progress(progress, new_job_progress_exists)
                if new_job_progress_exists:
                    job_count = len(progress)

                if predicate(command_info):
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
        #     wait_time = time.time() - start_time
        #     command_info = Polling._get_command_info(command_info.uri)
        #     progress = command_info.progress
        #
        #     new_job_progress_exists = job_count < len(progress)
        #     print_progress(progress, new_job_progress_exists)
        #
        #     if(new_job_progress_exists):
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
                command_info = Polling.poll(command_info.uri)
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


executor = Executor()
