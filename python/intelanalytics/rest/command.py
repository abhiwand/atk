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
logger = logging.getLogger(__name__)

from intelanalytics.rest.connection import http


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

    def update(self, payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, command ID mismatch %d when expecting %d"\
                  % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload = payload


class Polling(object):

    @staticmethod
    def poll(uri, predicate=None, interval_secs=0.5, backoff_factor=2, timeout_secs=10):
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
        interval_secs : float
            Initial sleep interval for the polling, in seconds
        backoff_factor : float
            Factor to increase the interval_secs on subsequent retries
        timeout_secs : float
            Maximum wall clock time to roughly spend in this function
        """
        # TODO - timeout appropriateness at scale
        if predicate is None:
            predicate = Polling._get_completion_status
        start_time = time.time()
        command_info = Polling._get_command_info(uri)
        if predicate(command_info):
            return command_info
        while True:
            time.sleep(interval_secs)
            wait_time = time.time() - start_time
            command_info = Polling._get_command_info(command_info.uri)
            if predicate(command_info):
                return command_info
            if wait_time > timeout_secs:
                msg = "Polling timeout for %s after ~%d seconds" \
                      % (str(command_info), wait_time)
                logger.error(msg)
                raise RuntimeError(msg)
            interval_secs *= backoff_factor

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
