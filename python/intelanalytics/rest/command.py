"""
Command object
"""

from intelanalytics.rest.connection import rest_http
import logging
logger = logging.getLogger(__name__)


class Command(object):

    def __init__(self, name, arguments):
        # this should match the first-level REST API command payload
        # this class should have a natively JSON object structure
        self.name = name
        self.arguments = arguments or {}
        self.id = 0
        self.complete = False
        self.links = []

    def get_payload(self):
        return dict(self.__dict__)

    def update(self, payload):    # command issue response provides info
        if self.id == 0:
            self.id = payload['id']
        elif self.id != payload['id']:
            raise ValueError("Received a different command id from server?")
        self.complete = payload['complete']


import time


class Polling(object):

    def poll(self,
             command,
             interval_secs=0.5,
             backoff_factor=2,
             timeout_secs=10):  # TODO - timeout appropriateness at scale :^)

        start_time = time.time()
        if self._get_completion_status(command):
            return True
        while True:
            time.sleep(interval_secs)
            wait_time = time.time() - start_time
            if self._get_completion_status(command):
                return True
            if wait_time > timeout_secs:
                msg = "Polling timeout for command %s after ~%d seconds" \
                      % (command.name, wait_time)
                logger.error(msg)
                raise RuntimeError(msg)
            interval_secs *= backoff_factor

    @staticmethod
    def _get_completion_status(command):
        response = rest_http.get(command.uri)
        return response.json()['complete']


class Executor(object):

    def __init__(self):
        self.queue = []

    def issue_command(self, command):
        logger.info("Issuing command " + command.name)
        self._enqueue_command(command)
        try:
            response = rest_http.post("commands", command.get_payload())
            command.update(response.json())
            if command.complete:
                self._dequeue_command(command)
            return response
        except KeyboardInterrupt:
            self.cancel_command(command)

    def _enqueue_command(self, command):
        self.queue.append(command)

    def _dequeue_command(self, command):
        self.queue.remove(command)

    def cancel_command(self, command):
        # TODO - implement command cancellation
        self._dequeue_command(command)


executor = Executor()
