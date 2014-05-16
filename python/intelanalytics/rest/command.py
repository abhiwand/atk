"""
Command object
"""

from intelanalytics.rest.connection import rest_http
import logging
logger = logging.getLogger(__name__)


class Command(object):
:memoryview
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

    def update(self, payload):    # command issue reponse provides info
        if self.id == 0:
            self.id = payload['id']
        elif self.id != payload['id']:
            raise ValueError("Received a different command id from server?")
        self.complete = payload['complete']


class Executor(object):

    def __init__(self):
        self.queue = []

    def issue_command(self, command):
        logger.info("Issuing command " + command.name)
        self._enqueue_command(command)
        response = rest_http.post("commands", command.get_payload())
        command.update(response.json())
        if command.complete:
            self._dequeue_command(command)
        return response

    def _enqueue_command(self, command):
        self.queue.append(command)

    def _dequeue_command(self, command):
        self.queue.remove(command)


executor = Executor()
