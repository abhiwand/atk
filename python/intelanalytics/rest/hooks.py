from bottle import request
from bottle import Bottle, HeaderDict
from message import Message


class WebhookServer(object):
    def __init__(self):
        self._app = Bottle()

        self._route()
        self.response_dict = HeaderDict()

    def _route(self):
        self._app.route('/notify/<id>', method="POST", callback=self.__notify)

    def start(self, port, message_queue):
        self.message_queue = message_queue
        self._app.run(host='localhost', port=port, quiet=True)

    def stop(self):
        self._app.close()

    def __notify(self, id):
        m = Message(id, request.json)
        self.message_queue.put(m)

