from multiprocessing import Process
import multiprocessing
from hooks import WebhookServer

message_queue = multiprocessing.Queue()

class _Launcher(object):

    def __init__(self):
        self.server = WebhookServer()

    def launch(self, port):
        self.server_process = Process(target=self.server.start, args=(port, message_queue))
        self.server_process.start()

    def shutdown(self):
        self.server_process.terminate()


launcher = _Launcher()





