import atexit
from multiprocessing import Process
import multiprocessing
from hooks import WebhookServer

message_queue = multiprocessing.Queue()
web_server = WebhookServer()

class _Launcher(object):

    def __del__(self):
        self.shutdown()

    def launch(self, port):
        self.server_process = Process(target=web_server.start, args=(port, message_queue))
        self.server_process.start()

    def shutdown(self):
        self.server_process.terminate()


webhook_launcher = _Launcher()
atexit.register(webhook_launcher.shutdown)





