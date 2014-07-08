import atexit
from multiprocessing import Process
import multiprocessing
from hooks import WebhookServer

message_queue = multiprocessing.Queue()
web_server = WebhookServer()

class _Launcher(object):
    """
    Webhook launcher.
    """

    def __del__(self):
        """
        Shutdown the webhook server on object delete
        """
        self.shutdown()

    def launch(self, port):
        """
        Launch webhook server with the specified port

        Parameters
        ----------
        port: int
            The port that the webhook server uses to serve the request


        Examples
        --------
        >>> webhook_launcher.launch(10050)
        """
        self.server_process = Process(target=web_server.start, args=(port, message_queue))
        self.server_process.start()

    def shutdown(self):
        """
        Shutdown the webhook server

        Examples
        --------
        >>> webhook_launcher.shutdown()
        """
        self.server_process.terminate()


webhook_launcher = _Launcher()
atexit.register(webhook_launcher.shutdown)





