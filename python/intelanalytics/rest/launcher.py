from multiprocessing import Process
import multiprocessing
from hooks import WebhookServer

server = WebhookServer()
message_queue = multiprocessing.Queue()

def launch(port):
    server_process = Process(target=start_webhook_server, args=(message_queue, port))
    server_process.start()

def shutdown():
    stop_webhookserver()



def start_webhook_server(message_queue, port):
    server.start(port, message_queue)


def stop_webhookserver():
    server.stop()

