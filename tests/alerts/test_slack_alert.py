import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Queue

import pytest
from loguru import logger

from bizon.alerting.slack.config import SlackConfig
from bizon.alerting.slack.handler import SlackHandler


class DummyWebhookHandler(BaseHTTPRequestHandler):
    # Shared queue to store payloads
    payload_queue = Queue()

    def do_POST(self):
        # Read and store the payload
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length)
        self.payload_queue.put(post_data.decode("utf-8"))

        # Send a response back
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")


# Function to start the server in a separate thread
def start_dummy_server(host="localhost", port=8123):
    server = HTTPServer((host, port), DummyWebhookHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    return server, server_thread


@pytest.fixture
def dummy_webhook_server():
    # Start the dummy server
    server, server_thread = start_dummy_server()

    # Yield control to the test
    yield server

    # Shutdown the server after the test
    server.shutdown()
    server_thread.join()

    # Clear the payload queue
    DummyWebhookHandler.payload_queue.queue.clear()


def test_slack_log_handler(dummy_webhook_server):
    slack_handler = SlackHandler(SlackConfig(webhook_url="http://localhost:8123"))

    slack_handler.add_handlers(levels=["ERROR", "WARNING"])

    ERROR_MESSAGE = "This is an error message"
    WARNING_MESSAGE = "This is a warning message"

    logger.error(ERROR_MESSAGE)

    error_payload = DummyWebhookHandler.payload_queue.get(timeout=1)
    assert ERROR_MESSAGE in error_payload

    DummyWebhookHandler.payload_queue.queue.clear()

    logger.warning(WARNING_MESSAGE)
    warning_payload = DummyWebhookHandler.payload_queue.get(timeout=1)
    assert WARNING_MESSAGE in warning_payload
