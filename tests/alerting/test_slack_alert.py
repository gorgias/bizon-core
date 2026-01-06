import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Queue

import pytest
import yaml
from loguru import logger

from bizon.alerting.models import LogLevel
from bizon.alerting.slack.config import SlackConfig
from bizon.alerting.slack.handler import SlackHandler
from bizon.engine.engine import RunnerFactory


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


# Subclass HTTPServer to set allow_reuse_address
class ReusableHTTPServer(HTTPServer):
    allow_reuse_address = True


# Function to start the server in a separate thread
def start_dummy_server(host="localhost", port=8123):
    server = ReusableHTTPServer((host, port), DummyWebhookHandler)
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


@pytest.fixture
def webhook_url():
    return "http://localhost:8123"


def test_slack_log_handler(dummy_webhook_server, webhook_url):
    slack_handler = SlackHandler(SlackConfig(webhook_url=webhook_url), log_levels=[LogLevel.ERROR, LogLevel.WARNING])

    slack_handler.add_handlers()

    ERROR_MESSAGE = "This is an error message"
    WARNING_MESSAGE = "This is a warning message"

    logger.error(ERROR_MESSAGE)

    error_payload = DummyWebhookHandler.payload_queue.get(timeout=1)
    assert ERROR_MESSAGE in error_payload

    DummyWebhookHandler.payload_queue.queue.clear()

    logger.warning(WARNING_MESSAGE)
    warning_payload = DummyWebhookHandler.payload_queue.get(timeout=1)
    assert WARNING_MESSAGE in warning_payload


def test_e2e_logger_to_file(dummy_webhook_server, webhook_url):
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        BIZON_CONFIG_DUMMY_TO_FILE = f"""
        name: test_job_9

        source:
          name: dummy
          stream: creatures
          authentication:
            type: api_key
            params:
              token: dummy_key

        destination:
          name: file
          config:
            destination_id: {temp.name}

        transforms:
          - label: transform_data
            python: |
              if 'name' in data:
                data['name'] = fake_variable # this is purposely wrong to trigger an error

        engine:
          backend:
            type: postgres
            config:
              database: bizon_test
              schema: public
              syncCursorInDBEvery: 2
              host: {os.environ.get("POSTGRES_HOST", "localhost")}
              port: 5432
              username: postgres
              password: bizon

        alerting:
            type: slack

            config:
              webhook_url: {webhook_url}

            log_levels:
                - ERROR
        """

        runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))

        runner.run()

        error_payload = DummyWebhookHandler.payload_queue.get(timeout=1)
        assert "Error applying transformation" in error_payload
        assert "fake_variable" in error_payload
