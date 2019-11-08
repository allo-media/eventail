#!/usr/bin/env python
import sys

from eventail.async_service.pika import Service, ReconnectingSupervisor
from eventail.log_criticity import ERROR, INFO, NOTICE


class EchoService(Service):

    PREFETCH_COUNT = 10
    RETRY_DELAY = 2

    def on_EchoMessage(self, message, conversation_id, reply_to, correlation_id):
        assert "message" in message, "missing 'message' key!"
        self.log(
            INFO,
            "Echoing",
            "Sending back {}".format(message),
            conversation_id=conversation_id,
        )
        self.return_success(reply_to, message, conversation_id, correlation_id)

    def on_ShutdownStarted(self, payload, conversation_id):
        self.log(INFO, "Received signal for shutdown.")
        self.stop()

    def handle_returned_message(self, key, message, envelope):
        self.log(ERROR, "unroutable message", "{}.{}.{}".format(key, message, envelope))

    def on_ready(self):
        self.healthcheck()

    def healthcheck(self):
        self.log(NOTICE, "I'm fine!")
        self.call_later(60, self.healthcheck)


if __name__ == "__main__":
    # import logging
    # logger = logging.getLogger("async_service")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)
    urls = sys.argv[1:] if len(sys.argv) > 1 else ["amqp://localhost"]
    echo = ReconnectingSupervisor(
        EchoService, urls, ["ShutdownStarted"], ["pong.EchoMessage"], "pong"
    )
    print("To exit press CTRL+C")
    echo.run()
    print("Bye!")
