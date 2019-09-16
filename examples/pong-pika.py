#!/usr/bin/env python
import sys

from async_service.pika import Service, ReconnectingSupervisor


class EchoService(Service):

    PREFETCH_COUNT = 10
    RETRY_DELAY = 2

    def on_EchoMessage(self, message, reply_to, correlation_id):
        self.log("info", "Echoing {}".format(message))
        self.return_success(reply_to, message, correlation_id)

    def on_ShutdownStarted(self, payload):
        self.log("info", "Received signal for shutdown.")
        self.stop()

    def handle_returned_message(self, key, message, envelope):
        self.log("error", "unroutable {}.{}.{}".format(key, message, envelope))

    def on_ready(self):
        self.healthcheck()

    def healthcheck(self):
        self.log("health", "I'm fine!")
        self.call_later(60, self.healthcheck)


if __name__ == "__main__":
    # import logging
    # logger = logging.getLogger("async_service")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)
    url = sys.argv[1] if len(sys.argv) > 1 else "amqp://localhost"
    echo = ReconnectingSupervisor(
        EchoService, url, ["ShutdownStarted"], ["pong.EchoMessage"], "pong"
    )
    print("To exit press CTRL+C")
    echo.run()
    print("Bye!")
