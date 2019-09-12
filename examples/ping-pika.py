#!/usr/bin/env python
import sys
from random import choice

from async_service.pika import Service, ReconnectingSupervisor

MESSAGES = [
    "hello",
    "proof of concept",
    "how are you?",
    "ça cause français?",
    "allo?",
    "y'a quelqu'un?",
    "arrête de répéter ce que je dit !",
    "t'es lourd!",
    "stop that!",
    "so childish…"
]


class Ping(Service):

    PREFETCH_COUNT = 10

    def __init__(self, host, logical_service):
        self.return_key = logical_service + ".EchoMessage"
        super().__init__(host, [], [self.return_key], logical_service)

    def on_ready(self):
        self.ping()

    def handle_result(self, key, message, status, correlation_id):
        self.log("info", "Received {} {}".format(key, status))
        if key == self.return_key:
            self.log("info", "Got echo: {} {}".format(message, correlation_id))
        else:
            # should never happen: means we misconfigured the routing keys
            self.log("error", "Unexpected message {} {}".format(key, status))

    def handle_returned_message(self, key, message, envelope):
        self.log("critical", "unroutable {}.{}.{}".format(
            key, message, envelope))
        raise ValueError(f"Wrong routing key {key}")

    def ping(self):
        message = {"message": choice(MESSAGES)}
        self.log("info", "Sending: {} {}".format(message, self._message_number))
        self.send_command("pong.EchoMessage", message, self.return_key, "anyid" + str(self._message_number))
        self.call_later(1, self.ping)


if __name__ == '__main__':
    import logging
    logger = logging.getLogger("async_service")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    service_name = sys.argv[1]
    url = sys.argv[2] if len(sys.argv) > 2 else "amqp://localhost"
    ping = ReconnectingSupervisor(Ping, url, service_name)
    print('To exit press CTRL+C')
    ping.run()
    print("Bye!")
